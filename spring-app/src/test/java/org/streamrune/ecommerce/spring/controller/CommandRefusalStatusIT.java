package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.ecommerce.spring.SpringEcommerceApplication;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * A command the framework refuses for a reason that passes is answered {@code 503} with {@code
 * Retry-After: 5} and a JSON body naming the reason; a genuine failure stays {@code 500}.
 *
 * <p>Each condition is forced on the app's real command bus, in a Spring context of its own on a
 * database of its own ({@code command_refusals}): the tests leave its circuit breaker open and its
 * bus closed, which the cached test context must not see. The outbox is off, as no test here
 * delivers. The lock timeout is one second here, so a command that waits for a held lock is refused
 * after the bus's three attempts, in about three seconds. The tests run in order:
 *
 * <ol>
 *   <li><b>A held aggregate lock.</b> A price change of a product stops in its audit write, which
 *       the command bus makes while it still holds the product's lock: a trigger on {@code
 *       audit_log} waits for an advisory lock the test holds. A stock adjustment of the same
 *       product waits for the product's lock until the lock timeout, on every attempt of the bus's
 *       retry policy, and is refused with {@code AGGREGATE_LOCK_UNAVAILABLE}. Released, the price
 *       change completes with {@code 200}.
 *   <li><b>A version conflict on every attempt.</b> A trigger on {@code event_stream} raises {@code
 *       unique_violation} for a price change of a product, which the event store reports as an
 *       {@code OptimisticLockException}, as it does when a second writer stored the version first.
 *       The bus tries the command on every attempt of its retry policy and is refused with {@code
 *       AGGREGATE_VERSION_CONFLICT}; nothing is written.
 *   <li><b>An open circuit breaker.</b> The store refuses every price change of a second product (a
 *       trigger on {@code event_stream}), so each is a genuine failure, {@code 500}, until the
 *       breaker opens; the next command is refused with {@code CIRCUIT_BREAKER_OPEN}.
 *   <li><b>A closed command bus.</b> Once the bus is closed, as on shutdown, a command is refused
 *       with {@code COMMAND_BUS_CLOSED}.
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CommandRefusalStatusIT {

  private static final String DATABASE = "command_refusals";

  /** The advisory lock key the audit trigger waits for. */
  private static final long HELD_AUDIT_LOCK = 7_421_001L;

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private static ConfigurableApplicationContext app;

  @BeforeAll
  static void start() throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    var rabbit = AbstractIntegrationTest.RABBIT;
    try (var conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + DATABASE + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + DATABASE);
    }
    app =
        new SpringApplicationBuilder(SpringEcommerceApplication.class)
            .run(
                "--server.port=0",
                "--spring.datasource.url=" + databaseUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.rabbitmq.host=" + rabbit.getHost(),
                "--spring.rabbitmq.port=" + rabbit.getAmqpPort(),
                "--spring.rabbitmq.username=" + rabbit.getAdminUsername(),
                "--spring.rabbitmq.password=" + rabbit.getAdminPassword(),
                "--spring.rabbitmq.listener.simple.auto-startup=false",
                "--streamrune.outbox.enabled=false",
                "--streamrune.lock-timeout=1s",
                "--streamrune.projections.auto-discovery.enabled=false",
                "--streamrune.sse.keep-alive-interval=1h");
  }

  @AfterAll
  static void stop() {
    if (app != null) {
      app.close();
    }
  }

  @Test
  @Order(1)
  void aCommandThatCannotTakeItsAggregateLockIsRefusedWithRetryAfter() throws Exception {
    String productId = "refusal-lock-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);

    DataSource dataSource = app.getBean(DataSource.class);
    var postgres = AbstractIntegrationTest.POSTGRES;
    // A connection of its own, not one of the pool's: closing it ends the session and with it the
    // advisory lock, also when an assertion fails before the release.
    try (var auditHold = new AuditWriteHold(dataSource, productId, "UpdatePrice");
        var holder =
            DriverManager.getConnection(
                databaseUrl(), postgres.getUsername(), postgres.getPassword())) {
      holdAdvisoryLock(holder);
      CompletableFuture<HttpResponse<String>> priceChange =
          CompletableFuture.supplyAsync(
              () -> send("PUT", "/api/products/" + productId + "/price", "{\"price\": 25.00}"));
      // Committed, then stopped in its audit write: the product's lock is held from here.
      await()
          .atMost(Duration.ofSeconds(30))
          .until(() -> eventCount(dataSource, productId, "PriceUpdated") == 1);

      long waitStart = System.nanoTime();
      HttpResponse<String> adjusted =
          send(
              "PUT",
              "/api/products/" + productId + "/stock",
              "{\"quantity\": 1, \"reason\": \"recount\"}");
      assertThat(Duration.ofNanos(System.nanoTime() - waitStart))
          .as("the configured lock timeout, one second, on each of the bus's three attempts")
          .isLessThan(Duration.ofSeconds(10));

      assertRefused(adjusted, "AGGREGATE_LOCK_UNAVAILABLE");
      releaseAdvisoryLock(holder);
      assertThat(priceChange.get().statusCode()).isEqualTo(200);
    }
  }

  @Test
  @Order(2)
  void aVersionConflictOnEveryAttemptIsRefusedWithRetryAfter() throws Exception {
    String productId = "refusal-version-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);

    try (var conflict =
        new VersionConflictInjection(app.getBean(DataSource.class), productId, "PriceUpdated")) {
      HttpResponse<String> changed =
          send("PUT", "/api/products/" + productId + "/price", "{\"price\": 27.00}");

      assertRefused(changed, "AGGREGATE_VERSION_CONFLICT");
      assertThat(eventCount(app.getBean(DataSource.class), productId, "PriceUpdated"))
          .as("nothing was written")
          .isZero();
    }
  }

  @Test
  @Order(3)
  void anOpenCircuitBreakerRefusesWithRetryAfterAfterTheFailuresThatOpenedIt() throws Exception {
    String productId = "refusal-breaker-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);

    try (var store = new AppendFailureInjection(app.getBean(DataSource.class))) {
      store.failAppend(ProductState.TYPE, productId, "PriceUpdated");
      HttpResponse<String> answer = null;
      int genuineFailures = 0;
      for (int attempt = 0; attempt < 10; attempt++) {
        answer = send("PUT", "/api/products/" + productId + "/price", "{\"price\": 30.00}");
        if (answer.statusCode() != 500) {
          break;
        }
        genuineFailures++;
      }
      assertThat(genuineFailures).as("a store failure is a genuine failure: 500").isPositive();
      assertRefused(answer, "CIRCUIT_BREAKER_OPEN");
      assertRefused(
          createProduct("refusal-breaker-other-" + System.nanoTime()), "CIRCUIT_BREAKER_OPEN");
    }
  }

  @Test
  @Order(4)
  void aClosedCommandBusRefusesWithRetryAfter() {
    app.getBean(VirtualThreadCommandBus.class).close();

    assertRefused(createProduct("refusal-closed-" + System.nanoTime()), "COMMAND_BUS_CLOSED");
  }

  private static void assertRefused(HttpResponse<String> answer, String reason) {
    assertThat(answer.statusCode()).as("status of %s", answer.body()).isEqualTo(503);
    assertThat(answer.headers().firstValue("Retry-After")).contains("5");
    assertThat(answer.headers().firstValue("Content-Type").orElse("")).contains("application/json");
    JsonNode body;
    try {
      body = JSON.readTree(answer.body());
    } catch (Exception e) {
      throw new AssertionError("not JSON: " + answer.body(), e);
    }
    assertThat(body.path("reason").asText()).isEqualTo(reason);
    assertThat(body.path("message").asText()).isNotBlank();
  }

  private static HttpResponse<String> createProduct(String productId) {
    return send(
        "POST",
        "/api/products",
        """
        {"productId": "%s", "name": "Lamp", "description": "refusals", "category": "Home",
         "price": 20.00, "initialStock": 3}"""
            .formatted(productId));
  }

  private static HttpResponse<String> send(String method, String path, String json) {
    try {
      return HTTP.send(
          HttpRequest.newBuilder(
                  URI.create(
                      "http://localhost:"
                          + app.getEnvironment().getRequiredProperty("local.server.port")
                          + path))
              .header("Content-Type", "application/json")
              .header("X-User-Id", "admin-1")
              .header("X-User-Role", "ADMIN")
              .method(method, HttpRequest.BodyPublishers.ofString(json))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static long eventCount(DataSource dataSource, String productId, String eventType)
      throws Exception {
    try (var conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM event_stream WHERE aggregate_type = 'product'"
                    + " AND aggregate_id = ? AND event_type = ?")) {
      ps.setString(1, productId);
      ps.setString(2, eventType);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static void holdAdvisoryLock(java.sql.Connection holder) throws Exception {
    try (Statement st = holder.createStatement()) {
      st.execute("SELECT pg_advisory_lock(" + HELD_AUDIT_LOCK + ")");
    }
  }

  private static void releaseAdvisoryLock(java.sql.Connection holder) throws Exception {
    try (Statement st = holder.createStatement()) {
      st.execute("SELECT pg_advisory_unlock(" + HELD_AUDIT_LOCK + ")");
    }
  }

  private static String databaseUrl() {
    var postgres = AbstractIntegrationTest.POSTGRES;
    return "jdbc:postgresql://"
        + postgres.getHost()
        + ":"
        + postgres.getMappedPort(5432)
        + "/"
        + DATABASE;
  }

  /**
   * Makes every append of one event type to one aggregate end in the event store's version
   * conflict: a trigger on {@code event_stream} raises {@code unique_violation} (SQLSTATE 23505),
   * which the store reports as an {@code OptimisticLockException}, as it does when a second writer
   * stored the version first. {@link #close()} drops the trigger and its function.
   */
  private static final class VersionConflictInjection implements AutoCloseable {

    private final DataSource dataSource;

    VersionConflictInjection(DataSource dataSource, String aggregateId, String eventType)
        throws Exception {
      this.dataSource = dataSource;
      execute(
          """
          CREATE OR REPLACE FUNCTION version_test_conflict() RETURNS trigger
            LANGUAGE plpgsql AS $$
          BEGIN
            IF NEW.aggregate_id = '%s' AND NEW.event_type = '%s' THEN
              RAISE EXCEPTION 'injected version conflict' USING ERRCODE = 'unique_violation';
            END IF;
            RETURN NEW;
          END $$;
          DROP TRIGGER IF EXISTS version_test_conflict ON event_stream;
          CREATE TRIGGER version_test_conflict BEFORE INSERT ON event_stream
            FOR EACH ROW EXECUTE FUNCTION version_test_conflict();
          """
              .formatted(aggregateId, eventType));
    }

    @Override
    public void close() throws Exception {
      execute(
          """
          DROP TRIGGER IF EXISTS version_test_conflict ON event_stream;
          DROP FUNCTION IF EXISTS version_test_conflict();
          """);
    }

    private void execute(String sql) throws Exception {
      try (var conn = dataSource.getConnection();
          Statement st = conn.createStatement()) {
        st.execute(sql);
        if (!conn.getAutoCommit()) {
          conn.commit();
        }
      }
    }
  }

  /**
   * Holds a command in its audit write: a trigger on {@code audit_log} waits for the advisory lock
   * {@link #HELD_AUDIT_LOCK} before it inserts the row of the given command type for the given
   * aggregate. The command bus writes that row in the command's {@code after()} phase, with the
   * aggregate's lock still held. {@link #close()} drops the trigger and its function.
   */
  private static final class AuditWriteHold implements AutoCloseable {

    private final DataSource dataSource;

    AuditWriteHold(DataSource dataSource, String aggregateId, String commandType) throws Exception {
      this.dataSource = dataSource;
      execute(
          """
          CREATE OR REPLACE FUNCTION audit_test_hold_row() RETURNS trigger
            LANGUAGE plpgsql AS $$
          BEGIN
            IF NEW.aggregate_id = '%s' AND NEW.command_type = '%s' THEN
              PERFORM pg_advisory_xact_lock(%d);
            END IF;
            RETURN NEW;
          END $$;
          DROP TRIGGER IF EXISTS audit_test_hold_row ON audit_log;
          CREATE TRIGGER audit_test_hold_row BEFORE INSERT ON audit_log
            FOR EACH ROW EXECUTE FUNCTION audit_test_hold_row();
          """
              .formatted(aggregateId, commandType, HELD_AUDIT_LOCK));
    }

    @Override
    public void close() throws Exception {
      execute(
          """
          DROP TRIGGER IF EXISTS audit_test_hold_row ON audit_log;
          DROP FUNCTION IF EXISTS audit_test_hold_row();
          """);
    }

    private void execute(String sql) throws Exception {
      try (var conn = dataSource.getConnection();
          Statement st = conn.createStatement()) {
        st.execute(sql);
        if (!conn.getAutoCommit()) {
          conn.commit();
        }
      }
    }
  }
}
