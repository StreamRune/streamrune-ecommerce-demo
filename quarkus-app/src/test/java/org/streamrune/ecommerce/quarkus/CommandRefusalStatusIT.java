package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * A command the framework refuses for a reason that passes is answered {@code 503} with {@code
 * Retry-After: 5} and a JSON body naming the reason; a genuine failure stays {@code 500}. The
 * Spring and Micronaut apps answer the same.
 *
 * <p>Each condition is forced on the app's real command bus, in the app of this test's profile,
 * which has a PostgreSQL of its own ({@link PostgresTestResource}): the tests leave its circuit
 * breaker open and its bus closed, which the other test classes must not see. The profile sets the
 * lock timeout to one second, so a command that waits for a held lock is refused after the bus's
 * three attempts, in about three seconds. The tests run in order:
 *
 * <ol>
 *   <li><b>A held aggregate lock.</b> A price change of a product stops in its audit write, which
 *       the command bus makes while it still holds the product's lock: a trigger on {@code
 *       audit_log} waits for an advisory lock the test holds. A stock adjustment of the same
 *       product waits for the product's lock until the lock timeout, on every attempt, and is
 *       refused with {@code AGGREGATE_LOCK_UNAVAILABLE}. Released, the price change completes with
 *       {@code 200}.
 *   <li><b>An open circuit breaker.</b> The store refuses every price change of a second product (a
 *       trigger on {@code event_stream}), so each is a genuine failure, {@code 500}, until the
 *       breaker opens; the next command is refused with {@code CIRCUIT_BREAKER_OPEN}.
 *   <li><b>A closed command bus.</b> Once the bus is closed, as on shutdown, a command is refused
 *       with {@code COMMAND_BUS_CLOSED}.
 * </ol>
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
@TestProfile(CommandRefusalStatusIT.RefusalProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CommandRefusalStatusIT {

  /** A one-second lock timeout instead of five. */
  public static class RefusalProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("streamrune.lock-timeout", "1s");
    }
  }

  /** The advisory lock key the audit trigger waits for. */
  private static final long HELD_AUDIT_LOCK = 7_421_002L;

  private static final ObjectMapper JSON = new ObjectMapper();

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @TestHTTPResource("/")
  URI server;

  @Inject DataSource dataSource;

  @Inject VirtualThreadCommandBus commandBus;

  @Test
  @Order(1)
  void aCommandThatCannotTakeItsAggregateLockIsRefusedWithRetryAfter() throws Exception {
    String productId = "q-refusal-lock-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);

    try (var auditHold = new AuditWriteHold(dataSource, productId, "UpdatePrice");
        Connection holder = dataSource.getConnection()) {
      holdAdvisoryLock(holder);
      try {
        CompletableFuture<HttpResponse<String>> priceChange =
            CompletableFuture.supplyAsync(
                () -> send("PUT", "/api/products/" + productId + "/price", "{\"price\": 25.00}"));
        // Committed, then stopped in its audit write: the product's lock is held from here.
        await()
            .atMost(Duration.ofSeconds(30))
            .until(() -> eventCount(productId, "PriceUpdated") == 1);

        HttpResponse<String> adjusted =
            send(
                "PUT",
                "/api/products/" + productId + "/stock",
                "{\"quantity\": 1, \"reason\": \"recount\"}");

        assertRefused(adjusted, "AGGREGATE_LOCK_UNAVAILABLE");
        releaseAdvisoryLock(holder);
        assertThat(priceChange.get().statusCode()).isEqualTo(200);
      } finally {
        // A pooled connection keeps its session: never hand it back with the lock held.
        releaseAdvisoryLock(holder);
      }
    }
  }

  @Test
  @Order(2)
  void anOpenCircuitBreakerRefusesWithRetryAfterAfterTheFailuresThatOpenedIt() throws Exception {
    String productId = "q-refusal-breaker-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);

    try (var store = new AppendFailureInjection(dataSource)) {
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
          createProduct("q-refusal-breaker-other-" + System.nanoTime()), "CIRCUIT_BREAKER_OPEN");
    }
  }

  @Test
  @Order(3)
  void aClosedCommandBusRefusesWithRetryAfter() {
    commandBus.close();

    assertRefused(createProduct("q-refusal-closed-" + System.nanoTime()), "COMMAND_BUS_CLOSED");
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

  private HttpResponse<String> createProduct(String productId) {
    return send(
        "POST",
        "/api/products",
        """
        {"productId": "%s", "name": "Lamp", "description": "refusals", "category": "Home",
         "price": 20.00, "initialStock": 3}"""
            .formatted(productId));
  }

  private HttpResponse<String> send(String method, String path, String json) {
    try {
      return http.send(
          HttpRequest.newBuilder(server.resolve(path))
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

  private long eventCount(String productId, String eventType) throws Exception {
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

  private static void holdAdvisoryLock(Connection holder) throws Exception {
    try (Statement st = holder.createStatement()) {
      st.execute("SELECT pg_advisory_lock(" + HELD_AUDIT_LOCK + ")");
    }
  }

  private static void releaseAdvisoryLock(Connection holder) throws Exception {
    try (Statement st = holder.createStatement()) {
      st.execute("SELECT pg_advisory_unlock_all()");
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
