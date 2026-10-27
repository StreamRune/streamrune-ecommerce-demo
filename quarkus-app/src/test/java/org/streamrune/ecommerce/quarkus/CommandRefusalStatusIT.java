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
import java.util.Optional;
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
 * Retry-After: 5} and a JSON body naming the reason. A command the bus dead-lettered is pending,
 * whatever it failed on, and the answer carries its entry's {@code commandId}: {@code 503} with
 * {@code Retry-After} and {@code QUEUED_FOR_REPLAY} when it was sent with an {@code
 * Idempotency-Key}, {@code 500} with {@code AWAITING_OPERATOR} and no {@code Retry-After} when it
 * was not. The Spring and Micronaut apps answer the same. An answer with {@code Retry-After}
 * exposes it to the frontend's origin, whose page reads it to wait that long before it sends the
 * request again. No answer allows credentials.
 *
 * <p>Each condition is forced on the app's real command bus, in the app of this test's profile,
 * which has a PostgreSQL of its own ({@link PostgresTestResource}): the tests leave its circuit
 * breaker open and its bus closed, which the other test classes must not see. The profile sets the
 * lock timeout to one second, so a command that waits for a held lock gives up after the bus's
 * three attempts, in about three seconds. The tests run in order:
 *
 * <ol>
 *   <li><b>A held aggregate lock.</b> A price change of a product stops in its audit write, which
 *       the command bus makes while it still holds the product's lock: a trigger on {@code
 *       audit_log} waits for an advisory lock the test holds. A stock adjustment of the same
 *       product waits for the product's lock until the lock timeout, on every attempt, and is
 *       dead-lettered: sent with a key it is {@code QUEUED_FOR_REPLAY}, sent without one {@code
 *       AWAITING_OPERATOR}. Released, the price change completes with {@code 200}.
 *   <li><b>A version conflict on every attempt.</b> A trigger on {@code event_stream} raises {@code
 *       unique_violation} for a stock adjustment of a product, which the event store reports as an
 *       {@code OptimisticLockException}, as it does when a second writer stored the version first.
 *       The bus tries the command on every attempt of its retry policy and dead-letters it, keyed
 *       or not, with the two answers above; nothing is written.
 *   <li><b>An entry that cannot be stored.</b> With the same version conflict and a trigger on
 *       {@code dead_letter_queue} that refuses the entry, nothing is pending: the conflict itself
 *       reaches the caller, refused with {@code AGGREGATE_VERSION_CONFLICT}, key or no key.
 *   <li><b>An open circuit breaker.</b> The store refuses every price change of a second product (a
 *       trigger on {@code event_stream}), so each is a genuine failure, dead-lettered and awaiting
 *       an operator ({@code 500}), until the breaker opens; the next command is refused with {@code
 *       CIRCUIT_BREAKER_OPEN}.
 *   <li><b>A closed command bus.</b> Once the bus is closed, as on shutdown, a command is refused
 *       with {@code COMMAND_BUS_CLOSED}.
 *   <li><b>A preflight.</b> The question a browser asks before the page sends a keyed stock
 *       adjustment is answered for the frontend's origin, without credentials.
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

  /**
   * The origin of the frontend, which every request here comes from, as a browser sends it: the
   * answers carry the CORS headers the page sees.
   */
  private static final String FRONTEND_ORIGIN = "http://localhost:3000";

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @TestHTTPResource("/")
  URI server;

  @Inject DataSource dataSource;

  @Inject VirtualThreadCommandBus commandBus;

  @Test
  @Order(1)
  void aCommandThatCannotTakeItsAggregateLockIsDeadLettered_andTheAnswerSaysWhatHappensToIt()
      throws Exception {
    String productId = "q-refusal-lock-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);
    String stock = "/api/products/" + productId + "/stock";
    String recount = "{\"quantity\": 1, \"reason\": \"recount\"}";

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

        long waitStart = System.nanoTime();
        HttpResponse<String> keyed = send("PUT", stock, recount, "lock-key-1");
        assertThat(Duration.ofNanos(System.nanoTime() - waitStart))
            .as("the configured lock timeout, one second, on each of the bus's three attempts")
            .isLessThan(Duration.ofSeconds(10));
        assertQueuedForReplay(keyed, "lock-key-1");

        assertAwaitingOperator(send("PUT", stock, recount, null));

        releaseAdvisoryLock(holder);
        assertThat(priceChange.get().statusCode()).isEqualTo(200);
      } finally {
        // A pooled connection keeps its session: never hand it back with the lock held.
        releaseAdvisoryLock(holder);
      }
    }
    assertThat(eventCount(productId, "StockAdjusted"))
        .as("no adjustment ran while the lock was held")
        .isZero();
  }

  @Test
  @Order(2)
  void aVersionConflictOnEveryAttemptIsDeadLettered_andTheAnswerSaysWhatHappensToIt()
      throws Exception {
    String productId = "q-refusal-version-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);
    String stock = "/api/products/" + productId + "/stock";
    String recount = "{\"quantity\": 2, \"reason\": \"recount\"}";

    try (var conflict = new VersionConflictInjection(dataSource, productId, "StockAdjusted")) {
      assertQueuedForReplay(send("PUT", stock, recount, "version-key-1"), "version-key-1");
      assertAwaitingOperator(send("PUT", stock, recount, null));

      assertThat(eventCount(productId, "StockAdjusted")).as("nothing was written").isZero();
    }
  }

  @Test
  @Order(3)
  void aFailureWhoseEntryCannotBeStoredIsAnsweredAsTheFailure() throws Exception {
    String productId = "q-refusal-unstored-" + System.nanoTime();
    assertThat(createProduct(productId).statusCode()).isEqualTo(200);

    try (var conflict = new VersionConflictInjection(dataSource, productId, "StockAdjusted");
        var refused = new DeadLetterWriteFailure(dataSource, productId)) {
      HttpResponse<String> adjusted =
          send(
              "PUT",
              "/api/products/" + productId + "/stock",
              "{\"quantity\": 3, \"reason\": \"recount\"}",
              "unstored-key-1");

      // Nothing is pending: the version conflict itself reaches the caller, a refusal.
      assertRefused(adjusted, "AGGREGATE_VERSION_CONFLICT");
      assertThat(eventCount(productId, "StockAdjusted")).as("nothing was written").isZero();
    }
  }

  @Test
  @Order(4)
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
      assertThat(genuineFailures)
          .as("a store failure is a genuine failure, dead-lettered unkeyed: 500")
          .isPositive();
      assertRefused(answer, "CIRCUIT_BREAKER_OPEN");
      assertRefused(
          createProduct("q-refusal-breaker-other-" + System.nanoTime()), "CIRCUIT_BREAKER_OPEN");
    }
  }

  @Test
  @Order(5)
  void aClosedCommandBusRefusesWithRetryAfter() {
    commandBus.close();

    assertRefused(createProduct("q-refusal-closed-" + System.nanoTime()), "COMMAND_BUS_CLOSED");
  }

  /**
   * Before the page sends a command, the browser asks whether it may: a preflight with the method
   * and the headers of the request, here a keyed stock adjustment's. The answer admits the
   * frontend's origin and allows no credentials.
   */
  @Test
  @Order(6)
  void aPreflightAdmitsTheFrontendsOrigin_andAllowsNoCredentials() throws Exception {
    HttpResponse<String> answer = preflight("/api/products/q-refusal-preflight/stock", "PUT");

    assertThat(answer.statusCode()).as("status of the preflight").isEqualTo(200);
    assertThat(answer.headers().firstValue("Access-Control-Allow-Origin"))
        .contains(FRONTEND_ORIGIN);
    assertNoCredentialsAllowed(answer);
  }

  private static void assertRefused(HttpResponse<String> answer, String reason) {
    assertThat(answer.statusCode()).as("status of %s", answer.body()).isEqualTo(503);
    assertThat(answer.headers().firstValue("Retry-After")).contains("5");
    assertRetryAfterReadable(answer);
    JsonNode body = jsonBody(answer);
    assertThat(body.path("reason").asText()).isEqualTo(reason);
    assertThat(body.path("message").asText()).isNotBlank();
    assertThat(body.has("commandId")).as("a refusal leaves no entry").isFalse();
  }

  /**
   * The page runs on another origin, and a browser shows it a response header outside the CORS
   * safelist only when the answer exposes it: without that the page cannot read how long to wait.
   * The answer allows no credentials ({@link #assertNoCredentialsAllowed}).
   */
  private static void assertRetryAfterReadable(HttpResponse<String> answer) {
    assertThat(answer.headers().firstValue("Access-Control-Expose-Headers"))
        .as("Access-Control-Expose-Headers of the answer to %s", FRONTEND_ORIGIN)
        .hasValueSatisfying(exposed -> assertThat(exposed).containsIgnoringCase("Retry-After"));
    assertNoCredentialsAllowed(answer);
  }

  /**
   * A browser shows another origin the answer to a request it sent with its cookies or its HTTP
   * authentication only when the answer says {@code Access-Control-Allow-Credentials: true}. The
   * demo's caller is two request headers, not a cookie, so no answer says it: an identity the
   * browser sends on its own never makes an answer readable to another origin.
   */
  private static void assertNoCredentialsAllowed(HttpResponse<String> answer) {
    assertThat(answer.headers().firstValue("Access-Control-Allow-Credentials"))
        .as("Access-Control-Allow-Credentials of the answer to %s", FRONTEND_ORIGIN)
        .isNotEqualTo(Optional.of("true"));
  }

  /**
   * {@code 503} with {@code Retry-After} and {@code QUEUED_FOR_REPLAY}, and the {@code commandId}
   * of an entry stored under the request's key.
   */
  private void assertQueuedForReplay(HttpResponse<String> answer, String key) throws Exception {
    assertThat(answer.statusCode()).as("status of %s", answer.body()).isEqualTo(503);
    assertThat(answer.headers().firstValue("Retry-After")).contains("5");
    assertRetryAfterReadable(answer);
    JsonNode body = jsonBody(answer);
    assertThat(body.path("reason").asText()).isEqualTo("QUEUED_FOR_REPLAY");
    assertThat(body.path("message").asText()).contains("same key");
    assertThat(storedKeyOf(body.path("commandId").asText()))
        .as("the entry the answer names keeps the request's key")
        .endsWith("|" + key);
  }

  /**
   * {@code 500} with {@code AWAITING_OPERATOR} and no {@code Retry-After}, and the {@code
   * commandId} of an entry stored without a key.
   */
  private void assertAwaitingOperator(HttpResponse<String> answer) throws Exception {
    assertThat(answer.statusCode()).as("status of %s", answer.body()).isEqualTo(500);
    assertThat(answer.headers().firstValue("Retry-After")).isEmpty();
    JsonNode body = jsonBody(answer);
    assertThat(body.path("reason").asText()).isEqualTo("AWAITING_OPERATOR");
    assertThat(body.path("message").asText()).contains("operator");
    assertThat(storedKeyOf(body.path("commandId").asText()))
        .as("the entry the answer names has no key")
        .isEqualTo(NO_KEY);
  }

  private static JsonNode jsonBody(HttpResponse<String> answer) {
    assertThat(answer.headers().firstValue("Content-Type").orElse("")).contains("application/json");
    try {
      return JSON.readTree(answer.body());
    } catch (Exception e) {
      throw new AssertionError("not JSON: " + answer.body(), e);
    }
  }

  /** What {@link #storedKeyOf} answers for an entry without an idempotency key. */
  private static final String NO_KEY = "(no key)";

  /** The idempotency key the dead-letter entry {@code commandId} was stored with. */
  private String storedKeyOf(String commandId) throws Exception {
    try (var conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT idempotency_key FROM dead_letter_queue WHERE command_id = ?")) {
      ps.setString(1, commandId);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("an entry %s in dead_letter_queue", commandId).isTrue();
        String key = rs.getString(1);
        return key == null ? NO_KEY : key;
      }
    }
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

  /** The preflight a browser sends before a {@code method} request of the page to {@code path}. */
  private HttpResponse<String> preflight(String path, String method) throws Exception {
    return http.send(
        HttpRequest.newBuilder(server.resolve(path))
            .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
            .header("Origin", FRONTEND_ORIGIN)
            .header("Access-Control-Request-Method", method)
            .header(
                "Access-Control-Request-Headers",
                "content-type,idempotency-key,x-user-id,x-user-role")
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> send(String method, String path, String json) {
    return send(method, path, json, null);
  }

  /** Sends the request as {@code admin-1}, with {@code key} as its Idempotency-Key unless null. */
  private HttpResponse<String> send(String method, String path, String json, String key) {
    try {
      var request =
          HttpRequest.newBuilder(server.resolve(path))
              .header("Content-Type", "application/json")
              .header("Origin", FRONTEND_ORIGIN)
              .header("X-User-Id", "admin-1")
              .header("X-User-Role", "ADMIN");
      if (key != null) {
        request.header("Idempotency-Key", key);
      }
      return http.send(
          request.method(method, HttpRequest.BodyPublishers.ofString(json)).build(),
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
   * Makes the dead-letter queue refuse every entry of one aggregate: a trigger on {@code
   * dead_letter_queue} raises an error, so the bus's publish fails, nothing is pending and the
   * failure itself reaches the caller. {@link #close()} drops the trigger and its function.
   */
  private static final class DeadLetterWriteFailure implements AutoCloseable {

    private final DataSource dataSource;

    DeadLetterWriteFailure(DataSource dataSource, String aggregateId) throws Exception {
      this.dataSource = dataSource;
      execute(
          """
          CREATE OR REPLACE FUNCTION dead_letter_test_refuse() RETURNS trigger
            LANGUAGE plpgsql AS $$
          BEGIN
            IF NEW.aggregate_id = '%s' THEN
              RAISE EXCEPTION 'injected dead-letter queue failure';
            END IF;
            RETURN NEW;
          END $$;
          DROP TRIGGER IF EXISTS dead_letter_test_refuse ON dead_letter_queue;
          CREATE TRIGGER dead_letter_test_refuse BEFORE INSERT ON dead_letter_queue
            FOR EACH ROW EXECUTE FUNCTION dead_letter_test_refuse();
          """
              .formatted(aggregateId));
    }

    @Override
    public void close() throws Exception {
      execute(
          """
          DROP TRIGGER IF EXISTS dead_letter_test_refuse ON dead_letter_queue;
          DROP FUNCTION IF EXISTS dead_letter_test_refuse();
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
