package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest;
import org.streamrune.core.EventStore;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * Which dead-lettered commands the retry runner replays on its own, and what the operator's Retry
 * does with the rest, as in the Spring and Micronaut apps. A keyed entry is replayed on the
 * schedule, and the command inbox answers it with the recorded result when the caller already
 * retried with the same key. An unkeyed entry waits for an operator; the plain Retry answers {@code
 * 409}, and the operator then replays it ({@code ?force=true}) or discards it ({@code DELETE}).
 * Either decision is written into the audit log ({@code DLQ_FORCED_REPLAY}, {@code DLQ_DISCARDED})
 * before it is acted on, and an audit row that cannot be written stops it.
 *
 * <p>The tests run with {@code %test.app.dead-letter.poll-interval=PT1S}, and move an entry's
 * {@code published_at} past the policy's initial delay (60 s with jitter) where they wait for the
 * schedule.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class DeadLetterReplayPolicyIT {

  private static final Duration SCHEDULE = Duration.ofSeconds(20);

  @Inject DataSource dataSource;
  @Inject DeadLetterQueue deadLetterQueue;
  @Inject EventStore eventStore;
  @Inject MeterRegistry meterRegistry;
  @Inject DeadLetterRetryRunner deadLetterRetryRunner;

  @Test
  void aKeyedAdjustmentThatFailed_isReplayedOnTheSchedule() throws Exception {
    String pid = createProduct();
    CommandId commandId = failedAdjustment(pid, UUID.randomUUID().toString());

    becomeDue(commandId);

    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(deadLetterQueue.find(commandId)).isEmpty());
    assertThat(stockAdjustments(pid)).as("the scheduled replay applied the adjustment").isOne();
  }

  @Test
  void aKeyedAdjustmentTheCallerRetried_isNotAppliedAgainByTheSchedule() throws Exception {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();
    CommandId commandId = failedAdjustment(pid, key);

    // The caller saw the 500 and sent the request again, with the same key: it succeeds.
    adjustStock(pid, key).statusCode(200);
    assertThat(stockAdjustments(pid)).isOne();

    becomeDue(commandId);

    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(deadLetterQueue.find(commandId)).isEmpty());
    assertThat(stockAdjustments(pid))
        .as("the inbox answered the replay with the result of the caller's retry")
        .isOne();
  }

  @Test
  void anUnkeyedAdjustmentTheCallerRetried_waitsForAnOperator_andIsCounted() throws Exception {
    String pid = createProduct();
    CommandId commandId = failedAdjustment(pid, null);
    adjustStock(pid, null).statusCode(200);
    assertThat(stockAdjustments(pid)).isOne();

    becomeDue(commandId);
    // A keyed entry made due after it: once the schedule has replayed this one, a poll has run
    // while the unkeyed entry was due.
    CommandId witness = failedAdjustment(createProduct(), UUID.randomUUID().toString());
    becomeDue(witness);
    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(deadLetterQueue.find(witness)).isEmpty());

    var entry = deadLetterQueue.find(commandId);
    assertThat(entry).as("the schedule leaves an unkeyed entry alone").isPresent();
    assertThat(entry.get().dlqAttempts()).isZero();
    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(awaitingOperatorGauge()).isEqualTo(awaitingOperator()));
    assertThat(awaitingOperatorGauge())
        .as("the runner samples the entries it leaves to an operator")
        .isGreaterThanOrEqualTo(1.0);

    retry(commandId, false, "ops-1")
        .statusCode(409)
        .body("outcome", equalTo("NOT_DEDUPLICATED"))
        .body("message", containsString("?force=true"));
    assertThat(stockAdjustments(pid)).as("a plain Retry never runs it a second time").isOne();
    assertThat(decisions(commandId, "DLQ_FORCED_REPLAY")).isEmpty();
  }

  @Test
  void aForcedRetryReplaysTheEntry_andTheAuditLogNamesTheOperator() throws Exception {
    String pid = createProduct();
    CommandId commandId = failedAdjustment(pid, null);

    retry(commandId, true, "ops-7").statusCode(200).body("outcome", equalTo("REPLAYED"));

    assertThat(stockAdjustments(pid)).isOne();
    assertThat(deadLetterQueue.find(commandId)).isEmpty();
    assertThat(decisions(commandId, "DLQ_FORCED_REPLAY"))
        .containsExactly("ops-7 SUCCESS product:" + pid);
  }

  @Test
  void aForcedRetryWhoseAuditRowCannotBeWritten_replaysNothing() throws Exception {
    String pid = createProduct();
    CommandId commandId = failedAdjustment(pid, null);

    try (var refused = new AuditFailureInjection(dataSource, commandId)) {
      retry(commandId, true, "ops-7").statusCode(greaterThanOrEqualTo(500));
    }

    assertThat(stockAdjustments(pid)).as("an unrecorded decision is not acted on").isZero();
    assertThat(deadLetterQueue.find(commandId)).isPresent();
    assertThat(decisions(commandId, "DLQ_FORCED_REPLAY")).isEmpty();
  }

  @Test
  void aDiscardRemovesTheEntry_namesTheOperator_andTheAwaitingGaugeDrops() throws Exception {
    String pid = createProduct();
    CommandId commandId = failedAdjustment(pid, null);
    // The caller's retry succeeded: the effect is present, so the operator discards the entry.
    adjustStock(pid, null).statusCode(200);
    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(awaitingOperatorGauge()).isEqualTo(awaitingOperator()));
    double before = awaitingOperatorGauge();

    discard(commandId, "ops-7").statusCode(200).body("outcome", equalTo("DISCARDED"));

    assertThat(deadLetterQueue.find(commandId)).isEmpty();
    assertThat(stockAdjustments(pid)).as("a discard runs nothing").isOne();
    assertThat(decisions(commandId, "DLQ_DISCARDED"))
        .containsExactly("ops-7 SUCCESS product:" + pid);
    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(awaitingOperatorGauge()).isEqualTo(before - 1));
  }

  @Test
  void aDiscardWhoseAuditRowCannotBeWritten_keepsTheEntry() throws Exception {
    CommandId commandId = failedAdjustment(createProduct(), null);

    try (var refused = new AuditFailureInjection(dataSource, commandId)) {
      discard(commandId, "ops-7").statusCode(greaterThanOrEqualTo(500));
    }

    assertThat(deadLetterQueue.find(commandId)).isPresent();
    assertThat(decisions(commandId, "DLQ_DISCARDED")).isEmpty();
  }

  @Test
  void aDiscardOfAnUnknownEntryIs404_andAuditsNothing() throws Exception {
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());

    discard(commandId, "ops-7").statusCode(404);

    assertThat(decisions(commandId, "DLQ_DISCARDED")).isEmpty();
  }

  @Test
  void aKeyedEntryOlderThanTheInboxWindow_isRefused_counted_andReplayedOnlyWhenForced() {
    String pid = createProduct();
    double refusedBefore = staleRefusals();
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());
    deadLetterQueue.publish(
        new DeadLetterPublishRequest(
            "{\"productId\":\"" + pid + "\",\"quantityChange\":5,\"reason\":\"restock\"}",
            ProductCommand.AdjustStock.class.getName(),
            commandId,
            StreamId.of(ProductState.TYPE, AggregateId.of(pid)),
            IllegalStateException.class.getName(),
            "transient downstream failure",
            3,
            // First received eight days ago: the inbox (7 days) may have pruned the key.
            Instant.now().minus(Duration.ofDays(8)),
            CorrelationId.of("corr-" + UUID.randomUUID()),
            UserId.of("admin-1"),
            null,
            IdempotencyKey.scopedTo("admin-1", UUID.randomUUID().toString())));

    retry(commandId, false, "ops-1")
        .statusCode(409)
        .body("outcome", equalTo("STALE_REPLAY_BLOCKED"));
    assertThat(staleRefusals() - refusedBefore).isEqualTo(1.0);
    assertThat(stockAdjustments(pid)).isZero();

    retry(commandId, true, "ops-1").statusCode(200);
    assertThat(stockAdjustments(pid)).isOne();
  }

  /**
   * A replay on a replica whose code reads the product's stream at an older event schema than one
   * it holds — a replica still on the previous release during a rolling deployment — fails before
   * anything is appended ({@code READER_BEHIND}): the endpoint answers {@code 503} with {@code
   * Retry-After}, the entry keeps its attempt count, and the replay succeeds once the stream is
   * readable. The test raises the stored {@code schema_version} of the product's events above every
   * version the app's upcasters reach, then puts it back.
   */
  @Test
  void aReplayOnAReplicaBehindTheStoredSchema_is503_andLeavesTheEntryAsItWas() throws Exception {
    String pid = createProduct();
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());
    deadLetterQueue.publish(
        new DeadLetterPublishRequest(
            "{\"productId\":\"" + pid + "\",\"quantityChange\":5,\"reason\":\"restock\"}",
            ProductCommand.AdjustStock.class.getName(),
            commandId,
            StreamId.of(ProductState.TYPE, AggregateId.of(pid)),
            IllegalStateException.class.getName(),
            "transient downstream failure",
            3,
            Instant.now(),
            CorrelationId.of("corr-" + UUID.randomUUID()),
            UserId.of("admin-1"),
            null,
            IdempotencyKey.scopedTo("admin-1", UUID.randomUUID().toString())));
    try (var ahead = new StoredSchemaAhead(dataSource, pid)) {
      retry(commandId, false, "ops-1")
          .statusCode(503)
          .header("Retry-After", equalTo("5"))
          .body("outcome", equalTo("READER_BEHIND"));
    }

    var entry = deadLetterQueue.find(commandId);
    assertThat(entry).as("the entry stays for a replica that reads the stream").isPresent();
    assertThat(entry.get().dlqAttempts()).as("the refusal is not counted").isZero();
    assertThat(stockAdjustments(pid)).isZero();

    retry(commandId, false, "ops-1").statusCode(200).body("outcome", equalTo("REPLAYED"));
    assertThat(stockAdjustments(pid)).isOne();
  }

  @Test
  void aForcedRetryOfAnUnknownEntryIs404_andAuditsNothing() throws Exception {
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());

    retry(commandId, true, "ops-1").statusCode(404);

    assertThat(decisions(commandId, "DLQ_FORCED_REPLAY")).isEmpty();
  }

  /** {@code PUT /stock} while the store refuses the event: the bus dead-letters the command. */
  private CommandId failedAdjustment(String pid, String key) throws Exception {
    String answered;
    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(ProductState.TYPE, pid, "StockAdjusted");
      answered =
          adjustStock(pid, key)
              .statusCode(key == null ? 500 : 503)
              .header("Retry-After", key == null ? nullValue() : equalTo("60"))
              .body("reason", equalTo(key == null ? "AWAITING_OPERATOR" : "QUEUED_FOR_REPLAY"))
              .extract()
              .path("commandId");
    }
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT command_id, idempotency_key FROM dead_letter_queue WHERE aggregate_id = ?")) {
      ps.setString(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("the failed adjustment is dead-lettered").isTrue();
        CommandId commandId = CommandId.of(rs.getString(1));
        assertThat(answered).as("the answer names the entry").isEqualTo(commandId.value());
        if (key == null) {
          assertThat(rs.getString(2)).isNull();
        } else {
          assertThat(rs.getString(2)).as("the entry keeps the caller's key").endsWith(key);
        }
        return commandId;
      }
    }
  }

  /**
   * Raises the stored {@code schema_version} of every event of one product above what the app's
   * upcasters reach, as a newer release would have stored them; {@link #close()} puts it back.
   */
  private static final class StoredSchemaAhead implements AutoCloseable {

    private static final int AHEAD = 1000;

    private final DataSource dataSource;
    private final String productId;

    StoredSchemaAhead(DataSource dataSource, String productId) throws Exception {
      this.dataSource = dataSource;
      this.productId = productId;
      shift(AHEAD);
    }

    @Override
    public void close() throws Exception {
      shift(-AHEAD);
    }

    private void shift(int by) throws Exception {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "UPDATE event_stream SET schema_version = schema_version + ?"
                      + " WHERE aggregate_type = 'product' AND aggregate_id = ?")) {
        ps.setInt(1, by);
        ps.setString(2, productId);
        assertThat(ps.executeUpdate()).isPositive();
        if (!conn.getAutoCommit()) {
          conn.commit();
        }
      }
    }
  }

  /** Moves the entry past the policy's initial delay, so the next poll finds it due. */
  private void becomeDue(CommandId commandId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE dead_letter_queue SET published_at = now() - interval '10 minutes'"
                    + " WHERE command_id = ?")) {
      ps.setString(1, commandId.value());
      assertThat(ps.executeUpdate()).isOne();
      if (!conn.getAutoCommit()) {
        conn.commit();
      }
    }
  }

  /** The audit rows of an operator's decision about the entry: "user outcome type:id". */
  private List<String> decisions(CommandId commandId, String decision) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT user_id, outcome, aggregate_type, aggregate_id FROM audit_log"
                    + " WHERE command_id = ? AND command_type = ?")) {
      ps.setString(1, commandId.value());
      ps.setString(2, decision);
      try (ResultSet rs = ps.executeQuery()) {
        List<String> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(
              rs.getString(1)
                  + " "
                  + rs.getString(2)
                  + " "
                  + rs.getString(3)
                  + ":"
                  + rs.getString(4));
        }
        return rows;
      }
    }
  }

  private double awaitingOperatorGauge() {
    return meterRegistry.get(MetricNames.DLQ_AWAITING_OPERATOR).gauge().value();
  }

  /** What the gauge counts, read from the queue the way the runner samples it. */
  private double awaitingOperator() {
    return deadLetterQueue.countAwaitingOperator(
        deadLetterRetryRunner.policy().maxRetries(),
        true,
        Instant.now().minus(deadLetterRetryRunner.inboxRetentionMaxAge()));
  }

  private double staleRefusals() {
    return meterRegistry.find(MetricNames.DLQ_STALE_REPLAY_BLOCKED).counters().stream()
        .mapToDouble(Counter::count)
        .sum();
  }

  private static String createProduct() {
    String pid = "q-dlq-policy-" + System.nanoTime();
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .contentType(ContentType.JSON)
        .body(
            """
            {"productId": "%s", "name": "Replayed", "description": "dead-letter policy",
             "category": "Test", "price": 5.00, "initialStock": 3}"""
                .formatted(pid))
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);
    return pid;
  }

  private static ValidatableResponse adjustStock(String pid, String key) {
    RequestSpecification request =
        given().header("X-User-Role", "ADMIN").header("X-User-Id", "admin-1");
    if (key != null) {
      request = request.header("Idempotency-Key", key);
    }
    return request
        .contentType(ContentType.JSON)
        .body("{\"quantity\": 5, \"reason\": \"restock\"}")
        .when()
        .put("/api/products/" + pid + "/stock")
        .then();
  }

  private static ValidatableResponse discard(CommandId commandId, String operator) {
    return given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", operator)
        .when()
        .delete("/api/admin/dead-letters/" + commandId.value())
        .then();
  }

  private static ValidatableResponse retry(CommandId commandId, boolean force, String operator) {
    return given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", operator)
        .when()
        .post(
            "/api/admin/dead-letters/"
                + commandId.value()
                + "/retry"
                + (force ? "?force=true" : ""))
        .then();
  }

  private long stockAdjustments(String pid) {
    return eventStore
        .readStream(
            StreamId.of(ProductState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> e.event() instanceof ProductEvent.StockAdjusted)
        .count();
  }
}
