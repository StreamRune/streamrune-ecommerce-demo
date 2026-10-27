package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
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
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * Which dead-lettered commands the retry runner replays on its own, and what the operator's Retry
 * does with the rest. A keyed entry is replayed on the schedule: it runs under the key its caller
 * sent, so if the caller already retried with that key, the command inbox answers the replay with
 * the recorded result and the stock changes once. An unkeyed entry waits for an operator: nothing
 * can tell a replay from a second execution of a command its caller already retried. The operator's
 * plain Retry answers {@code 409}. The operator then decides: {@code ?force=true} replays the
 * entry, {@code DELETE} discards it. Either decision is written into the audit log ({@code
 * DLQ_FORCED_REPLAY}, {@code DLQ_DISCARDED}, naming the operator) before it is acted on, and an
 * audit row that cannot be written stops it.
 *
 * <p>The tests run with {@code app.dead-letter.poll-interval=1s}. An entry the bus dead-letters is
 * due for its first scheduled retry after the policy's initial delay (30 s with jitter), so the
 * tests that wait for the schedule move the entry's {@code published_at} into the past.
 */
class DeadLetterReplayPolicyIT extends AbstractIntegrationTest {

  private static final Duration SCHEDULE = Duration.ofSeconds(20);

  @Autowired private DataSource dataSource;
  @Autowired private DeadLetterQueue deadLetterQueue;
  @Autowired private EventStore eventStore;
  @Autowired private MeterRegistry meterRegistry;
  @Autowired private DeadLetterRetryRunner deadLetterRetryRunner;

  @Test
  void aKeyedAdjustmentThatFailed_isReplayedOnTheSchedule() throws Exception {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();
    CommandId commandId = failedAdjustment(pid, key);

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
    adjustStock(pid, key).expectStatus().isOk();
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
    adjustStock(pid, null).expectStatus().isOk();
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
        .expectStatus()
        .isEqualTo(409)
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("NOT_DEDUPLICATED")
        .jsonPath("$.message")
        .value(m -> assertThat((String) m).contains("?force=true"));
    assertThat(stockAdjustments(pid)).as("a plain Retry never runs it a second time").isOne();
    assertThat(decisions(commandId, "DLQ_FORCED_REPLAY")).isEmpty();
  }

  @Test
  void aForcedRetryReplaysTheEntry_andTheAuditLogNamesTheOperator() throws Exception {
    String pid = createProduct();
    CommandId commandId = failedAdjustment(pid, null);

    retry(commandId, true, "ops-7")
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("REPLAYED");

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
      retry(commandId, true, "ops-7").expectStatus().is5xxServerError();
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
    adjustStock(pid, null).expectStatus().isOk();
    await()
        .atMost(SCHEDULE)
        .untilAsserted(() -> assertThat(awaitingOperatorGauge()).isEqualTo(awaitingOperator()));
    double before = awaitingOperatorGauge();

    discard(commandId, "ops-7")
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("DISCARDED");

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
      discard(commandId, "ops-7").expectStatus().is5xxServerError();
    }

    assertThat(deadLetterQueue.find(commandId)).isPresent();
    assertThat(decisions(commandId, "DLQ_DISCARDED")).isEmpty();
  }

  @Test
  void aDiscardOfAnUnknownEntryIs404_andAuditsNothing() throws Exception {
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());

    discard(commandId, "ops-7").expectStatus().isNotFound();

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
        .expectStatus()
        .isEqualTo(409)
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("STALE_REPLAY_BLOCKED");
    assertThat(staleRefusals() - refusedBefore).isEqualTo(1.0);
    assertThat(stockAdjustments(pid)).isZero();

    retry(commandId, true, "ops-1").expectStatus().isOk();
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
          .expectStatus()
          .isEqualTo(503)
          .expectHeader()
          .valueEquals("Retry-After", "5")
          .expectBody()
          .jsonPath("$.outcome")
          .isEqualTo("READER_BEHIND");
    }

    var entry = deadLetterQueue.find(commandId);
    assertThat(entry).as("the entry stays for a replica that reads the stream").isPresent();
    assertThat(entry.get().dlqAttempts()).as("the refusal is not counted").isZero();
    assertThat(stockAdjustments(pid)).isZero();

    retry(commandId, false, "ops-1")
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("REPLAYED");
    assertThat(stockAdjustments(pid)).isOne();
  }

  @Test
  void aForcedRetryOfAnUnknownEntryIs404_andAuditsNothing() throws Exception {
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());

    retry(commandId, true, "ops-1").expectStatus().isNotFound();

    assertThat(decisions(commandId, "DLQ_FORCED_REPLAY")).isEmpty();
  }

  /**
   * {@code PUT /stock} while the store refuses the event: the bus dead-letters the command, and the
   * answer names the entry: {@code 503} with {@code Retry-After} and {@code QUEUED_FOR_REPLAY} for
   * a keyed adjustment, {@code 500} with {@code AWAITING_OPERATOR} and no {@code Retry-After} for
   * an unkeyed one.
   */
  private CommandId failedAdjustment(String pid, String key) throws Exception {
    String[] answered = new String[1];
    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(ProductState.TYPE, pid, "StockAdjusted");
      var answer = adjustStock(pid, key).expectStatus().isEqualTo(key == null ? 500 : 503);
      answer =
          key == null
              ? answer.expectHeader().doesNotExist("Retry-After")
              : answer.expectHeader().valueEquals("Retry-After", "5");
      answer
          .expectBody()
          .jsonPath("$.reason")
          .isEqualTo(key == null ? "AWAITING_OPERATOR" : "QUEUED_FOR_REPLAY")
          .jsonPath("$.commandId")
          .value(id -> answered[0] = (String) id);
    }
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT command_id, idempotency_key FROM dead_letter_queue WHERE aggregate_id = ?")) {
      ps.setString(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("the failed adjustment is dead-lettered").isTrue();
        CommandId commandId = CommandId.of(rs.getString(1));
        assertThat(answered[0]).as("the answer names the entry").isEqualTo(commandId.value());
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

  private String createProduct() {
    String pid = "dlq-policy-" + System.nanoTime();
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "%s", "name": "Replayed", "description": "dead-letter policy",
             "category": "Test", "price": 5.00, "initialStock": 3}"""
                .formatted(pid))
        .exchange()
        .expectStatus()
        .isOk();
    return pid;
  }

  private WebTestClient.ResponseSpec adjustStock(String pid, String key) {
    var request =
        client
            .put()
            .uri("/api/products/" + pid + "/stock")
            .header("X-User-Role", "ADMIN")
            .header("X-User-Id", "admin-1");
    if (key != null) {
      request = request.header("Idempotency-Key", key);
    }
    return request
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantity\": 5, \"reason\": \"restock\"}")
        .exchange();
  }

  private WebTestClient.ResponseSpec discard(CommandId commandId, String operator) {
    return client
        .delete()
        .uri("/api/admin/dead-letters/" + commandId.value())
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", operator)
        .exchange();
  }

  private WebTestClient.ResponseSpec retry(CommandId commandId, boolean force, String operator) {
    return client
        .post()
        .uri(
            "/api/admin/dead-letters/"
                + commandId.value()
                + "/retry"
                + (force ? "?force=true" : ""))
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", operator)
        .exchange();
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
