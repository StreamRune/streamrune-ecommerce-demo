package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.arc.All;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;
import org.streamrune.runtime.SagaEventPathRetention;
import org.streamrune.runtime.SagaRunner;

/**
 * Saga recovery in the Quarkus app: the order-fulfillment saga runner is a bean, so the framework
 * starts its compensation retry sweeper and arms its key-age guard, and the admin API drains,
 * forces and resumes faulted sagas through a replayer built with the inbox retention window.
 *
 * <p>Poison and give-up faults are seeded through the stores (the order-fulfillment saga never
 * throws on a real event): a FAULTED row with a dead-letter entry pointing at a real {@code
 * StockReserved} event, and a row moved to {@code COMPENSATING} — and faulted — as the sweeper
 * would. The test profile runs the compensation retry sweeper every second ({@code
 * %test.streamrune.saga.compensation-retry-interval}).
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class SagaRecoveryAdminIT {

  /** Longer than the app's inbox retention window ({@code streamrune.inbox.retention-max-age}). */
  private static final Duration OLDER_THAN_THE_INBOX_WINDOW = Duration.ofDays(8);

  private static final SagaType SAGA_TYPE = SagaType.fromClass(OrderFulfillmentState.class);

  @Inject @All List<SagaRunner<?>> sagaRunners;
  @Inject SagaStore sagaStore;
  @Inject SagaDeadLetterStore sagaDeadLetterStore;
  @Inject EventStore eventStore;
  @Inject DataSource dataSource;

  @Test
  void theSagaRunnerIsABeanWhoseKeyAgeGuardTheFrameworkArmed() {
    assertThat(sagaRunners).hasSize(1);
    assertThat(SagaEventPathRetention.effectiveWindowOf(sagaRunners.getFirst()))
        .as("streamrune.inbox.retention-max-age, applied by the framework")
        .isEqualTo(Duration.ofDays(7));
  }

  @Test
  void aCompensationLeftCompensatingIsFinishedByTheSweeper() {
    String orderId = "q-sweep-" + System.nanoTime();
    SagaId sagaId = seedCompensating(orderId);

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATED));
    assertThat(eventTypes(orderId)).containsExactly("OrderCancelled");
  }

  @Test
  void aReplayWhoseFirstAttemptIsOlderThanTheInboxWindowIsRefusedUntilForced() throws Exception {
    String orderId = "q-dlq-stale-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);
    seedDeadLetter(sagaId, offset);
    update(
        "UPDATE saga_dead_letters SET first_replay_started_at = NOW() - make_interval(days => ?)"
            + " WHERE saga_id = ? AND event_offset = ?",
        (int) OLDER_THAN_THE_INBOX_WINDOW.toDays(),
        sagaId.value(),
        offset.value());

    assertThat(postForOutcome(replayPath(sagaId, offset, false)))
        .isEqualTo("STALE_REDRIVE_BLOCKED");
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).hasSize(1);
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.FAULTED);

    assertThat(postForOutcome(replayPath(sagaId, offset, true))).isEqualTo("REPLAYED");
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
    assertThat(loadSaga(sagaId).status()).isNotEqualTo(SagaStatus.FAULTED);
  }

  @Test
  void replayAllDrainsTheSagaOldestFirstWhereASingleReplayIsRefused() {
    String orderId = "q-dlq-drain-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset older = appendStockReservedEvent(sagaId, orderId);
    GlobalOffset newer = appendStockReservedEvent(sagaId, orderId);
    // The live saga subscription holds both events behind the fault.
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(sagaDeadLetterStore.findBySaga(sagaId))
                    .extracting(SagaDeadLetterStore.SagaDeadLetterEntry::eventOffset)
                    .containsExactlyInAnyOrder(older, newer));

    assertThat(postForOutcome(replayPath(sagaId, newer, false))).isEqualTo("OLDER_ENTRY_PENDING");

    List<String> outcomes =
        given()
            .header("X-User-Role", "ADMIN")
            .queryParam("sagaId", sagaId.value())
            .when()
            .post("/api/admin/saga-dead-letters/replay-all")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getList("outcomes", String.class);

    assertThat(outcomes).containsExactly("REPLAYED", "REPLAYED");
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
    assertThat(loadSaga(sagaId).status()).isNotEqualTo(SagaStatus.FAULTED);
    assertThat(loadSaga(sagaId).deadLetterPending()).isFalse();
  }

  @Test
  void resumeRunsAFaultedCompensationToItsEnd() {
    String orderId = "q-resume-" + System.nanoTime();
    SagaId sagaId = seedFaultedCompensation(orderId);

    assertThat(postForOutcome(resumePath(sagaId, false))).isEqualTo("RESUMED");

    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(eventTypes(orderId)).containsExactly("OrderCancelled");
  }

  @Test
  void resumeOfACompensationOlderThanTheInboxWindowIsRefusedUntilForced() throws Exception {
    String orderId = "q-resume-stale-" + System.nanoTime();
    SagaId sagaId = seedFaultedCompensation(orderId);
    update(
        "UPDATE saga_state SET episode_claimed_at = NOW() - make_interval(days => ?)"
            + " WHERE saga_id = ?",
        (int) OLDER_THAN_THE_INBOX_WINDOW.toDays(),
        sagaId.value());

    assertThat(postForOutcome(resumePath(sagaId, false))).isEqualTo("STALE_COMPENSATION_BLOCKED");
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(eventTypes(orderId)).isEmpty();

    assertThat(postForOutcome(resumePath(sagaId, true))).isEqualTo("RESUMED");
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void theFaultedListingAndTheDeadLetterListingShowTheSeededSaga() {
    String orderId = "q-dlq-list-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);
    seedDeadLetter(sagaId, offset);

    List<String> faulted =
        given()
            .queryParam("limit", 500)
            .when()
            .get("/api/admin/sagas/faulted")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getList("$", String.class);
    assertThat(faulted).contains(sagaId.value());

    List<Map<String, Object>> entries =
        given()
            .queryParam("limit", 500)
            .when()
            .get("/api/admin/saga-dead-letters")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getList("$");
    assertThat(entries)
        .anySatisfy(
            entry -> {
              assertThat(entry.get("sagaId")).isEqualTo(sagaId.value());
              assertThat(((Number) entry.get("eventOffset")).longValue()).isEqualTo(offset.value());
              assertThat(entry.get("eventType")).isEqualTo("StockReserved");
            });

    assertThat(
            given()
                .header("X-User-Role", "ADMIN")
                .queryParam("sagaId", sagaId.value())
                .queryParam("eventOffset", offset.value())
                .when()
                .delete("/api/admin/saga-dead-letters")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getBoolean("discarded"))
        .isTrue();
  }

  @Test
  void resumeNamesWhyItDidNothing() {
    SagaId forwardFault = seedFaultedSaga("q-resume-forward-" + System.nanoTime());

    assertThat(postForOutcome(resumePath(forwardFault, false))).isEqualTo("FORWARD_FAULT");
    assertThat(
            postForOutcome(resumePath(new SagaId("fulfillment-none-" + System.nanoTime()), false)))
        .isEqualTo("SAGA_NOT_FOUND");
  }

  @Test
  void theRecoveryEndpointsRequireTheAdminRole() {
    given()
        .queryParam("sagaId", "fulfillment-any")
        .when()
        .post("/api/admin/saga-dead-letters/replay-all")
        .then()
        .statusCode(403);
    given()
        .queryParam("sagaId", "fulfillment-any")
        .when()
        .post("/api/admin/sagas/faulted/resume")
        .then()
        .statusCode(403);
    given()
        .queryParam("sagaId", "fulfillment-any")
        .queryParam("eventOffset", 1)
        .queryParam("force", true)
        .when()
        .post("/api/admin/saga-dead-letters/replay")
        .then()
        .statusCode(403);
    given()
        .queryParam("sagaId", "fulfillment-any")
        .queryParam("eventOffset", 1)
        .when()
        .delete("/api/admin/saga-dead-letters")
        .then()
        .statusCode(403);
  }

  // ── seeding ────────────────────────────────────────────────────────────────

  private static OrderFulfillmentState state(
      SagaId sagaId, String orderId, OrderFulfillmentStatus status) {
    return new OrderFulfillmentState(
        sagaId,
        orderId,
        "cust-" + orderId,
        "pay-" + orderId,
        new Money(new BigDecimal("19.99"), "USD"),
        List.of(),
        status);
  }

  /** A saga awaiting inventory that a poison event faulted. */
  private SagaId seedFaultedSaga(String orderId) {
    SagaId sagaId = new SagaId("fulfillment-" + orderId);
    sagaStore.create(
        sagaId,
        SAGA_TYPE,
        state(sagaId, orderId, OrderFulfillmentStatus.AWAITING_INVENTORY),
        SagaStatus.FAULTED);
    return sagaId;
  }

  /**
   * A saga awaiting payment claimed for compensation: {@code COMPENSATING}, as after a compensation
   * command failed transiently. Its compensation is a single {@code CancelOrder}.
   */
  private SagaId seedCompensating(String orderId) {
    SagaId sagaId = new SagaId("fulfillment-" + orderId);
    OrderFulfillmentState state = state(sagaId, orderId, OrderFulfillmentStatus.AWAITING_PAYMENT);
    sagaStore.create(sagaId, SAGA_TYPE, state, SagaStatus.RUNNING);
    sagaStore.claimCompensating(sagaId, SAGA_TYPE, state, 1L);
    return sagaId;
  }

  /** A compensation the retry sweeper gave up on: FAULTED out of COMPENSATING, no entry. */
  private SagaId seedFaultedCompensation(String orderId) {
    SagaId sagaId = seedCompensating(orderId);
    assertThat(sagaStore.markFaulted(sagaId, SAGA_TYPE, 2L)).isTrue();
    return sagaId;
  }

  /** Appends a real {@code StockReserved} correlated to the saga; returns its global offset. */
  private GlobalOffset appendStockReservedEvent(SagaId sagaId, String orderId) {
    var event = new InventoryEvent.StockReserved("prod-" + orderId, orderId, 1, 9);
    StreamId streamId =
        StreamId.of(InventoryState.TYPE, AggregateId.of("stream-" + UUID.randomUUID()));
    var result =
        eventStore.append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    Version.initial().next(),
                    EventType.fromClass(InventoryEvent.StockReserved.class),
                    event,
                    new EventMetadata(
                        EventId.of("evt-" + UUID.randomUUID()),
                        CommandId.of("cmd-" + UUID.randomUUID()),
                        null,
                        null,
                        CorrelationId.of(sagaId.value()),
                        null,
                        null,
                        Instant.now(),
                        Map.of()))),
            Version.initial());
    return result.globalOffsets().get(0);
  }

  private void seedDeadLetter(SagaId sagaId, GlobalOffset eventOffset) {
    sagaDeadLetterStore.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sagaId,
            SAGA_TYPE,
            eventOffset,
            EventType.fromClass(InventoryEvent.StockReserved.class),
            "java.lang.RuntimeException",
            "simulated poison for admin IT",
            Instant.now()));
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private LoadedSaga<OrderFulfillmentState> loadSaga(SagaId sagaId) {
    return sagaStore.load(sagaId, SAGA_TYPE, OrderFulfillmentState.class).orElseThrow();
  }

  private static String replayPath(SagaId sagaId, GlobalOffset offset, boolean force) {
    return "/api/admin/saga-dead-letters/replay?sagaId="
        + sagaId.value()
        + "&eventOffset="
        + offset.value()
        + "&force="
        + force;
  }

  private static String resumePath(SagaId sagaId, boolean force) {
    return "/api/admin/sagas/faulted/resume?sagaId=" + sagaId.value() + "&force=" + force;
  }

  private static String postForOutcome(String path) {
    return given()
        .header("X-User-Role", "ADMIN")
        .when()
        .post(path)
        .then()
        .statusCode(200)
        .extract()
        .jsonPath()
        .getString("outcome");
  }

  private List<String> eventTypes(String orderId) {
    return eventStore
        .readStream(StreamId.of(OrderState.TYPE, AggregateId.of(orderId)), Version.initial(), 100)
        .stream()
        .map(e -> e.eventType().name())
        .toList();
  }

  private void update(String sql, Object... params) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }
      assertThat(ps.executeUpdate()).as("rows updated by: %s", sql).isEqualTo(1);
      if (!conn.getAutoCommit()) {
        conn.commit();
      }
    }
  }
}
