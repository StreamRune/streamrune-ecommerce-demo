package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
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
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * Verifies the saga recovery endpoints of {@link AdminController}: dead-letter listing, faulted
 * saga enumeration, single replay, drain ({@code replay-all}), resume of a faulted compensation,
 * discard, and the key-age refusals the replayer applies with the app's inbox retention window.
 * Seeds quarantine/saga rows directly via the autowired stores (the same trick the framework's
 * {@code SagaDeadLetterReplayerTest} uses) rather than trying to provoke a real orchestrator
 * failure through HTTP — {@link org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga} never
 * throws on a real domain event, so "poison" here is a fabricated dead-letter entry pointing at a
 * real, already-appended event, and a give-up fault is a row moved to {@code COMPENSATING} and
 * faulted through the store, as the compensation retry sweeper would.
 */
class SagaDeadLetterAdminIT extends AbstractIntegrationTest {

  @Autowired private SagaDeadLetterStore sagaDeadLetterStore;
  @Autowired private SagaStore sagaStore;
  @Autowired private EventStore eventStore;
  @Autowired private DataSource dataSource;

  /** Longer than the app's inbox retention window ({@code streamrune.inbox.retention-max-age}). */
  private static final Duration OLDER_THAN_THE_INBOX_WINDOW = Duration.ofDays(8);

  /** Seeds a FAULTED order-fulfillment saga awaiting inventory, and returns its id. */
  private SagaId seedFaultedSaga(String orderId) {
    SagaId sagaId = new SagaId("fulfillment-" + orderId);
    OrderFulfillmentState state =
        new OrderFulfillmentState(
            sagaId,
            orderId,
            "cust-" + orderId,
            "pay-" + orderId,
            new Money(new BigDecimal("19.99"), "USD"),
            List.of(),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    sagaStore.create(
        sagaId, SagaType.fromClass(OrderFulfillmentState.class), state, SagaStatus.FAULTED);
    return sagaId;
  }

  /**
   * Appends a real {@code StockReserved} event correlated to {@code sagaId}, so a replay can feed
   * it back through the real {@link org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga}
   * without throwing (evolve() handles it, moving AWAITING_INVENTORY -> AWAITING_CONFIRMATION).
   * Returns the appended event's global offset — the dead-letter idempotency key.
   */
  private GlobalOffset appendStockReservedEvent(SagaId sagaId, String orderId) {
    InventoryEvent.StockReserved event =
        new InventoryEvent.StockReserved("prod-" + orderId, orderId, 1, 9);
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

  /** Loads the order-fulfillment saga row through the type-scoped read. */
  private LoadedSaga<OrderFulfillmentState> loadSaga(SagaId sagaId) {
    return sagaStore
        .load(sagaId, SagaType.fromClass(OrderFulfillmentState.class), OrderFulfillmentState.class)
        .orElseThrow();
  }

  private void seedDeadLetter(SagaId sagaId, GlobalOffset eventOffset) {
    sagaDeadLetterStore.publish(
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sagaId,
            SagaType.fromClass(OrderFulfillmentState.class),
            eventOffset,
            EventType.fromClass(InventoryEvent.StockReserved.class),
            "java.lang.RuntimeException",
            "simulated poison for admin IT",
            Instant.now()));
  }

  @Test
  void listSagaDeadLettersReturnsSeededEntry() {
    String orderId = "dlq-list-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);
    seedDeadLetter(sagaId, offset);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> body =
        client
            .get()
            .uri("/api/admin/saga-dead-letters?limit=200")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body)
        .as("seeded quarantine entry should appear in the admin list")
        .anySatisfy(
            entry -> {
              assertThat(entry.get("sagaId")).isEqualTo(sagaId.value());
              assertThat(((Number) entry.get("eventOffset")).longValue()).isEqualTo(offset.value());
              assertThat(entry.get("eventType")).isEqualTo("StockReserved");
              assertThat(entry.get("errorType")).isEqualTo("java.lang.RuntimeException");
              assertThat(entry.get("errorMessage")).isEqualTo("simulated poison for admin IT");
              assertThat(entry.get("faultedAt")).isNotNull();
            });
  }

  @Test
  void listSagaDeadLettersWithLimitQueryParamReturnsOk() {
    client.get().uri("/api/admin/saga-dead-letters?limit=10").exchange().expectStatus().isOk();
  }

  @Test
  void listFaultedSagasReturnsSeededFaultedSaga() {
    String orderId = "dlq-faulted-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);

    @SuppressWarnings("unchecked")
    List<String> body =
        client
            .get()
            .uri("/api/admin/sagas/faulted?limit=200")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body).contains(sagaId.value());
  }

  @Test
  void replayOfFixedPoisonReturnsReplayedAndDiscardsEntry() {
    String orderId = "dlq-replay-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);
    seedDeadLetter(sagaId, offset);

    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).hasSize(1);
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.FAULTED);

    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .post()
            .uri(
                "/api/admin/saga-dead-letters/replay?sagaId={sagaId}&eventOffset={offset}",
                sagaId.value(),
                offset.value())
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("outcome")).isEqualTo("REPLAYED");

    // Entry discarded and the fault cleared by the fed step's own write: the real orchestrator
    // processed the event cleanly (evolve() moves AWAITING_INVENTORY -> AWAITING_CONFIRMATION).
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
    assertThat(loadSaga(sagaId).status()).isNotEqualTo(SagaStatus.FAULTED);
  }

  @Test
  void replayOfUnknownEntryReturnsEntryNotFound() {
    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .post()
            .uri(
                "/api/admin/saga-dead-letters/replay?sagaId={sagaId}&eventOffset={offset}",
                "fulfillment-nonexistent-" + System.nanoTime(),
                999999999L)
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("outcome")).isEqualTo("ENTRY_NOT_FOUND");
  }

  @Test
  void discardRemovesSeededEntry() {
    String orderId = "dlq-discard-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);
    seedDeadLetter(sagaId, offset);

    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).hasSize(1);

    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .delete()
            .uri(
                "/api/admin/saga-dead-letters?sagaId={sagaId}&eventOffset={offset}",
                sagaId.value(),
                offset.value())
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("discarded")).isEqualTo(true);
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
  }

  /**
   * A correlated event that reaches a FAULTED saga is held by the live saga runner: the entry and
   * the saga's dead-letter shield ({@code dead_letter_pending}) are written together. Discarding
   * the saga's last entry through the admin endpoint must release that shield, or every later live
   * event of the saga stays held behind a backlog that no longer exists.
   */
  @Test
  void discardOfTheLastEntryReleasesTheSagaDeadLetterShield() {
    String orderId = "dlq-discard-shield-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);

    // The live saga subscription holds the event (the saga is FAULTED): entry and shield together.
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(sagaDeadLetterStore.findBySaga(sagaId))
                    .extracting(SagaDeadLetterStore.SagaDeadLetterEntry::eventOffset)
                    .containsExactly(offset));
    assertThat(loadSaga(sagaId).deadLetterPending()).isTrue();

    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .delete()
            .uri(
                "/api/admin/saga-dead-letters?sagaId={sagaId}&eventOffset={offset}",
                sagaId.value(),
                offset.value())
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("discarded")).isEqualTo(true);
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
    assertThat(loadSaga(sagaId).deadLetterPending())
        .as("discarding the saga's last entry releases its dead-letter shield")
        .isFalse();
  }

  @Test
  void discardOfUnknownEntryReturnsFalse() {
    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .delete()
            .uri(
                "/api/admin/saga-dead-letters?sagaId={sagaId}&eventOffset={offset}",
                "fulfillment-nonexistent-" + System.nanoTime(),
                888888888L)
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("discarded")).isEqualTo(false);
  }

  /**
   * The replayer is built with the app's inbox retention window, so a re-drive whose first attempt
   * started longer ago than that window — the forward commands' deduplication keys may be pruned —
   * is refused until the operator forces it.
   */
  @Test
  void aReplayWhoseFirstAttemptIsOlderThanTheInboxWindowIsRefusedUntilForced() throws Exception {
    String orderId = "dlq-stale-" + System.nanoTime();
    SagaId sagaId = seedFaultedSaga(orderId);
    GlobalOffset offset = appendStockReservedEvent(sagaId, orderId);
    seedDeadLetter(sagaId, offset);
    update(
        "UPDATE saga_dead_letters SET first_replay_started_at = NOW() - make_interval(days => ?)"
            + " WHERE saga_id = ? AND event_offset = ?",
        (int) OLDER_THAN_THE_INBOX_WINDOW.toDays(),
        sagaId.value(),
        offset.value());

    assertThat(postForOutcome(replayUri(sagaId, offset, false))).isEqualTo("STALE_REDRIVE_BLOCKED");
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).hasSize(1);
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.FAULTED);

    assertThat(postForOutcome(replayUri(sagaId, offset, true))).isEqualTo("REPLAYED");
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
    assertThat(loadSaga(sagaId).status()).isNotEqualTo(SagaStatus.FAULTED);
  }

  /**
   * A single replay is refused while an older entry of the same saga is pending; {@code replay-all}
   * drains the saga's entries oldest first and releases its dead-letter shield.
   */
  @Test
  void replayAllDrainsTheSagaOldestFirstWhereASingleReplayIsRefused() {
    String orderId = "dlq-drain-" + System.nanoTime();
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

    assertThat(postForOutcome(replayUri(sagaId, newer, false))).isEqualTo("OLDER_ENTRY_PENDING");

    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .post()
            .uri("/api/admin/saga-dead-letters/replay-all?sagaId={sagaId}", sagaId.value())
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("outcomes")).isEqualTo(List.of("REPLAYED", "REPLAYED"));
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();
    LoadedSaga<OrderFulfillmentState> saga = loadSaga(sagaId);
    assertThat(saga.status()).isNotEqualTo(SagaStatus.FAULTED);
    assertThat(saga.deadLetterPending()).isFalse();
  }

  /**
   * A compensation the retry sweeper gave up on is FAULTED with no dead-letter entry; the resume
   * endpoint re-runs it to a terminal status.
   */
  @Test
  void resumeRunsAFaultedCompensationToItsEnd() {
    String orderId = "resume-" + System.nanoTime();
    SagaId sagaId = seedFaultedCompensation(orderId);
    assertThat(sagaDeadLetterStore.findBySaga(sagaId)).isEmpty();

    assertThat(postForOutcome(resumeUri(sagaId, false))).isEqualTo("RESUMED");

    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(eventTypes(OrderState.TYPE, orderId)).containsExactly("OrderCancelled");
  }

  /**
   * A faulted compensation whose episode began longer ago than the inbox retention window is not
   * resumed until forced: a compensation that already ran may have lost its deduplication key.
   */
  @Test
  void resumeOfACompensationOlderThanTheInboxWindowIsRefusedUntilForced() throws Exception {
    String orderId = "resume-stale-" + System.nanoTime();
    SagaId sagaId = seedFaultedCompensation(orderId);
    update(
        "UPDATE saga_state SET episode_claimed_at = NOW() - make_interval(days => ?)"
            + " WHERE saga_id = ?",
        (int) OLDER_THAN_THE_INBOX_WINDOW.toDays(),
        sagaId.value());

    assertThat(postForOutcome(resumeUri(sagaId, false))).isEqualTo("STALE_COMPENSATION_BLOCKED");
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(eventTypes(OrderState.TYPE, orderId)).isEmpty();

    assertThat(postForOutcome(resumeUri(sagaId, true))).isEqualTo("RESUMED");
    assertThat(loadSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(eventTypes(OrderState.TYPE, orderId)).containsExactly("OrderCancelled");
  }

  @Test
  void resumeNamesWhyItDidNothing() {
    String orderId = "resume-forward-" + System.nanoTime();
    SagaId forwardFault = seedFaultedSaga(orderId);

    assertThat(postForOutcome(resumeUri(forwardFault, false))).isEqualTo("FORWARD_FAULT");
    assertThat(
            postForOutcome(resumeUri(new SagaId("fulfillment-none-" + System.nanoTime()), false)))
        .isEqualTo("SAGA_NOT_FOUND");
  }

  @Test
  void theRecoveryEndpointsRequireTheAdminRole() {
    client
        .post()
        .uri("/api/admin/saga-dead-letters/replay-all?sagaId=fulfillment-any")
        .exchange()
        .expectStatus()
        .isForbidden();
    client
        .post()
        .uri("/api/admin/sagas/faulted/resume?sagaId=fulfillment-any")
        .exchange()
        .expectStatus()
        .isForbidden();
    client
        .post()
        .uri("/api/admin/saga-dead-letters/replay?sagaId=fulfillment-any&eventOffset=1&force=true")
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  /**
   * Seeds a compensation the retry sweeper gave up on: a running saga awaiting payment is claimed
   * for compensation and then faulted, so the row is FAULTED out of COMPENSATING with no
   * dead-letter entry. Its compensation is a single {@code CancelOrder} for the order.
   */
  private SagaId seedFaultedCompensation(String orderId) {
    SagaId sagaId = new SagaId("fulfillment-" + orderId);
    SagaType type = SagaType.fromClass(OrderFulfillmentState.class);
    OrderFulfillmentState state =
        new OrderFulfillmentState(
            sagaId,
            orderId,
            "cust-" + orderId,
            "pay-" + orderId,
            new Money(new BigDecimal("19.99"), "USD"),
            List.of(),
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    sagaStore.create(sagaId, type, state, SagaStatus.RUNNING);
    sagaStore.claimCompensating(sagaId, type, state, 1L);
    assertThat(sagaStore.markFaulted(sagaId, type, 2L)).isTrue();
    return sagaId;
  }

  private String replayUri(SagaId sagaId, GlobalOffset offset, boolean force) {
    return "/api/admin/saga-dead-letters/replay?sagaId="
        + sagaId.value()
        + "&eventOffset="
        + offset.value()
        + "&force="
        + force;
  }

  private String resumeUri(SagaId sagaId, boolean force) {
    return "/api/admin/sagas/faulted/resume?sagaId=" + sagaId.value() + "&force=" + force;
  }

  private String postForOutcome(String uri) {
    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .post()
            .uri(uri)
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();
    assertThat(body).isNotNull();
    return (String) body.get("outcome");
  }

  private List<String> eventTypes(AggregateType type, String id) {
    return eventStore
        .readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 100)
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
