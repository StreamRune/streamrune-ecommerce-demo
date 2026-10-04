package org.streamrune.ecommerce.spring.controller;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.spring.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.OutboxFailedReplayer;
import org.streamrune.runtime.SagaDeadLetterReplayer;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

  private final CircuitBreakerCommandInterceptor circuitBreaker;
  private final PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker;
  private final PaymentGatewaySimulator paymentGateway;
  private final DeadLetterQueue deadLetterQueue;
  private final DeadLetterRetryRunner deadLetterRetryRunner;
  private final OutboxStore outboxStore;
  private final SagaDeadLetterStore sagaDeadLetterStore;
  private final SagaStore sagaStore;
  private final SagaDeadLetterReplayer sagaDeadLetterReplayer;
  private final ObjectProvider<OutboxFailedReplayer> outboxFailedReplayer;

  public AdminController(
      CircuitBreakerCommandInterceptor circuitBreaker,
      PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker,
      PaymentGatewaySimulator paymentGateway,
      DeadLetterQueue deadLetterQueue,
      DeadLetterRetryRunner deadLetterRetryRunner,
      OutboxStore outboxStore,
      SagaDeadLetterStore sagaDeadLetterStore,
      SagaStore sagaStore,
      SagaDeadLetterReplayer sagaDeadLetterReplayer,
      ObjectProvider<OutboxFailedReplayer> outboxFailedReplayer) {
    this.circuitBreaker = circuitBreaker;
    this.paymentGatewayCircuitBreaker = paymentGatewayCircuitBreaker;
    this.paymentGateway = paymentGateway;
    this.deadLetterQueue = deadLetterQueue;
    this.deadLetterRetryRunner = deadLetterRetryRunner;
    this.outboxStore = outboxStore;
    this.sagaDeadLetterStore = sagaDeadLetterStore;
    this.sagaStore = sagaStore;
    this.sagaDeadLetterReplayer = sagaDeadLetterReplayer;
    this.outboxFailedReplayer = outboxFailedReplayer;
  }

  @GetMapping("/circuit-breaker")
  public Map<String, Object> circuitBreakerState() {
    // "state" = the command-bus breaker; "paymentGateway" = the breaker around the payment gateway
    // call, which is the one the payment-failure toggle exercises.
    return Map.of(
        "state", circuitBreaker.circuitState(),
        "paymentGateway", paymentGatewayCircuitBreaker.state().name());
  }

  @GetMapping("/dead-letters")
  public List<DeadLetterQueue.DeadLetterEntry> listDeadLetters(
      @RequestParam(defaultValue = "50") int limit) {
    return deadLetterQueue.read(limit);
  }

  @PostMapping("/dead-letters/{id}/retry")
  public ResponseEntity<Void> retryDeadLetter(@PathVariable String id) {
    requireRole("ADMIN");
    // Re-dispatch the dead-lettered command through the bus (under DLQ_REPLAY + its original
    // request context); discarded on success, attempt count bumped on failure. 404 if no entry.
    boolean retried = deadLetterRetryRunner.retry(new CommandId(id));
    return retried ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
  }

  /** Read-only listing: findByStatus never claims; loadPending did — it stalled delivery. */
  @GetMapping("/outbox")
  public List<OutboxEntry> listOutboxEntries(@RequestParam(defaultValue = "50") int limit) {
    return outboxStore.findByStatus(OutboxStatus.PENDING, limit);
  }

  /**
   * Unresolved FAILED entries, oldest first. On this STRICT channel each one blocks its aggregate.
   */
  @GetMapping("/outbox/failed")
  public List<OutboxEntry> listFailedOutboxEntries(@RequestParam(defaultValue = "50") int limit) {
    return outboxStore.findByStatus(OutboxStatus.FAILED, limit);
  }

  /**
   * Replays one FAILED entry to PENDING at its original position (delivered before its successors).
   */
  @PostMapping("/outbox/failed/{id}/replay")
  public Map<String, String> replayFailedOutboxEntry(@PathVariable String id) {
    requireRole("ADMIN");
    var outcome = replayer().replay(OutboxEntryId.of(id));
    return Map.of("outcome", outcome.name());
  }

  public record SkipRequest(String reason) {}

  /**
   * Skips one FAILED entry: it will never be delivered; the row records who, when and why, and the
   * aggregate's later entries are claimable on the next poll. {@code skippedBy} is the request
   * identity the framework filter resolved ({@code X-User-Id} in this demo's trusted-gateway mode)
   * — never a header this controller reads itself; 400 when no identity is bound.
   */
  @PostMapping("/outbox/failed/{id}/skip")
  public Map<String, String> skipFailedOutboxEntry(
      @PathVariable String id, @RequestBody SkipRequest request) {
    requireRole("ADMIN");
    String by = currentUserId();
    if (by == null) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "no request identity (X-User-Id)");
    }
    if (request.reason() == null || request.reason().isBlank()) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "reason is required");
    }
    var outcome = replayer().skip(OutboxEntryId.of(id), by, request.reason());
    return Map.of("outcome", outcome.name());
  }

  private OutboxFailedReplayer replayer() {
    OutboxFailedReplayer r = outboxFailedReplayer.getIfAvailable();
    if (r == null) {
      throw new ResponseStatusException(
          HttpStatus.SERVICE_UNAVAILABLE, "outbox disabled (streamrune.outbox.enabled=false)");
    }
    return r;
  }

  /**
   * Request identity from the bound StreamRune context; null when unbound (same as
   * CustomerCommandController).
   */
  private static String currentUserId() {
    if (StreamRuneContext.CURRENT.isBound()) {
      var ctx = StreamRuneContext.CURRENT.get();
      if (ctx != null && ctx.userId() != null) {
        return ctx.userId().value();
      }
    }
    return null;
  }

  /**
   * DEMO SHORTCUT, the gateway stand-in ch. 9 describes: the role is whatever the client sent in
   * X-User-Role (copied into the context baggage by the framework filter). It is checked here so
   * the tutorial does not model an admin surface anyone can call; a real deployment authenticates.
   * Package-private because {@code SagaController}'s inject-failure route flips the same
   * payment-failure flag as {@link #togglePaymentFailure()} and must apply the same check.
   */
  static void requireRole(String role) {
    String current = "GUEST";
    if (StreamRuneContext.CURRENT.isBound() && StreamRuneContext.CURRENT.get() != null) {
      current = StreamRuneContext.CURRENT.get().baggage().getOrDefault("role", "GUEST");
    }
    if (!role.equals(current)) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Required role: " + role);
    }
  }

  /**
   * Lists quarantined saga dead-letter entries (poison events that could not be processed by the
   * order-fulfillment saga — routing failure, extract/correlate errors, decider exceptions). Mapped
   * to a JSON-friendly shape since {@code SagaDeadLetterEntry} carries value-object fields; the
   * offending event's payload is never included here — see {@code eventOffset} and read {@code
   * event_stream} directly if inspection is needed (see {@link SagaDeadLetterStore} javadoc).
   */
  @GetMapping("/saga-dead-letters")
  public List<Map<String, Object>> listSagaDeadLetters(
      @RequestParam(defaultValue = "50") int limit) {
    return sagaDeadLetterStore.findAll(limit).stream().map(this::toJson).toList();
  }

  /** Maps a {@code SagaDeadLetterEntry} to a JSON-friendly shape for the admin API. */
  private Map<String, Object> toJson(SagaDeadLetterStore.SagaDeadLetterEntry entry) {
    var json = new LinkedHashMap<String, Object>();
    json.put("sagaId", entry.sagaId() == null ? null : entry.sagaId().value());
    json.put("sagaType", entry.sagaType().value());
    json.put("eventOffset", entry.eventOffset().value());
    json.put("eventType", entry.eventType().name());
    json.put("errorType", entry.errorType());
    json.put("errorMessage", entry.errorMessage());
    json.put("faultedAt", entry.faultedAt().toString());
    return json;
  }

  /**
   * Lists the ids of order-fulfillment sagas in {@code FAULTED} status, newest first. That covers
   * both kinds of fault: a saga a poison event faulted, which owns dead-letter entries ({@code
   * /saga-dead-letters/replay-all} drains them), and a compensation faulted without an entry — the
   * compensation retry sweeper gave up on it ({@code /sagas/faulted/resume} resumes it).
   */
  @GetMapping("/sagas/faulted")
  public List<String> listFaultedSagas(@RequestParam(defaultValue = "50") int limit) {
    return sagaStore
        .findByStatus(SagaType.fromClass(OrderFulfillmentState.class), SagaStatus.FAULTED, limit)
        .stream()
        .map(SagaId::value)
        .toList();
  }

  /**
   * Replays one quarantined saga dead-letter entry through {@link SagaDeadLetterReplayer#replay}:
   * the event is fed through the saga runner's normal step, against the saga row as it stands. The
   * replayer never un-faults a saga itself — a {@code FAULTED} row is cleared only by the fed
   * step's own successful write. Call it once the cause of the poison (an orchestrator bug) is
   * fixed and deployed; {@code /saga-dead-letters/replay-all} is the usual way to drain a saga,
   * because a single replay is refused while an older entry of the same saga is pending.
   *
   * <p>The response is {@code {"outcome": "<ReplayOutcome>"}}, any of the replayer's outcomes:
   * {@code REPLAYED}, {@code STILL_POISON}, {@code ENTRY_NOT_FOUND}, {@code EVENT_NOT_FOUND},
   * {@code STALE_REDRIVE_BLOCKED}, {@code STALE_COMPENSATION_BLOCKED}, {@code TARGET_PENDING},
   * {@code SAGA_ROW_PENDING} or {@code OLDER_ENTRY_PENDING}.
   *
   * @param sagaId the saga the entry is quarantined under, or omitted for an entry whose saga could
   *     never be identified
   * @param eventOffset the quarantined event's global offset
   * @param force lifts only the two key-age refusals, {@code STALE_REDRIVE_BLOCKED} and {@code
   *     STALE_COMPENSATION_BLOCKED}. Send it only after checking which of the saga's commands
   *     already executed: a forced replay may run a command whose deduplication key has expired a
   *     second time.
   */
  @PostMapping("/saga-dead-letters/replay")
  public Map<String, String> replaySagaDeadLetter(
      @RequestParam(required = false) String sagaId,
      @RequestParam long eventOffset,
      @RequestParam(defaultValue = "false") boolean force) {
    requireRole("ADMIN");
    var outcome =
        sagaDeadLetterReplayer.replay(
            sagaId == null ? null : SagaId.of(sagaId), GlobalOffset.of(eventOffset), force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Drains every quarantined entry of one saga through {@link SagaDeadLetterReplayer#replayAll},
   * oldest event first, and stops at the first entry it cannot feed (nothing is fed ahead of it).
   * The response lists one outcome per attempt in attempt order, as {@code {"outcomes": [...]}}; a
   * deferred entry that is fed later in the same drain appears twice. {@code force} has the meaning
   * it has for a single replay, for every entry of the drain.
   */
  @PostMapping("/saga-dead-letters/replay-all")
  public Map<String, List<String>> replayAllSagaDeadLetters(
      @RequestParam String sagaId, @RequestParam(defaultValue = "false") boolean force) {
    requireRole("ADMIN");
    var outcomes = sagaDeadLetterReplayer.replayAll(SagaId.of(sagaId), force);
    return Map.of("outcomes", outcomes.stream().map(Enum::name).toList());
  }

  /**
   * Resumes a compensation that was faulted without a dead-letter entry, through {@link
   * SagaDeadLetterReplayer#resumeFaulted}. The compensation retry sweeper faults a compensation it
   * has re-driven past {@code streamrune.saga.compensation-retry-give-up-after}; such a saga shows
   * in {@code GET /sagas/faulted} but has nothing to replay. The resume re-dispatches the
   * compensation under its original keys (a compensation that already executed deduplicates) and
   * only its final write clears the fault.
   *
   * <p>The response is {@code {"outcome": "<ResumeOutcome>"}}: {@code RESUMED}, {@code
   * STILL_POISON}, {@code NO_PROGRESS}, {@code STALE_COMPENSATION_BLOCKED}, {@code SAGA_NOT_FOUND},
   * {@code NOT_FAULTED}, {@code FORWARD_FAULT} (use {@code replay-all}) or {@code ENTRIES_PENDING}
   * (use {@code replay-all}). {@code force} lifts only {@code STALE_COMPENSATION_BLOCKED}, after
   * the operator has checked which compensations already executed.
   */
  @PostMapping("/sagas/faulted/resume")
  public Map<String, String> resumeFaultedSaga(
      @RequestParam String sagaId, @RequestParam(defaultValue = "false") boolean force) {
    requireRole("ADMIN");
    var outcome = sagaDeadLetterReplayer.resumeFaulted(SagaId.of(sagaId), force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Discards a quarantined saga dead-letter entry without replaying it (e.g. an operator has
   * manually resolved it out-of-band) via {@link SagaDeadLetterReplayer#discard}: the removal is
   * scoped to the order-fulfillment saga type, and once the saga's backlog is empty the replayer
   * releases its dead-letter shield, so the saga's live events are no longer held. Omit {@code
   * sagaId} for a correlate-poison entry (the saga could never be identified); a {@code null} saga
   * id matches only such entries.
   */
  @DeleteMapping("/saga-dead-letters")
  public Map<String, Boolean> discardSagaDeadLetter(
      @RequestParam(required = false) String sagaId, @RequestParam long eventOffset) {
    requireRole("ADMIN");
    boolean discarded =
        sagaDeadLetterReplayer.discard(
            sagaId == null ? null : SagaId.of(sagaId), GlobalOffset.of(eventOffset));
    return Map.of("discarded", discarded);
  }

  @PostMapping("/payment-failure/toggle")
  public Map<String, Boolean> togglePaymentFailure() {
    requireRole("ADMIN");
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }

  @GetMapping("/payment-failure")
  public Map<String, Boolean> paymentFailureState() {
    return Map.of("failureInjected", paymentGateway.isFailureInjected());
  }

  @GetMapping("/health")
  public Map<String, String> health() {
    return Map.of("status", "UP");
  }
}
