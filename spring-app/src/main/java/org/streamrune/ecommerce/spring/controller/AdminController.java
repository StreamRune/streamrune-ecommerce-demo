package org.streamrune.ecommerce.spring.controller;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
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
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.spring.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.OutboxFailedReplayer;
import org.streamrune.runtime.SagaDeadLetterReplayer;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

  /**
   * What the operator does with an entry the runner refused: look at the command's effect, then
   * replay it ({@code ?force=true}) or discard it ({@code DELETE /api/admin/dead-letters/{id}}).
   */
  private static final String REMEDY =
      " Check whether its effect is present: if it is missing, retry with ?force=true; if it is"
          + " present, discard the entry (DELETE /api/admin/dead-letters/{id}).";

  private static final String NOT_DEDUPLICATED_MESSAGE =
      "The command ran without an idempotency key: if its caller already retried it, a replay"
          + " applies it twice."
          + REMEDY;

  private static final String STALE_REPLAY_MESSAGE =
      "The entry is older than the command-inbox retention window, so a replay may not be"
          + " deduplicated."
          + REMEDY;

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
  private final PostgresAuditStore auditStore;

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
      ObjectProvider<OutboxFailedReplayer> outboxFailedReplayer,
      PostgresAuditStore auditStore) {
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
    this.auditStore = auditStore;
  }

  @GetMapping("/circuit-breaker")
  public Map<String, Object> circuitBreakerState() {
    requireAdmin();
    // "state" = the command-bus breaker; "paymentGateway" = the breaker around the payment gateway
    // call, which is the one the payment-failure toggle exercises.
    return Map.of(
        "state", circuitBreaker.circuitState(),
        "paymentGateway", paymentGatewayCircuitBreaker.state().name());
  }

  @GetMapping("/dead-letters")
  public List<DeadLetterQueue.DeadLetterEntry> listDeadLetters(
      @RequestParam(defaultValue = "50") int limit) {
    requireAdmin();
    return deadLetterQueue.read(limit);
  }

  @PostMapping("/dead-letters/{id}/retry")
  public ResponseEntity<Map<String, String>> retryDeadLetter(
      @PathVariable String id, @RequestParam(defaultValue = "false") boolean force) {
    requireAdmin();
    // Re-dispatch the dead-lettered command through the bus (under DLQ_REPLAY + its original
    // request context); discarded on success, attempt count bumped on failure. A replay the inbox
    // cannot deduplicate waits for the operator's ?force=true. A forced replay is audited first:
    // if the audit write fails, nothing is replayed.
    var commandId = new CommandId(id);
    if (force) {
      var entry = deadLetterQueue.find(commandId);
      if (entry.isEmpty()) {
        return ResponseEntity.notFound().build();
      }
      auditOperatorDecision("DLQ_FORCED_REPLAY", entry.get());
    }
    var outcome = deadLetterRetryRunner.retry(commandId, force);
    return switch (outcome) {
      case REPLAYED -> ResponseEntity.ok(Map.of("outcome", outcome.name()));
      case NOT_FOUND -> ResponseEntity.notFound().build();
      case NOT_DEDUPLICATED ->
          ResponseEntity.status(HttpStatus.CONFLICT)
              .body(Map.of("outcome", outcome.name(), "message", NOT_DEDUPLICATED_MESSAGE));
      case STALE_REPLAY_BLOCKED ->
          ResponseEntity.status(HttpStatus.CONFLICT)
              .body(Map.of("outcome", outcome.name(), "message", STALE_REPLAY_MESSAGE));
      case ADMISSION_REFUSED ->
          ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
              .header("Retry-After", "5")
              .body(Map.of("outcome", outcome.name()));
      case FAILED ->
          ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
              .body(Map.of("outcome", outcome.name()));
    };
  }

  /**
   * Discards a dead-letter entry whose effect is present: the command already ran (its caller
   * retried it), so a replay would apply it a second time. {@code 404} when no entry exists. The
   * decision is audited ({@code DLQ_DISCARDED}) before the entry is removed.
   */
  @DeleteMapping("/dead-letters/{id}")
  public ResponseEntity<Map<String, String>> discardDeadLetter(@PathVariable String id) {
    requireAdmin();
    var commandId = new CommandId(id);
    var entry = deadLetterQueue.find(commandId);
    if (entry.isEmpty()) {
      return ResponseEntity.notFound().build();
    }
    auditOperatorDecision("DLQ_DISCARDED", entry.get());
    deadLetterQueue.discard(commandId);
    return ResponseEntity.ok(Map.of("outcome", "DISCARDED"));
  }

  /**
   * Records an operator's decision about a dead-letter entry in the audit log ({@code audit_log},
   * beside the command's own rows): {@code DLQ_FORCED_REPLAY} or {@code DLQ_DISCARDED}, the entry's
   * command id and stream, and the operator. The row is written before the decision is acted on, so
   * it is the durable record of it: an audit write that fails throws, and nothing is replayed or
   * discarded. The outcome of a forced replay shows elsewhere — a replayed entry is gone and its
   * command has an audit row of its own, a failed one is still listed.
   */
  private void auditOperatorDecision(String decision, DeadLetterQueue.DeadLetterEntry entry) {
    var caller = RequestCaller.current();
    var stream = entry.streamId();
    auditStore.save(
        new AuditEntry(
            entry.commandId(),
            decision,
            stream == null ? null : stream.aggregateType(),
            stream == null ? null : stream.aggregateId(),
            caller.userId(),
            Instant.now(),
            AuditOutcome.SUCCESS,
            null,
            0,
            caller.correlationId()));
  }

  /** Read-only listing: findByStatus never claims; loadPending did — it stalled delivery. */
  @GetMapping("/outbox")
  public List<OutboxEntry> listOutboxEntries(@RequestParam(defaultValue = "50") int limit) {
    requireAdmin();
    return outboxStore.findByStatus(OutboxStatus.PENDING, limit);
  }

  /**
   * Unresolved FAILED entries, oldest first. On this STRICT channel each one blocks its aggregate.
   */
  @GetMapping("/outbox/failed")
  public List<OutboxEntry> listFailedOutboxEntries(@RequestParam(defaultValue = "50") int limit) {
    requireAdmin();
    return outboxStore.findByStatus(OutboxStatus.FAILED, limit);
  }

  /**
   * Replays one FAILED entry to PENDING at its original position (delivered before its successors).
   */
  @PostMapping("/outbox/failed/{id}/replay")
  public Map<String, String> replayFailedOutboxEntry(@PathVariable String id) {
    requireAdmin();
    var outcome = replayer().replay(OutboxEntryId.of(id));
    return Map.of("outcome", outcome.name());
  }

  public record SkipRequest(String reason) {}

  /**
   * Skips one FAILED entry: it will never be delivered; the row records who, when and why, and the
   * aggregate's later entries are claimable on the next poll. {@code skippedBy} is the request
   * identity the framework filter resolved ({@code X-User-Id} in this demo's trusted-gateway mode)
   * — never a header this controller reads itself. An ADMIN always has one: {@code requireAdmin()}
   * refuses a caller without an identity.
   */
  @PostMapping("/outbox/failed/{id}/skip")
  public Map<String, String> skipFailedOutboxEntry(
      @PathVariable String id, @RequestBody SkipRequest request) {
    requireAdmin();
    String by = currentUserId();
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

  /** The request identity: the ADMIN who is calling, once {@code requireAdmin()} has passed. */
  private static String currentUserId() {
    return OwnerOrAdminAccess.identityOf(RequestCaller.current());
  }

  /**
   * Answers {@code 403} unless the caller is an ADMIN: a caller with an identity whose request
   * carries the ADMIN role ({@code OwnerOrAdminAccess#isAdmin}, the demo's one definition of it).
   * Every route of this controller but {@code /health} asks: the reads show operational state and,
   * in the dead-letter and outbox lists, payloads of other people's commands and events.
   *
   * <p>DEMO SHORTCUT, the gateway stand-in ch. 9 describes: the identity and the role are whatever
   * the client sent in X-User-Id and X-User-Role (the framework filter puts them into the request
   * context). They are checked here so the tutorial does not model an admin surface anyone can
   * call; a real deployment authenticates.
   */
  private static void requireAdmin() {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
  }

  /**
   * Lists quarantined saga dead-letter entries (poison events that could not be processed by the
   * order-fulfillment saga — a routing method that throws ({@code extractSagaId}, {@code
   * correlateByEventData}) or an exception in the saga's own logic). Mapped to a JSON-friendly
   * shape since {@code SagaDeadLetterEntry} carries value-object fields; the offending event's
   * payload is never included here — see {@code eventOffset} and read {@code event_stream} directly
   * if inspection is needed (see {@link SagaDeadLetterStore} javadoc).
   */
  @GetMapping("/saga-dead-letters")
  public List<Map<String, Object>> listSagaDeadLetters(
      @RequestParam(defaultValue = "50") int limit) {
    requireAdmin();
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
    requireAdmin();
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
    requireAdmin();
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
    requireAdmin();
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
    requireAdmin();
    var outcome = sagaDeadLetterReplayer.resumeFaulted(SagaId.of(sagaId), force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Discards a quarantined saga dead-letter entry without replaying it (e.g. an operator has
   * manually resolved it out-of-band) via {@link SagaDeadLetterReplayer#discard}: the removal is
   * scoped to the order-fulfillment saga type, and once the saga's backlog is empty the replayer
   * releases its dead-letter shield, so the saga's live events are not held. Omit {@code sagaId}
   * for an entry whose saga could never be identified (a routing method threw); a {@code null} saga
   * id matches only such entries.
   */
  @DeleteMapping("/saga-dead-letters")
  public Map<String, Boolean> discardSagaDeadLetter(
      @RequestParam(required = false) String sagaId, @RequestParam long eventOffset) {
    requireAdmin();
    boolean discarded =
        sagaDeadLetterReplayer.discard(
            sagaId == null ? null : SagaId.of(sagaId), GlobalOffset.of(eventOffset));
    return Map.of("discarded", discarded);
  }

  @PostMapping("/payment-failure/toggle")
  public Map<String, Boolean> togglePaymentFailure() {
    requireAdmin();
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }

  @GetMapping("/payment-failure")
  public Map<String, Boolean> paymentFailureState() {
    requireAdmin();
    return Map.of("failureInjected", paymentGateway.isFailureInjected());
  }

  /** Open to every caller: the answer says that the application runs and nothing else. */
  @GetMapping("/health")
  public Map<String, String> health() {
    return Map.of("status", "UP");
  }
}
