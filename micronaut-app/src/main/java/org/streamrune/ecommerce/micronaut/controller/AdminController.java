package org.streamrune.ecommerce.micronaut.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.streamrune.ecommerce.micronaut.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.micronaut.config.PaymentGatewaySimulator;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.OutboxFailedReplayer;
import org.streamrune.runtime.SagaDeadLetterReplayer;

@Controller("/api/admin")
public class AdminController {

  @Inject CircuitBreakerCommandInterceptor circuitBreaker;
  @Inject PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject DeadLetterQueue deadLetterQueue;
  @Inject DeadLetterRetryRunner deadLetterRetryRunner;
  @Inject OutboxStore outboxStore;
  @Inject ObjectMapper objectMapper;
  @Inject SagaDeadLetterStore sagaDeadLetterStore;
  @Inject SagaStore sagaStore;
  @Inject SagaDeadLetterReplayer sagaDeadLetterReplayer;
  @Inject PostgresAuditStore auditStore;

  /**
   * The framework factory is {@code @Requires(property = "streamrune.outbox.enabled", value =
   * "true")}, so the bean is absent while the outbox is disabled (as in the integration tests); see
   * {@link #replayer()}.
   */
  @Inject @Nullable OutboxFailedReplayer outboxFailedReplayer;

  /** Open to every caller: the answer says that the application runs and nothing else. */
  @Get("/health")
  public Map<String, String> health() {
    return Map.of("status", "UP");
  }

  @Get("/circuit-breaker")
  public Map<String, Object> circuitBreakerState() {
    requireAdmin();
    return Map.of(
        "state", circuitBreaker.circuitState(),
        "paymentGateway", paymentGatewayCircuitBreaker.state().name());
  }

  @Get("/dead-letters")
  public List<Map<String, Object>> listDeadLetters(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    requireAdmin();
    TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
    return deadLetterQueue.read(limit).stream()
        .map(entry -> objectMapper.convertValue(entry, typeRef))
        .toList();
  }

  @Post("/dead-letters/{id}/retry")
  public HttpResponse<Map<String, String>> retryDeadLetter(
      @PathVariable String id, @QueryValue(defaultValue = "false") boolean force) {
    requireAdmin();
    // A replay the inbox cannot deduplicate waits for the operator's ?force=true, and a forced
    // replay is audited.
    var commandId = new CommandId(id);
    var outcome = deadLetterRetryRunner.retry(commandId, force);
    if (force && outcome != DeadLetterRetryRunner.RetryOutcome.NOT_FOUND) {
      auditForcedReplay(commandId, outcome);
    }
    return switch (outcome) {
      case REPLAYED -> HttpResponse.ok(Map.of("outcome", outcome.name()));
      case NOT_FOUND -> HttpResponse.notFound();
      case NOT_DEDUPLICATED ->
          HttpResponse.<Map<String, String>>status(HttpStatus.CONFLICT)
              .body(
                  Map.of(
                      "outcome",
                      outcome.name(),
                      "message",
                      "The command ran without an idempotency key: if its caller already retried it, a replay applies it twice. Check that its effect is missing, then retry with ?force=true, or discard the entry."));
      case STALE_REPLAY_BLOCKED ->
          HttpResponse.<Map<String, String>>status(HttpStatus.CONFLICT)
              .body(
                  Map.of(
                      "outcome",
                      outcome.name(),
                      "message",
                      "The entry is older than the command-inbox retention window, so a replay may not be deduplicated. Check that its effect is missing, then retry with ?force=true, or discard the entry."));
      case ADMISSION_REFUSED ->
          HttpResponse.<Map<String, String>>status(HttpStatus.SERVICE_UNAVAILABLE)
              .header("Retry-After", "5")
              .body(Map.of("outcome", outcome.name()));
      case FAILED ->
          HttpResponse.<Map<String, String>>status(HttpStatus.INTERNAL_SERVER_ERROR)
              .body(Map.of("outcome", outcome.name()));
    };
  }

  /**
   * Records a forced replay in the audit log ({@code audit_log}, command type {@code
   * DLQ_FORCED_REPLAY}): the entry's command id, the operator who forced it, {@code SUCCESS} when
   * the command ran, otherwise {@code FAILURE} with the outcome. A forced replay overrides the
   * runner's refusal to run a command the inbox cannot deduplicate, so the decision is the
   * operator's and is kept beside the command's own audit rows.
   */
  private void auditForcedReplay(CommandId commandId, DeadLetterRetryRunner.RetryOutcome outcome) {
    var caller = RequestCaller.current();
    boolean replayed = outcome == DeadLetterRetryRunner.RetryOutcome.REPLAYED;
    auditStore.save(
        new AuditEntry(
            commandId,
            "DLQ_FORCED_REPLAY",
            null,
            null,
            caller.userId(),
            Instant.now(),
            replayed ? AuditOutcome.SUCCESS : AuditOutcome.FAILURE,
            replayed ? null : outcome.name(),
            0,
            caller.correlationId()));
  }

  /** Read-only listing: findByStatus never claims; loadPending did — it stalled delivery. */
  @Get("/outbox")
  public List<Map<String, Object>> listOutboxEntries(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    requireAdmin();
    return toJson(outboxStore.findByStatus(OutboxStatus.PENDING, limit));
  }

  /**
   * Unresolved FAILED entries, oldest first. On this STRICT channel each one blocks its aggregate.
   */
  @Get("/outbox/failed")
  public List<Map<String, Object>> listFailedOutboxEntries(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    requireAdmin();
    return toJson(outboxStore.findByStatus(OutboxStatus.FAILED, limit));
  }

  /**
   * Replays one FAILED entry to PENDING at its original position (delivered before its successors).
   */
  @Post("/outbox/failed/{id}/replay")
  public Map<String, String> replayFailedOutboxEntry(@PathVariable String id) {
    requireAdmin();
    var outcome = replayer().replay(OutboxEntryId.of(id));
    return Map.of("outcome", outcome.name());
  }

  /**
   * {@code @Serdeable}, as every request body in this app: micronaut-serde decodes only types it
   * generated a deserializer for — a bare {@code @Introspected} record fails with a 500.
   */
  @Serdeable
  public record SkipRequest(String reason) {}

  /**
   * Skips one FAILED entry: it will never be delivered; the row records who, when and why, and the
   * aggregate's later entries are claimable on the next poll. {@code skippedBy} is the request
   * identity the framework filter resolved ({@code X-User-Id} in this demo's trusted-gateway mode)
   * — never a header this controller reads itself. An ADMIN always has one: {@code requireAdmin()}
   * refuses a caller without an identity. The {@code @Body} is required, so Micronaut answers 400
   * for a missing or JSON-null body before this method runs.
   */
  @Post("/outbox/failed/{id}/skip")
  public Map<String, String> skipFailedOutboxEntry(
      @PathVariable String id, @Body SkipRequest request) {
    requireAdmin();
    String by = currentUserId();
    if (request.reason() == null || request.reason().isBlank()) {
      throw new HttpStatusException(HttpStatus.BAD_REQUEST, "reason is required");
    }
    var outcome = replayer().skip(OutboxEntryId.of(id), by, request.reason());
    return Map.of("outcome", outcome.name());
  }

  private List<Map<String, Object>> toJson(List<OutboxEntry> entries) {
    TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
    return entries.stream().map(entry -> objectMapper.convertValue(entry, typeRef)).toList();
  }

  private OutboxFailedReplayer replayer() {
    if (outboxFailedReplayer == null) {
      throw new HttpStatusException(
          HttpStatus.SERVICE_UNAVAILABLE, "outbox disabled (streamrune.outbox.enabled=false)");
    }
    return outboxFailedReplayer;
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
   * Lists quarantined saga dead-letter entries, newest first. The offending event's payload is
   * never included — read {@code event_stream} at {@code eventOffset} to inspect it.
   */
  @Get("/saga-dead-letters")
  public List<Map<String, Object>> listSagaDeadLetters(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    requireAdmin();
    return sagaDeadLetterStore.findAll(limit).stream().map(AdminController::sagaEntryJson).toList();
  }

  private static Map<String, Object> sagaEntryJson(SagaDeadLetterStore.SagaDeadLetterEntry entry) {
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
   * Lists the ids of order-fulfillment sagas in {@code FAULTED} status, newest first: sagas a
   * poison event faulted ({@code /saga-dead-letters/replay-all} drains their entries) and
   * compensations faulted without an entry ({@code /sagas/faulted/resume} resumes them).
   */
  @Get("/sagas/faulted")
  public List<String> listFaultedSagas(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    requireAdmin();
    return sagaStore
        .findByStatus(SagaType.fromClass(OrderFulfillmentState.class), SagaStatus.FAULTED, limit)
        .stream()
        .map(SagaId::value)
        .toList();
  }

  /**
   * Replays one quarantined entry through {@link SagaDeadLetterReplayer#replay}. The replayer never
   * un-faults a saga itself: a {@code FAULTED} row is cleared only by the fed step's own successful
   * write. A single replay is refused ({@code OLDER_ENTRY_PENDING}) while an older entry of the
   * saga is pending; {@code /saga-dead-letters/replay-all} drains a saga. {@code force} lifts only
   * the two key-age refusals ({@code STALE_REDRIVE_BLOCKED}, {@code STALE_COMPENSATION_BLOCKED}),
   * after the operator has checked which commands already executed. Omit {@code sagaId} for an
   * entry whose saga could never be identified.
   */
  @Post("/saga-dead-letters/replay")
  public Map<String, String> replaySagaDeadLetter(
      @QueryValue("sagaId") @Nullable String sagaId,
      @QueryValue("eventOffset") @Nullable Long eventOffset,
      @QueryValue(value = "force", defaultValue = "false") boolean force) {
    GlobalOffset offset = requiredOffset(eventOffset);
    requireAdmin();
    var outcome =
        sagaDeadLetterReplayer.replay(sagaId == null ? null : SagaId.of(sagaId), offset, force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Drains every quarantined entry of one saga through {@link SagaDeadLetterReplayer#replayAll},
   * oldest event first, stopping at the first entry it cannot feed; one outcome per attempt.
   */
  @Post("/saga-dead-letters/replay-all")
  public Map<String, List<String>> replayAllSagaDeadLetters(
      @QueryValue("sagaId") @Nullable String sagaId,
      @QueryValue(value = "force", defaultValue = "false") boolean force) {
    SagaId saga = requiredSagaId(sagaId);
    requireAdmin();
    var outcomes = sagaDeadLetterReplayer.replayAll(saga, force);
    return Map.of("outcomes", outcomes.stream().map(Enum::name).toList());
  }

  /**
   * Resumes a compensation faulted without a dead-letter entry — the compensation retry sweeper
   * gave up on it — through {@link SagaDeadLetterReplayer#resumeFaulted}. {@code force} lifts only
   * {@code STALE_COMPENSATION_BLOCKED}.
   */
  @Post("/sagas/faulted/resume")
  public Map<String, String> resumeFaultedSaga(
      @QueryValue("sagaId") @Nullable String sagaId,
      @QueryValue(value = "force", defaultValue = "false") boolean force) {
    SagaId saga = requiredSagaId(sagaId);
    requireAdmin();
    var outcome = sagaDeadLetterReplayer.resumeFaulted(saga, force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Discards a quarantined entry without replaying it, through {@link
   * SagaDeadLetterReplayer#discard}, which also releases the saga's dead-letter shield once the
   * saga has no entry left. Omit {@code sagaId} for an entry whose saga could never be identified.
   */
  @Delete("/saga-dead-letters")
  public Map<String, Boolean> discardSagaDeadLetter(
      @QueryValue("sagaId") @Nullable String sagaId,
      @QueryValue("eventOffset") @Nullable Long eventOffset) {
    GlobalOffset offset = requiredOffset(eventOffset);
    requireAdmin();
    boolean discarded =
        sagaDeadLetterReplayer.discard(sagaId == null ? null : SagaId.of(sagaId), offset);
    return Map.of("discarded", discarded);
  }

  private static SagaId requiredSagaId(String sagaId) {
    if (sagaId == null) {
      throw new HttpStatusException(HttpStatus.BAD_REQUEST, "sagaId is required");
    }
    return SagaId.of(sagaId);
  }

  private static GlobalOffset requiredOffset(Long eventOffset) {
    if (eventOffset == null) {
      throw new HttpStatusException(HttpStatus.BAD_REQUEST, "eventOffset is required");
    }
    return GlobalOffset.of(eventOffset);
  }

  @Post("/payment-failure/toggle")
  public Map<String, Boolean> togglePaymentFailure() {
    requireAdmin();
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }

  @Get("/payment-failure")
  public Map<String, Boolean> paymentFailureState() {
    requireAdmin();
    return Map.of("failureInjected", paymentGateway.isFailureInjected());
  }
}
