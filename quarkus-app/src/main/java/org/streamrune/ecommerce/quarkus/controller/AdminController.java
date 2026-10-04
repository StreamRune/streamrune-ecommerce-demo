package org.streamrune.ecommerce.quarkus.controller;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.streamrune.ecommerce.quarkus.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.quarkus.config.PaymentGatewaySimulator;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.OutboxFailedReplayer;
import org.streamrune.runtime.SagaDeadLetterReplayer;

@Path("/api/admin")
@Produces(MediaType.APPLICATION_JSON)
public class AdminController {

  @Inject CircuitBreakerCommandInterceptor circuitBreaker;
  @Inject PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject DeadLetterQueue deadLetterQueue;
  @Inject DeadLetterRetryRunner deadLetterRetryRunner;
  @Inject OutboxStore outboxStore;
  @Inject SagaDeadLetterStore sagaDeadLetterStore;
  @Inject SagaStore sagaStore;
  @Inject SagaDeadLetterReplayer sagaDeadLetterReplayer;

  /**
   * The framework producer is {@code @Dependent} and returns {@code null} while the outbox is
   * disabled ({@code streamrune.outbox.enabled=false}, as under {@code %test}); see {@link
   * #replayer()}.
   */
  @Inject Instance<OutboxFailedReplayer> outboxFailedReplayer;

  /**
   * The request context the StreamRune filter parsed from the headers. A JAX-RS filter cannot bind
   * {@code StreamRuneContext.CURRENT} around the resource method, so identity and role are read
   * from this holder (as {@code CustomerCommandController} does).
   */
  @Inject StreamRuneRequestContextHolder requestContext;

  @GET
  @Path("/circuit-breaker")
  public Map<String, Object> circuitBreakerState() {
    return Map.of(
        "state", circuitBreaker.circuitState(),
        "paymentGateway", paymentGatewayCircuitBreaker.state().name());
  }

  @GET
  @Path("/dead-letters")
  public List<DeadLetterQueue.DeadLetterEntry> listDeadLetters(
      @QueryParam("limit") @DefaultValue("50") int limit) {
    return deadLetterQueue.read(limit);
  }

  @POST
  @Path("/dead-letters/{id}/retry")
  public Response retryDeadLetter(@PathParam("id") String id) {
    requireRole("ADMIN");
    boolean retried = deadLetterRetryRunner.retry(new CommandId(id));
    return retried ? Response.ok().build() : Response.status(Response.Status.NOT_FOUND).build();
  }

  /** Read-only listing: findByStatus never claims; loadPending did — it stalled delivery. */
  @GET
  @Path("/outbox")
  public List<OutboxEntry> listOutboxEntries(@QueryParam("limit") @DefaultValue("50") int limit) {
    return outboxStore.findByStatus(OutboxStatus.PENDING, limit);
  }

  /**
   * Unresolved FAILED entries, oldest first. On this STRICT channel each one blocks its aggregate.
   */
  @GET
  @Path("/outbox/failed")
  public List<OutboxEntry> listFailedOutboxEntries(
      @QueryParam("limit") @DefaultValue("50") int limit) {
    return outboxStore.findByStatus(OutboxStatus.FAILED, limit);
  }

  /**
   * Replays one FAILED entry to PENDING at its original position (delivered before its successors).
   */
  @POST
  @Path("/outbox/failed/{id}/replay")
  public Map<String, String> replayFailedOutboxEntry(@PathParam("id") String id) {
    requireRole("ADMIN");
    var outcome = replayer().replay(OutboxEntryId.of(id));
    return Map.of("outcome", outcome.name());
  }

  public record SkipRequest(String reason) {}

  /**
   * Skips one FAILED entry: it will never be delivered; the row records who, when and why, and the
   * aggregate's later entries are claimable on the next poll. {@code skippedBy} is the request
   * identity the framework filter resolved ({@code X-User-Id} in this demo's trusted-gateway mode)
   * — never a header this controller reads itself; 400 when no identity is bound. JAX-RS hands a
   * missing or JSON-null body to the method as {@code null}, so the reason check covers it.
   */
  @POST
  @Path("/outbox/failed/{id}/skip")
  @Consumes(MediaType.APPLICATION_JSON)
  public Map<String, String> skipFailedOutboxEntry(
      @PathParam("id") String id, SkipRequest request) {
    requireRole("ADMIN");
    String by = currentUserId();
    if (by == null) {
      throw new WebApplicationException("no request identity (X-User-Id)", 400);
    }
    if (request == null || request.reason() == null || request.reason().isBlank()) {
      throw new WebApplicationException("reason is required", 400);
    }
    var outcome = replayer().skip(OutboxEntryId.of(id), by, request.reason());
    return Map.of("outcome", outcome.name());
  }

  private OutboxFailedReplayer replayer() {
    OutboxFailedReplayer r =
        outboxFailedReplayer.isResolvable() ? outboxFailedReplayer.get() : null;
    if (r == null) {
      throw new WebApplicationException("outbox disabled (streamrune.outbox.enabled=false)", 503);
    }
    return r;
  }

  /**
   * The request context: the filter's request-scoped holder first; {@code
   * StreamRuneContext.CURRENT} when bound; else {@code null}.
   */
  private StreamRuneContext.RequestContext context() {
    return context(requestContext);
  }

  private static StreamRuneContext.RequestContext context(StreamRuneRequestContextHolder holder) {
    var ctx = holder != null ? holder.context() : null;
    if (ctx == null && StreamRuneContext.CURRENT.isBound()) {
      ctx = StreamRuneContext.CURRENT.get();
    }
    return ctx;
  }

  /** Request identity; null when none is bound (same as CustomerCommandController). */
  private String currentUserId() {
    var ctx = context();
    return ctx != null && ctx.userId() != null ? ctx.userId().value() : null;
  }

  /**
   * DEMO SHORTCUT, the gateway stand-in ch. 9 describes: the role is whatever the client sent in
   * X-User-Role (copied into the context baggage by the framework filter). It is checked here so
   * the tutorial does not model an admin surface anyone can call; a real deployment authenticates.
   */
  private void requireRole(String role) {
    requireRole(requestContext, role);
  }

  /**
   * {@link #requireRole(String)} against the given request-context holder. Package-private because
   * {@code SagaController}'s inject-failure route flips the same payment-failure flag as {@link
   * #togglePaymentFailure()} and must apply the same check.
   */
  static void requireRole(StreamRuneRequestContextHolder holder, String role) {
    var ctx = context(holder);
    String current = ctx == null ? "GUEST" : ctx.baggage().getOrDefault("role", "GUEST");
    if (!role.equals(current)) {
      throw new WebApplicationException("Required role: " + role, 403);
    }
  }

  /**
   * Lists quarantined saga dead-letter entries, newest first. The offending event's payload is
   * never included — read {@code event_stream} at {@code eventOffset} to inspect it.
   */
  @GET
  @Path("/saga-dead-letters")
  public List<Map<String, Object>> listSagaDeadLetters(
      @QueryParam("limit") @DefaultValue("50") int limit) {
    return sagaDeadLetterStore.findAll(limit).stream().map(AdminController::toJson).toList();
  }

  private static Map<String, Object> toJson(SagaDeadLetterStore.SagaDeadLetterEntry entry) {
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
  @GET
  @Path("/sagas/faulted")
  public List<String> listFaultedSagas(@QueryParam("limit") @DefaultValue("50") int limit) {
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
  @POST
  @Path("/saga-dead-letters/replay")
  public Map<String, String> replaySagaDeadLetter(
      @QueryParam("sagaId") String sagaId,
      @QueryParam("eventOffset") Long eventOffset,
      @QueryParam("force") @DefaultValue("false") boolean force) {
    requireRole("ADMIN");
    if (eventOffset == null) {
      throw new WebApplicationException("eventOffset is required", 400);
    }
    var outcome =
        sagaDeadLetterReplayer.replay(
            sagaId == null ? null : SagaId.of(sagaId), GlobalOffset.of(eventOffset), force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Drains every quarantined entry of one saga through {@link SagaDeadLetterReplayer#replayAll},
   * oldest event first, stopping at the first entry it cannot feed; one outcome per attempt.
   */
  @POST
  @Path("/saga-dead-letters/replay-all")
  public Map<String, List<String>> replayAllSagaDeadLetters(
      @QueryParam("sagaId") String sagaId,
      @QueryParam("force") @DefaultValue("false") boolean force) {
    requireRole("ADMIN");
    var outcomes = sagaDeadLetterReplayer.replayAll(requiredSagaId(sagaId), force);
    return Map.of("outcomes", outcomes.stream().map(Enum::name).toList());
  }

  /**
   * Resumes a compensation faulted without a dead-letter entry — the compensation retry sweeper
   * gave up on it — through {@link SagaDeadLetterReplayer#resumeFaulted}. {@code force} lifts only
   * {@code STALE_COMPENSATION_BLOCKED}.
   */
  @POST
  @Path("/sagas/faulted/resume")
  public Map<String, String> resumeFaultedSaga(
      @QueryParam("sagaId") String sagaId,
      @QueryParam("force") @DefaultValue("false") boolean force) {
    requireRole("ADMIN");
    var outcome = sagaDeadLetterReplayer.resumeFaulted(requiredSagaId(sagaId), force);
    return Map.of("outcome", outcome.name());
  }

  /**
   * Discards a quarantined entry without replaying it, through {@link
   * SagaDeadLetterReplayer#discard}, which also releases the saga's dead-letter shield once the
   * saga has no entry left. Omit {@code sagaId} for an entry whose saga could never be identified.
   */
  @DELETE
  @Path("/saga-dead-letters")
  public Map<String, Boolean> discardSagaDeadLetter(
      @QueryParam("sagaId") String sagaId, @QueryParam("eventOffset") Long eventOffset) {
    requireRole("ADMIN");
    if (eventOffset == null) {
      throw new WebApplicationException("eventOffset is required", 400);
    }
    boolean discarded =
        sagaDeadLetterReplayer.discard(
            sagaId == null ? null : SagaId.of(sagaId), GlobalOffset.of(eventOffset));
    return Map.of("discarded", discarded);
  }

  private static SagaId requiredSagaId(String sagaId) {
    if (sagaId == null) {
      throw new WebApplicationException("sagaId is required", 400);
    }
    return SagaId.of(sagaId);
  }

  @POST
  @Path("/payment-failure/toggle")
  public Map<String, Boolean> togglePaymentFailure() {
    requireRole("ADMIN");
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }

  @GET
  @Path("/payment-failure")
  public Map<String, Boolean> paymentFailureState() {
    return Map.of("failureInjected", paymentGateway.isFailureInjected());
  }

  @GET
  @Path("/health")
  public Map<String, String> health() {
    return Map.of("status", "UP");
  }
}
