# Chapter 14: Dead Letter Queue & Subscriptions

> **What you'll learn:** What to do when a projection fails to process an event, how to configure `ProjectionErrorStrategy.DLQ` per projection inside `MultiProjectionRunner` so that failed batches are durably stored instead of halting the system, and how `DeadLetterRetryRunner` automatically retries failed commands.

---

## What We're Building and Why

Production event-driven systems fail. A downstream database is temporarily unavailable, a deserialization assumption no longer holds, or a third-party API returns an unexpected error. The question is not whether failures happen — it is how the system behaves when they do.

StreamRune gives you three choices when a projection fails to process a batch of events. You select one via `ProjectionErrorStrategy`:

- **`HALT`** — the default. Stop the projection immediately and surface the error. Good for catching bugs in development, dangerous in production where a single bad event can freeze all projections.
- **`SKIP`** — log the failure and advance the offset past the failing batch. Guarantees liveness at the cost of potentially missing events silently.
- **`DLQ`** — write the failed batch to a `ProjectionDeadLetterStore` and continue. The projection stays live; the failed batch is recorded for later inspection and replay — a replay (`ProjectionDeadLetterReplayer`) is at-least-once and runs outside the checkpoint transaction for every delivery mode: it calls the one-argument `process`, a `BaseProjection` writes through its captured repository, possibly while the live runner commits later batches on the same rows, so `process` must tolerate re-application and an older event after a newer one. Check both before you replay into a projection: of the demo's read models, products, customers and orders tolerate re-application but not an older event after a newer one (a dead-lettered `OrderPlaced` replayed after the live runner has processed that order's `OrderConfirmed` — a no-op then, because the row did not exist — leaves the order `CREATED`), and `InventoryProjection`'s counters tolerate neither. This is the right choice for production.

In this chapter you will see how to configure `ProjectionErrorStrategy.DLQ` on individual projections inside the `MultiProjectionRunner` with `PostgresProjectionDeadLetterStore` as the persistent store (an optional hardening step: the demo's projections keep the default `HALT`), expose dead letter entries through `AdminController` (together with the outbox's blocked `FAILED` entries and their replay/skip operations, behind an ADMIN role check), look at the command-side `DeadLetterRetryRunner` that automatically retries failed commands, and wire the saga-side dead-letter admin endpoints (list/faulted/replay/discard) built on the `SagaDeadLetterReplayer` introduced in Chapter 11.

---

## Step by Step

### Step 1: Create the `PostgresProjectionDeadLetterStore` Bean (only with Step 4)

This bean is needed only for the optional per-projection DLQ wiring in Step 4. The demo's `StreamRuneConfig` does **not** declare it — its projections keep the default `HALT` strategy — so add it only if you follow Step 4. It uses the same `DataSource` as the event store. The `projection_dead_letters` table is part of the demo schema created by `scripts/init-db.sql` (the Docker initdb script) — the same file that creates `event_stream` and `snapshot_store`. (`init-db.sql` is regenerated from StreamRune's `V001__streamrune_baseline.sql` via `scripts/regenerate-init-db.sh`, which defines this table, but the demo sets `streamrune.event-store.schema.auto-initialize: false` and does not run Flyway at startup; see Chapter 1.)

```java
@Bean
public PostgresProjectionDeadLetterStore projectionDeadLetterStore(DataSource ds) {
    return new PostgresProjectionDeadLetterStore(ds);
}
```

### Step 2: Create the `PostgresDeadLetterQueue` Bean

The `DeadLetterQueue` is separate from `ProjectionDeadLetterStore`. The projection DLQ stores failed event batches for operator inspection. The `DeadLetterQueue` stores commands that exhausted all retry attempts after being dispatched through the `CommandBus`. Both are needed.

```java
@Bean
public PostgresDeadLetterQueue deadLetterQueue(DataSource ds) {
    return new PostgresDeadLetterQueue(ds);
}
```

The queue on its own records nothing: the command bus writes to it only when the bus is given it. Open the `commandBus` bean and add two parameters, the queue and the crypto engine from chapter 6, and two builder calls. Keep everything else as it is (the five `register(...)` calls are elided here):

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit,
    CircuitBreakerCommandInterceptor circuitBreaker,
    PostgresCommandInbox commandInbox,
    PostgresDeadLetterQueue deadLetterQueue,   // <-- added
    PostgresCryptoEngine cryptoEngine) {       // <-- added
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .commandInbox(commandInbox)
      .interceptors(audit, auth, validation, circuitBreaker)
      .deadLetterQueue(deadLetterQueue)                                      // <-- added
      .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))  // <-- added
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
      .register(/* ProductCommand ... */)      // keep all five register(...) calls
      .register(/* OrderCommand ... */)
      .register(/* CustomerCommand ... */)
      .register(/* PaymentCommand ... */)
      .register(/* InventoryCommand ... */)
      .build();
}
```

From now on a command that fails on infrastructure — a database or network error — is recorded in `dead_letter_queue` before the error reaches the caller. A business rejection is not: a `DomainException` (for example "Customer not found") or an `IllegalArgumentException` is the domain's final answer, and replaying it later would only repeat it. A command a saga sends is not recorded either; the saga handles its own failures (chapter 11).

The bus stores the command as JSON, written by the mapper you pass to `.objectMapper(...)`. `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)` registers the `CryptoShreddingModule`, so the `@Encrypted` fields of `RegisterCustomer` and `UpdateProfile` (chapter 6) are stored as ciphertext under the customer's key, and the forget flow (chapter 7) erases them with the events. Do not pass the application's `ObjectMapper` bean here: it has no crypto module, and the queue would hold the customer's name, email, address and phone in plaintext.

### Step 3: Create the `DeadLetterRetryRunner` Bean

`DeadLetterRetryRunner` polls the `DeadLetterQueue` on a virtual thread, deserializes each stored command, re-dispatches it through the `CommandBus`, and discards the entry on success. On failure it increments a DLQ attempt counter so that poison messages eventually stop being retried.

You must tell the runner which Java types to use when deserializing each command type. The DLQ entry stores the command's fully-qualified class name (`Class#getName()`) — the **concrete** class, e.g. `InventoryCommand$ReceiveShipment` — and `registerCommand` keys each class by that same name, so the runner resolves a stored entry with one exact lookup. Two commands with the same simple name in different packages never collide. You do not have to list every concrete command: registering a sealed root such as `InventoryCommand.class` also registers every command it permits, recursively. The demo registers the five sealed command roots, the same roots the command bus routes to deciders, so a dead-lettered command of any of the five aggregates resolves.

```java
@Bean
public DeadLetterRetryRunner deadLetterRetryRunner(
    PostgresDeadLetterQueue dlq,
    VirtualThreadCommandBus commandBus,
    PostgresCryptoEngine cryptoEngine,
    BackgroundRelayHealthContributor relayHealth) {

  var runner =
      DeadLetterRetryRunner.builder()
          .deadLetterQueue(dlq)
          .commandBus(commandBus)
          .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
          .registerCommand(ProductCommand.class)
          .registerCommand(OrderCommand.class)
          .registerCommand(CustomerCommand.class)
          .registerCommand(PaymentCommand.class)
          .registerCommand(InventoryCommand.class)
          .build();
  relayHealth.registerDlqRetryRunner(runner);
  runner.start();
  return runner;
}
```

`BackgroundRelayHealthContributor` (from `org.streamrune.runtime`) is the framework bean that tells the health endpoint whether each background relay is still running. The framework registers the relays it builds; this runner replaces the framework's, so the bean method registers it. The `streamRune` health component (Chapter 15) then turns `DOWN` if the runner's poll thread dies, instead of reporting `UP` while dead-letter entries stop being replayed.

The runner reads the entries with the same kind of mapper the bus wrote them with, so the `@Encrypted` fields decrypt to the original values before the command is replayed. A mapper without the crypto module would read the Base64 ciphertext into the command's `String` fields and replay it as the customer's name and email. If the customer was forgotten while the entry waited, the fields decrypt to `[REDACTED]`; the runner then does not replay the command but counts a failed attempt, so the entry ages out under the retry policy.

The five command interfaces are already imported in `StreamRuneConfig` for the command bus. The expansion reads `Class#getPermittedSubclasses()`, which a GraalVM native image answers only for a sealed type registered for reflection — for any other it reports the type as sealed with no permitted subclasses, and `registerCommand` then refuses to start the application rather than registering the root alone. The demo's `EcommerceNativeHints` registers the roots through `StreamRuneRuntimeHints.registerDomainPackages`, and the native smoke test (`scripts/native-image-smoke-test.sh spring`) replays a dead-lettered `InventoryCommand$ReceiveShipment` through `POST /api/admin/dead-letters/{id}/retry` to prove it (see Chapter 17, *Deploy to production*). The demo's Quarkus app registers the same five roots with its dead-letter runner and lists them in its `reachability-metadata.json`, because Quarkus' `@RegisterForReflection` does not make the image list permitted subclasses; `scripts/native-image-smoke-test.sh quarkus` runs the same replay (see Chapter 17, *Explore Quarkus and Micronaut variants*). The Micronaut app does the same: its runner registers the five roots, they are listed in its own `reachability-metadata.json` (Micronaut's `@TypeHint` does not register permitted subclasses either), and `scripts/native-image-smoke-test.sh micronaut` runs the replay.

A runner you define yourself **replaces** the framework's default one, on all three frameworks, and the framework then starts nothing of that type: the runner is yours to start and stop. On Spring the bean method above calls `runner.start()` (Spring closes it at shutdown). The demo's Quarkus and Micronaut apps define their runner the same way (5 attempts, 60 s initial backoff, 60 s poll, the same crypto-aware mapper) and start it from their `EcommerceSubscriptionLifecycle`, next to the saga subscriptions, closing it at shutdown; their `DeadLetterRetryRunnerOwnershipIT` checks that the demo's runner is running and that the framework built none of its own. Their command bus comes from the framework, which hands it the `DeadLetterQueue` bean and the crypto-aware mapper by itself. In all three apps `CustomerCommandDeadLetterIT` makes the event store refuse a customer's event, then checks that the dead-lettered command holds no plaintext, that a replay registers the customer with the original values, and that after a forget the queued copy reads `[REDACTED]` and is not replayed.

The runner polls every minute by default. You can override this with `.pollInterval(Duration.ofSeconds(30))`. The batch size defaults to 10, configurable with `.batchSize(20)`.

### Step 4: Configure `ProjectionErrorStrategy.DLQ` per projection (optional production hardening)

> **The real demo's `MultiProjectionRunner` does not use `PostgresProjectionDeadLetterStore`.** All four projections use the default error strategy (`HALT`). The per-projection DLQ wiring below is an optional production hardening step — include it in your project if you want failed event batches stored durably instead of halting the projection.

The error strategy is set **per registration** using the four-argument `register` overload — name, projection, error strategy, delivery mode. If you want to enable DLQ for the orders projection, update the `MultiProjectionRunner` bean like this (you will also need `import org.streamrune.core.projection.ProjectionErrorStrategy;`, since `StreamRuneConfig` imports `org.streamrune.core.projection` types individually rather than with a wildcard):

```java
@Bean(destroyMethod = "close")
public MultiProjectionRunner projectionRunner(
    EventStore eventStore,
    OffsetStore offsetStore,
    JdbcProjectionRepository projectionRepository,
    ProductProjection productProjection,
    OrderProjection orderProjection,
    CustomerProjection customerProjection,
    InventoryProjection inventoryProjection,
    PostgresProjectionDeadLetterStore projectionDlq,   // only needed if using DLQ
    CacheInvalidator cacheInvalidator) {

  var runner =
      MultiProjectionRunner.builder()
          .eventStore(eventStore)
          .offsetStore(offsetStore)
          .atomicProcessor(projectionRepository)
          .deadLetterStore(projectionDlq)              // required when any registration uses DLQ
          .register("products",
              new CacheAwareProjection(productProjection,  cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .register("orders",
              new CacheAwareProjection(orderProjection,    cacheInvalidator),
              ProjectionErrorStrategy.DLQ,             // orders projection uses DLQ
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .register("customers",
              new CacheAwareProjection(customerProjection, cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .register("inventory",
              new CacheAwareProjection(inventoryProjection,cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .build();
  runner.start();
  return runner;
}
```

Two things to note:

- **`deadLetterStore` on the builder is shared** across all registrations that use `DLQ` strategy. The builder validates at `build()` time that a `deadLetterStore` is supplied whenever any registration requests `DLQ` — it throws `IllegalArgumentException` otherwise.
- **If you are not using DLQ**, omit the `projectionDlq` parameter and the `.deadLetterStore(...)` call and leave every `register` call in the three-argument form — name, projection, delivery mode (as in Chapters 5–13).

When `orderProjection.process(batch)` throws, the internal runner writes a `ProjectionDeadLetterEntry` containing the projection name, the offset range of the failed batch, the exception class, the exception message, and the timestamp — then continues processing the next batch. The other three projections are unaffected.

### Step 5: Extend `AdminController` with Dead-Letter and Outbox Endpoints

`AdminController` was created in Chapter 12 with only the circuit-breaker and payment-failure endpoints. Now that both `DeadLetterQueue` and `OutboxStore` beans exist, extend it to add the dead-letter and outbox endpoints. Step 5b below adds a further round of saga dead-letter endpoints once the `SagaDeadLetterReplayer` bean exists.

Replace the existing `AdminController` with the full version:

```java
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
```

> The constructor now also injects `PaymentGatewayCircuitBreaker` (from Chapter 12), so `/circuit-breaker` reports both the command-bus breaker (`state`) and the payment-gateway breaker (`paymentGateway`). It takes the outbox replayer as an `ObjectProvider<OutboxFailedReplayer>` rather than the replayer itself: the framework auto-configures `OutboxFailedReplayer` only while `streamrune.outbox.enabled` is `true`, so with the outbox switched off the controller still starts and the two outbox operations answer `503`.

`deadLetterQueue.read(limit)` returns entries ordered newest-first. Each `DeadLetterEntry` carries the original command payload as a JSON string, the command type name, error details, the number of original attempts, and the number of DLQ retry attempts so far. The `/retry` endpoint **re-dispatches the command through the bus on demand**: it calls `deadLetterRetryRunner.retry(new CommandId(id))`, which loads the entry, deserializes its command, and executes it under a `DLQ_REPLAY` marker plus the entry's original request context (correlation id, user). A guarded command such as `ShipOrder` replays too: the replay has no request for `HeaderUserRoleResolver` to read the role from, so the framework does not repeat the role check, and the command, authorized once before it was dead-lettered, runs under the dead-letter-replay system principal as the recorded user, with a `WARN` naming it (Chapter 9 explains why the resolver declares `requiresRequestContext()`). On success the entry is discarded; on failure its DLQ attempt count is incremented. The method returns `200 OK` when an entry was found and retried, or `404 Not Found` when no entry exists for that id. This is the same `retry(CommandId)` operation the background `DeadLetterRetryRunner` performs on its own schedule — the endpoint just lets an operator trigger a single entry immediately.

The `/outbox` endpoint lists `PENDING` entries with `outboxStore.findByStatus(OutboxStatus.PENDING, limit)`. It deliberately does **not** call `loadPending`: that method *claims* — it moves every returned row to `IN_PROGRESS` under the caller's lease, so an admin page refreshing the list would stall delivery of every listed aggregate for two minutes. `findByStatus` is read-only and safe while the relay polls.

`GET /api/admin/outbox/failed` lists the `FAILED` entries, oldest first — on the demo's strict channel each one is blocking its order, payment or product. Two mutating endpoints resolve them, both delegating to the framework's auto-configured `OutboxFailedReplayer`:

- `POST /api/admin/outbox/failed/{id}/replay` → `{"outcome":"REPLAYED"}` — the entry goes back to `PENDING` at its original position and is delivered before the entries that waited behind it. `NOT_FAILED` means someone already resolved it; `NOT_FOUND` means the id is unknown.
- `POST /api/admin/outbox/failed/{id}/skip` with `{"reason":"…"}` → `{"outcome":"SKIPPED"}` — the entry will never be delivered; the row records who, when and why, and the aggregate's later entries flow again. The **who** is `skippedBy`, and the controller does not read it from a header: it takes the user id the framework's request filter already resolved into `StreamRuneContext.CURRENT` — in this demo's trusted-gateway mode that is `X-User-Id` (Chapter 9) — exactly as `CustomerCommandController.currentRequesterId()` does for the GDPR audit, and answers `400` when no identity is bound. `reason` is required.

**Every mutating admin endpoint now checks a role.** `requireRole("ADMIN")` reads the `role` entry from the request context's baggage — the same entry `HeaderUserRoleResolver` reads, copied by the framework filter from `X-User-Role` — and answers `403` otherwise. It guards `dead-letters/{id}/retry`, the three saga recovery operations (`saga-dead-letters/replay`, `saga-dead-letters/replay-all`, `sagas/faulted/resume`), `DELETE saga-dead-letters`, `payment-failure/toggle` and the two new outbox operations; read-only listings stay open. The helper is package-private rather than private because one route outside this controller flips the same switch: `SagaController`'s `POST /api/saga/fulfillments/{id}/inject-failure` shorthand (Chapter 11) calls `AdminController.requireRole("ADMIN")` too, so the payment-failure flag cannot be flipped without the role through either route. This is the same demo shortcut Chapter 9 is honest about: the role is whatever the client claims, so this helper is a stand-in for the gateway that would authenticate the caller in a real deployment — without it the chapter would teach an admin surface that lets anyone drop a message. The frontend already sends `X-User-Role` on every call, so the Admin page keeps working when you pick the ADMIN persona.

Try it (the ids come from the `failed` listing):

```bash
curl -s "http://localhost:8080/api/admin/outbox/failed" | jq .
curl -s -X POST -H "X-User-Id: ops-1" -H "X-User-Role: ADMIN" -H "Content-Type: application/json" \
  -d '{"reason":"OPS-42: consumer schema rejected the payload"}' \
  "http://localhost:8080/api/admin/outbox/failed/<entry-id>/skip"
# {"outcome":"SKIPPED"}
```

### Step 5b: Saga Dead-Letter Admin Endpoints

Chapter 11 introduced the `sagaDeadLetterStore` bean and explained that a poison saga event is quarantined in `saga_dead_letters` with the saga marked `FAULTED`. Until now, the only way to find a `FAULTED` saga or inspect its quarantine reason was a direct database query. The saga endpoints of the controller above close that gap — two listings (`listSagaDeadLetters`, `listFaultedSagas`), three recovery operations (`replayAllSagaDeadLetters`, `replaySagaDeadLetter`, `resumeFaultedSaga`) and `discardSagaDeadLetter` — on top of a new `SagaDeadLetterReplayer` bean added to `StreamRuneConfig` next to `orderFulfillmentSagaRunner`:

```java
@Bean
public SagaDeadLetterReplayer orderFulfillmentSagaDeadLetterReplayer(
    PostgresSagaDeadLetterStore sagaDeadLetterStore,
    PostgresSagaStore sagaStore,
    EventStore eventStore,
    SagaRunner<?> orderFulfillmentSagaRunner,
    StreamRuneProperties properties,
    ObjectProvider<StreamRuneMetrics> metrics) {
  return SagaDeadLetterReplayer.builder()
      .sagaDeadLetterStore(sagaDeadLetterStore)
      .sagaStore(sagaStore)
      .eventStore(eventStore)
      .sagaRunner(orderFulfillmentSagaRunner)
      .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
      .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
      .build();
}
```

Add the imports `org.springframework.beans.factory.ObjectProvider` and `org.streamrune.spring.StreamRuneProperties`; `StreamRuneMetrics` comes with the `org.streamrune.core.*` wildcard.

There is deliberately no polling runner behind this bean — poison failures are deterministic orchestrator bugs that only resolve once a fix is deployed, so recovery is always operator-triggered. The replayer reuses the same `orderFulfillmentSagaRunner` + stores + event store the live saga subscription uses, so a replayed event is fed through the identical orchestrator path (poison guard included).

**Why `inboxRetentionMaxAge` is not optional.** Every command a saga dispatches carries an idempotency key, and the command inbox recognises a repeated key (Chapter 11, Step 6). The inbox keeps keys for `streamrune.inbox.retention-max-age` — 7 days unless you configure it — and then prunes them. Picture an order saga whose `CapturePayment` and `ReserveStock` committed before a bug faulted it. An operator replays on day 1, before the fix, and gets `STILL_POISON`; the fix ships on day 9. By then the two commands' keys are gone: a replay would dispatch them again, the inbox would not recognise them, and the customer would be charged twice. The replayer records when an entry's first replay started, and with the inbox window it refuses a re-drive whose first attempt is older than the window (`STALE_REDRIVE_BLOCKED`) — and likewise a faulted compensation whose episode is older (`STALE_COMPENSATION_BLOCKED`: a refund could run twice). Without the window both refusals are off, and nothing else guards the forward path. `properties.inbox().retentionMaxAge()` is the same value the inbox sweeper uses, so the two cannot drift apart. `metrics` counts every outcome (`streamrune.saga.replayed`, `streamrune.saga.resume_faulted`).

**The replayer never un-faults a saga.** It feeds the event through the saga runner's normal step against the row as it stands; a `FAULTED` row is cleared only by that step's own successful write, and a feed that poisons again re-records the fault.

**Two kinds of `FAULTED` saga.** `GET /api/admin/sagas/faulted` lists both:

- **A saga a poison event faulted.** It owns dead-letter entries: the poison event, and every later event of the saga, which the live runner holds behind the fault so that nothing is applied out of order. `replay-all` drains them.
- **A compensation faulted without an entry.** A compensation command that fails *transiently* — a store blip, an open circuit breaker — leaves the saga `COMPENSATING`, not `FAILED`. The compensation retry sweeper, which the Spring integration starts for every `SagaRunner` bean (that is why `orderFulfillmentSagaRunner` is a bean), re-drives it every minute under the same keys. After `streamrune.saga.compensation-retry-give-up-after` (1 hour by default) it gives up and faults the saga, with no dead-letter entry, because there is no event to quarantine. `resume` runs that compensation again once the cause is fixed.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/admin/saga-dead-letters?limit=50` | Lists quarantined saga dead-letter entries, newest first. Each entry: `sagaId` (nullable), `sagaType` (nullable), `eventOffset`, `eventType`, `errorType`, `errorMessage`, `faultedAt`. |
| `GET` | `/api/admin/sagas/faulted?limit=50` | Lists the ids of order-fulfillment sagas in `FAULTED` status, both kinds. |
| `POST` | `/api/admin/saga-dead-letters/replay-all?sagaId=…` | Drains one saga's entries through `replayAll`, oldest event first, and stops at the first entry it cannot feed. Returns `{"outcomes": [...]}`, one outcome per attempt in attempt order. |
| `POST` | `/api/admin/saga-dead-letters/replay?sagaId=…&eventOffset=…` | Replays one entry through `replay`. Refused with `OLDER_ENTRY_PENDING` while an older entry of the same saga is pending. `sagaId` is omitted for an entry whose saga could never be identified. Returns `{"outcome": "..."}`. |
| `POST` | `/api/admin/sagas/faulted/resume?sagaId=…` | Resumes a compensation faulted without an entry, through `resumeFaulted`. Returns `{"outcome": "..."}`. |
| `DELETE` | `/api/admin/saga-dead-letters?sagaId=…&eventOffset=…` | Discards a quarantined entry without replaying it, through `SagaDeadLetterReplayer.discard`. `sagaId` is omitted for an entry whose saga could never be identified. Returns `{"discarded": true/false}`. |

The three `POST` operations also take `force=true` (below). The four mutating endpoints carry the ADMIN role header from Step 5.

```bash
# List quarantined saga dead-letters
curl -s "http://localhost:8080/api/admin/saga-dead-letters?limit=50" | jq .

# Enumerate FAULTED sagas
curl -s "http://localhost:8080/api/admin/sagas/faulted?limit=50" | jq .

# Drain a saga after redeploying the fix
curl -s -X POST -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/saga-dead-letters/replay-all?sagaId=fulfillment-o-100"
# {"outcomes":["REPLAYED","REPLAYED"]}

# Resume a compensation the sweeper gave up on
curl -s -X POST -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/sagas/faulted/resume?sagaId=fulfillment-o-101"
# {"outcome":"RESUMED"}

# Discard an entry without replaying it
curl -s -X DELETE -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/saga-dead-letters?sagaId=fulfillment-o-100&eventOffset=42"
# {"discarded":true}
```

A replay answers with one of nine outcomes — `replay` once, `replay-all` once per attempt:

- `REPLAYED` — the event was fed and the step succeeded, or the feed was moot (the saga is already terminal, or the event was already applied). The entry is discarded.
- `STILL_POISON` — the step poisoned again (or a resumed compensation made no durable progress). The entry stays and the saga stays `FAULTED`: the fix has not resolved the failure.
- `ENTRY_NOT_FOUND` — no entry of the order-fulfillment saga type at that `(sagaId, eventOffset)`.
- `EVENT_NOT_FOUND` — the event store can no longer read that offset. Nothing was fed; `DELETE` the entry.
- `STALE_REDRIVE_BLOCKED`, `STALE_COMPENSATION_BLOCKED` — the key-age refusals above. Find out which of the saga's commands already executed (the payment, inventory and order streams, the `command_inbox` table), then repeat with `force=true`.
- `OLDER_ENTRY_PENDING` — a single replay with an older entry of the saga still pending; use `replay-all`.
- `SAGA_ROW_PENDING` — deferred, not fed: the saga has no row yet, or its start has not been applied. `replay-all` feeds it once the saga's start entry lands in the same drain.
- `TARGET_PENDING` — an entry whose saga could not be identified now resolves to a saga that is itself `FAULTED` or has older entries pending; drain that saga with `replay-all`.

A resume answers `RESUMED` (the compensation ran to its end: `COMPENSATED`, or `FAILED` when a compensation was rejected for a business reason), `NO_PROGRESS` (a compensation failed transiently again; the sweeper never re-drives a `FAULTED` saga, so resume again later — compensations that already ran deduplicate), `STILL_POISON` (`compensate()` still throws), `STALE_COMPENSATION_BLOCKED`, `FORWARD_FAULT` or `ENTRIES_PENDING` (the saga has dead-letter entries: use `replay-all`, whose first feed resumes the compensation), `NOT_FAULTED` or `SAGA_NOT_FOUND`.

**`force=true` lifts only the two `STALE_*` refusals.** It never overrides ordering. It is your acknowledgement that a command whose key was already pruned may run a second time, so send it only after you have checked what executed; the framework logs a `WARNING` naming the saga, and counts `streamrune.saga.forced_stale_resume` when a forced compensation passes the runner's own key-age guard. The full outcome tables are in the framework's saga guide (`docs/guide/advanced/saga.md` in the StreamRune repository, sections "Replaying quarantined events" and "Resuming a give-up fault").

`discardSagaDeadLetter` goes through the same `SagaDeadLetterReplayer` bean rather than the store, for two reasons. The removal is scoped to the replayer's saga type (the state class's fully-qualified name), so an id that another saga type happens to share can never lose its quarantine record through this endpoint. And a saga with quarantined entries carries a dead-letter shield (`saga_state.dead_letter_pending`) that makes the live `SagaRunner` hold every later correlated event of that saga in the quarantine instead of applying it out of order; `discard` releases the shield once the saga has no entry left (for an entry whose saga could not be identified, the shield of the saga the replayer resolved it to). Deleting the row with a bare `sagaDeadLetterStore` call would leave that shield set, and the saga's live events would keep being held behind a backlog that no longer exists.

The demo's Quarkus and Micronaut apps are wired the same way. Their `orderFulfillmentSagaRunner` is a bean (`StreamRuneProducer`, `StreamRuneFactory`) that `EcommerceSubscriptionLifecycle` subscribes to the event stream, so their integrations start its sweeper and arm its key-age guard too; their replayer reads the window from `StreamRuneQuarkusProperties.inbox().retentionMaxAge()` and `StreamRuneMicronautProperties.inboxRetentionMaxAge()`; and their `AdminController` exposes the same six saga endpoints. Their `SagaRecoveryAdminIT` also checks that the framework armed the runner's guard with the 7-day window and that the sweeper finishes a compensation left `COMPENSATING`.

`SagaDeadLetterAdminIT` exercises each endpoint against the running app: a replay whose first attempt is eight days old answers `STALE_REDRIVE_BLOCKED` and goes through with `force=true`; a single replay behind an older entry answers `OLDER_ENTRY_PENDING` while `replay-all` drains both; a compensation faulted out of `COMPENSATING` resumes to `COMPENSATED`, and one whose episode is eight days old needs `force=true`.

### Step 6: Projection lifecycle via `MultiProjectionRunner`

`MultiProjectionRunner` exposes coordinated lifecycle control for all registered projections:

- **`runner.start()`** — launches every projection on its own virtual thread. The demo calls this immediately after building the runner in the `@Bean` method.
- **`runner.stop()`** — signals every internal `ContinuousProjectionRunner` to stop and waits up to 30 seconds for each projection's thread to exit (the builder's `stopTimeout`). A batch that is being committed when the stop arrives finishes and records its checkpoint first; only sleeps and reads are cut short. Spring calls `close()` (the same as `stop()`) through the `destroyMethod = "close"` on the `@Bean` annotation, so projections stop gracefully on application shutdown.
- **`runner.status()`** — returns a `Map<String, ProjectionStatus>` keyed by projection name. Each `ProjectionStatus` carries the projection name, its current `ProjectionState` (`PENDING`, `CATCHING_UP`, `LIVE`, `STANDBY` — another instance holds the projection's leadership — `ERROR`, or `STOPPED`), the last committed offset (a `GlobalOffset`, read from the `OffsetStore` on every call) and the last error message, or `null`.

The `AdminController` already exposes a `/api/admin/circuit-breaker` endpoint for the circuit breaker state. The real demo's `AdminController` does **not** expose a projection-status endpoint, but you can add a similar one by injecting `MultiProjectionRunner`. The fragment below is illustrative — to use it, add a `private final MultiProjectionRunner runner;` field with constructor injection (alongside the existing dependencies) and `import org.streamrune.runtime.MultiProjectionRunner;`:

```java
@GetMapping("/projections")
public Map<String, Object> projectionStatus() {
    return runner.status().entrySet().stream()
        .collect(java.util.stream.Collectors.toMap(
            Map.Entry::getKey,
            e -> Map.of(
                "state", e.getValue().state().name(),
                "lastOffset", e.getValue().lastOffset().value())));
}
```

`MultiProjectionRunner` does not expose per-projection pause/resume — it operates on the whole group. If you need to pause an individual projection in production (for example to replay it from a specific offset), the correct approach is to stop the entire runner, create a new `MultiProjectionRunner` with the corrected offset in the `OffsetStore`, and restart. The offset is persisted in `PostgresOffsetStore`, so the other projections resume exactly where they left off.

### Step 7: Verify End-to-End

**Scenario 1: Projection failure routes to DLQ**

This scenario needs the Step 1 bean and the Step 4 wiring. On the demo as shipped (`HALT`) the same failure does not reach `projection_dead_letters`: the orders projection retries the batch with a growing backoff and, after 50 consecutive failures, stops with state `ERROR` (visible in `runner.status()`, Step 6), while the other projections keep running.

Temporarily introduce a failure into `OrderProjection.process` by throwing a `RuntimeException` for any `OrderPlaced` event. Place an order:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"o-fail-1","customerId":"c-1","lines":[{"productId":"p-1","quantity":1,"unitPrice":29.99}]}'
```

Check the projection dead letters table directly:

```sql
SELECT projection_name, from_offset, to_offset, error_type, error_message, failed_at
FROM projection_dead_letters
ORDER BY failed_at DESC
LIMIT 5;
```

```
 projection_name | from_offset | to_offset | error_type                 | error_message              | failed_at
-----------------+-------------+-----------+----------------------------+----------------------------+--------------------------
 orders          | 42          | 43        | java.lang.RuntimeException | Simulated failure          | 2026-04-25 09:14:22.134
```

The projection runner is still alive — it logged the failure, persisted the entry, and continued polling. Its checkpoint has moved past the failed batch, so removing the artificial failure and restarting the application does not re-read it. Applying the range after the fix is a replay through `ProjectionDeadLetterReplayer`, with the requirements on `process` described under `DLQ` at the top of this chapter; the demo does not wire a replayer, so inspect the entry and remove it from `projection_dead_letters` directly in the database.

**Scenario 2: Stop, accumulate events, restart and catch up**

Stop the application. While the runner is stopped, the event store still accepts commands — command processing does not depend on projections. Query the order projection endpoint:

```bash
curl -s http://localhost:8080/api/orders/o-2
```

The response will not reflect orders written after the shutdown because the projection has not been updated.

Start the application again. On startup `MultiProjectionRunner.start()` is called, each projection resumes from its last committed offset in `PostgresOffsetStore`, processes all accumulated events in one or more batches, and transitions to LIVE. The query then returns the correct result. Because offsets are persisted in PostgreSQL, the projection never loses its position across restarts.

---

## What We Learned

**`ProjectionErrorStrategy`** is an enum with three values: `HALT`, `SKIP`, and `DLQ`. It is set per projection with `MultiProjectionRunner.builder().register(name, projection, strategy, mode)`; the demo registers every projection with the three-argument form, which keeps the default `HALT`. The choice is a tradeoff between fail-fast safety and production liveness.

**`ProjectionDeadLetterStore`** is the interface for persisting failed projection batches. `PostgresProjectionDeadLetterStore` is the durable implementation backed by a `projection_dead_letters` table. Each `ProjectionDeadLetterEntry` carries the projection name, the global offset range of the failed batch, the event count, the exception class and message, the consecutive error count at the time of failure, and the timestamp.

**`DeadLetterQueue`** is the interface for storing failed commands. `PostgresDeadLetterQueue` is the Postgres implementation. It is separate from `ProjectionDeadLetterStore` because commands and projection batches are different units of failure: a command lands in the queue because an infrastructure error failed it (a rejection by the aggregate is never recorded); a projection batch fails because the read-side handler threw. The command bus writes to the queue only when it is built with `.deadLetterQueue(...)`, and it stores the command record as JSON through the mapper given to `.objectMapper(...)`. Use `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)` on both the bus and the runner, so a command's `@Encrypted` fields are stored as ciphertext and are erased with the customer.

**`DeadLetterRetryRunner`** polls the `DeadLetterQueue` on a virtual thread. For each retryable entry it deserializes the command using the `commandTypeRegistry`, re-dispatches it through the `CommandBus`, and discards the entry on success. On failure it increments `dlqAttempts`. Once `dlqAttempts` reaches the policy's `maxRetries`, the entry is either discarded (if `discardAfterMaxRetries` is true) or left in place as a permanent record of a poison message. The default policy (`DeadLetterRetryPolicy.DEFAULT`) retries up to five times with exponential backoff and does **not** auto-discard, so an exhausted entry stays in the queue as a permanent record.

**`SagaDeadLetterReplayer`** is the saga-side counterpart introduced in this chapter: manual, operator-triggered recovery of `FAULTED` sagas. Unlike `DeadLetterRetryRunner`, it has no polling loop — poison saga failures are deterministic orchestrator bugs that only resolve once a fix is deployed, so recovery is always explicit. `replayAll` drains a saga's quarantined events oldest first and `replay` feeds one, through the same saga runner; `resumeFaulted` re-runs a compensation the retry sweeper gave up on. It never un-faults a saga itself: only the fed step's own write clears the fault. Built with the deployment's inbox retention window, it refuses a re-drive or a compensation older than that window (`STALE_REDRIVE_BLOCKED`, `STALE_COMPENSATION_BLOCKED`) until an operator who has checked what already ran repeats it with `force`.

**The outbox admin endpoints** sit on the same controller. Listing the outbox is `findByStatus`, never `loadPending` (which claims). A blocked aggregate is resolved by `replay` or an audited `skip`; `skippedBy` is the request identity; mutating admin endpoints require the ADMIN role.

**`MultiProjectionRunner`** manages all projections as a group. `start()` launches each projection on its own virtual thread. `stop()` (aliased as `close()`) signals all runners and waits up to 30 seconds for graceful shutdown. `status()` returns a `Map<String, ProjectionStatus>` with the current `ProjectionState`, last committed offset and last error for each projection.

**`ProjectionState`** is the enum returned per projection by `status()`. Its values are `PENDING`, `CATCHING_UP`, `LIVE`, `STANDBY`, `ERROR`, and `STOPPED`.

**Poison message handling** is the scenario where a command or event batch can never succeed, regardless of how many times it is retried. The `DeadLetterRetryRunner` handles this by capping retries at `maxRetries`. A projection registered with `DLQ` strategy writes the batch to the dead letter store and continues — the poison batch is isolated and does not block all subsequent events. Saga poison events follow the same principle via `SagaDeadLetterStore` + `SagaDeadLetterReplayer` (Step 5b), except replay there is always operator-triggered rather than polled.

---

## Next Up

Our system handles failures gracefully — dead letters are stored, projections recover automatically, and command retries are automated. But how do we know what is happening inside at runtime? Next: observability.
