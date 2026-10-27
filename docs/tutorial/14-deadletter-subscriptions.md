# Chapter 14: Dead Letter Queue & Subscriptions

> **What you'll learn:** What to do when a projection fails to process an event, how to configure `ProjectionErrorStrategy.DLQ` per projection inside `MultiProjectionRunner` so that failed batches are durably stored instead of halting the system, and how `DeadLetterRetryRunner` replays failed commands: on its own schedule the commands that carry an idempotency key, and the others only when an operator confirms that their effect is absent — because a replay of an unkeyed command whose caller already retried it is a second execution.

---

## What We're Building and Why

Production event-driven systems fail. A downstream database is temporarily unavailable, a deserialization assumption stops holding, or a third-party API returns an unexpected error. The question is not whether failures happen — it is how the system behaves when they do.

StreamRune gives you three choices when a projection fails to process a batch of events. You select one via `ProjectionErrorStrategy`:

- **`HALT`** — the default. Stop the projection immediately and surface the error. Good for catching bugs in development, dangerous in production where a single bad event can freeze all projections.
- **`SKIP`** — log the failure and advance the offset past the failing batch. Guarantees liveness at the cost of potentially missing events silently.
- **`DLQ`** — write the failed batch to a `ProjectionDeadLetterStore` and continue. The projection stays live; the failed batch is recorded for later inspection and replay. A replay (`ProjectionDeadLetterReplayer`, Step 7) takes the lock a live batch of the projection takes, so the two never run at the same time, and under `TRANSACTIONAL_LOCAL` it applies a range all-or-nothing through the replay transaction's repository. It is still at-least-once, and it applies the range after the events that followed it, so `process` must tolerate re-application and an older event after a newer one. Check both before you replay into a projection: of the demo's read models, products, customers and orders tolerate re-application but not an older event after a newer one (a dead-lettered `OrderPlaced` replayed after the live runner has processed that order's `OrderConfirmed` — a no-op then, because the row did not exist — leaves the order `CREATED`), and `InventoryProjection`'s counters tolerate neither. This is the right choice for production.

In this chapter you will see how to configure `ProjectionErrorStrategy.DLQ` on individual projections inside the `MultiProjectionRunner` with `PostgresProjectionDeadLetterStore` as the persistent store (an optional hardening step: the demo's projections keep the default `HALT`), expose dead letter entries through `AdminController` (together with the outbox's blocked `FAILED` entries and their replay/skip operations, behind an ADMIN role check), look at the command-side `DeadLetterRetryRunner`, which replays keyed failed commands on its schedule and leaves unkeyed ones to an operator, give the stock adjustment an `Idempotency-Key` so that its failures are replayed safely, and wire the saga-side dead-letter admin endpoints (list/faulted/replay/discard) built on a `SagaDeadLetterReplayer` over the saga dead-letter store of Chapter 11.

---

## Step by Step

### Step 1: The `PostgresProjectionDeadLetterStore` (only with Step 4)

A `PostgresProjectionDeadLetterStore` bean is needed only for the optional per-projection DLQ wiring in Step 4. The demo's `StreamRuneConfig` does **not** declare it — its projections keep the default `HALT` strategy — so the listings of this chapter leave it out; add it only if you follow Step 4. It uses the same `DataSource` as the event store. The `projection_dead_letters` table is part of StreamRune's `V001__streamrune_baseline.sql` — the same baseline that creates `event_stream` and `snapshot_store`, applied by the event store factory on the application's first start (`streamrune.event-store.schema.auto-initialize: true`; see Chapter 1).

The bean method returns `new PostgresProjectionDeadLetterStore(ds)` for the application's `DataSource`, like the `PostgresDeadLetterQueue` of Step 2.


### Step 2: Create the `PostgresDeadLetterQueue` Bean

The `DeadLetterQueue` is separate from `ProjectionDeadLetterStore`. The projection DLQ stores failed event batches for operator inspection. The `DeadLetterQueue` stores commands that exhausted all retry attempts after being dispatched through the `CommandBus`. Both are needed.

```java
@Bean
public PostgresDeadLetterQueue deadLetterQueue(DataSource ds) {
  return new PostgresDeadLetterQueue(ds);
}
```

The queue on its own records nothing: the command bus writes to it only when the bus is given it. Open the `commandBus` bean and add two parameters, the queue and the crypto engine from chapter 6, and four builder calls: `.deadLetterQueue(deadLetterQueue)`, `.objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))`, and the two rules the retry runner of Step 3 replays by, `.deadLetterRetryPolicy(DeadLetterRetryPolicy.DEFAULT)` and `.inboxRetentionMaxAge(properties.inbox().retentionMaxAge())`. The bean now reads:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit,
    CircuitBreakerCommandInterceptor circuitBreaker,
    PostgresCommandInbox commandInbox,
    PostgresDeadLetterQueue deadLetterQueue,
    PostgresCryptoEngine cryptoEngine,
    AggregateLocker locker,
    StreamRuneProperties properties) {
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .locker(locker)
      .lockTimeout(properties.lockTimeout())
      // Required for saga command dispatch: SagaCommandDispatch calls
      // commandBus.execute(command, IdempotencyKey), which throws IllegalStateException
      // unless the bus has a commandInbox wired in.
      .commandInbox(commandInbox)
      .interceptors(audit, auth, validation, circuitBreaker)
      .deadLetterQueue(deadLetterQueue)
      .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
      // The rules deadLetterRetryRunner replays by: a keyed command that fails again under
      // its key is told whether the entry stored for the key is still replayed.
      .deadLetterRetryPolicy(DeadLetterRetryPolicy.DEFAULT)
      .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
      .register(
          ProductState.TYPE,
          ProductCommand.class,
          cmd ->
              AggregateId.of(
                  switch (cmd) {
                    case ProductCommand.CreateProduct c -> c.productId();
                    case ProductCommand.UpdatePrice c -> c.productId();
                    case ProductCommand.AdjustStock c -> c.productId();
                    case ProductCommand.DiscontinueProduct c -> c.productId();
                  }),
          new ProductDecider())
      .register(
          OrderState.TYPE,
          OrderCommand.class,
          cmd ->
              AggregateId.of(
                  switch (cmd) {
                    case OrderCommand.PlaceOrder c -> c.orderId();
                    case OrderCommand.ConfirmOrder c -> c.orderId();
                    case OrderCommand.ShipOrder c -> c.orderId();
                    case OrderCommand.DeliverOrder c -> c.orderId();
                    case OrderCommand.CancelOrder c -> c.orderId();
                  }),
          new OrderDecider())
      .register(
          CustomerState.TYPE,
          CustomerCommand.class,
          cmd ->
              AggregateId.of(
                  switch (cmd) {
                    case CustomerCommand.RegisterCustomer c -> c.customerId();
                    case CustomerCommand.UpdateProfile c -> c.customerId();
                    case CustomerCommand.RequestDataExport c -> c.customerId();
                    case CustomerCommand.ForgetCustomer c -> c.customerId();
                  }),
          new CustomerDecider())
      .register(
          PaymentState.TYPE,
          PaymentCommand.class,
          cmd ->
              AggregateId.of(
                  switch (cmd) {
                    case PaymentCommand.InitiatePayment c -> c.paymentId();
                    case PaymentCommand.CapturePayment c -> c.paymentId();
                    case PaymentCommand.FailPayment c -> c.paymentId();
                    case PaymentCommand.CancelPayment c -> c.paymentId();
                  }),
          new PaymentDecider())
      .register(
          InventoryState.TYPE,
          InventoryCommand.class,
          cmd ->
              AggregateId.of(
                  switch (cmd) {
                    case InventoryCommand.ReserveStock c -> c.productId();
                    case InventoryCommand.ReleaseStock c -> c.productId();
                    case InventoryCommand.ConfirmReservation c -> c.productId();
                    case InventoryCommand.ReceiveShipment c -> c.productId();
                  }),
          new InventoryDecider())
      .build();
}
```

From now on a command that fails on infrastructure — a database or network error, a lock it could not take within the lock timeout on any attempt, a version conflict on every attempt — is recorded in `dead_letter_queue`, and the bus tells its caller so: it throws `CommandDeadLetteredException` in place of the failure, which is its cause, with the entry's id as `commandId()`, the command's idempotency key, if it ran under one, as `idempotencyKey()`, and what happens to the entry as `pendingOutcome()`: `QUEUED_FOR_REPLAY` while the retry runner of Step 3 replays it on its schedule, `AWAITING_OPERATOR` when an operator decides — for an unkeyed command, and for a keyed one whose entry has used up the policy's replays or was first received longer ago than the command-inbox window. The command is not applied, and a replay of the entry can still apply it: its outcome is pending, not final. Step 3b gives the stock adjustment its key and shows how the app answers such a command. The other refusals of Chapter 12, Step 2b — an open circuit breaker (`CIRCUIT_BREAKER_OPEN`), a closing bus (`COMMAND_BUS_CLOSED`), an overloaded one (`COMMAND_BUS_OVERLOADED`) — are never recorded: nothing was attempted, and they keep their `503`. The lock's and the version conflict's own answers, `AGGREGATE_LOCK_UNAVAILABLE` and `AGGREGATE_VERSION_CONFLICT`, remain for a command whose entry could not be stored, when the failure itself reaches the caller. A business rejection is not: a `DomainException` (for example "Customer not found") or an `IllegalArgumentException` is the domain's final answer, and replaying it later would only repeat it. Two kinds of command never reach this queue, whatever fails: a command a saga sends, and a command a listener dispatches inside `StreamRuneContext.continuing(event)` — the payment process manager's `CapturePayment`, the order cancellation process manager's `ReleaseStock` and `CancelPayment`, the order shipment process manager's `ConfirmReservation`, and just as much a continuation of an event no saga caused — because an entry has no place for the causing event or for the saga origin such a command carries, so a replay would store its events without them. Their caller retries instead: the saga handles its own failures, and a listener's command is run again by the redelivery of the event it reacts to (chapter 11, Step 5b).

**One entry per keyed command.** Before the bus stores an entry for a keyed command, it looks the key up. When the queue holds an entry of the same command under the key — a send under the key failed and was recorded already — the bus stores nothing and answers with that entry: its `commandId` and its outcome. A client that sends a keyed request again and again during an outage leaves one entry, not one per request, and each answer says whether that entry is still replayed: an entry whose replays are used up, or that is older than the inbox window, waits for an operator, and a send against it is answered `AWAITING_OPERATOR`, a `500`. The bus judges the entry by the retry runner's rules, which is why it takes the runner's policy and inbox window, `.deadLetterRetryPolicy(...)` and `.inboxRetentionMaxAge(...)`; the runner logs a `WARN` at startup when its bus answers by other ones. An unkeyed command stores an entry for every send that fails: nothing tells two of its sends apart from two operations.

The bus stores the command as JSON, written by the mapper you pass to `.objectMapper(...)`. `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)` registers the `CryptoShreddingModule`, so the `@Encrypted` fields of `RegisterCustomer` and `UpdateProfile` (chapter 6) are stored as ciphertext under the customer's key, and the forget flow (chapter 7) erases them with the events. Do not pass the application's `ObjectMapper` bean here: it has no crypto module, and the queue would hold the customer's name, email, address and phone in plaintext.

### Step 3: Create the `DeadLetterRetryRunner` Bean

`DeadLetterRetryRunner` polls the `DeadLetterQueue` on a virtual thread, deserializes each stored command, re-dispatches it through the `CommandBus`, and discards the entry on success. On failure it increments a DLQ attempt counter so that poison messages eventually stop being retried.

**Which entries the schedule replays.** A command lands in the queue because its first attempt failed on infrastructure, and the caller saw that failure. The caller may send the request again on its own, and that retry may succeed. A replay of the dead-letter entry afterwards is then a second execution of the same request: for `AdjustStock`, which changes the stock *by* a quantity, a second, silent change of the stock. The runner cannot tell this case from a command that never ran. It can rely on one thing: the command inbox (Chapter 11, Step 6). A command the caller sent with an idempotency key keeps that key in its entry, and the runner replays it under the same key, so if the caller's own retry already ran, the inbox answers the replay with the recorded result and nothing runs twice. The default policy (`DeadLetterRetryPolicy.DEFAULT`, `UnkeyedReplay.OPERATOR_ONLY`) therefore replays on the schedule only the entries of keyed commands. The bean passes it as `.policy(DeadLetterRetryPolicy.DEFAULT)`, the policy the command bus of Step 2 answers a keyed re-send by. An entry of an unkeyed command waits for an operator, who checks whether its effect is there and then replays it on purpose or discards it (Step 5); an entry nobody decides about is removed by the retention sweeper after `streamrune.dead-letter.retention-max-age` (7 days, the inbox's window). Step 3b gives the demo's stock adjustment and shipment receipt their keys.

You must tell the runner which Java types to use when deserializing each command type. The DLQ entry stores the command's fully-qualified class name (`Class#getName()`) — the **concrete** class, e.g. `InventoryCommand$ReceiveShipment` — and `registerCommand` keys each class by that same name, so the runner resolves a stored entry with one exact lookup. Two commands with the same simple name in different packages never collide. You do not have to list every concrete command: registering a sealed root such as `InventoryCommand.class` also registers every command it permits, recursively. The demo registers the five sealed command roots, the same roots the command bus routes to deciders, so a dead-lettered command of any of the five aggregates resolves.

```java
@Bean
public DeadLetterRetryRunner deadLetterRetryRunner(
    PostgresDeadLetterQueue dlq,
    VirtualThreadCommandBus commandBus,
    PostgresCryptoEngine cryptoEngine,
    BackgroundRelayHealthContributor relayHealth,
    org.streamrune.spring.StreamRuneProperties properties,
    ObjectProvider<StreamRuneMetrics> metrics,
    @Value("${app.dead-letter.poll-interval:60s}") Duration pollInterval) {
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
          // The policy and the window the command bus answers a keyed re-send by.
          .policy(DeadLetterRetryPolicy.DEFAULT)
          // The window the inbox sweeper prunes with: a keyed entry older than it is refused.
          .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
          .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
          .pollInterval(pollInterval)
          .build();
  relayHealth.registerDlqRetryRunner(runner);
  runner.start();
  return runner;
}
```

**`inboxRetentionMaxAge` closes the window in which a key is gone.** The inbox keeps keys for `streamrune.inbox.retention-max-age`, 7 days unless configured, and then prunes them. A keyed entry first received longer ago than that may not find its key, so the inbox could not recognise the caller's retry. The runner skips such an entry on the schedule, and the operator's Retry answers `STALE_REPLAY_BLOCKED` (Step 5). `properties.inbox().retentionMaxAge()` is the value the inbox sweeper uses, so the two cannot drift apart. The framework also refuses to start when the dead-letter retention is longer than the inbox's.

**`metrics`** is the framework's `StreamRuneMetrics` bean (Chapter 15). The runner reports `streamrune.dlq.pending` (entries not yet exhausted), `streamrune.dlq.awaiting_operator` (entries the schedule will never replay: unkeyed ones, and keyed ones older than the inbox window) and `streamrune.dlq.stale_replay_blocked` (operator retries refused for that age). Alert on `awaiting_operator` above zero: it is work for a person, not a fault, so it does not turn the health endpoint `DOWN`, and the person's decision — a forced replay or a discard (Step 5) — brings it down again on the next poll. **`app.dead-letter.poll-interval`** (60 seconds in `application.yml`) is how often it looks for due entries; the integration tests set it to one second. `ObjectProvider`, `StreamRuneMetrics`, `Value` and `Duration` are imported in `StreamRuneConfig` already.

`BackgroundRelayHealthContributor` (from `org.streamrune.runtime`) is the framework bean that tells the health endpoint whether each background relay is still running. The framework registers the relays it builds; this runner replaces the framework's, so the bean method registers it. The `streamRune` health component (Chapter 15) then turns `DOWN` if the runner's poll thread dies, instead of reporting `UP` while dead-letter entries stop being replayed.

The runner reads the entries with the same kind of mapper the bus wrote them with, so the `@Encrypted` fields decrypt to the original values before the command is replayed. A mapper without the crypto module would read the Base64 ciphertext into the command's `String` fields and replay it as the customer's name and email. If the customer was forgotten while the entry waited, the fields decrypt to `[REDACTED]`; the runner then does not replay the command but counts a failed attempt, so the entry ages out under the retry policy.

The five command interfaces are already imported in `StreamRuneConfig` for the command bus. The expansion reads `Class#getPermittedSubclasses()`, which a GraalVM native image answers only for a sealed type registered for reflection — for any other it reports the type as sealed with no permitted subclasses, and `registerCommand` then refuses to start the application rather than registering the root alone. `EcommerceNativeHints`, below, registers the roots through `StreamRuneRuntimeHints.registerDomainPackages`, and the native smoke test (`scripts/native-image-smoke-test.sh spring`) replays a dead-lettered `InventoryCommand$ReceiveShipment` — a keyed entry, which a plain retry replays — through `POST /api/admin/dead-letters/{id}/retry` to prove it (see Chapter 17, *Deploy to production*). The demo's Quarkus app registers the same five roots with its dead-letter runner and lists them in its `reachability-metadata.json`, because Quarkus' `@RegisterForReflection` does not make the image list permitted subclasses; `scripts/native-image-smoke-test.sh quarkus` runs the same replay (see Chapter 17, *Explore Quarkus and Micronaut variants*). The Micronaut app does the same: its runner registers the five roots, they are listed in its own `reachability-metadata.json` (Micronaut's `@TypeHint` does not register permitted subclasses either), and `scripts/native-image-smoke-test.sh micronaut` runs the replay.

The Spring app registers its domain types for reflection in one class. Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/EcommerceNativeHints.java`:

```java
package org.streamrune.ecommerce.spring.config;

import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.streamrune.spring.StreamRuneRuntimeHints;

/**
 * GraalVM native-image reflection hints for the demo's domain types.
 *
 * <p>The StreamRune event store serialises every domain event (and snapshot state) through Jackson,
 * and its {@code CryptoShreddingModule} inspects each serialised record via {@link
 * Class#getRecordComponents()} to discover {@code @Encrypted} components — a call that fails in a
 * native image unless the record's component accessor methods are registered at build time. Spring
 * Boot's AOT engine auto-registers the request/response types reachable from
 * {@code @RestController} signatures, but the event and state records are reached only dynamically
 * through the event store, so it cannot see them.
 *
 * <p>Delegates to the framework helper {@link
 * StreamRuneRuntimeHints#registerDomainPackages(RuntimeHints, ClassLoader, String...)}, which scans
 * the framework-agnostic domain modules and registers every concrete type found — events, commands,
 * aggregate states, value objects (e.g. {@code Money}), query/view DTOs and enums. New records are
 * covered automatically; over-registration (e.g. a never-serialised decider) is harmless.
 *
 * <p>Wired via {@code @ImportRuntimeHints(EcommerceNativeHints.class)} on {@link StreamRuneConfig}.
 */
public class EcommerceNativeHints implements RuntimeHintsRegistrar {

  /** Base packages of the framework-agnostic domain modules shared by all three runtime apps. */
  private static final String[] DOMAIN_PACKAGES = {
    "org.streamrune.ecommerce.domain",
    "org.streamrune.ecommerce.commands",
    "org.streamrune.ecommerce.queries",
    "org.streamrune.ecommerce.projections",
  };

  @Override
  public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
    StreamRuneRuntimeHints.registerDomainPackages(hints, classLoader, DOMAIN_PACKAGES);
  }
}
```

and annotate `StreamRuneConfig` with `@ImportRuntimeHints(EcommerceNativeHints.class)` (import `org.springframework.context.annotation.ImportRuntimeHints`), next to `@Configuration`. A JVM run ignores both; Chapter 17 builds the native image that reads them.

A runner you define yourself **replaces** the framework's default one, on all three frameworks, and the framework then starts nothing of that type: the runner is yours to start and stop. On Spring the bean method above calls `runner.start()` (Spring closes it at shutdown). The demo's Quarkus and Micronaut apps define their runner the same way (the policy the framework builds their command bus with, from `streamrune.dead-letter.*`: 5 attempts, 60 s initial backoff and `OPERATOR_ONLY` unless configured; the inbox window; the metrics bean; `app.dead-letter.poll-interval`; the same crypto-aware mapper) and start it from their `EcommerceSubscriptionLifecycle`, next to the saga subscriptions, closing it at shutdown; their `DeadLetterRetryRunnerOwnershipIT` checks that the demo's runner is running and that the framework built none of its own. Their command bus comes from the framework, which hands it the `DeadLetterQueue` bean, the crypto-aware mapper and the runner's policy and inbox window by itself. In all three apps `CustomerCommandDeadLetterIT` makes the event store refuse a customer's event, then checks that the dead-lettered command holds no plaintext, that a replay registers the customer with the original values, and that after a forget the queued copy reads `[REDACTED]` and is not replayed. `DeadLetterReplayPolicyIT` makes the store refuse a `StockAdjusted` event and checks the policy: a keyed adjustment is replayed on the schedule, and is not applied a second time when its caller already retried it with the same key; an unkeyed one stays in the queue, is counted by `awaiting_operator`, and is refused by a plain Retry.

Without `.pollInterval(...)` the runner polls every minute. The batch size defaults to 10, configurable with `.batchSize(20)`.

The bean reads its interval from `app.dead-letter.poll-interval`, a minute unless configured. Add the key to `spring-app/src/main/resources/application.yml`, under the `app` block of Chapter 11:

```yaml
app:
  dead-letter:
    poll-interval: 60s
```

### Step 3b: Give the stock adjustment an idempotency key

`PUT /api/products/{id}/stock` sends `AdjustStock`, a change *by* a quantity, and `POST /api/inventory/{productId}/receive` sends `ReceiveShipment`, which adds to the inventory. A client that sends either request twice — after a timeout, or from a retry button — changes the stock twice, and so would a replay of its dead-letter entry. The other writes of the demo do not have this problem: a create names the id the client chose, and a second create answers `400 already exists`; a price is set, not changed by an amount; shipping, delivering and cancelling move an order from one state to the next, and the decider refuses the second move.

The two endpoints take an optional `Idempotency-Key` header. With it the command goes through `commandBus.execute(command, key)`: the bus runs the command once per key and answers a repeat with the recorded result. Without it the request runs unkeyed, like every other command of the demo. The value comes from the client, and a key is a bearer token for the result recorded under it, so the key is built inside the caller's namespace with `IdempotencyKey.scopedTo` — two admins who happen to send the same value get two keys. The three apps share the helper, in the `commands` module:

```java
package org.streamrune.ecommerce.commands;

import java.util.Objects;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.UserId;

/**
 * Dispatches a request's command under the {@code Idempotency-Key} the client sent, or without a
 * key when it sent none. The three apps call it from the endpoints whose command changes a quantity
 * by an amount ({@code PUT /api/products/{id}/stock}, {@code POST
 * /api/inventory/{productId}/receive}): applied twice, such a command is a different stock. A
 * client that sends such a request again — after a timeout, from a retry button — sends the same
 * key, and the command bus runs the command once and answers the repeat with the recorded result.
 *
 * <p>The key is also what makes the command safe to replay from the dead-letter queue: an entry
 * keeps the key, and the replay runs under it, so a replay after the caller's own successful retry
 * is answered by the command inbox instead of applying the change a second time. The retry runner
 * therefore replays keyed entries on its schedule and leaves unkeyed ones to an operator.
 *
 * <p><b>The key is the caller's.</b> The value comes from the client, so the bus would hand the
 * result recorded under it to anyone who presents it ({@link IdempotencyKey#scopedTo}). The key is
 * built inside the caller's namespace, their user id: two callers who send the same value get two
 * keys.
 */
public final class IdempotentDispatch {

  /** The request header a client puts the key in. */
  public static final String HEADER = "Idempotency-Key";

  private IdempotentDispatch() {}

  /**
   * The key the bus records the command under, or {@code null} when the request carries no {@code
   * Idempotency-Key} header.
   *
   * @param header the header's value, {@code null} when absent
   * @param caller the identity of the caller; required when a header is present
   * @throws IllegalArgumentException when the value is blank, longer than the framework accepts or
   *     holds a control character, or when the caller has no identity (the apps answer {@code 400})
   */
  public static IdempotencyKey keyFor(String header, UserId caller) {
    if (header == null) {
      return null;
    }
    if (caller == null) {
      throw new IllegalArgumentException("An Idempotency-Key needs the caller's X-User-Id");
    }
    return IdempotencyKey.scopedTo(caller.value(), header);
  }

  /** Runs {@code command} under {@code key}, or unkeyed when {@code key} is {@code null}. */
  public static CommandBus.CommandResult execute(
      CommandBus commandBus, Command command, IdempotencyKey key) {
    Objects.requireNonNull(commandBus, "commandBus is required");
    return key == null ? commandBus.execute(command) : commandBus.execute(command, key);
  }
}
```

The stock handler of `ProductCommandController` binds the header and dispatches through the helper:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The product commands: the catalogue is the operators', so every write is for an {@code ADMIN}
 * ({@code 403} for everyone else). Creating a product, adjusting its stock and changing its price
 * ask {@link OwnerOrAdminAccess#requireAdmin} as their first line; discontinuing is guarded in the
 * command bus by {@code @RequireRole("ADMIN")} on the command. Reading the catalogue is open
 * ({@code ProductQueryController}).
 */
@RestController
@RequestMapping("/api/products")
public class ProductCommandController {

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> createProduct(@Valid @RequestBody CreateProductRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD"),
            req.initialStock()));
    return ResponseEntity.ok().build();
  }

  /**
   * A change of the stock by {@code quantity}: sent twice, it is applied twice, unless the client
   * sends an {@code Idempotency-Key}. With the header the bus runs the command once per key and
   * answers a repeat with the recorded result, and a dead-lettered adjustment is replayed under the
   * same key, so the retry runner replays it on its schedule.
   */
  @PutMapping("/{id}/stock")
  public ResponseEntity<Void> adjustStock(
      @PathVariable String id,
      @RequestHeader(name = IdempotentDispatch.HEADER, required = false) String idempotencyKey,
      @Valid @RequestBody AdjustStockRequest req) {
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    IdempotentDispatch.execute(
        commandBus,
        new ProductCommand.AdjustStock(id, req.quantity(), req.reason()),
        IdempotentDispatch.keyFor(idempotencyKey, caller.userId()));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}/price")
  public ResponseEntity<Void> updatePrice(
      @PathVariable String id, @Valid @RequestBody UpdatePriceRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new ProductCommand.UpdatePrice(id, new Money(req.price(), "USD")));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/discontinue")
  public ResponseEntity<Void> discontinueProduct(@PathVariable String id) {
    commandBus.execute(new ProductCommand.DiscontinueProduct(id));
    return ResponseEntity.ok().build();
  }

  public record CreateProductRequest(
      @NotBlank String productId,
      @NotBlank String name,
      String description,
      String category,
      @NotNull @Positive BigDecimal price,
      int initialStock) {}

  public record AdjustStockRequest(int quantity, String reason) {}

  public record UpdatePriceRequest(@NotNull @Positive BigDecimal price) {}
}
```

`InventoryCommandController#receiveShipment` (Chapter 11) does the same with `ReceiveShipment`:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Receiving a shipment changes the stock every order reserves from: for an {@code ADMIN} only. */
@RestController
@RequestMapping("/api/inventory")
public class InventoryCommandController {
  private final VirtualThreadCommandBus commandBus;

  public InventoryCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  /**
   * Adds {@code quantity} to the inventory: like a stock adjustment, it is applied once per {@code
   * Idempotency-Key} the client sends, and every time without one.
   */
  @PostMapping("/{productId}/receive")
  public ResponseEntity<Void> receiveShipment(
      @PathVariable String productId,
      @RequestHeader(name = IdempotentDispatch.HEADER, required = false) String idempotencyKey,
      @Valid @RequestBody ReceiveShipmentRequest req) {
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    IdempotentDispatch.execute(
        commandBus,
        new InventoryCommand.ReceiveShipment(productId, req.quantity()),
        IdempotentDispatch.keyFor(idempotencyKey, caller.userId()));
    return ResponseEntity.ok().build();
  }

  public record ReceiveShipmentRequest(@Positive int quantity) {}
}
```

A key that is blank, that is longer than 512 characters together with the caller's id (the scoped key is `<userId>|<key>`), or that holds a control character is refused with `400`, and so is a key presented again for another product: the bus never hands one stream's result to another. Try it — the second request answers `200` and leaves the stock of `p-1` as the first one set it (`curl http://localhost:8080/api/products/p-1` shows it five above where it was):

```bash
# One key for one intended change, this restock of five: mint it once, and send the request again
# with the same key after a lost answer or a 5xx, until a 2xx or a 4xx answers it.
KEY=$(uuidgen)
for i in 1 2; do
  curl -s -o /dev/null -w '%{http_code}\n' -X PUT http://localhost:8080/api/products/p-1/stock \
    -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" -H "Idempotency-Key: $KEY" \
    -H "Content-Type: application/json" -d '{"quantity": 5, "reason": "restock"}'
done
```

**One key per intended change.** The key belongs to the change you mean, not to a request: the block mints `KEY` once for the restock of five, and every request of that restock carries it. A request that fails is sent again with the same key. A new key would be a new restock.

**A failure that left an entry is pending.** The key is what makes a failed adjustment safe to replay: the bus stores it with the dead-letter entry, the runner replays the entry under it, and a request sent again with the same key meets the replay in the command inbox — whichever comes first runs the command, and the other is answered with the recorded result. So a `5xx` to a keyed request does not say "not applied, start over": the command may be queued for a replay that applies it. A request sent under a new key is a second operation, which the replay applies beside the first one: the stock goes up by ten. The answer therefore says what happened. `CommandRefusal` (Chapter 12) answers `CommandDeadLetteredException` before it looks at anything else, whatever the command failed on — a lock timeout or a version conflict that left an entry is pending as well — and by what happens to the entry, the exception's `pendingOutcome()`:

| What the bus did | Status | Body | What the client does |
|---|---|---|---|
| Dead-lettered a keyed command whose entry the runner replays (`QUEUED_FOR_REPLAY`) | `503` with `Retry-After: 60` | `{"reason": "QUEUED_FOR_REPLAY", "commandId": ..., "message": ...}` | Waits for the replay, or sends it again with the same key. Either way the command runs once. |
| Dead-lettered an unkeyed command (`AWAITING_OPERATOR`) | `500`, no `Retry-After` | `{"reason": "AWAITING_OPERATOR", "commandId": ..., "message": ...}` | Does not send it again: an operator replays or discards the entry (Step 5), and a request sent as well would apply it a second time. |
| Answered a keyed command sent again, which failed again, from the entry an operator holds: its replays are used up, or it is older than the inbox window (`AWAITING_OPERATOR`) | `500`, no `Retry-After` | `{"reason": "AWAITING_OPERATOR", "commandId": ..., "message": ...}`, the entry's `commandId` | Waits for the operator, who replays or discards the entry; the queue keeps the one entry for the key. |
| Refused it without an entry (Chapter 12, Step 2b) | `503` with `Retry-After: 5` | `{"reason": "CIRCUIT_BREAKER_OPEN", ...}` and the others | Sends it again later. |
| Failed without an entry: a business rejection, a store error with the entry's write failing too | `400`, or `500` | the error | Treats a `400` as final; a `500` says nothing was recorded. |

Neither answer is `202 Accepted`: a client takes a `2xx` for done and never sends the request again, while a replay can fail in its turn and be kept for an operator. The `commandId` is the entry's id, the one `POST /api/admin/dead-letters/{id}/retry` takes (Step 5). An answer that awaits an operator is a `500` without `Retry-After` rather than a `503`: a client that retries every `503` on its own would send the command again while the entry waits, and for an unkeyed command an operator's replay would then apply it twice. The queued answer's `Retry-After` is a minute, not the five seconds of a refusal: the retry runner polls for due entries once a minute (`app.dead-letter.poll-interval`) and an entry is due after the policy's initial delay, so a request sent again sooner finds the entry still queued and gains nothing; waiting for the replay applies the command as a send under the same key does. A send under the key that fails again stores nothing: the bus answers it from the entry the queue holds (Step 2).

`CommandDeadLetteredException` is in `streamrune-runtime`. Add it to `commands/build.gradle.kts`, inside the `dependencies { }` block:

```kotlin
// commands/build.gradle.kts — inside the dependencies { } block
implementation("org.streamrune:streamrune-runtime:$sr")
```

`CommandRefusal` with the two new reasons, the entry's id and the status of each answer, the whole file:

```java
package org.streamrune.ecommerce.commands;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.runtime.CommandDeadLetteredException;

/**
 * A command the framework did not apply, for a reason that passes or with an outcome that is still
 * pending, and how the three apps answer it: a status, {@code Retry-After} when the client may send
 * the request again, and a JSON body that names the reason and says what happened, {@code
 * {"reason": "CIRCUIT_BREAKER_OPEN", "message": "..."}}. Every other failure keeps its own answer:
 * a broken business rule is {@code 400}, a refused caller {@code 403}, and a genuine failure that
 * left nothing behind, a store error with no dead-letter queue to record it, {@code 500}.
 *
 * <p>The five refusals are answered {@code 503 Service Unavailable} with {@code Retry-After:}
 * {@value #RETRY_AFTER_SECONDS}: the same request succeeds later.
 *
 * <ul>
 *   <li>{@link CircuitBreakerOpenException} — the command bus's circuit breaker is open (or
 *       half-open with its probe in flight) after consecutive infrastructure failures, and refuses
 *       every command until a probe succeeds. Nothing was attempted. The breaker does not say how
 *       long it stays open, so {@code Retry-After} is the same short wait as for the others.
 *   <li>{@link CommandBusOverloadedException} — the bus's budget of in-flight asynchronous commands
 *       is used up. Nothing was attempted. Only {@code executeAsync} has the budget; the demo's
 *       routes run commands synchronously, so it is mapped for completeness.
 *   <li>{@link CommandBusClosedException} — the bus is closing (the application is shutting down)
 *       and admits no new command. Nothing was attempted; another instance, or this one restarted,
 *       runs the command when it is sent again.
 *   <li>{@link LockException} — the command's aggregate lock could not be taken within the lock
 *       timeout ({@code streamrune.lock-timeout}), on every attempt of the bus's retry policy: a
 *       command on the same aggregate held it all along, or the locker could not reach its store.
 *       The decider never ran and nothing was written. A later attempt takes the lock once it is
 *       free; nothing in the request is wrong, so the answer is {@code 503} with {@code
 *       Retry-After} and not {@code 409 Conflict}, which tells a client its request conflicts with
 *       the state of the resource. With a dead-letter queue the bus records the command there and
 *       throws {@link CommandDeadLetteredException} instead (below); the lock's own exception
 *       reaches a controller when there is no queue or the entry could not be stored.
 *   <li>{@link OptimisticLockException} — the aggregate's stream had moved past the version the
 *       command was decided on, on every attempt of the bus's retry policy: another writer appended
 *       to the stream between the command's load and its append each time (an instance whose lock
 *       is its own, a writer outside the command bus). Nothing was written. The bus reloads the
 *       aggregate on each attempt, and the client sent no version that could conflict, so the
 *       answer is the lock's: {@code 503} with {@code Retry-After}, the same request succeeding
 *       later, and with a dead-letter queue the bus records the command there like the lock's. Only
 *       the command bus's append conflict reaches a controller; the subtypes the framework raises
 *       for projections and saga rows are thrown on the threads of subscriptions.
 * </ul>
 *
 * <p>{@link CommandDeadLetteredException} is not a refusal: the command failed, its entry is in the
 * dead-letter queue, and a replay of the entry can still apply it. Its outcome is pending, and the
 * answer says what the client does about it, whatever the failure was, a lock timeout and a version
 * conflict included: what happens to the entry, the exception's {@code pendingOutcome()}, decides
 * it. The body carries the entry's {@code commandId}. The queue keeps one entry for a keyed
 * command: a send under the key that fails again stores nothing new and is answered from the entry
 * the first one stored.
 *
 * <ul>
 *   <li><b>Queued for replay</b> ({@code QUEUED_FOR_REPLAY}): a command that ran under an {@code
 *       Idempotency-Key}, whose entry the retry runner replays on its schedule under the key.
 *       {@code 503} with {@code Retry-After:} {@value #QUEUED_RETRY_AFTER_SECONDS}: the client
 *       waits for the replay, or sends the request again with the same key. The command inbox runs
 *       the command once, whichever of the two comes first. A new key is a new operation, and the
 *       replay applies the first one beside it.
 *   <li><b>Awaiting an operator</b> ({@code AWAITING_OPERATOR}), who replays or discards the entry:
 *       a command that ran without a key, since nothing pairs a replay with a request the client
 *       sends again and the two would apply the command twice, and a keyed command whose entry the
 *       schedule does not replay, its replays used up or its command first received longer ago than
 *       the command-inbox window. {@code 500} without {@code Retry-After}: the client does not send
 *       it again on its own, and a send under the key meanwhile is answered the same.
 * </ul>
 *
 * <p>Neither is {@code 202 Accepted}: a client takes a {@code 2xx} for done and never sends the
 * request again, while a replay can fail in its turn and be kept for an operator.
 *
 * @param reason what refused the command, or what happens to it now
 * @param message what the body tells the client
 * @param commandId the dead-letter entry's id, the {@code id} of {@code POST
 *     /api/admin/dead-letters/{id}/retry}; {@code null} for a refusal, which leaves no entry
 */
public record CommandRefusal(Reason reason, String message, String commandId) {

  /** How long a client waits before it sends a refused command again, in seconds. */
  public static final int RETRY_AFTER_SECONDS = 5;

  /**
   * How long a client waits before it sends a command queued for replay again, in seconds. The
   * retry runner polls for due entries once a minute ({@code app.dead-letter.poll-interval}) and an
   * entry is due after the policy's initial delay, so a send before then finds the entry still
   * queued and gains nothing; waiting for the replay applies the command as a send under the same
   * key does.
   */
  public static final int QUEUED_RETRY_AFTER_SECONDS = 60;

  /** What refused the command, or what happens to it now; the body's {@code reason}. */
  public enum Reason {
    CIRCUIT_BREAKER_OPEN,
    COMMAND_BUS_OVERLOADED,
    COMMAND_BUS_CLOSED,
    AGGREGATE_LOCK_UNAVAILABLE,
    AGGREGATE_VERSION_CONFLICT,
    QUEUED_FOR_REPLAY,
    AWAITING_OPERATOR
  }

  /**
   * The answer {@code failure} gets, or empty when it is none of these: a failure the app answers
   * otherwise. A dead-lettered command is told apart before its cause is looked at: a lock timeout
   * that left an entry is pending, not refused.
   *
   * @param failure what the command bus threw
   */
  public static Optional<CommandRefusal> of(Throwable failure) {
    return switch (failure) {
      case CommandDeadLetteredException deadLettered -> Optional.of(pending(deadLettered));
      case CircuitBreakerOpenException _ ->
          refusal(
              Reason.CIRCUIT_BREAKER_OPEN,
              "The command bus refuses commands while its circuit breaker is open.");
      case CommandBusOverloadedException _ ->
          refusal(Reason.COMMAND_BUS_OVERLOADED, "The command bus admits no more commands now.");
      case CommandBusClosedException _ ->
          refusal(
              Reason.COMMAND_BUS_CLOSED,
              "The application is shutting down and admits no more commands.");
      case LockException _ ->
          refusal(
              Reason.AGGREGATE_LOCK_UNAVAILABLE,
              "The aggregate's lock could not be taken in time.");
      case OptimisticLockException _ ->
          refusal(
              Reason.AGGREGATE_VERSION_CONFLICT,
              "The aggregate changed while the command ran; nothing was written.");
      case null, default -> Optional.empty();
    };
  }

  /**
   * The HTTP status: {@code 500 Internal Server Error} for a command that awaits an operator,
   * {@code 503 Service Unavailable} for every other answer.
   */
  public int status() {
    return reason == Reason.AWAITING_OPERATOR ? 500 : 503;
  }

  /**
   * Whether the client may send the request again: the answer then carries {@code Retry-After}
   * ({@link #retryAfterSeconds()}). A command that awaits an operator is not sent again.
   */
  public boolean retryLater() {
    return status() == 503;
  }

  /**
   * The answer's {@code Retry-After}, in seconds, when {@link #retryLater()}: {@value
   * #QUEUED_RETRY_AFTER_SECONDS} for a command queued for replay, {@value #RETRY_AFTER_SECONDS} for
   * a refusal.
   */
  public int retryAfterSeconds() {
    return reason == Reason.QUEUED_FOR_REPLAY ? QUEUED_RETRY_AFTER_SECONDS : RETRY_AFTER_SECONDS;
  }

  /**
   * The JSON body: {@code reason}, then {@code commandId} when there is an entry, {@code message}.
   */
  public Map<String, String> body() {
    Map<String, String> body = new LinkedHashMap<>();
    body.put("reason", reason.name());
    if (commandId != null) {
      body.put("commandId", commandId);
    }
    body.put("message", message);
    return body;
  }

  /** The answer to a dead-lettered command, by what happens to its entry. */
  private static CommandRefusal pending(CommandDeadLetteredException deadLettered) {
    String commandId = deadLettered.commandId().value();
    return switch (deadLettered.pendingOutcome()) {
      case QUEUED_FOR_REPLAY ->
          new CommandRefusal(
              Reason.QUEUED_FOR_REPLAY,
              "The command is not applied yet: it is queued for a replay under its Idempotency-Key."
                  + " Wait for the replay, or send it again with the same key; a new key is a new"
                  + " operation.",
              commandId);
      case AWAITING_OPERATOR ->
          new CommandRefusal(
              Reason.AWAITING_OPERATOR,
              "The command failed and is recorded for an operator, who replays or discards it: it"
                  + " ran without a key, or its entry's replays are used up or it is older than the"
                  + " inbox window. Do not send it again until then.",
              commandId);
    };
  }

  private static Optional<CommandRefusal> refusal(Reason reason, String message) {
    return Optional.of(new CommandRefusal(reason, message, null));
  }
}
```

`GlobalExceptionHandler` maps the new type in the handler of Chapter 12, which now takes its status from the refusal and sends `Retry-After` only when the client may send the request again. Spring picks a handler by the thrown type before it looks at the cause, so the dead-lettered command reaches this handler as itself:

```java
package org.streamrune.ecommerce.spring.config;

import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.DomainException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.ecommerce.commands.CommandRefusal;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;
import org.streamrune.runtime.CommandDeadLetteredException;

@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(DomainException.class)
  public ResponseEntity<String> handleDomainException(DomainException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }

  /**
   * A command the caller's role does not allow ({@code @RequireRole}, checked in the command bus):
   * {@code 403}, the status {@link #handleAccessRefused} answers with. {@code
   * AuthorizationException} is a {@code DomainException}; this handler is the more specific one, so
   * a refusal is not reported as the {@code 400} of a broken business rule.
   */
  @ExceptionHandler(AuthorizationException.class)
  public ResponseEntity<String> handleAuthorization(AuthorizationException e) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }

  /**
   * Terminal erasure: a command tried to encrypt PII for a crypto-shredded subject (e.g.
   * re-registering a forgotten customer id). The subject was erased under GDPR Article 17, so the
   * write is refused — 410 Gone is the honest status for a resource that was deliberately removed.
   */
  @ExceptionHandler(SubjectForgottenException.class)
  public ResponseEntity<String> handleSubjectForgotten(SubjectForgottenException e) {
    return ResponseEntity.status(HttpStatus.GONE).body(e.getMessage());
  }

  /**
   * The caller may not read or change what the request names ({@code OwnerOrAdminAccess}): {@code
   * 403}, the same answer for a request without an identity and for one with the wrong identity.
   */
  @ExceptionHandler(AccessRefusedException.class)
  public ResponseEntity<String> handleAccessRefused(AccessRefusedException e) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
  }

  /**
   * A command the framework refused for a reason that passes ({@link CommandRefusal}): an open
   * circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock
   * that could not be taken in time, a version conflict on every attempt. {@code 503} with {@code
   * Retry-After} and a JSON body naming the reason, the answer the Quarkus and Micronaut apps give
   * too.
   *
   * <p>A command the bus dead-lettered ({@link CommandDeadLetteredException}, whatever it failed
   * on) is pending, and the body carries its entry's {@code commandId}. What happens to the entry,
   * {@link CommandDeadLetteredException#pendingOutcome()}, decides the answer: {@code 503} with
   * {@code Retry-After} and {@code QUEUED_FOR_REPLAY} while the entry is replayed under its {@code
   * Idempotency-Key}, {@code 500} with {@code AWAITING_OPERATOR} and no {@code Retry-After} when an
   * operator decides about it, for a command that ran without a key or a keyed one whose entry's
   * replays are used up or that is older than the inbox window. Spring picks this handler for the
   * dead-lettered command's own type before it looks at the cause. A genuine failure that left no
   * entry, such as a store error, is none of these and stays {@code 500}.
   */
  @ExceptionHandler({
    CircuitBreakerOpenException.class,
    CommandBusOverloadedException.class,
    CommandBusClosedException.class,
    LockException.class,
    OptimisticLockException.class,
    CommandDeadLetteredException.class
  })
  public ResponseEntity<Map<String, String>> handleCommandRefused(RuntimeException e) {
    CommandRefusal refusal =
        CommandRefusal.of(e).orElseThrow(() -> new IllegalStateException("not a refusal", e));
    ResponseEntity.BodyBuilder answer = ResponseEntity.status(refusal.status());
    if (refusal.retryLater()) {
      answer.header(HttpHeaders.RETRY_AFTER, String.valueOf(refusal.retryAfterSeconds()));
    }
    return answer.contentType(MediaType.APPLICATION_JSON).body(refusal.body());
  }
}
```

The Quarkus and Micronaut apps answer the same: JAX-RS and Micronaut pick a mapper by the thrown type, so each app has one more, `CommandDeadLettered`, nested in `CommandRefusalExceptionMappers` and `CommandRefusalExceptionHandlers`; a lock timeout or a version conflict that the bus dead-lettered reaches it, not the lock's mapper. `CommandRefusalStatusIT` in each app sends a keyed and an unkeyed stock adjustment while the product's lock is held, and again while every append meets a version conflict, and checks the two answers and the entries they name; with the entry's write refused as well, the version conflict reaches the caller as itself, `AGGREGATE_VERSION_CONFLICT`. `DeadLetterReplayPolicyIT` checks the answer to every adjustment it dead-letters.

The frontend's Adjust Stock and Receive Shipment dialogs keep the key of a change until it is answered for good, and say that a `QUEUED_FOR_REPLAY` change will be applied (Chapter 16). `IdempotencyKeyIT` in each app checks the rules above.

### Step 4: Configure `ProjectionErrorStrategy.DLQ` per projection (optional production hardening)

> **The real demo's `MultiProjectionRunner` does not use `PostgresProjectionDeadLetterStore`.** All four projections use the default error strategy (`HALT`). The per-projection DLQ wiring below is an optional production hardening step — include it in your project if you want failed event batches stored durably instead of halting the projection.

The error strategy is set **per registration** using the four-argument `register` overload — name, projection, error strategy, delivery mode. To enable DLQ for the orders projection, give the `projectionRunner` bean a `PostgresProjectionDeadLetterStore projectionDlq` parameter, hand it to the builder, and register the orders projection with the strategy (the import is `org.streamrune.core.projection.ProjectionErrorStrategy`, since `StreamRuneConfig` imports `org.streamrune.core.projection` types individually rather than with a wildcard). The lines of the builder that change:

```java
.deadLetterStore(projectionDlq) // required when any registration uses DLQ
.register(
    "orders",
    new CacheAwareProjection(orderProjection, cacheInvalidator),
    ProjectionErrorStrategy.DLQ,
    ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
```

Two things to note:

- **`deadLetterStore` on the builder is shared** across all registrations that use `DLQ` strategy. The builder validates at `build()` time that a `deadLetterStore` is supplied whenever any registration requests `DLQ` — it throws `IllegalArgumentException` otherwise.
- **If you are not using DLQ**, omit the `projectionDlq` parameter and the `.deadLetterStore(...)` call and leave every `register` call in the three-argument form — name, projection, delivery mode (as in Chapters 5–13).

When `orderProjection.process(batch)` throws, the internal runner writes a `ProjectionDeadLetterEntry` containing the projection name, the offset range of the failed batch, the exception class, the exception message, and the timestamp — then continues processing the next batch. The other three projections are unaffected.

### Step 5: Extend `AdminController` with Dead-Letter and Outbox Endpoints

`AdminController` was created in Chapter 12 with only the circuit-breaker endpoint and the payment gateway's two switches. Now that both `DeadLetterQueue` and `OutboxStore` beans exist, extend it to add the dead-letter and outbox endpoints. Step 5b below adds a further round of saga dead-letter endpoints once the `SagaDeadLetterReplayer` bean exists.

Replace the existing `AdminController` with the full version:

```java
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
      // Nothing ran and the entry is untouched, so the operator tries again later: the bus admits
      // no more work (ADMISSION_REFUSED), or this replica's code reads the command's aggregate at
      // an older event schema than one it holds, during a rolling deployment (READER_BEHIND).
      case ADMISSION_REFUSED, READER_BEHIND ->
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

  /**
   * Switches the payment gateway's outage on or off. While it is on the gateway answers no call: a
   * new payment stays {@code PENDING} and its event is delivered again, the gateway breaker opens
   * after three calls without an answer, and the payments go through once it is off again.
   */
  @PostMapping("/payment-outage/toggle")
  public Map<String, Boolean> togglePaymentOutage() {
    requireAdmin();
    boolean current = paymentGateway.isOutage();
    paymentGateway.setOutage(!current);
    return Map.of("outage", !current);
  }

  @GetMapping("/payment-outage")
  public Map<String, Boolean> paymentOutageState() {
    requireAdmin();
    return Map.of("outage", paymentGateway.isOutage());
  }

  /** Open to every caller: the answer says that the application runs and nothing else. */
  @GetMapping("/health")
  public Map<String, String> health() {
    return Map.of("status", "UP");
  }
}
```

> The constructor also injects `PaymentGatewayCircuitBreaker` (from Chapter 12), so `/circuit-breaker` reports both the command-bus breaker (`state`) and the payment-gateway breaker (`paymentGateway`). It takes the outbox replayer as an `ObjectProvider<OutboxFailedReplayer>` rather than the replayer itself: the framework auto-configures `OutboxFailedReplayer` only while `streamrune.outbox.enabled` is `true`, so with the outbox switched off the controller still starts and the two outbox operations answer `503`.

`deadLetterQueue.read(limit)` returns entries ordered newest-first. Each `DeadLetterEntry` carries the original command payload as a JSON string, the command type name, error details, the number of original attempts, and the number of DLQ retry attempts so far. The `/retry` endpoint **re-dispatches the command through the bus on demand**: it calls `deadLetterRetryRunner.retry(new CommandId(id), force)`, which loads the entry, deserializes its command, and executes it under a `DLQ_REPLAY` marker plus the entry's original request context (correlation id, user). A guarded command such as `ShipOrder` replays too: the replay has no request for `HeaderUserRoleResolver` to read the role from, so the framework does not repeat the role check, and the command, authorized once before it was dead-lettered, runs under the dead-letter-replay system principal as the recorded user, with a `WARN` naming it (Chapter 9 explains why the resolver declares `requiresRequestContext()`). On success the entry is discarded; on failure its DLQ attempt count is incremented. The endpoint maps the runner's `RetryOutcome`:

| `RetryOutcome` | Answer | What it means |
|---|---|---|
| `REPLAYED` | `200 OK` | The command ran, or the inbox recognised its key; the entry is gone. |
| `NOT_FOUND` | `404 Not Found` | No entry has that id. |
| `NOT_DEDUPLICATED` | `409 Conflict` with a message | The command carries no idempotency key: if its caller already retried it, a replay applies it twice. |
| `STALE_REPLAY_BLOCKED` | `409 Conflict` with a message | A keyed entry older than the inbox window. |
| `ADMISSION_REFUSED` | `503` with `Retry-After: 5` | The command bus admits no more work. Nothing ran; the entry is unchanged. |
| `READER_BEHIND` | `503` with `Retry-After: 5` | This backend's code reads the command's aggregate at an older event schema than one the aggregate holds — a backend still on the previous release during a rolling deployment of an event-schema change. Nothing was appended, and the entry keeps its attempt count, so the deployment can neither exhaust nor discard it: only the time of its last replay changes, and it is replayed again after its backoff, by the schedule or a later retry, until a backend that reads the event runs it. |
| `FAILED` | `500` | The command failed again; the entry's attempt count went up. |

Without `force` the endpoint refuses exactly what the schedule leaves alone, so a plain click is never the second execution the policy of Step 3 prevents. A keyed, fresh entry the endpoint replays at once, as the schedule would on its next poll.

**The operator's two ways out.** A `409` leaves the decision to the operator, who looks at the command's effect — for a stock adjustment, whether the product's stock changed — and the message names both remedies. When the effect is missing, `?force=true` replays the entry, and the runner logs a `WARN` naming the operator. When it is present, the command already ran (its caller retried it), and `DELETE /api/admin/dead-letters/{id}` removes the entry through `deadLetterQueue.discard(commandId)`, so that nothing replays it; it answers `200 {"outcome": "DISCARDED"}`, or `404` when the entry is gone. Either way the entry leaves `streamrune.dlq.awaiting_operator`.

**The decision is audited before it is acted on.** `auditOperatorDecision` writes the decision into Chapter 10's `audit_log` (beside the command's own rows, shown by `GET /api/audit/commands`): command type `DLQ_FORCED_REPLAY` or `DLQ_DISCARDED`, the entry's command id and stream, and the operator. Both endpoints look the entry up first — that answers the `404` and supplies the stream — then write the row, and only then replay or discard. A row that cannot be written throws, so nothing is replayed or removed without its record; written afterwards, a crash or an audit store that is down for a moment would leave a second execution that names nobody. The row records the request, not its result: a forced replay that fails leaves the entry listed with one more attempt, and one that succeeds shows as the replayed command's own audit row. A `?force=true` for an entry the runner would have replayed anyway (a keyed, fresh one) is recorded too, because the operator asked for it. `DeadLetterReplayPolicyIT` makes the audit store refuse the row and checks that the entry is then neither replayed nor discarded.

```bash
# An unkeyed entry: 409 and the reason
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" \
  http://localhost:8080/api/admin/dead-letters/<commandId>/retry
# Its effect is missing: replay it
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" \
  "http://localhost:8080/api/admin/dead-letters/<commandId>/retry?force=true"
# Its effect is present: discard it instead
curl -s -X DELETE -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" \
  http://localhost:8080/api/admin/dead-letters/<commandId>
```

The `/outbox` endpoint lists `PENDING` entries with `outboxStore.findByStatus(OutboxStatus.PENDING, limit)`. It deliberately does **not** call `loadPending`: that method *claims* — it moves every returned row to `IN_PROGRESS` under the caller's lease, so an admin page refreshing the list would stall delivery of every listed aggregate for two minutes. `findByStatus` is read-only and safe while the relay polls.

`GET /api/admin/outbox/failed` lists the `FAILED` entries, oldest first — on the demo's strict channel each one is blocking its order, payment or product. Two mutating endpoints resolve them, both delegating to the framework's auto-configured `OutboxFailedReplayer`:

- `POST /api/admin/outbox/failed/{id}/replay` → `{"outcome":"REPLAYED"}` — the entry goes back to `PENDING` at its original position and is delivered before the entries that waited behind it. `NOT_FAILED` means someone already resolved it; `NOT_FOUND` means the id is unknown.
- `POST /api/admin/outbox/failed/{id}/skip` with `{"reason":"…"}` → `{"outcome":"SKIPPED"}` — the entry will never be delivered; the row records who, when and why, and the aggregate's later entries flow again. The **who** is `skippedBy`, and the controller does not read it from a header: it takes the user id the framework's request filter already resolved into `StreamRuneContext.CURRENT` — in this demo's trusted-gateway mode that is `X-User-Id` (Chapter 9) — exactly as `CustomerCommandController.currentRequesterId()` does for the GDPR audit. An `ADMIN` always has one: `requireAdmin()` refuses a caller without an identity before the handler reads it. `reason` is required.

**Every route of the controller but `/health` asks for an `ADMIN`.** `requireAdmin()` is the helper Chapter 12 gave the controller: it calls `OwnerOrAdminAccess.requireAdmin` (Chapter 9), which wants an identity and the `ADMIN` role — the `role` entry of the request context's baggage, the same entry `HeaderUserRoleResolver` reads, copied by the framework filter from `X-User-Role` — and answers `403` otherwise, also to `X-User-Role: ADMIN` without `X-User-Id`. It guards the operations — `dead-letters/{id}/retry`, `DELETE dead-letters/{id}`, the three saga recovery operations (`saga-dead-letters/replay`, `saga-dead-letters/replay-all`, `sagas/faulted/resume`), `DELETE saga-dead-letters`, `payment-failure/toggle`, `payment-outage/toggle` and the two new outbox operations — and the listings too: a dead-lettered command and an outbox entry carry the payload of somebody's command or event, and the saga listings name other people's orders. One route outside this controller flips the same switch as `payment-failure/toggle`: `SagaController`'s `POST /api/saga/fulfillments/{id}/inject-failure` shorthand (Chapter 11) calls `requireAdmin` too, so the payment-failure flag cannot be flipped without the role through either route. This is the same demo shortcut Chapter 9 is honest about: the role is whatever the client claims, so the check is a stand-in for the gateway that would authenticate the caller in a real deployment — without it the chapter would teach an admin surface that lets anyone read or drop a message. The frontend already sends `X-User-Role` on every call, so the Admin page keeps working when you pick the ADMIN persona.

Try it (the ids come from the `failed` listing):

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/outbox/failed" | jq .
curl -s -X POST -H "X-User-Id: ops-1" -H "X-User-Role: ADMIN" -H "Content-Type: application/json" \
  -d '{"reason":"the consumer's schema rejected the payload"}' \
  "http://localhost:8080/api/admin/outbox/failed/<entry-id>/skip"
# {"outcome":"SKIPPED"}
```

### Step 5b: Saga Dead-Letter Admin Endpoints

Chapter 11 introduced the `sagaDeadLetterStore` bean and explained that a poison saga event is quarantined in `saga_dead_letters` with the saga marked `FAULTED`. Without them, the only way to find a `FAULTED` saga or inspect its quarantine reason is a direct database query. The saga endpoints of the controller above close that gap — two listings (`listSagaDeadLetters`, `listFaultedSagas`), three recovery operations (`replayAllSagaDeadLetters`, `replaySagaDeadLetter`, `resumeFaultedSaga`) and `discardSagaDeadLetter` — on top of a new `SagaDeadLetterReplayer` bean added to `StreamRuneConfig` next to `orderFulfillmentSagaRunner`:

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

`ObjectProvider` and `StreamRuneProperties` are imported since Chapter 11; `SagaDeadLetterReplayer` and `StreamRuneMetrics` come with the `org.streamrune.runtime.*` and `org.streamrune.core.*` wildcards. That is the last change to `StreamRuneConfig` in this chapter. The whole class:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 14</summary>

```java
package org.streamrune.ecommerce.spring.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.streamrune.core.*;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.commands.customer.CustomerDecider;
import org.streamrune.ecommerce.commands.integration.EcommerceIntegrationEventMapper;
import org.streamrune.ecommerce.commands.inventory.InventoryDecider;
import org.streamrune.ecommerce.commands.order.OrderCancellationProcessManager;
import org.streamrune.ecommerce.commands.order.OrderDecider;
import org.streamrune.ecommerce.commands.order.OrderPricing;
import org.streamrune.ecommerce.commands.order.OrderShipmentProcessManager;
import org.streamrune.ecommerce.commands.payment.PaymentDecider;
import org.streamrune.ecommerce.commands.product.ProductDecider;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.commands.upcaster.ProductCreatedUpcaster;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.projections.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.query.ListProducts;
import org.streamrune.postgres.*;
import org.streamrune.runtime.*;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.spring.StreamRuneProperties;

@Configuration(proxyBeanMethods = false)
@ImportRuntimeHints(EcommerceNativeHints.class)
public class StreamRuneConfig {

  // Spring Boot 4.0 auto-configures Jackson 3.x (tools.jackson.databind.ObjectMapper).
  // StreamRune framework still uses Jackson 2.x — provide the legacy ObjectMapper explicitly.
  @Bean
  public ObjectMapper legacyObjectMapper() {
    return JsonMapper.builder().build();
  }

  /**
   * Per-subject AES-256 crypto engine backing the {@code @Encrypted} fields on {@code
   * CustomerEvent}. Wired into the event store below so PII is encrypted at rest in {@code
   * event_stream.payload}; deleting a subject's key (the forget flow) crypto-shreds that subject.
   * Its {@code encryption_keys}, {@code forgotten_subjects} and {@code erased_key_generations}
   * tables come from the framework's crypto migration series, which the event store factory applies
   * at startup next to its own ({@code streamrune.event-store.schema.auto-initialize}).
   */
  @Bean
  public PostgresCryptoEngine cryptoEngine(DataSource ds) {
    return PostgresCryptoEngine.builder().dataSource(ds).build();
  }

  /**
   * Exposes the event registry for the auto-configured {@code postgresEventStoreFactory} to pick up
   * via {@code ObjectProvider<EventTypeRegistry>}.
   */
  @Bean
  public EventTypeRegistry eventTypeRegistry() {
    return SimpleEventTypeRegistry.builder()
        // Product events
        .registerEvent(
            "ProductCreated",
            org.streamrune.ecommerce.domain.product.ProductEvent.ProductCreated.class)
        .registerEvent(
            "PriceUpdated", org.streamrune.ecommerce.domain.product.ProductEvent.PriceUpdated.class)
        .registerEvent(
            "StockAdjusted",
            org.streamrune.ecommerce.domain.product.ProductEvent.StockAdjusted.class)
        .registerEvent(
            "ProductDiscontinued",
            org.streamrune.ecommerce.domain.product.ProductEvent.ProductDiscontinued.class)
        // Order events
        .registerEvent(
            "OrderPlaced", org.streamrune.ecommerce.domain.order.OrderEvent.OrderPlaced.class)
        .registerEvent(
            "OrderConfirmed", org.streamrune.ecommerce.domain.order.OrderEvent.OrderConfirmed.class)
        .registerEvent(
            "OrderShipped", org.streamrune.ecommerce.domain.order.OrderEvent.OrderShipped.class)
        .registerEvent(
            "OrderDelivered", org.streamrune.ecommerce.domain.order.OrderEvent.OrderDelivered.class)
        .registerEvent(
            "OrderCancelled", org.streamrune.ecommerce.domain.order.OrderEvent.OrderCancelled.class)
        // Customer events
        .registerEvent(
            "CustomerRegistered",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.CustomerRegistered.class)
        .registerEvent(
            "ProfileUpdated",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.ProfileUpdated.class)
        .registerEvent(
            "DataExportRequested",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.DataExportRequested.class)
        .registerEvent(
            "CustomerForgotten",
            org.streamrune.ecommerce.domain.customer.CustomerEvent.CustomerForgotten.class)
        // Payment events
        .registerEvent(
            "PaymentInitiated",
            org.streamrune.ecommerce.domain.payment.PaymentEvent.PaymentInitiated.class)
        .registerEvent(
            "PaymentCaptured",
            org.streamrune.ecommerce.domain.payment.PaymentEvent.PaymentCaptured.class)
        .registerEvent(
            "PaymentRefunded",
            org.streamrune.ecommerce.domain.payment.PaymentEvent.PaymentRefunded.class)
        .registerEvent(
            "PaymentFailed",
            org.streamrune.ecommerce.domain.payment.PaymentEvent.PaymentFailed.class)
        .registerEvent(
            "PaymentVoided",
            org.streamrune.ecommerce.domain.payment.PaymentEvent.PaymentVoided.class)
        // Inventory events
        .registerEvent(
            "StockReserved",
            org.streamrune.ecommerce.domain.inventory.InventoryEvent.StockReserved.class)
        .registerEvent(
            "StockReleased",
            org.streamrune.ecommerce.domain.inventory.InventoryEvent.StockReleased.class)
        .registerEvent(
            "ReservationConfirmed",
            org.streamrune.ecommerce.domain.inventory.InventoryEvent.ReservationConfirmed.class)
        .registerEvent(
            "ShipmentReceived",
            org.streamrune.ecommerce.domain.inventory.InventoryEvent.ShipmentReceived.class)
        // Aggregate states, by simple class name: the bus snapshots a state under that name and
        // refuses to write a snapshot for a state the registry does not resolve.
        .registerState("ProductState", ProductState.class)
        .registerState("OrderState", OrderState.class)
        .registerState("CustomerState", CustomerState.class)
        .registerState("PaymentState", PaymentState.class)
        .registerState("InventoryState", InventoryState.class)
        .build();
  }

  /**
   * Exposes the product-created upcaster so the auto-configured {@code postgresEventStoreFactory}
   * can collect it via {@code ObjectProvider<EventUpcaster>}.
   */
  @Bean
  public EventUpcaster productCreatedUpcaster() {
    return new ProductCreatedUpcaster();
  }

  /**
   * Outbox store bean: needed by both the auto-configured {@code postgresEventStoreFactory} (so
   * outbox entries are written inside the same append transaction) and the auto-configured {@code
   * OutboxPoller} (so it can poll + claim pending entries).
   *
   * <p>The claim lease (2 minutes) must strictly exceed the {@link
   * org.streamrune.rabbitmq.RabbitMqOutboxPublisher}'s in-flight horizon: {@link
   * RabbitMqConfig#outboxPublisher} uses the default 30s {@code publishBlockBudget} plus its own
   * 30s {@code confirmTimeout}, for a 60s horizon, plus the framework's recommended 30s margin —
   * 90s minimum. {@code OutboxPoller.Builder.build()} refuses to boot (fails fast rather than
   * risking a duplicate-and-reorder publish) when the lease does not exceed the horizon.
   *
   * <p>STRICT_PER_AGGREGATE (the default, stated here so the choice is visible): the demo's outbox
   * carries order lifecycle, payment outcomes and stock changes — consumers that must not see n+1
   * before n. A terminal FAILED entry therefore blocks its aggregate until an operator replays or
   * skips it from the admin API (tutorial ch. 14). The mapper sets aggregateId on every entry,
   * which strict mode requires.
   */
  @Bean
  public PostgresOutboxStore outboxStore(DataSource ds) {
    return new PostgresOutboxStore(
        ds, Duration.ofMinutes(2), OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  /**
   * Curated, PII-safe integration event mapper. Consumed by the auto-configured {@code
   * postgresEventStoreFactory} alongside {@link #outboxStore} to enable transactional outbox
   * emission. Only allowlisted, non-PII events are forwarded to the broker.
   */
  @Bean
  public OutboxEventMapper outboxEventMapper() {
    return new EcommerceIntegrationEventMapper();
  }

  // Declared as the CONCRETE type: it is both the ProjectionRepository every projection writes
  // through and the AtomicBatchProcessor the runner commits with. Startup verifies the pairing:
  // every TRANSACTIONAL_LOCAL projection below must write to THIS object, and each one does.
  @Bean
  public JdbcProjectionRepository projectionRepository(DataSource ds) {
    return new JdbcProjectionRepository(ds);
  }

  @Bean
  public PostgresAuditStore auditStore(DataSource ds) {
    return new PostgresAuditStore(ds);
  }

  @Bean
  public PostgresSagaStore sagaStore(DataSource ds, ObjectMapper objectMapper) {
    return new PostgresSagaStore(ds, objectMapper);
  }

  /**
   * Quarantine store for poison saga events (a routing method that throws — {@code extractSagaId},
   * {@code correlateByEventData} — or an exception in the saga's own logic). Required by {@link
   * SagaRunner.Builder#sagaDeadLetterStore} below — without it the saga runner cannot be built.
   * Mirrors {@link #sagaStore}.
   */
  @Bean
  public PostgresSagaDeadLetterStore sagaDeadLetterStore(DataSource ds) {
    return new PostgresSagaDeadLetterStore(ds);
  }

  /**
   * Records commands processed under a caller-supplied {@code IdempotencyKey} so a redelivered saga
   * command (broker at-least-once redelivery) applies its events effectively once. Picked up
   * automatically by the framework's auto-configured {@code postgresEventStoreFactory} (for the
   * atomic keyed append) and wired explicitly into {@link #commandBus} below (saga command dispatch
   * requires a command bus with an inbox — see {@code SagaCommandDispatch}).
   */
  @Bean
  public PostgresCommandInbox commandInbox(DataSource ds) {
    return new PostgresCommandInbox(ds);
  }

  @Bean
  public HeaderUserRoleResolver userRoleResolver() {
    return new HeaderUserRoleResolver();
  }

  @Bean
  public PaymentGatewaySimulator paymentGateway() {
    return new PaymentGatewaySimulator();
  }

  // Interceptors
  @Bean
  public BeanValidationInterceptor validationInterceptor() {
    var factory = jakarta.validation.Validation.buildDefaultValidatorFactory();
    return new BeanValidationInterceptor(factory.getValidator());
  }

  @Bean
  public AnnotationAuthorizationInterceptor authInterceptor(HeaderUserRoleResolver resolver) {
    return new AnnotationAuthorizationInterceptor(resolver);
  }

  @Bean
  public AuditCommandInterceptor auditInterceptor(PostgresAuditStore store) {
    return new AuditCommandInterceptor(store);
  }

  @Bean
  public CircuitBreakerCommandInterceptor circuitBreaker() {
    return new CircuitBreakerCommandInterceptor(3, Duration.ofSeconds(30));
  }

  /**
   * Circuit breaker around the payment gateway call (used by {@link PaymentProcessManager}). Opens
   * after 3 consecutive gateway calls without an answer, then half-opens after a 30s cooldown so a
   * probe can recover. Distinct from {@code circuitBreaker} above, which guards the command bus
   * generally.
   */
  @Bean
  public PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker() {
    return new PaymentGatewayCircuitBreaker(3, Duration.ofSeconds(30));
  }

  /**
   * The aggregate lock, taken in PostgreSQL: an advisory lock per stream ({@code
   * pg_advisory_xact_lock}), held on a connection of the locker's own pool for the whole command.
   * Every backend on the database takes the same lock, so a command waits for a command on the same
   * aggregate on any backend; an in-process lock serializes the commands of one process only, and
   * two backends writing one aggregate would meet in version conflicts, retried and in the end
   * dead-lettered. A command holds its lock connection for its whole run and borrows a connection
   * of the application's pool for its load and its append, so the lock pool has the application
   * pool's size, ten (Hikari's default, as the demo sets none): as many commands as the application
   * pool serves at once. Each backend opens both pools, which together stay below PostgreSQL's
   * {@code max_connections}. A command waits for its lock up to {@code streamrune.lock-timeout},
   * and before that, when every connection of the lock pool is taken, up to ten seconds for one of
   * them (the pool's connection timeout); either wait that runs out is a {@code LockException}.
   *
   * <p>The framework's in-process locker steps aside for this bean
   * ({@code @ConditionalOnMissingBean(AggregateLocker.class)}). The locker owns its pool, which
   * Spring closes with it.
   */
  @Bean(destroyMethod = "close")
  public PgAdvisoryLocker aggregateLocker(
      @Value("${spring.datasource.url}") String url,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password) {
    return PgAdvisoryLocker.withDedicatedPool(url, username, password, 10);
  }

  /**
   * The command bus, with all interceptors, listed in the framework's canonical order ({@link
   * org.streamrune.runtime.CommandInterceptorOrdering}), the order the Quarkus and Micronaut
   * integrations give the chains they assemble. The bus runs {@code before()} in the listed order
   * and {@code after()}/{@code onError()} in reverse, and only on interceptors whose {@code
   * before()} completed. Audit comes first, so a command the authorization interceptor refuses
   * still reaches {@code audit.onError()} and is recorded as a {@code FAILURE}; authorization comes
   * before validation, so a caller without the role learns nothing about the validation rules; the
   * circuit breaker comes last, so it counts only execution failures.
   *
   * <p>A command that fails on infrastructure (a store or network error, not a business rejection)
   * is recorded in {@link #deadLetterQueue} before the error reaches the caller, and {@link
   * #deadLetterRetryRunner} replays it later. The bus writes the command as JSON with {@link
   * DeadLetterRetryRunner#createObjectMapper(org.streamrune.core.crypto.CryptoEngine)}, which
   * encrypts the {@code @Encrypted} fields of {@code RegisterCustomer} and {@code UpdateProfile}
   * under the customer's key: the queued copy of the customer's personal data is ciphertext and is
   * erased with the customer. {@link #legacyObjectMapper} has no crypto module and would store it
   * in plaintext. The queue keeps one entry for a keyed command: one that fails again under its key
   * is answered from the entry its earlier send stored, queued for replay or awaiting an operator
   * by the policy and the inbox window the runner replays by, which the bus takes too.
   *
   * <p>The bus takes the {@link AggregateLocker} bean, {@link #aggregateLocker}, and {@code
   * streamrune.lock-timeout} (5 s by default), as the bus the framework builds does: a command
   * waits that long for its aggregate's lock on each attempt before the bus gives up with a {@link
   * LockException}, which the app answers {@code 503} ({@link GlobalExceptionHandler}).
   */
  @Bean
  public VirtualThreadCommandBus commandBus(
      EventStore store,
      BeanValidationInterceptor validation,
      AnnotationAuthorizationInterceptor auth,
      AuditCommandInterceptor audit,
      CircuitBreakerCommandInterceptor circuitBreaker,
      PostgresCommandInbox commandInbox,
      PostgresDeadLetterQueue deadLetterQueue,
      PostgresCryptoEngine cryptoEngine,
      AggregateLocker locker,
      StreamRuneProperties properties) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(locker)
        .lockTimeout(properties.lockTimeout())
        // Required for saga command dispatch: SagaCommandDispatch calls
        // commandBus.execute(command, IdempotencyKey), which throws IllegalStateException
        // unless the bus has a commandInbox wired in.
        .commandInbox(commandInbox)
        .interceptors(audit, auth, validation, circuitBreaker)
        .deadLetterQueue(deadLetterQueue)
        .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
        // The rules deadLetterRetryRunner replays by: a keyed command that fails again under
        // its key is told whether the entry stored for the key is still replayed.
        .deadLetterRetryPolicy(DeadLetterRetryPolicy.DEFAULT)
        .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
        .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
        .register(
            ProductState.TYPE,
            ProductCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case ProductCommand.CreateProduct c -> c.productId();
                      case ProductCommand.UpdatePrice c -> c.productId();
                      case ProductCommand.AdjustStock c -> c.productId();
                      case ProductCommand.DiscontinueProduct c -> c.productId();
                    }),
            new ProductDecider())
        .register(
            OrderState.TYPE,
            OrderCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case OrderCommand.PlaceOrder c -> c.orderId();
                      case OrderCommand.ConfirmOrder c -> c.orderId();
                      case OrderCommand.ShipOrder c -> c.orderId();
                      case OrderCommand.DeliverOrder c -> c.orderId();
                      case OrderCommand.CancelOrder c -> c.orderId();
                    }),
            new OrderDecider())
        .register(
            CustomerState.TYPE,
            CustomerCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case CustomerCommand.RegisterCustomer c -> c.customerId();
                      case CustomerCommand.UpdateProfile c -> c.customerId();
                      case CustomerCommand.RequestDataExport c -> c.customerId();
                      case CustomerCommand.ForgetCustomer c -> c.customerId();
                    }),
            new CustomerDecider())
        .register(
            PaymentState.TYPE,
            PaymentCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case PaymentCommand.InitiatePayment c -> c.paymentId();
                      case PaymentCommand.CapturePayment c -> c.paymentId();
                      case PaymentCommand.FailPayment c -> c.paymentId();
                      case PaymentCommand.CancelPayment c -> c.paymentId();
                    }),
            new PaymentDecider())
        .register(
            InventoryState.TYPE,
            InventoryCommand.class,
            cmd ->
                AggregateId.of(
                    switch (cmd) {
                      case InventoryCommand.ReserveStock c -> c.productId();
                      case InventoryCommand.ReleaseStock c -> c.productId();
                      case InventoryCommand.ConfirmReservation c -> c.productId();
                      case InventoryCommand.ReceiveShipment c -> c.productId();
                    }),
            new InventoryDecider())
        .build();
  }

  // Projections
  @Bean
  public ProductProjection productProjection(ProjectionRepository repo) {
    return new ProductProjection(repo);
  }

  @Bean
  public OrderProjection orderProjection(ProjectionRepository repo) {
    return new OrderProjection(repo);
  }

  @Bean
  public CustomerProjection customerProjection(ProjectionRepository repo) {
    return new CustomerProjection(repo);
  }

  /**
   * Read-model purger for the customer subject. {@link ForgetSubjectService} runs it after the
   * encryption key is shredded, deleting the {@code customers_view} row so the forget removes the
   * plaintext PII the projection captured (crypto-shredding alone only kills the ciphertext).
   */
  @Bean
  public CustomerSubjectDataPurger customerSubjectDataPurger(ProjectionRepository repo) {
    return new CustomerSubjectDataPurger(repo);
  }

  /**
   * GDPR Article 17 orchestrator. {@link CustomerCommandController}'s forget endpoint calls {@code
   * forget(customerId, requester)} after emitting {@code ForgetCustomer}: step 1 deletes the
   * subject's key (crypto-shred + terminal-erasure tombstone), step 2 runs every registered purger
   * to remove the subject's read-model rows, step 3 evicts the {@link CachingQueryBus}, so no
   * cached query answer taken before the erasure is served after it. The audit store records each
   * outcome in {@code audit_log} (command_type {@code GDPR_FORGET}).
   */
  @Bean
  public ForgetSubjectService forgetSubjectService(
      PostgresCryptoEngine cryptoEngine,
      PostgresAuditStore auditStore,
      CustomerSubjectDataPurger customerPurger,
      DataSource dataSource,
      CachingQueryBus cachingQueryBus) {
    return ForgetSubjectService.builder()
        .cryptoEngine(cryptoEngine)
        .auditStore(auditStore)
        .purgers(List.of(customerPurger, customerSnapshotPurger(dataSource)))
        .queryCache(cachingQueryBus)
        .build();
  }

  /**
   * Purges the snapshots of a forgotten customer. A snapshot holds the aggregate's state as plain
   * JSON, and {@code CustomerState} carries the customer's name, email, address and phone in the
   * clear (the encryption lives on the events), so the key shred alone leaves them readable in
   * {@code snapshot_store}. The purger empties the state of the customer stream's snapshot row and
   * raises its purge generation, so a command that loaded the customer before the forget cannot
   * store its snapshot again. The customer's stream is {@code customer:<subject id>}.
   */
  static PostgresSnapshotStorePurger customerSnapshotPurger(DataSource dataSource) {
    return new PostgresSnapshotStorePurger(
        dataSource,
        subject -> List.of(StreamId.of(CustomerState.TYPE, AggregateId.of(subject.value()))));
  }

  @Bean
  public InventoryProjection inventoryProjection(ProjectionRepository repo) {
    return new InventoryProjection(repo);
  }

  @Bean
  public OffsetStore offsetStore(DataSource ds) {
    return new PostgresOffsetStore(ds);
  }

  /**
   * How often each projection reads the event store for new events: {@code
   * streamrune.polling-interval-ms} plus up to {@code streamrune.polling-jitter-ms} of jitter, five
   * seconds and one second unless configured. They are the two properties the framework's own
   * projection runner reads in the Quarkus and Micronaut apps; this app builds its runner itself,
   * so it hands them over itself. The integration tests set a shorter interval.
   */
  private static SubscriptionConfig projectionPolling(StreamRuneProperties properties) {
    return new SubscriptionConfig(
        true,
        Duration.ofMillis(properties.pollingIntervalMs()),
        Duration.ofMillis(properties.pollingJitterMs()));
  }

  @Bean(destroyMethod = "close")
  public MultiProjectionRunner projectionRunner(
      EventStore eventStore,
      OffsetStore offsetStore,
      JdbcProjectionRepository projectionRepository,
      StreamRuneProperties properties,
      ProductProjection productProjection,
      OrderProjection orderProjection,
      CustomerProjection customerProjection,
      InventoryProjection inventoryProjection,
      CacheInvalidator cacheInvalidator) {
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(projectionPolling(properties))
            // Every TRANSACTIONAL_LOCAL registration below commits its read-model write and its
            // checkpoint in ONE transaction — the inventory counter can never double-reserve.
            .atomicProcessor(projectionRepository)
            .register(
                "products",
                new CacheAwareProjection(productProjection, cacheInvalidator),
                ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .register(
                "orders",
                new CacheAwareProjection(orderProjection, cacheInvalidator),
                ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .register(
                "customers",
                new CacheAwareProjection(customerProjection, cacheInvalidator),
                ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .register(
                "inventory",
                new CacheAwareProjection(inventoryProjection, cacheInvalidator),
                ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .build();
    runner.start();
    return runner;
  }

  // Dead Letter Queue
  @Bean
  public PostgresDeadLetterQueue deadLetterQueue(DataSource ds) {
    return new PostgresDeadLetterQueue(ds);
  }

  /**
   * Replays dead-lettered commands. Registers the same five sealed command roots the command bus
   * routes to deciders, so any command of the five aggregates resolves: a dead-letter entry names
   * the CONCRETE command class, and {@code registerCommand} expands a sealed root into every
   * command it permits. In a GraalVM native image that expansion reads {@code
   * getPermittedSubclasses()}, which works only for a root registered for reflection — {@link
   * EcommerceNativeHints} registers the roots through {@code
   * StreamRuneRuntimeHints.registerDomainPackages}. A root the image cannot expand makes {@code
   * registerCommand} refuse to start the app instead of registering the root alone.
   *
   * <p>The runner reads the entries with the same kind of mapper the {@link #commandBus} writes
   * them with, so the {@code @Encrypted} fields decrypt to the original values. A plain mapper
   * would read the ciphertext into the command and replay it as the customer's data. If the
   * customer was forgotten while the entry waited, the fields decrypt to {@code [REDACTED]} and the
   * runner does not replay the command.
   *
   * <p>This bean replaces the framework's runner, so the framework does not register it for
   * liveness; the method does, with the framework's {@link BackgroundRelayHealthContributor}. The
   * {@code streamRune} health component then reports it as {@code relay.dead-letter-retry} and
   * turns {@code DOWN} if the runner's poll thread dies, instead of reporting {@code UP} while
   * dead-letter entries stop being replayed.
   *
   * <p>The runner takes the default policy, which {@link #commandBus} takes as well, with the same
   * inbox window, so that the bus answers a keyed command that fails again under its key by the
   * rules this runner replays by; the framework warns at startup when the two differ. The policy
   * replays on its schedule only the entries the command inbox can deduplicate: those of commands
   * that ran under an idempotency key. An entry of an unkeyed command waits for an operator ({@code
   * POST /api/admin/dead-letters/{id}/retry}), because its caller may already have retried it
   * successfully. {@code metrics} counts those entries ({@code streamrune.dlq.awaiting_operator})
   * and the operator retries refused as older than the inbox window ({@code
   * streamrune.dlq.stale_replay_blocked}). It looks for due entries every {@code
   * app.dead-letter.poll-interval} (60 seconds unless configured).
   */
  @Bean
  public DeadLetterRetryRunner deadLetterRetryRunner(
      PostgresDeadLetterQueue dlq,
      VirtualThreadCommandBus commandBus,
      PostgresCryptoEngine cryptoEngine,
      BackgroundRelayHealthContributor relayHealth,
      org.streamrune.spring.StreamRuneProperties properties,
      ObjectProvider<StreamRuneMetrics> metrics,
      @Value("${app.dead-letter.poll-interval:60s}") Duration pollInterval) {
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
            // The policy and the window the command bus answers a keyed re-send by.
            .policy(DeadLetterRetryPolicy.DEFAULT)
            // The window the inbox sweeper prunes with: a keyed entry older than it is refused.
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
            .pollInterval(pollInterval)
            .build();
    relayHealth.registerDlqRetryRunner(runner);
    runner.start();
    return runner;
  }

  // Query Bus with caching + auditing
  @Bean
  public CachingQueryBus cachingQueryBus(ProductProjection productProjection) {
    var simpleQueryBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleQueryBus).build();
    cachingBus.register(ListProducts.class, (ListProducts q) -> productProjection.listAll());
    return cachingBus;
  }

  @Bean
  public QueryBus queryBus(CachingQueryBus cachingBus, PostgresAuditStore auditStore) {
    return new AuditingQueryBus(cachingBus, auditStore);
  }

  @Bean
  public CacheInvalidator cacheInvalidator(CachingQueryBus cachingBus) {
    return cachingBus.cacheInvalidator();
  }

  /**
   * The customer and order read models as one caller may see them: an authenticated {@code ADMIN}
   * or the owner, the rule of {@code OwnerOrAdminAccess}. The customer and order controllers answer
   * every read from it.
   */
  @Bean
  public OwnerOrAdminQueries ownerOrAdminQueries(
      CustomerProjection customerProjection, OrderProjection orderProjection) {
    return new OwnerOrAdminQueries(customerProjection, orderProjection);
  }

  /**
   * The order-fulfillment saga, with its deadline: {@code app.saga.fulfillment-timeout} (15 minutes
   * unless configured). The saga runner and the timeout runner below share this one instance.
   */
  @Bean
  public OrderFulfillmentSaga orderFulfillmentSaga(
      @Value("${app.saga.fulfillment-timeout:15m}") Duration fulfillmentTimeout) {
    return new OrderFulfillmentSaga(fulfillmentTimeout);
  }

  /**
   * The order-fulfillment saga runner. It is a bean so that the Spring integration finds it: for
   * every {@code SagaRunner} bean it starts a compensation retry sweeper, which re-drives a
   * compensation that failed transiently and faults it once {@code
   * streamrune.saga.compensation-retry-give-up-after} (1 hour by default) has passed — {@code POST
   * /api/admin/sagas/faulted/resume} resumes such a saga — it applies {@code
   * streamrune.inbox.retention-max-age} to the runner's key-age guard for compensations resumed by
   * a live event, and it applies {@code streamrune.saga.forward-fence} ({@code auto} unless
   * configured, which is on while single-active-consumer leadership is on) to the runner's forward
   * fence. {@code metrics} counts, among the saga meters, the forward commands the fence refuses
   * ({@code streamrune.saga.forward_commands_fenced}) and the steps run again in place after a lost
   * write of the saga row ({@code streamrune.saga.delivery_retried}).
   *
   * <p>{@code leadership} is the integration's single-active-consumer leadership ({@code
   * streamrune.subscription.single-active-consumer.enabled}, on unless configured). The runner
   * carries a lease over it, {@code "saga:" + saga type}: with more than one backend on the
   * database, one of them delivers the saga's events, claims its deadlines and retries its
   * compensations, and the others stand by and take over when it stops or dies. The lease is taken
   * by the subscription built with {@code sagaRunner(...)} below; without leadership ({@link
   * SubscriptionLeadership#NOOP}) every backend delivers.
   */
  @Bean
  public SagaRunner<?> orderFulfillmentSagaRunner(
      OrderFulfillmentSaga orderFulfillmentSaga,
      PostgresSagaStore sagaStore,
      VirtualThreadCommandBus commandBus,
      PostgresSagaDeadLetterStore sagaDeadLetterStore,
      ObjectProvider<StreamRuneMetrics> metrics,
      ObjectProvider<SubscriptionLeadership> leadership) {
    return SagaRunner.<OrderFulfillmentState>builder()
        .orchestrator(orderFulfillmentSaga)
        .sagaStore(sagaStore)
        .commandBus(commandBus)
        // Required: poison events (routing/decider failures) are quarantined here instead of
        // blocking the subscription.
        .sagaDeadLetterStore(sagaDeadLetterStore)
        .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
        // One backend delivers this saga type; the others stand by.
        .leadership(leadership.getIfAvailable(() -> SubscriptionLeadership.NOOP))
        .build();
  }

  /**
   * Compensates a fulfillment that has not ended by its deadline ({@link
   * OrderFulfillmentSaga#timeout()}): every {@code app.saga.timeout-poll-interval} (10 seconds
   * unless configured) it claims each saga past its deadline and dispatches its compensation, which
   * is the order's cancellation whatever the state.
   *
   * <p>It is built from {@link #orderFulfillmentSagaRunner}, shares that runner's step lock and
   * follows its lease: it claims deadlines only while this backend leads the saga type through
   * {@link #orderFulfillmentSagaSubscription}, so it claims where the saga's steps run, and in this
   * process a claim at a saga's deadline waits for a delivery that is dispatching the saga's
   * forward commands. Around a handover two backends can overlap for a moment — a backend that dies
   * keeps its lease until the lease expires, and a deposed one may finish its page; there the saga
   * runner's forward fence keeps the order: a forward command is stored before the claim, and its
   * compensation has to cover it, or it is refused at its commit. {@code inboxRetentionMaxAge} is
   * the deployment's {@code streamrune.inbox.retention-max-age}: a compensation this runner resumes
   * after its keys may have been pruned is faulted instead of dispatched again. Started here, as
   * the subscriptions below are, and stopped with the context, after the saga subscription.
   */
  @Bean(destroyMethod = "stop")
  public SagaTimeoutRunner<OrderFulfillmentState> orderFulfillmentTimeoutRunner(
      OrderFulfillmentSaga orderFulfillmentSaga,
      PostgresSagaStore sagaStore,
      VirtualThreadCommandBus commandBus,
      SagaRunner<?> orderFulfillmentSagaRunner,
      StreamRuneProperties properties,
      ObjectProvider<StreamRuneMetrics> metrics,
      @Value("${app.saga.timeout-poll-interval:10s}") Duration pollInterval) {
    var runner =
        SagaTimeoutRunner.<OrderFulfillmentState>builder()
            .decider(orderFulfillmentSaga)
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            // The saga runner stores its rows under the state class; the sweep selects by it.
            .sagaType(SagaType.fromClass(OrderFulfillmentState.class))
            .sagaRunner(orderFulfillmentSagaRunner)
            .pollInterval(pollInterval)
            .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .build();
    runner.start();
    return runner;
  }

  /**
   * Operator-triggered recovery of FAULTED order-fulfillment sagas, behind {@link
   * AdminController}'s saga endpoints. It drains a saga's quarantined events ({@code replayAll},
   * {@code replay}), resumes a compensation the auto-configured compensation retry sweeper gave up
   * on ({@code resumeFaulted}) and abandons an entry ({@code discard}). Nothing runs it on a
   * schedule: a poison event is a deterministic orchestrator bug that resolves only once a fix is
   * deployed. It feeds events through the same {@code orderFulfillmentSagaRunner} the live
   * subscription uses, so a replayed event takes the identical path, poison guard included.
   *
   * <p>{@code inboxRetentionMaxAge} is the deployment's {@code streamrune.inbox.retention-max-age}
   * (7 days unless configured). It arms the replayer's two key-age refusals: once a replay's first
   * attempt, or a faulted compensation's episode, is older than that window, the command-inbox keys
   * that deduplicate its commands may already be pruned, and re-running it could charge or refund a
   * customer twice. Such a replay answers {@code STALE_REDRIVE_BLOCKED} or {@code
   * STALE_COMPENSATION_BLOCKED} instead, until an operator who has reconciled what already executed
   * repeats it with {@code force}. Without the window both refusals are inert, and the forward path
   * has no other key-age guard. {@code metrics} records every outcome ({@code
   * streamrune.saga.replayed}, {@code streamrune.saga.resume_faulted}).
   */
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

  /**
   * Subscribes the saga runner to the global event stream. Without this bean the saga would persist
   * nothing because no event listener is wired to feed the {@code SagaRunner}.
   *
   * <p>{@code sagaRunner(...)} delivers to the runner under the runner's lease: this subscription
   * takes the lease and holds it while it runs. On the backend that holds it, it delivers, and the
   * timeout runner and the compensation retry sweeper of the runner act; on every other backend it
   * reads nothing and asks for the lease again every polling interval. Closing it gives the lease
   * back after the page in flight. {@code healthContributor(...)} lists it in the health details as
   * {@code subscription.order-fulfillment-saga}, with its {@code role}: {@code LEADING} or {@code
   * STANDBY}.
   *
   * <p>Uses a 500 ms polling interval (matching the Quarkus/Micronaut wiring) so that the
   * multi-step saga chain (OrderPlaced → payment → stock → ConfirmOrder) completes well within the
   * 15-second window that integration tests await for {@code CONFIRMED}; the interval also spaces a
   * standby's attempts to take the lease. It depends on {@link #orderFulfillmentTimeoutRunner}, so
   * it is closed before that runner stops: a runner that stops while this backend leads logs a
   * warning, and closing the subscription first ends the lead.
   */
  @Bean(destroyMethod = "close")
  @DependsOn("orderFulfillmentTimeoutRunner")
  public PollingEventSubscription orderFulfillmentSagaSubscription(
      EventStore eventStore,
      OffsetStore offsetStore,
      SagaRunner<?> orderFulfillmentSagaRunner,
      ObjectProvider<SubscriptionHealthContributor> health) {
    var subscription =
        PollingEventSubscription.builder()
            .subscriptionName("order-fulfillment-saga")
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(500)))
            .sagaRunner(orderFulfillmentSagaRunner)
            .healthContributor(health.getIfAvailable())
            .build();
    subscription.start();
    return subscription;
  }

  // Payment process manager: reacts to PaymentInitiated by capturing (or failing) the payment.
  @Bean
  public PaymentProcessManager paymentProcessManager(
      PaymentGatewaySimulator paymentGateway,
      PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker,
      VirtualThreadCommandBus commandBus,
      EventStore eventStore) {
    return new PaymentProcessManager(
        paymentGateway, paymentGatewayCircuitBreaker, commandBus, eventStore);
  }

  /**
   * Feeds the global event stream to the {@link PaymentProcessManager}. Without this subscription
   * nothing reacts to {@code PaymentInitiated}, so the saga would stall in {@code
   * AWAITING_PAYMENT}.
   *
   * <p>{@code leadership(...)} delivers under a lease named after the subscription, {@code
   * payment-process-manager}: with more than one backend on the database one of them captures the
   * payments, and the others stand by. {@code healthContributor(...)} lists it in the health
   * details with its {@code role}.
   *
   * <p>Uses a 500 ms polling interval (matching the Quarkus/Micronaut wiring) so that the payment
   * capture leg of the saga completes promptly.
   */
  @Bean(destroyMethod = "close")
  public PollingEventSubscription paymentProcessSubscription(
      EventStore eventStore,
      OffsetStore offsetStore,
      PaymentProcessManager paymentProcessManager,
      ObjectProvider<SubscriptionLeadership> leadership,
      ObjectProvider<SubscriptionHealthContributor> health) {
    var subscription =
        PollingEventSubscription.builder()
            .subscriptionName("payment-process-manager")
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(500)))
            .listener(paymentProcessManager)
            .leadership(leadership.getIfAvailable(() -> SubscriptionLeadership.NOOP))
            .healthContributor(health.getIfAvailable())
            .build();
    subscription.start();
    return subscription;
  }

  /** Prices every order from the product catalogue, as the product streams hold it. */
  @Bean
  public OrderPricing orderPricing(EventStore eventStore) {
    return new OrderPricing(eventStore);
  }

  /** Undoes the money and stock of every cancelled order (OrderCancellationProcessManager). */
  @Bean
  public OrderCancellationProcessManager orderCancellationProcessManager(
      VirtualThreadCommandBus commandBus) {
    return new OrderCancellationProcessManager(commandBus);
  }

  /**
   * Feeds the global event stream to {@link #orderCancellationProcessManager}: with more than one
   * backend on the database, the one that holds the lease {@code order-cancellation} delivers.
   * Listed in the health details with its role.
   */
  @Bean(destroyMethod = "close")
  public PollingEventSubscription orderCancellationSubscription(
      EventStore eventStore,
      OffsetStore offsetStore,
      OrderCancellationProcessManager orderCancellationProcessManager,
      ObjectProvider<SubscriptionLeadership> leadership,
      ObjectProvider<SubscriptionHealthContributor> health) {
    var subscription =
        PollingEventSubscription.builder()
            .subscriptionName(OrderCancellationProcessManager.SUBSCRIPTION_NAME)
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(500)))
            .listener(orderCancellationProcessManager)
            .leadership(leadership.getIfAvailable(() -> SubscriptionLeadership.NOOP))
            .healthContributor(health.getIfAvailable())
            .build();
    subscription.start();
    return subscription;
  }

  /** Commits the stock of every shipped order (OrderShipmentProcessManager). */
  @Bean
  public OrderShipmentProcessManager orderShipmentProcessManager(
      VirtualThreadCommandBus commandBus) {
    return new OrderShipmentProcessManager(commandBus);
  }

  /**
   * Feeds the global event stream to {@link #orderShipmentProcessManager}: with more than one
   * backend on the database, the one that holds the lease {@code order-shipment} delivers. Listed
   * in the health details with its role.
   */
  @Bean(destroyMethod = "close")
  public PollingEventSubscription orderShipmentSubscription(
      EventStore eventStore,
      OffsetStore offsetStore,
      OrderShipmentProcessManager orderShipmentProcessManager,
      ObjectProvider<SubscriptionLeadership> leadership,
      ObjectProvider<SubscriptionHealthContributor> health) {
    var subscription =
        PollingEventSubscription.builder()
            .subscriptionName(OrderShipmentProcessManager.SUBSCRIPTION_NAME)
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(500)))
            .listener(orderShipmentProcessManager)
            .leadership(leadership.getIfAvailable(() -> SubscriptionLeadership.NOOP))
            .healthContributor(health.getIfAvailable())
            .build();
    subscription.start();
    return subscription;
  }
}
```

</details>

There is deliberately no polling runner behind this bean — poison failures are deterministic orchestrator bugs that only resolve once a fix is deployed, so recovery is always operator-triggered. The replayer reuses the same `orderFulfillmentSagaRunner` + stores + event store the live saga subscription uses, so a replayed event is fed through the identical orchestrator path (poison guard included).

**Why `inboxRetentionMaxAge` is not optional.** Every command a saga dispatches carries an idempotency key, and the command inbox recognises a repeated key (Chapter 11, Step 6). The inbox keeps keys for `streamrune.inbox.retention-max-age` — 7 days unless you configure it — and then prunes them. Picture an order saga whose `CapturePayment` and `ReserveStock` committed before a bug faulted it. An operator replays on day 1, before the fix, and gets `STILL_POISON`; the fix ships on day 9. By then the two commands' keys are gone: a replay would dispatch them again, the inbox would not recognise them, and the customer would be charged twice. The replayer records when an entry's first replay started, and with the inbox window it refuses a re-drive whose first attempt is older than the window (`STALE_REDRIVE_BLOCKED`) — and likewise a faulted compensation whose episode is older (`STALE_COMPENSATION_BLOCKED`: a refund could run twice). Without the window both refusals are off, and nothing else guards the forward path. `properties.inbox().retentionMaxAge()` is the same value the inbox sweeper uses, so the two cannot drift apart. `metrics` counts every outcome (`streamrune.saga.replayed`, `streamrune.saga.resume_faulted`).

**The replayer never un-faults a saga.** It feeds the event through the saga runner's normal step against the row as it stands; a `FAULTED` row is cleared only by that step's own successful write, and a feed that poisons again re-records the fault.

**Two kinds of `FAULTED` saga.** `GET /api/admin/sagas/faulted` lists both:

- **A saga a poison event faulted.** It owns dead-letter entries: the poison event, and every later event of the saga, which the live runner holds behind the fault so that nothing is applied out of order. `replay-all` drains them.
- **A compensation faulted without an entry.** A compensation command that fails *transiently* — a store blip, an open circuit breaker — leaves the saga `COMPENSATING`, not `FAILED`. The compensation retry sweeper, which the Spring integration starts for every `SagaRunner` bean (that is why `orderFulfillmentSagaRunner` is a bean), re-drives it every `streamrune.saga.compensation-retry-interval` (10 s in the demo, one minute by default) under the same keys — with several backends, on the one whose saga subscription leads, since the sweeper follows the saga runner's lease (Chapter 11, Step 6); the admin endpoints below run on whichever backend receives the request. After `streamrune.saga.compensation-retry-give-up-after` (1 hour by default) it gives up and faults the saga, with no dead-letter entry, because there is no event to quarantine. `resume` runs that compensation again once the cause is fixed.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/admin/saga-dead-letters?limit=50` | Lists quarantined saga dead-letter entries, newest first. Each entry: `sagaId` (nullable), `sagaType`, `eventOffset`, `eventType`, `errorType`, `errorMessage`, `faultedAt`. |
| `GET` | `/api/admin/sagas/faulted?limit=50` | Lists the ids of order-fulfillment sagas in `FAULTED` status, both kinds. |
| `POST` | `/api/admin/saga-dead-letters/replay-all?sagaId=…` | Drains one saga's entries through `replayAll`, oldest event first, and stops at the first entry it cannot feed. Returns `{"outcomes": [...]}`, one outcome per attempt in attempt order. |
| `POST` | `/api/admin/saga-dead-letters/replay?sagaId=…&eventOffset=…` | Replays one entry through `replay`. Refused with `OLDER_ENTRY_PENDING` while an older entry of the same saga is pending. `sagaId` is omitted for an entry whose saga could never be identified. Returns `{"outcome": "..."}`. |
| `POST` | `/api/admin/sagas/faulted/resume?sagaId=…` | Resumes a compensation faulted without an entry, through `resumeFaulted`. Returns `{"outcome": "..."}`. |
| `DELETE` | `/api/admin/saga-dead-letters?sagaId=…&eventOffset=…` | Discards a quarantined entry without replaying it, through `SagaDeadLetterReplayer.discard`. `sagaId` is omitted for an entry whose saga could never be identified. Returns `{"discarded": true/false}`. |

The three `POST` operations also take `force=true` (below). The four mutating endpoints carry the ADMIN role header from Step 5. With the state the earlier chapters left — `fulfillment-o-100` completed, `fulfillment-o-101` failed, nothing quarantined — each call finds nothing to do, and answers so; the comment after it says what a saga in the situation it is made for answers:

```bash
# List quarantined saga dead-letters
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/saga-dead-letters?limit=50" | jq .

# Enumerate FAULTED sagas
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/sagas/faulted?limit=50" | jq .

# Drain a saga after redeploying the fix
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/saga-dead-letters/replay-all?sagaId=fulfillment-o-100"
# {"outcomes":[]}
# A saga with two quarantined entries: {"outcomes":["REPLAYED","REPLAYED"]}

# Resume a compensation the sweeper gave up on
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/sagas/faulted/resume?sagaId=fulfillment-o-101"
# {"outcome":"NOT_FAULTED"}
# A FAULTED saga whose compensation then runs to its end: {"outcome":"RESUMED"}

# Discard an entry without replaying it
curl -s -X DELETE -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/admin/saga-dead-letters?sagaId=fulfillment-o-100&eventOffset=42"
# {"discarded":false}
# An entry at that offset: {"discarded":true}
```

A replay answers with one of nine outcomes — `replay` once, `replay-all` once per attempt:

- `REPLAYED` — the event was fed and the step succeeded, or the feed was moot (the saga is already terminal, or the event was already applied). The entry is discarded.
- `STILL_POISON` — the step poisoned again (or a resumed compensation made no durable progress). The entry stays and the saga stays `FAULTED`: the fix has not resolved the failure.
- `ENTRY_NOT_FOUND` — no entry of the order-fulfillment saga type at that `(sagaId, eventOffset)`.
- `EVENT_NOT_FOUND` — the event store cannot read that offset. Nothing was fed; `DELETE` the entry.
- `STALE_REDRIVE_BLOCKED`, `STALE_COMPENSATION_BLOCKED` — the key-age refusals above. Find out which of the saga's commands already executed (the payment, inventory and order streams, the `command_inbox` table), then repeat with `force=true`.
- `OLDER_ENTRY_PENDING` — a single replay with an older entry of the saga still pending; use `replay-all`.
- `SAGA_ROW_PENDING` — deferred, not fed: the saga has no row yet, or its start has not been applied. `replay-all` feeds it once the saga's start entry lands in the same drain.
- `TARGET_PENDING` — an entry whose saga could not be identified now resolves to a saga that is itself `FAULTED` or has older entries pending; drain that saga with `replay-all`.

A resume answers `RESUMED` (the compensation ran to its end: `COMPENSATED`, or `FAILED` when a compensation was rejected for a business reason), `NO_PROGRESS` (a compensation failed transiently again; the sweeper never re-drives a `FAULTED` saga, so resume again later — compensations that already ran deduplicate), `STILL_POISON` (`compensate()` still throws), `STALE_COMPENSATION_BLOCKED`, `FORWARD_FAULT` or `ENTRIES_PENDING` (the saga has dead-letter entries: use `replay-all`, whose first feed resumes the compensation), `NOT_FAULTED` or `SAGA_NOT_FOUND`.

**`force=true` lifts only the two `STALE_*` refusals.** It never overrides ordering. It is your acknowledgement that a command whose key was already pruned may run a second time, so send it only after you have checked what executed; the framework logs a `WARNING` naming the saga, and counts `streamrune.saga.forced_stale_resume` when a forced compensation passes the runner's own key-age guard. The full outcome tables are in the framework's saga guide (`docs/guide/advanced/saga.md` in the StreamRune repository, sections "Replaying quarantined events" and "Resuming a give-up fault").

`discardSagaDeadLetter` goes through the same `SagaDeadLetterReplayer` bean rather than the store, for two reasons. The removal is scoped to the replayer's saga type (the state class's fully-qualified name), so an id that another saga type happens to share can never lose its quarantine record through this endpoint. And a saga with quarantined entries carries a dead-letter shield (`saga_state.dead_letter_pending`) that makes the live `SagaRunner` hold every later event of that saga in the quarantine instead of applying it out of order; `discard` releases the shield once the saga has no entry left (for an entry whose saga could not be identified, the shield of the saga the replayer resolved it to). Deleting the row with a bare `sagaDeadLetterStore` call would leave that shield set, and the saga's live events would keep being held behind a backlog that is gone.

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
  -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" \
  -d '{"orderId":"o-fail-1","customerId":"c-1","lines":[{"productId":"p-1","quantity":1}]}'
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

The projection runner is still alive — it logged the failure, persisted the entry, and continued polling. Its checkpoint has moved past the failed batch, so removing the artificial failure and restarting the application does not re-read it. Applying the range after the fix is a replay through `ProjectionDeadLetterReplayer`:

```java
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.ProjectionDeadLetterReplayer;

var replayer =
    new ProjectionDeadLetterReplayer(
        eventStore,
        projectionDlq,
        projectionRepository);   // the processor the runner commits through

ProjectionDeadLetterReplayer.ReplayResult result =
    replayer.replay(
        ProjectionName.of("orders"),
        new CacheAwareProjection(orderProjection, cacheInvalidator),  // as registered
        ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,                   // as registered
        100);
// result.replayed(), result.failed(), result.fenced()
```

- **The constructor takes the processor.** Its third argument is the `AtomicBatchProcessor` the projection's runner commits through — here `projectionRepository`, the `JdbcProjectionRepository` handed to `.atomicProcessor(...)` in Step 4. A replay takes that processor's per-projection lock (the projection's checkpoint row, `FOR UPDATE`), the lock a live batch takes, so a replay and a live batch of one projection never run at the same time, in one process or across replicas, and the replay is safe while the runner is live. A different processor here serializes nothing.
- **`replay(name, projection, mode, maxEntries)`** names the projection, passes the projection as it is registered (with its `CacheAwareProjection` wrapper, so the replay invalidates the cache too) and the delivery mode it is registered with. The mode decides what the projection is handed, exactly as for a live batch: under `TRANSACTIONAL_LOCAL` it is the replay transaction's repository, so a range is applied all-or-nothing. The call replays up to `maxEntries` entries, oldest range first, discards each entry it applied, and never moves the checkpoint: it fills the hole behind it. `ReplayResult` counts the entries replayed, the entries that failed and were kept, and the entries a self-fencing projection applied nothing of and were kept.
- **Replay is refused for the non-atomic at-least-once processor.** `AtomicBatchProcessor.nonAtomicAtLeastOnce()` holds no per-projection lock, so `replay` throws an `IllegalStateException` for it before it reads an entry: beside a live batch both sides could read a row before either saves it, and one of the two writes would be lost. For such a registration stop the projection's runner and call `replayWithRunnerStopped(name, projection, maxEntries)`. The demo's runner commits through `JdbcProjectionRepository`, so `replay` is the method to call.

The lock does not lift the requirements on `process` described under `DLQ` at the top of this chapter: a replay is at-least-once (a crash after the replay commits and before the entry is discarded leaves the entry queued, and the next replay applies the range again) and out of order. The demo does not wire a replayer; call it from an admin endpoint or a maintenance task of your own, or inspect the entry and remove it from `projection_dead_letters` directly in the database.

**Scenario 2: Restart and catch up**

A projection keeps its place in `projection_offset`, one row per projection, updated with every batch it commits. Read the checkpoints and the head of the store:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT projection_name, last_offset FROM projection_offset ORDER BY projection_name;" \
  -c "SELECT max(global_offset) AS head FROM event_stream;"
```

With the application running and idle, every projection's `last_offset` is at the head, or one poll behind it (`projection_offset` also holds the checkpoints of the subscriptions, the saga's and the process managers'). Stop the application and start it again: on startup `MultiProjectionRunner.start()` reads each projection's checkpoint from `PostgresOffsetStore` and reads only the events stored after it, so nothing applied before the restart is applied twice. The event store keeps every event a writer appends while this application is down — with two backends on one database (Chapter 17), the other one goes on writing — and the projections apply them, in one or more batches, before they report `LIVE` again (Step 6).

---

## What We Learned

**`ProjectionErrorStrategy`** is an enum with three values: `HALT`, `SKIP`, and `DLQ`. It is set per projection with `MultiProjectionRunner.builder().register(name, projection, strategy, mode)`; the demo registers every projection with the three-argument form, which keeps the default `HALT`. The choice is a tradeoff between fail-fast safety and production liveness.

**`ProjectionDeadLetterStore`** is the interface for persisting failed projection batches. `PostgresProjectionDeadLetterStore` is the durable implementation backed by a `projection_dead_letters` table. Each `ProjectionDeadLetterEntry` carries the projection name, the global offset range of the failed batch, the event count, the exception class and message, the consecutive error count at the time of failure, and the timestamp.

**`DeadLetterQueue`** is the interface for storing failed commands. `PostgresDeadLetterQueue` is the Postgres implementation. It is separate from `ProjectionDeadLetterStore` because commands and projection batches are different units of failure: a command lands in the queue because an infrastructure error failed it (a rejection by the aggregate is never recorded); a projection batch fails because the read-side handler threw. The command bus writes to the queue only when it is built with `.deadLetterQueue(...)`, and it stores the command record as JSON through the mapper given to `.objectMapper(...)`. Use `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)` on both the bus and the runner, so a command's `@Encrypted` fields are stored as ciphertext and are erased with the customer.

**`DeadLetterRetryRunner`** polls the `DeadLetterQueue` on a virtual thread. For each retryable entry it deserializes the command using the `commandTypeRegistry`, re-dispatches it through the `CommandBus`, and discards the entry on success. On failure it increments `dlqAttempts`. Once `dlqAttempts` reaches the policy's `maxRetries`, the entry is either discarded (if `discardAfterMaxRetries` is true) or left in place as a permanent record of a poison message. The default policy (`DeadLetterRetryPolicy.DEFAULT`) retries up to five times with exponential backoff, does **not** auto-discard, and replays on its schedule only entries whose command ran under an idempotency key (`UnkeyedReplay.OPERATOR_ONLY`): a keyed replay is deduplicated by the command inbox against the caller's own retry, an unkeyed one could be a second execution. An unkeyed entry waits for an operator, who replays it (`retry(commandId, true)`) when its effect is missing or discards it (`deadLetterQueue.discard(commandId)`) when it is present, each decision audited before it is acted on; a keyed entry older than the inbox window (`inboxRetentionMaxAge`) is refused as `STALE_REPLAY_BLOCKED` until forced. `streamrune.dlq.awaiting_operator` counts the entries waiting for a person.

**`Idempotency-Key`** on `PUT /api/products/{id}/stock` and `POST /api/inventory/{productId}/receive` turns the two relative writes into keyed commands (`IdempotentDispatch`, `commandBus.execute(command, key)`): a repeat of the request is answered with the recorded result, and a failed one is replayed on the schedule. The key is scoped to the caller with `IdempotencyKey.scopedTo`.

**`SagaDeadLetterReplayer`** is the saga-side counterpart introduced in this chapter: manual, operator-triggered recovery of `FAULTED` sagas. Unlike `DeadLetterRetryRunner`, it has no polling loop — poison saga failures are deterministic orchestrator bugs that only resolve once a fix is deployed, so recovery is always explicit. `replayAll` drains a saga's quarantined events oldest first and `replay` feeds one, through the same saga runner; `resumeFaulted` re-runs a compensation the retry sweeper gave up on. It never un-faults a saga itself: only the fed step's own write clears the fault. Built with the deployment's inbox retention window, it refuses a re-drive or a compensation older than that window (`STALE_REDRIVE_BLOCKED`, `STALE_COMPENSATION_BLOCKED`) until an operator who has checked what already ran repeats it with `force`.

**The outbox admin endpoints** sit on the same controller. Listing the outbox is `findByStatus`, never `loadPending` (which claims). A blocked aggregate is resolved by `replay` or an audited `skip`; `skippedBy` is the request identity; mutating admin endpoints require the ADMIN role.

**`MultiProjectionRunner`** manages all projections as a group. `start()` launches each projection on its own virtual thread. `stop()` (aliased as `close()`) signals all runners and waits up to 30 seconds for graceful shutdown. `status()` returns a `Map<String, ProjectionStatus>` with the current `ProjectionState`, last committed offset and last error for each projection.

**`ProjectionState`** is the enum returned per projection by `status()`. Its values are `PENDING`, `CATCHING_UP`, `LIVE`, `STANDBY`, `ERROR`, and `STOPPED`.

**Poison message handling** is the scenario where a command or event batch can never succeed, regardless of how many times it is retried. The `DeadLetterRetryRunner` handles this by capping retries at `maxRetries` for the entries it replays on its schedule. A projection registered with `DLQ` strategy writes the batch to the dead letter store and continues — the poison batch is isolated and does not block all subsequent events; once the cause is fixed, `ProjectionDeadLetterReplayer.replay(name, projection, mode, maxEntries)` applies the range under the lock of the processor the runner commits through. Saga poison events follow the same principle via `SagaDeadLetterStore` + `SagaDeadLetterReplayer` (Step 5b), except replay there is always operator-triggered rather than polled.

---

## Next Up

Our system handles failures gracefully — dead letters are stored, projections recover automatically, failed keyed commands are replayed on a schedule, and the unkeyed ones wait for an operator who confirms the replay. But how do we know what is happening inside at runtime? Next: observability.
