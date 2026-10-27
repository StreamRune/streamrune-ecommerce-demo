# Chapter 12: Circuit Breaker

> **What you'll learn:** How to protect against cascading failures with two circuit breakers that sit in different places. The command-bus `CircuitBreakerCommandInterceptor` guards every command dispatch; a small `PaymentGatewayCircuitBreaker` guards the one flaky external dependency — the payment gateway — that the `PaymentProcessManager` calls. You will wire both, see why the gateway needs its own breaker rather than relying on the command-bus one, expose admin endpoints to inspect both states, and watch the gateway breaker trip when you toggle payment failure.

---

## What We're Building and Why

An event-sourced application leans on infrastructure for every command: the event store has to be reachable, locks have to be acquired, projections have to keep up. When one of those dependencies goes down, a naive system keeps hammering it — tying up threads, piling up failed commands, and making recovery slower than it needs to be. The same is true for any external service you call from a command handler, such as a payment gateway.

The circuit breaker pattern solves this by standing between your application and an unreliable dependency. It watches for failures and, once they cross a threshold, "opens" and immediately rejects new requests without even attempting them. After a cooldown period it allows one probe request through. If the probe succeeds, the circuit closes and normal operation resumes. If the probe fails, the circuit opens again and the cooldown restarts.

StreamRune expresses this as `CircuitBreakerCommandInterceptor`, a plain `CommandInterceptor` in the runtime module. Because interceptors sit on the command bus, the circuit breaker protects every command dispatch in the application — not just payment commands. The three states follow the standard pattern:

- **CLOSED** — normal operation. The interceptor tracks consecutive failures but passes every command through.
- **OPEN** — too many consecutive failures have occurred. The interceptor throws `CircuitBreakerOpenException` immediately, without executing the handler. One thread is allowed through after the cooldown elapses — that thread becomes the probe.
- **HALF_OPEN** — a single probe request is in flight. All other concurrent requests are rejected. If the probe succeeds the circuit closes; if it fails the circuit opens again. As a safety valve, if the designated probe never reports back within `probeTimeout` (default 30s) — say it hangs on a stuck connection — a later request is allowed to take over as a fresh probe, so the circuit can never stay stuck in HALF_OPEN forever.

The implementation is thread-safe. State transitions use `AtomicReference.compareAndSet`, which guarantees that exactly one thread becomes the probe even under heavy concurrency. A thread-local stack of probe flags (`probeStack`, one frame per command in flight on the thread) tracks whether the execution that is finishing is that probe, so `onError` can distinguish probe failure (re-open immediately) from an ordinary error during normal operation. It is a stack rather than a single flag because a nested command dispatched on the same thread gets its own frame and cannot take the outer command's probe designation.

---

## Step by Step

### Step 1: Create `CircuitBreakerCommandInterceptor` Bean

Open `StreamRuneConfig.java` and add a bean for the interceptor. The two constructor arguments are the **failure threshold** — how many consecutive failures trigger an open — and the **cooldown duration** — how long to wait before allowing a probe.

```java
@Bean
public CircuitBreakerCommandInterceptor circuitBreaker() {
  return new CircuitBreakerCommandInterceptor(3, Duration.ofSeconds(30));
}
```

Three consecutive failures will open the circuit. After 30 seconds it will allow one probe through. These values are tuned for demo purposes; production systems typically use 5–10 failures with longer cooldowns.

### Step 2: Add the Interceptor to the Command Bus

The `VirtualThreadCommandBus` takes an ordered list of interceptors. Their `before` methods run in that order around every command dispatch, and their `after` and `onError` methods in reverse order. Add `circuitBreaker` to both the method signature and the end of the builder call. The bean as it stands at the end of this chapter also takes the aggregate locker and the lock timeout, which Step 2b explains:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit,
    CircuitBreakerCommandInterceptor circuitBreaker,
    PostgresCommandInbox commandInbox,
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

The circuit breaker goes last, the innermost slot of the framework's canonical order (`CommandInterceptorOrdering`): it sees a command only after auditing, authorization, and validation have run. The position matters for one thing: the command bus invokes `onError` only on the interceptors whose `before()` completed, in reverse order. A command that validation or authorization rejects in its `before()` never reaches the breaker's `before()`, so the breaker's `onError` never sees that rejection. Failures after the interceptor chain — the decider, the event store — reach every interceptor's `onError`.

What keeps those later rejections from tripping the breaker is its default failure predicate, `CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES`, which is `CommandFailureClassification.BREAKER_ELIGIBLE` from `streamrune-core`. `onError` tests the failure against it before touching the failure counter, and it excludes three families: permanent rejections (`DomainException`, including `ValidationException` and `AuthorizationException`, and a top-level `IllegalArgumentException`), a `SubjectForgottenException` anywhere in the cause chain (a command for a crypto-shredded customer), and failures to read one aggregate's stored data (a corrupt payload or an unregistered event type). Only genuine infrastructure errors (event store failures, locking errors, unexpected runtime exceptions) trip the breaker. To widen or narrow what counts, pass a custom predicate to the four-argument constructor, built from `CommandFailureClassification` rather than retyped — a hand-written copy misses an exclusion the next release adds.

### Step 2b: Answer a Refused Command with `503`

An open breaker throws `CircuitBreakerOpenException` out of `commandBus.execute(...)`, and without a handler of its own Spring answers it `500 Internal Server Error`. That status says the server failed at the request. It did not: the command was refused before anything ran, and the same request succeeds once the breaker closes. The framework refuses a command for five such reasons, and the demo answers every one of them `503 Service Unavailable` with `Retry-After: 5` and a JSON body that names the reason:

| Framework exception | `reason` | What happened |
|---|---|---|
| `CircuitBreakerOpenException` | `CIRCUIT_BREAKER_OPEN` | The command bus's breaker is open, or half-open with its probe in flight. Nothing was attempted. |
| `CommandBusOverloadedException` | `COMMAND_BUS_OVERLOADED` | The bus's budget of in-flight asynchronous commands is used up. Only `executeAsync` has the budget, and the demo's routes call `execute`, so the demo maps it for completeness. |
| `CommandBusClosedException` | `COMMAND_BUS_CLOSED` | The bus is closing, because the application is shutting down. Nothing was attempted. |
| `LockException` | `AGGREGATE_LOCK_UNAVAILABLE` | The aggregate's lock could not be taken within `streamrune.lock-timeout` (5 s by default), on every attempt of the bus's retry policy: another command held it, or the locker could not reach its store. The decider never ran. |
| `OptimisticLockException` | `AGGREGATE_VERSION_CONFLICT` | The aggregate's stream moved past the version the command was decided on, on every attempt of the bus's retry policy: another writer appended to it between the load and the append each time — a writer outside the command bus, or an instance that takes its locks elsewhere (an in-process lock holds in its own process only). Nothing was written. |

```json
{"reason": "CIRCUIT_BREAKER_OPEN", "message": "The command bus refuses commands while its circuit breaker is open."}
```

**Why `503` and not `409` for a lock or a version conflict.** `LockException` is, in its own javadoc, an acquisition failure that the command bus treats as transient and retries — not a conflict with the state of the aggregate. `409 Conflict` tells a client that its request conflicts with the resource and that it may resolve the conflict and resubmit; here nothing in the request is wrong. The aggregate is busy, or the locker's store is out of reach, and the bus has already retried before it gave up: the request will succeed unchanged once the lock is free, which is what `503` with `Retry-After` says. A version conflict is the same answer for the same reason: the bus reloads the aggregate on every attempt, the client sent no version of its own that could conflict, and the identical request succeeds once the other writer is done. From Chapter 14 on, the bus records both commands in the dead-letter queue, like any infrastructure failure, and the answer then says that the command is pending instead (Chapter 14).

`Retry-After` is a constant five seconds: the breaker does not report how long it stays open, and the other refusals have no natural duration. A genuine failure — the event store refusing the append, say — is none of the five and stays `500`.

The lock timeout is the framework's `streamrune.lock-timeout`. The bus the framework auto-configures reads it; the demo builds its own bus, so it hands the builder the `AggregateLocker` bean and the configured timeout: the two parameters `AggregateLocker locker` and `StreamRuneProperties properties` of the `commandBus` bean in Step 2, and its calls `.locker(locker)` and `.lockTimeout(properties.lockTimeout())` (`AggregateLocker` is in `org.streamrune.core`, `StreamRuneProperties` in `org.streamrune.spring`).

**The lock is taken in PostgreSQL.** The framework auto-configures `LocalStripedLocker`, a lock in the memory of one process: it serializes the commands of one aggregate in that process and nothing beyond it. A second instance of the app on the same database — Chapter 17 runs two — takes its own locks, and two commands on one aggregate from the two instances both load it; the second append fails its version check, the bus reloads and decides again, and after three conflicts in a row the command is refused with `AGGREGATE_VERSION_CONFLICT`. The demo therefore declares the `AggregateLocker` itself, as `PgAdvisoryLocker` from `streamrune-postgres`: an advisory lock per stream (`pg_advisory_xact_lock`) that every instance on the database takes, held on a connection of the locker's own pool for the whole command. The auto-configured locker is `@ConditionalOnMissingBean(AggregateLocker.class)`, so it steps aside for the bean:

```java
@Bean(destroyMethod = "close")
public PgAdvisoryLocker aggregateLocker(
    @Value("${spring.datasource.url}") String url,
    @Value("${spring.datasource.username}") String username,
    @Value("${spring.datasource.password}") String password) {
  return PgAdvisoryLocker.withDedicatedPool(url, username, password, 10);
}
```

What it costs: a connection per command in flight, from a pool of the locker's own, held from the lock to the end of the command, and the round trip that takes the lock. The command also borrows a connection of the application's pool for its load and its append, so the lock pool has the application pool's size, ten — the commands that pool serves at once — and an instance opens both pools, which together stay below PostgreSQL's `max_connections`. `withDedicatedPool` builds that pool, and the locker owns it: `destroyMethod = "close"` closes it with the context. With one instance the lock works as the in-process one did; `streamrune.lock-timeout` bounds the wait for it all the same (`lock_timeout` in PostgreSQL), and Chapter 17 shows the second instance waiting for it.

The whole class at the end of this chapter:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 12</summary>

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
import org.streamrune.core.*;
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
   * pool's size, ten: as many commands as the application pool serves at once. Each backend opens
   * both pools, which together stay below PostgreSQL's {@code max_connections}.
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
      CacheInvalidator cacheInvalidator) {
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(projectionPolling(properties))
            // Every TRANSACTIONAL_LOCAL registration below commits its read-model write and its
            // checkpoint in ONE transaction.
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
            .build();
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
   * streamrune.saga.compensation-retry-give-up-after} (1 hour by default) has passed, it applies
   * {@code streamrune.inbox.retention-max-age} to the runner's key-age guard for compensations
   * resumed by a live event, and it applies {@code streamrune.saga.forward-fence} ({@code auto}
   * unless configured, which is on while single-active-consumer leadership is on) to the runner's
   * forward fence. {@code metrics} counts, among the saga meters, the forward commands the fence
   * refuses ({@code streamrune.saga.forward_commands_fenced}) and the steps run again in place
   * after a lost write of the saga row ({@code streamrune.saga.delivery_retried}).
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



The table lives in one place, a small class in the shared `commands` module that the Quarkus and Micronaut apps use too. Create `commands/src/main/java/org/streamrune/ecommerce/commands/CommandRefusal.java`:

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

/**
 * A command the framework refused for a reason that passes, and how the three apps answer it:
 * {@code 503 Service Unavailable} with {@code Retry-After: 5} and a JSON body that names the reason
 * and says what happened, {@code {"reason": "CIRCUIT_BREAKER_OPEN", "message": "..."}}. Every other
 * failure keeps its own answer: a broken business rule is {@code 400}, a refused caller {@code
 * 403}, and a genuine failure, a store error for one, {@code 500}.
 *
 * <p>The five refusals, as the framework reports them:
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
 *       the state of the resource. Like any infrastructure failure the bus records the command in
 *       the dead-letter queue: the retry runner replays a keyed command on its schedule, and an
 *       unkeyed one waits for an operator.
 *   <li>{@link OptimisticLockException} — the aggregate's stream had moved past the version the
 *       command was decided on, on every attempt of the bus's retry policy: another writer appended
 *       to the stream between the command's load and its append each time (a second instance whose
 *       lock is its own, a writer outside the command bus). Nothing was written. The bus reloads
 *       the aggregate on each attempt, and the client sent no version that could conflict, so the
 *       answer is the lock's: {@code 503} with {@code Retry-After}, the same request succeeding
 *       later. The bus records the command in the dead-letter queue like the lock's. Only the
 *       command bus's append conflict reaches a controller; the subtypes the framework raises for
 *       projections and saga rows are thrown on the threads of subscriptions.
 * </ul>
 *
 * @param reason what refused the command
 * @param message what the body tells the client
 */
public record CommandRefusal(Reason reason, String message) {

  /** How long a client waits before it sends a refused command again, in seconds. */
  public static final int RETRY_AFTER_SECONDS = 5;

  /** What refused the command; the body's {@code reason}. */
  public enum Reason {
    CIRCUIT_BREAKER_OPEN,
    COMMAND_BUS_OVERLOADED,
    COMMAND_BUS_CLOSED,
    AGGREGATE_LOCK_UNAVAILABLE,
    AGGREGATE_VERSION_CONFLICT
  }

  /**
   * The refusal {@code failure} reports, or empty when it is none of the five: a failure the app
   * answers otherwise.
   *
   * @param failure what the command bus threw
   */
  public static Optional<CommandRefusal> of(Throwable failure) {
    return switch (failure) {
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

  /** The JSON body: {@code reason}, then {@code message}. */
  public Map<String, String> body() {
    Map<String, String> body = new LinkedHashMap<>();
    body.put("reason", reason.name());
    body.put("message", message);
    return body;
  }

  private static Optional<CommandRefusal> refusal(Reason reason, String message) {
    return Optional.of(new CommandRefusal(reason, message));
  }
}
```

Then add one handler to `GlobalExceptionHandler` (imports: `java.util.Map`, `org.streamrune.core.CircuitBreakerOpenException`, `org.streamrune.core.CommandBusClosedException`, `org.streamrune.core.CommandBusOverloadedException`, `org.streamrune.core.LockException`, `org.streamrune.core.OptimisticLockException` and `org.streamrune.ecommerce.commands.CommandRefusal`):

```java
  /**
   * A command the framework refused for a reason that passes ({@link CommandRefusal}): an open
   * circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock
   * that could not be taken in time, a version conflict on every attempt. {@code 503} with {@code
   * Retry-After} and a JSON body naming the reason, the answer the Quarkus and Micronaut apps give
   * too. A genuine failure, such as a store error, is not one of these and stays {@code 500}.
   */
  @ExceptionHandler({
    CircuitBreakerOpenException.class,
    CommandBusOverloadedException.class,
    CommandBusClosedException.class,
    LockException.class,
    OptimisticLockException.class
  })
  public ResponseEntity<Map<String, String>> handleCommandRefused(RuntimeException e) {
    CommandRefusal refusal =
        CommandRefusal.of(e).orElseThrow(() -> new IllegalStateException("not a refusal", e));
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(CommandRefusal.RETRY_AFTER_SECONDS))
        .contentType(MediaType.APPLICATION_JSON)
        .body(refusal.body());
  }
```

The whole `GlobalExceptionHandler` at the end of this chapter:

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
   * too. A genuine failure, such as a store error, is not one of these and stays {@code 500}.
   */
  @ExceptionHandler({
    CircuitBreakerOpenException.class,
    CommandBusOverloadedException.class,
    CommandBusClosedException.class,
    LockException.class,
    OptimisticLockException.class
  })
  public ResponseEntity<Map<String, String>> handleCommandRefused(RuntimeException e) {
    CommandRefusal refusal =
        CommandRefusal.of(e).orElseThrow(() -> new IllegalStateException("not a refusal", e));
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(CommandRefusal.RETRY_AFTER_SECONDS))
        .contentType(MediaType.APPLICATION_JSON)
        .body(refusal.body());
  }
}
```

The command bus throws each of the five unwrapped, so the handler sees the exception itself. The Quarkus and Micronaut apps answer the same: JAX-RS and Micronaut pick a handler by the thrown type, so each app has one per exception type, nested in `CommandRefusalExceptionMappers` (Quarkus) and `CommandRefusalExceptionHandlers` (Micronaut). `CommandRefusalStatusIT` in each app forces the refusals on the app's real command bus, in an application of its own with a lock timeout of one second. The finished app has the dead-letter queue of Chapter 14, which records a lock timeout and a version conflict, so the test reaches their own answers with a trigger on `dead_letter_queue` that refuses the entry: a trigger on `event_stream` that raises `unique_violation` for a stock adjustment makes the event store report the version conflict it reports when a second writer stored the version first, on every attempt, and the adjustment is refused with `AGGREGATE_VERSION_CONFLICT`. A store that refuses every price change of a product answers `500` until the breaker opens and `503` after, and a closed bus refuses the next command. Each answer is checked for the status, `Retry-After` and the body; the answers to a command the queue did record are Chapter 14's. The overloaded bus cannot be reached through the demo's routes, so a unit test in each app checks its answer.

### Step 3: The gateway call, and why it needs its own breaker

You created `PaymentGatewaySimulator` in Chapter 11 (`spring-app/src/main/java/org/streamrune/ecommerce/spring/config/PaymentGatewaySimulator.java`). It has two switches. With `failureInjected` on, `processPayment` declines: it throws `PaymentDeclinedException` — unless the gateway already captured that payment, because the payment id is its idempotency key. With `outage` on, it answers no call: it throws an `IllegalStateException`, the way a call that times out or meets a `5xx` fails:

```java
public void processPayment(String paymentId) {
  if (outage.get()) {
    throw new IllegalStateException(
        "Simulated payment gateway outage: no answer for payment " + paymentId);
  }
  if (capturedPayments.contains(paymentId)) {
    return;
  }
  if (failureInjected.get()) {
    throw new PaymentDeclinedException(paymentId);
  }
  capturedPayments.add(paymentId);
}
```

This method is on the hot path: the `PaymentProcessManager` (Chapter 11) calls `gateway.processPayment(paymentId)` every time it reacts to a `PaymentInitiated` event. A decline is the gateway's answer: the manager fails the payment, and the saga compensates. A call without an answer is not: the gateway may have taken the money, so the manager asks it whether it holds a capture of the payment and otherwise lets the event be delivered again, and the payment waits. That makes the payment gateway exactly the kind of flaky external dependency a circuit breaker is meant to protect: an outage would have the manager call it again on every delivery.

**Why not just let the command-bus breaker handle it?** A gateway call is no command, so its outcome does not, on its own, reach the command-bus breaker. A call without an answer dispatches nothing at all: `onEvents` throws, and the subscription delivers the event again. A decline becomes a *successful* `FailPayment` command, and the saga then runs a *successful* `CancelOrder`. From the command bus's point of view those are normal successful dispatches, and the command-bus breaker resets its consecutive-failure count on **any** successful command (`after()` closes the circuit), so the saga's own traffic would keep clearing the counter. A breaker on the command bus can never "see" a run of gateway calls without an answer here.

The textbook fix is to put the breaker around the flaky dependency itself. `PaymentGatewayCircuitBreaker` counts only gateway-call outcomes: a call without an answer is a failure, and consecutive failures open it regardless of what the saga does on the command bus; a decline comes from a gateway that works, and counts as a success. While it is open the `PaymentProcessManager` does not call the gateway: the payment has no answer, so it waits — a capture the gateway reports is recorded, and otherwise the event is delivered again — until the cooldown admits a probe. A payment that the order's cancellation voided before the manager reached it is neither failed nor sent to the gateway: the manager reads the payment's stream first (Chapter 11, Step 5b).

Chapter 11 created that breaker, `PaymentGatewayCircuitBreaker` (`spring-app/src/main/java/org/streamrune/ecommerce/spring/config/PaymentGatewayCircuitBreaker.java`, Step 5b), together with the process manager that calls it.

It implements the same CLOSED → OPEN → HALF_OPEN → CLOSED state machine as the command-bus interceptor, but its inputs are explicit `recordSuccess()` / `recordFailure()` calls from the `PaymentProcessManager` rather than command outcomes. Because it is driven by a single polling subscription, plain `synchronized` methods suffice (the `synchronized` is mostly so the admin endpoint can read `state()` safely).

Its bean, declared in Chapter 11, sits in `StreamRuneConfig` next to the command-bus breaker of Step 1 — three consecutive calls without an answer open it, with a 30-second cooldown:

```java
@Bean
public PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker() {
  return new PaymentGatewayCircuitBreaker(3, Duration.ofSeconds(30));
}
```

The `PaymentProcessManager` bean (Chapter 11, Step 6) takes this breaker as a constructor argument — the manager consults `allowRequest()` before each gateway call and reports the outcome: `recordFailure()` for a call without an answer, `recordSuccess()` for a capture or a decline.

### Step 4: Add Admin Endpoints

Create `spring/controller/AdminController.java`. It exposes six endpoints useful during development and operations. In later chapters this same controller gains dead-letter and outbox endpoints (and the matching `DeadLetterQueue` / `OutboxStore` constructor arguments), so the version below is intentionally a chapter-12 subset — don't copy the full controller from the finished demo yet, or you will reference beans that don't exist until Chapters 13 and 14.

```java
package org.streamrune.ecommerce.spring.controller;

import java.util.Map;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.spring.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

  private final CircuitBreakerCommandInterceptor circuitBreaker;
  private final PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker;
  private final PaymentGatewaySimulator paymentGateway;

  public AdminController(
      CircuitBreakerCommandInterceptor circuitBreaker,
      PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker,
      PaymentGatewaySimulator paymentGateway) {
    this.circuitBreaker = circuitBreaker;
    this.paymentGatewayCircuitBreaker = paymentGatewayCircuitBreaker;
    this.paymentGateway = paymentGateway;
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

  /** Answers 403 unless the caller is an ADMIN: an identity with the ADMIN role (Chapter 9). */
  private static void requireAdmin() {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
  }
}
```

Every route but `/health` starts with `requireAdmin()`: the controller is the operators' panel, and what it shows and switches is internal state of the running system. The check is `OwnerOrAdminAccess.requireAdmin` from Chapter 9 — the `X-User-Id` and `X-User-Role` headers in this demo, a stand-in for the gateway that would authenticate an operator in a real deployment — so a call from anyone but an `ADMIN` is answered `403`, the role without an identity included. `/health` stays open: its answer says that the application runs and nothing else.

The `/circuit-breaker` endpoint reports **both** breakers. `circuitBreaker.circuitState()` delegates to `state.get().name()` inside the command-bus interceptor; `paymentGatewayCircuitBreaker.state().name()` is the gateway breaker. Both return one of `"CLOSED"`, `"OPEN"`, or `"HALF_OPEN"` as a plain string — no dedicated DTO needed. The two switches flip the simulator's flags: `payment-failure/toggle` makes the gateway decline, `payment-outage/toggle` makes it answer nothing. The outage in Step 5 exercises the **`paymentGateway`** value, not `state`.

### Step 5: Trip the Gateway Breaker

Start the application and confirm both circuits start closed:

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/circuit-breaker
# {"state":"CLOSED","paymentGateway":"CLOSED"}
```

A decline does not open the gateway breaker: it is an answer. Turn on payment failure and place an order — the gateway declines its payment, the manager dispatches `FailPayment`, the saga cancels the order and lands in `FAILED`, and `paymentGateway` stays `CLOSED`. Switch it off again:

```bash
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/payment-failure/toggle
# {"failureInjected":true}

curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" -H "X-User-Role: CUSTOMER" -H "X-User-Id: c-1" \
  -d '{"orderId":"cb-decline-1","customerId":"c-1","lines":[{"productId":"p-1","quantity":1}]}'

curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/saga/fulfillments/fulfillment-cb-decline-1 | jq .fulfillmentStatus
# "FAILED"

curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/payment-failure/toggle
# {"failureInjected":false}
```

An outage opens it. Turn the outage on and place three orders. Each order drives the saga to `InitiatePayment`; the `PaymentProcessManager` calls `processPayment`, which gets no answer; the manager records the failure on the gateway breaker, finds no capture at the gateway, and lets the event be delivered again. The subscription delivers the first order's `PaymentInitiated` again after a backoff, and the payments behind it wait for it. After three calls without an answer the gateway breaker opens, and the manager stops calling the gateway:

```bash
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/payment-outage/toggle
# {"outage":true}

# place three orders (different ids), e.g.:
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/api/orders \
    -H "Content-Type: application/json" -H "X-User-Role: CUSTOMER" -H "X-User-Id: c-1" \
    -d "{\"orderId\":\"cb-$i\",\"customerId\":\"c-1\",\"lines\":[{\"productId\":\"p-1\",\"quantity\":1}]}"
done

curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/circuit-breaker
# {"state":"CLOSED","paymentGateway":"OPEN"}
```

Nothing has decided the payments: their sagas wait for them, the orders are neither confirmed nor cancelled, and the subscription logs every delivery without an answer at `ERROR`:

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/saga/fulfillments/fulfillment-cb-1 | jq .fulfillmentStatus
# "AWAITING_PAYMENT"
```

Turn the outage off. Once the 30-second cooldown has passed, the next delivery is the breaker's probe: the gateway answers, the breaker closes, the payments are captured and the sagas carry on, so the orders end `CONFIRMED`. An outage longer than the saga's deadline (fifteen minutes) cancels its orders instead, and their pending payments are voided:

```bash
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/payment-outage/toggle
# {"outage":false}

curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/saga/fulfillments/fulfillment-cb-1 | jq .fulfillmentStatus
# "COMPLETED"
```

Note that the command-bus breaker (`state`) stays `CLOSED` throughout — exactly as Step 3 predicted. A gateway call is no command: a call without an answer dispatches nothing, and a decline becomes a successful `FailPayment`/`CancelOrder`, so only the dedicated gateway breaker reacts.

### Step 6: Walk the Full Lifecycle with a Test

The most reliable way to see every transition — and to do it deterministically without waiting 30 seconds of wall-clock cooldown — is to drive the interceptor directly from a unit test. The breaker exposes a constructor that takes a monotonic `LongSupplier` time source, so the test controls when the cooldown elapses. The test calls the interceptor the way the bus does: `before()` for every command, then `after()` with the command's result when it succeeds or `onError()` when it fails. Add `spring-app/src/test/java/org/streamrune/ecommerce/spring/CircuitBreakerLifecycleTest.java` (the demo ships the same file):

```java
package org.streamrune.ecommerce.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;

class CircuitBreakerLifecycleTest {

  /** What the bus passes to before() and onError(): the command, no result yet. */
  private static final CommandContext CTX =
      new CommandContext(
          new PaymentCommand.CapturePayment("pay-1"),
          "CapturePayment",
          CommandId.of("test-id"),
          PaymentState.TYPE,
          AggregateId.of("pay-1"),
          null,
          Instant.now());

  /** What the bus passes to after() once the command succeeded: the same context with a result. */
  private static final CommandContext SUCCEEDED =
      new CommandContext(
          CTX.command(),
          CTX.commandType(),
          CTX.commandId(),
          CTX.aggregateType(),
          CTX.aggregateId(),
          new CommandResult(
              List.of(),
              StreamId.of(PaymentState.TYPE, AggregateId.of("pay-1")),
              new Version(1),
              List.of()),
          CTX.timestamp());

  @Test
  void closed_to_open_to_halfOpen_to_closed() {
    AtomicLong clock = new AtomicLong(0);
    var cb =
        new CircuitBreakerCommandInterceptor(
            3,
            Duration.ofSeconds(30),
            CircuitBreakerCommandInterceptor.DEFAULT_PROBE_TIMEOUT,
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock::get);

    // CLOSED: commands pass through.
    assertTrue(cb.before(CTX));
    cb.after(SUCCEEDED);
    assertEquals("CLOSED", cb.circuitState());

    // Three infrastructure failures in a row open the circuit.
    failOnce(cb);
    failOnce(cb);
    assertEquals("CLOSED", cb.circuitState()); // still under threshold
    failOnce(cb);
    assertEquals("OPEN", cb.circuitState());

    // OPEN before the cooldown elapses: fail fast with no handler call.
    var rejected = assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
    assertTrue(rejected.getMessage().contains("CapturePayment"));

    // Advance past the cooldown: the next request becomes the probe (HALF_OPEN).
    clock.addAndGet(Duration.ofSeconds(31).toNanos());
    assertTrue(cb.before(CTX));
    assertEquals("HALF_OPEN", cb.circuitState());

    // Probe succeeds: after() closes the circuit and resets the counter.
    cb.after(SUCCEEDED);
    assertEquals("CLOSED", cb.circuitState());
  }

  /** One command the bus admits and then sees fail on infrastructure. */
  private static void failOnce(CircuitBreakerCommandInterceptor cb) {
    assertTrue(cb.before(CTX));
    cb.onError(CTX, new RuntimeException("gateway down"));
  }
}
```

Run it:

```bash
./gradlew :spring-app:test --tests "org.streamrune.ecommerce.spring.CircuitBreakerLifecycleTest"
```

The exception thrown while the circuit is `OPEN` interpolates the live command type via `ctx.commandType()`, so its message reads:

```
CircuitBreakerOpenException: Circuit breaker OPEN — rejecting command: CapturePayment
```

(with whatever command type was dispatched — `CapturePayment` here because that is the context the test supplies). Each transition maps to one interceptor method: `before()` allows or fast-rejects, `onError()` counts eligible failures and opens, and `after()` closes the circuit when the probe thread succeeds.

---

## What We Learned

**`CircuitBreakerCommandInterceptor`** is a `CommandInterceptor` with three entry points. `before` decides whether to allow the command through or throw immediately. `after` is called on success and closes the circuit if this thread was the probe. `onError` is called on failure and opens the circuit once the threshold is reached. The three methods together implement the complete state machine using only a small set of lock-free fields: three atomics (`AtomicInteger failureCount`, `AtomicReference<State> state`, `AtomicLong probeStartedAtNano`), a `volatile long openedAtNano`, and a thread-local stack of probe flags (`probeStack`).

**The three circuit states** map directly to a Java enum inside the interceptor. `CLOSED` is the default and permits all commands. `OPEN` blocks all commands except the first one through after the cooldown, which transitions the state to `HALF_OPEN`. `HALF_OPEN` blocks every subsequent command until the probe succeeds (close) or fails (re-open). The thread-local probe stack is the key design detail: it lets `after` and `onError` know whether the execution that is finishing is the probe or an ordinary command without adding extra synchronization.

**`PaymentGatewaySimulator`** models the pattern you will use for any external service: a plain class whose `processPayment` method succeeds silently, declines with a typed exception, or fails without an answer. In production you would replace the `AtomicBoolean` checks with an HTTP call that sends the payment id as the gateway's idempotency key, so a call repeated after a capture the app could not record charges nothing, and `hasCaptured` with the gateway's status lookup by that key. Only the gateway's decline fails a payment. A network timeout, an HTTP 500 or a reset connection is a call without an answer: the gateway may have taken the money, so the `PaymentProcessManager` asks for the payment's status and otherwise waits, and the breaker sees the `recordFailure()` call the manager makes for it.

**`PaymentGatewayCircuitBreaker`** is the second breaker, and the chapter's main lesson is *where* it lives. The command-bus `CircuitBreakerCommandInterceptor` is the right tool for command-dispatch failures (event-store outages, locking errors), but it is the wrong tool for the payment gateway: a call without an answer dispatches no command at all, a decline is converted into successful compensation commands, and the interceptor resets its counter on every success. The reverse holds too: when the command-bus breaker is open, the manager's `CapturePayment` or `FailPayment` is refused, and that refusal says nothing about the gateway — the manager lets it propagate, the subscription delivers `PaymentInitiated` again after a backoff, and the gateway breaker counts nothing. A breaker wrapped directly around the flaky dependency — counting only that dependency's outcomes — is the correct design. Same state machine, different inputs (`recordSuccess()` / `recordFailure()` instead of command outcomes), driven by the single-threaded `PaymentProcessManager`.

**A refusal is `503`, not `500`.** An open breaker, an overloaded or closing command bus, an aggregate lock that could not be taken in time and a version conflict on every attempt all refuse a command before anything is written, and the same request succeeds later. The demo answers each `503` with `Retry-After: 5` and `{"reason": ..., "message": ...}` (`CommandRefusal`); a genuine failure stays `500`.

**`circuitState()`** exposes the command-bus breaker's internal enum as a string, and the gateway breaker's `state()` does the same. The `/api/admin/circuit-breaker` endpoint returns both (`state` and `paymentGateway`). This is enough for a health dashboard or a simple alert rule. If you need richer observability — failure count, time since opened, probe history — you can extend either breaker to populate a metrics counter or publish a custom event.

---

## Next Up

Next: the inventory's read model and the transactional outbox pattern, which forwards a curated set of integration events to RabbitMQ.
