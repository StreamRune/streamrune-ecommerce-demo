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

The `VirtualThreadCommandBus` takes an ordered list of interceptors. Their `before` methods run in that order around every command dispatch, and their `after` and `onError` methods in reverse order. Add `circuitBreaker` to both the method signature and the end of the builder call:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit,
    CircuitBreakerCommandInterceptor circuitBreaker) {
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .interceptors(audit, auth, validation, circuitBreaker)
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
      // ... aggregate registrations unchanged
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
| `OptimisticLockException` | `AGGREGATE_VERSION_CONFLICT` | The aggregate's stream moved past the version the command was decided on, on every attempt of the bus's retry policy: another writer appended to it between the load and the append each time — a second instance whose lock is its own, or a writer outside the command bus. Nothing was written. |

```json
{"reason": "CIRCUIT_BREAKER_OPEN", "message": "The command bus refuses commands while its circuit breaker is open."}
```

**Why `503` and not `409` for a lock or a version conflict.** `LockException` is, in its own javadoc, an acquisition failure that the command bus treats as transient and retries — not a conflict with the state of the aggregate. `409 Conflict` tells a client that its request conflicts with the resource and that it may resolve the conflict and resubmit; here nothing in the request is wrong. The aggregate is busy, or the locker's store is out of reach, and the bus has already retried before it gave up: the request will succeed unchanged once the lock is free, which is what `503` with `Retry-After` says. A version conflict is the same answer for the same reason: the bus reloads the aggregate on every attempt, the client sent no version of its own that could conflict, and the identical request succeeds once the other writer is done. Like any infrastructure failure, both commands are also recorded in the dead-letter queue (Chapter 14).

`Retry-After` is a constant five seconds: the breaker does not report how long it stays open, and the other refusals have no natural duration. A genuine failure — the event store refusing the append, say — is none of the five and stays `500`.

The lock timeout is the framework's `streamrune.lock-timeout`. The bus the framework auto-configures reads it; the demo builds its own bus, so it hands the builder the auto-configured `AggregateLocker` and the configured timeout. Add the two parameters and the two calls to the `commandBus` bean (`AggregateLocker` is in `org.streamrune.core`, `StreamRuneProperties` in `org.streamrune.spring`):

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit,
    CircuitBreakerCommandInterceptor circuitBreaker,
    AggregateLocker locker,                       // <-- added
    StreamRuneProperties properties) {            // <-- added
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .locker(locker)                             // <-- added
      .lockTimeout(properties.lockTimeout())      // <-- added
      .interceptors(audit, auth, validation, circuitBreaker)
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
      // ... aggregate registrations unchanged
      .build();
}
```

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

The command bus throws each of the five unwrapped, so the handler sees the exception itself. The Quarkus and Micronaut apps answer the same: JAX-RS and Micronaut pick a handler by the thrown type, so each app has one per exception type, nested in `CommandRefusalExceptionMappers` (Quarkus) and `CommandRefusalExceptionHandlers` (Micronaut). `CommandRefusalStatusIT` in each app forces four of the refusals on the app's real command bus, in an application of its own with a lock timeout of one second: a price change stopped in its audit write while it holds the product's lock (a trigger on `audit_log` waits for an advisory lock the test holds) makes a stock adjustment of the same product wait out the lock timeout on each of three attempts; a trigger on `event_stream` that raises `unique_violation` for a price change makes the event store report the version conflict it reports when a second writer stored the version first, on every attempt; a store that refuses every price change of a product answers `500` until the breaker opens and `503` after; and a closed bus refuses the next command. Each answer is checked for the status, `Retry-After` and the body. The overloaded bus cannot be reached through the demo's routes, so a unit test in each app checks its answer.

### Step 3: The gateway call, and why it needs its own breaker

You created `PaymentGatewaySimulator` in Chapter 11 (`spring-app/src/main/java/org/streamrune/ecommerce/spring/config/PaymentGatewaySimulator.java`). The class holds an `AtomicBoolean failureInjected` toggle. When `failureInjected` is true, `processPayment` throws a `RuntimeException` — unless the gateway already captured that payment, because the payment id is its idempotency key:

```java
public void processPayment(String paymentId) {
  if (capturedPayments.contains(paymentId)) {
    return;
  }
  if (failureInjected.get()) {
    throw new RuntimeException("Simulated payment gateway failure for payment " + paymentId);
  }
  capturedPayments.add(paymentId);
}
```

Unlike the early scaffolding, this method is now genuinely on the hot path: the `PaymentProcessManager` (Chapter 11) calls `gateway.processPayment(paymentId)` every time it reacts to a `PaymentInitiated` event. So toggling `failureInjected` on really does make payments fail, which really does drive the saga down its compensation path. That makes the payment gateway exactly the kind of flaky external dependency a circuit breaker is meant to protect.

**Why not just let the command-bus breaker handle it?** The `processPayment` failure does not, on its own, reach the command-bus breaker — the manager catches it and turns it into a *successful* `FailPayment` command, and the saga then runs a *successful* `CancelOrder`. From the command bus's point of view, those are normal successful dispatches. Worse, the command-bus breaker resets its consecutive-failure count on **any** successful command (`after()` closes the circuit), so the saga's own compensation traffic would keep clearing the counter. A breaker on the command bus can never "see" a run of gateway failures here.

The textbook fix is to put the breaker around the flaky dependency itself. `PaymentGatewayCircuitBreaker` counts only gateway-call outcomes, so consecutive gateway failures open it regardless of what the saga does on the command bus. While it is open the `PaymentProcessManager` fast-fails the payment (dispatches `FailPayment` without calling the gateway) until the cooldown admits a probe. A payment that the order's cancellation voided before the manager reached it is neither failed nor sent to the gateway: the manager reads the payment's stream first (Chapter 11, Step 5b).

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/PaymentGatewayCircuitBreaker.java`:

```java
package org.streamrune.ecommerce.spring.config;

import java.time.Duration;
import java.time.Instant;

public class PaymentGatewayCircuitBreaker {

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int failureThreshold;
  private final Duration cooldown;

  private State state = State.CLOSED;
  private int consecutiveFailures = 0;
  private Instant openedAt = null;

  public PaymentGatewayCircuitBreaker(int failureThreshold, Duration cooldown) {
    this.failureThreshold = failureThreshold;
    this.cooldown = cooldown;
  }

  /**
   * Whether a gateway call may be attempted now. CLOSED always allows; OPEN allows a single probe
   * once the cooldown has elapsed (transitioning to HALF_OPEN); HALF_OPEN allows the probe.
   */
  public synchronized boolean allowRequest() {
    if (state == State.OPEN
        && openedAt != null
        && !Instant.now().isBefore(openedAt.plus(cooldown))) {
      state = State.HALF_OPEN;
    }
    return state != State.OPEN;
  }

  /** Records a successful gateway call: closes the breaker and clears the failure count. */
  public synchronized void recordSuccess() {
    consecutiveFailures = 0;
    state = State.CLOSED;
    openedAt = null;
  }

  /** Records a failed gateway call: opens the breaker once the threshold is reached. */
  public synchronized void recordFailure() {
    consecutiveFailures++;
    if (consecutiveFailures >= failureThreshold || state == State.HALF_OPEN) {
      state = State.OPEN;
      openedAt = Instant.now();
    }
  }

  /** Current state, applying the cooldown so a long-open breaker reports HALF_OPEN when due. */
  public synchronized State state() {
    if (state == State.OPEN
        && openedAt != null
        && !Instant.now().isBefore(openedAt.plus(cooldown))) {
      state = State.HALF_OPEN;
    }
    return state;
  }
}
```

It implements the same CLOSED → OPEN → HALF_OPEN → CLOSED state machine as the command-bus interceptor, but its inputs are explicit `recordSuccess()` / `recordFailure()` calls from the `PaymentProcessManager` rather than command outcomes. Because it is driven by a single polling subscription, plain `synchronized` methods suffice (the `synchronized` is mostly so the admin endpoint can read `state()` safely).

Register it in `StreamRuneConfig` next to the command-bus breaker — three consecutive failures open it, with a 30-second cooldown:

```java
@Bean
public PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker() {
  return new PaymentGatewayCircuitBreaker(3, Duration.ofSeconds(30));
}
```

The `PaymentProcessManager` bean (Chapter 11, Step 6) already takes this breaker as a constructor argument, so once the bean exists the wiring is complete — the manager consults `allowRequest()` before each gateway call and reports the outcome. A breaker-open `FailPayment` of a payment that was voided in the meantime stores nothing: the payment decider fails a pending payment only, so the cancellation's void stands.

### Step 4: Add Admin Endpoints

Create `spring/controller/AdminController.java`. It exposes four endpoints useful during development and operations. In later chapters this same controller gains dead-letter and outbox endpoints (and the matching `DeadLetterQueue` / `OutboxStore` constructor arguments), so the version below is intentionally a chapter-12 subset — don't copy the full controller from the finished demo yet, or you will reference beans that don't exist until Chapters 13 and 14.

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

The `/circuit-breaker` endpoint reports **both** breakers. `circuitBreaker.circuitState()` delegates to `state.get().name()` inside the command-bus interceptor; `paymentGatewayCircuitBreaker.state().name()` is the gateway breaker. Both return one of `"CLOSED"`, `"OPEN"`, or `"HALF_OPEN"` as a plain string — no dedicated DTO needed. The toggle in Step 5 exercises the **`paymentGateway`** value, not `state`.

### Step 5: Trip the Gateway Breaker

Start the application and confirm both circuits start closed:

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/circuit-breaker
# {"state":"CLOSED","paymentGateway":"CLOSED"}
```

Now turn on payment failure and place a few orders. Each order drives the saga to `InitiatePayment`; the `PaymentProcessManager` calls `processPayment`, which throws; the manager records the failure on the gateway breaker. After three consecutive gateway failures the gateway breaker opens:

```bash
curl -s -X POST -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/payment-failure/toggle
# {"failureInjected":true}

# place three orders (different ids), e.g.:
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/api/orders \
    -H "Content-Type: application/json" -H "X-User-Role: CUSTOMER" -H "X-User-Id: c-1" \
    -d "{\"orderId\":\"cb-$i\",\"customerId\":\"c-1\",\"lines\":[{\"productId\":\"prod-widget\",\"quantity\":1}]}"
done

curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/circuit-breaker
# {"state":"CLOSED","paymentGateway":"OPEN"}
```

Each of those orders fails at the payment step, so their sagas land in `FAILED`:

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/saga/fulfillments/fulfillment-cb-1 | jq .fulfillmentStatus
# "FAILED"
```

Note that the command-bus breaker (`state`) stays `CLOSED` throughout — exactly as Step 3 predicted. The gateway failures never surface as command-bus failures (the manager turns them into successful `FailPayment`/`CancelOrder` dispatches), so only the dedicated gateway breaker reacts. Toggle the flag back off and, after the 30-second cooldown, the next order's gateway call probes the gateway, succeeds, and closes the breaker again.

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

**`PaymentGatewaySimulator`** models the pattern you will use for any external service: a plain class whose `processPayment` method either succeeds silently or throws. In production you would replace the `AtomicBoolean` check with an HTTP call that sends the payment id as the gateway's idempotency key, so a call repeated after a capture the app could not record charges nothing. The breaker does not know or care whether the failure comes from a network timeout, an HTTP 500, or a thrown exception — it only sees the `recordFailure()` call the `PaymentProcessManager` makes when the call throws.

**`PaymentGatewayCircuitBreaker`** is the second breaker, and the chapter's main lesson is *where* it lives. The command-bus `CircuitBreakerCommandInterceptor` is the right tool for command-dispatch failures (event-store outages, locking errors), but it is the wrong tool for the payment gateway: the gateway failure is converted into successful compensation commands, and the interceptor resets its counter on every success. The reverse holds too: when the command-bus breaker is open, the manager's `CapturePayment` or `FailPayment` is refused, and that refusal says nothing about the gateway — the manager lets it propagate, the subscription delivers `PaymentInitiated` again after a backoff, and the gateway breaker counts nothing. A breaker wrapped directly around the flaky dependency — counting only that dependency's outcomes — is the correct design. Same state machine, different inputs (`recordSuccess()` / `recordFailure()` instead of command outcomes), driven by the single-threaded `PaymentProcessManager`.

**A refusal is `503`, not `500`.** An open breaker, an overloaded or closing command bus, an aggregate lock that could not be taken in time and a version conflict on every attempt all refuse a command before anything is written, and the same request succeeds later. The demo answers each `503` with `Retry-After: 5` and `{"reason": ..., "message": ...}` (`CommandRefusal`); a genuine failure stays `500`.

**`circuitState()`** exposes the command-bus breaker's internal enum as a string, and the gateway breaker's `state()` does the same. The `/api/admin/circuit-breaker` endpoint returns both (`state` and `paymentGateway`). This is enough for a health dashboard or a simple alert rule. If you need richer observability — failure count, time since opened, probe history — you can extend either breaker to populate a metrics counter or publish a custom event.

---

## Next Up

Let's add our final aggregate — Inventory — and learn about the transactional outbox pattern.
