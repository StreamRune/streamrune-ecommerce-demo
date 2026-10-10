# Chapter 15: Observability

> **What you'll learn:** How `MicrometerStreamRuneMetrics` exposes command, event, and projection counters through Spring Boot Actuator (auto-registered once a `MeterRegistry` is on the classpath), how to opt into `OpenTelemetryCommandInterceptor` so each command dispatch creates a trace span, how `StreamRuneHealthIndicator` surfaces database and subscription health at `/actuator/health`, how to serve a live SSE stream of all events using `EventExplorerController`, and how to switch on the framework's own SSE endpoint for the events of one aggregate.

---

## What We're Building and Why

A system that works but cannot be observed is a system you cannot trust in production. When an order fails to process, you need to know which command was running, how long it took, whether the database was reachable, and what events were written before the failure. Without instrumentation, your only tool is log scanning after the fact.

StreamRune's observability layer is built on three industry-standard foundations: Micrometer for metrics, OpenTelemetry for distributed tracing, and Spring Boot Actuator for health checks. A fourth capability — a live SSE stream of every event appended to the event store — gives operators and frontend dashboards a real-time view of system activity.

In this chapter you will:

- Confirm the Spring Boot Actuator and Micrometer dependencies are present so that the `/actuator` endpoints are available.
- Verify `MicrometerStreamRuneMetrics` records counters and timers for commands, events, and projections. Because the auto-configuration registers this bean automatically when a `MeterRegistry` is present — and actuator provides a `SimpleMeterRegistry` — the wiring reduces to verifying that the dependency is on the classpath.
- Opt into `OpenTelemetryCommandInterceptor` as a command interceptor so that every command dispatch creates an OpenTelemetry span with the command type, command ID, and aggregate ID as attributes. The shipped demo does not wire this — it is an addition you make in Step 3.
- Verify that `StreamRuneHealthIndicator` is registered and returns meaningful detail about the PostgreSQL connection and subscription lag.
- Create `EventExplorerController`, which exposes three endpoints: a paginated list of all events in the global stream and a per-stream event list, both for the ADMIN role because they show decrypted payloads, and an open SSE endpoint that announces new events in real time, without their payload.
- Switch on the framework's own SSE endpoint, `GET /api/sse/{aggregateType}/{aggregateId}`, which streams the events of one aggregate. The framework serves it and feeds it; the application adds a property and an `SseAuthorizer`.

---

## Step by Step

### Step 1: Add Spring Boot Actuator and Micrometer Dependencies

Open `spring-app/build.gradle.kts`. The actuator starter and `opentelemetry-api` are both already present — both from Chapter 1: the actuator starter for the health and metrics endpoints, and `opentelemetry-api` because `StreamRuneAutoConfiguration` references it for AOT (it is also needed by `OpenTelemetryCommandInterceptor` introduced in Step 3). The relevant lines are:

```kotlin
dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation(libs.opentelemetry.api)
    // … other deps
}
```

Actuator pulls in Micrometer core transitively, which is enough for the counters and timers that `MicrometerStreamRuneMetrics` registers. With Micrometer core but no registry on the classpath, Spring Boot auto-configures a `SimpleMeterRegistry` — meters are recorded in memory and surfaced at `/actuator/metrics`. The demo does **not** depend on `micrometer-registry-prometheus`, so there is no `/actuator/prometheus` endpoint. If you want Prometheus scraping, add `implementation("io.micrometer:micrometer-registry-prometheus")` first, then expose `prometheus` in the endpoint list below.

The demo configures management endpoints in `src/main/resources/application.yml`, not in `application.properties` (which holds only datasource/AOT settings). It exposes the `health` and `metrics` endpoints:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics
```

To see the full StreamRune health detail at `/actuator/health` rather than a bare `{"status":"UP"}`, add `show-details: always` under the same `management` block:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics
  endpoint:
    health:
      show-details: always
```

### Step 2: Verify MicrometerStreamRuneMetrics Auto-wiring

`StreamRuneAutoConfiguration` registers a `MicrometerStreamRuneMetrics` bean whenever a `MeterRegistry` bean is present and `streamrune.metrics.enabled` is not explicitly set to `false`. Because `spring-boot-starter-actuator` auto-configures a `SimpleMeterRegistry` (the demo ships no registry of its own; it would auto-configure a Prometheus registry instead only if `micrometer-registry-prometheus` were on the classpath), the metrics bean is registered without any additional code. Confirm it is live by listing `/actuator/metrics` and checking that `streamrune.commands.dispatched` appears.

The bean registers, among others, these meters (`MetricNames` in `streamrune-core` lists every name, including the outbox, dead-letter, saga, snapshot, query, and lock meters):

| Meter | Type | Description |
|---|---|---|
| `streamrune.commands.dispatched` | Counter | Total commands dispatched |
| `streamrune.commands.succeeded` | Counter | Commands that completed without error |
| `streamrune.commands.failed` | Counter | Commands that threw an exception |
| `streamrune.commands.duration` | Timer | Command execution latency |
| `streamrune.events.appended` | Counter | Events written to the event store |
| `streamrune.events.replayed` | Counter | Events folded into aggregate state on load: those after the snapshot, or the whole stream when there is none |
| `streamrune.events.duration` | Timer | Event append latency, sampled by the command bus for each append that carries events |
| `streamrune.projections.processed` | Counter | Events processed by projections |
| `streamrune.projections.failed` | Counter | Projection handler failures |
| `streamrune.projections.duration` | Timer | Projection processing latency |
| `streamrune.subscriptions.events.received` | Counter | Events received by subscriptions |
| `streamrune.subscriptions.delivery.latency` | Timer | Event delivery latency |

If you want to override the auto-configured bean with a custom `MeterRegistry` — for example, to attach tags for the deployment environment — declare the bean explicitly in `StreamRuneConfig.java`:

```java
@Bean
public MicrometerStreamRuneMetrics streamRuneMetrics(MeterRegistry registry) {
    return MicrometerStreamRuneMetrics.builder()
        .registry(registry)
        .build();
}
```

Because the auto-configured `micrometerStreamRuneMetrics` bean method is annotated with `@ConditionalOnMissingBean(StreamRuneMetrics.class)`, your explicit declaration wins.

### Step 3: Wire OpenTelemetryCommandInterceptor

`OpenTelemetryCommandInterceptor` implements `CommandInterceptor`. Its `before()` method starts an OpenTelemetry span named `command/<CommandType>` with `streamrune.command.type`, `streamrune.command.id`, and `streamrune.aggregate.id` as span attributes. Its `after()` method adds `streamrune.stream.id`, the stream the command committed to, and closes the span with `OK` status; `onError()` records the exception and closes it with `ERROR` status. Every attribute key carries the `streamrune.` prefix; `TraceAttributes` in `streamrune-runtime` lists them, so filter traces on `streamrune.command.type`, not `command.type`.

`StreamRuneAutoConfiguration` registers this interceptor automatically (with `@Order(ORDER_OPEN_TELEMETRY)`, so it sits outermost) whenever an `io.opentelemetry.api.OpenTelemetry` bean is present. The shipped demo's `StreamRuneConfig` defines **no** `OpenTelemetry` bean and does **not** add the interceptor to its hand-wired `commandBus` — so distributed tracing is an opt-in addition. Because the demo wires its `commandBus` by hand (rather than relying on `StreamRuneAutoConfiguration`), the auto-configured interceptor would not be picked up even if an `OpenTelemetry` bean existed. So declare the interceptor bean yourself and inject it into the `commandBus` constructor (next). Add this bean to `StreamRuneConfig.java`:

```java
@Bean
public OpenTelemetryCommandInterceptor otelInterceptor() {
    return new OpenTelemetryCommandInterceptor(
        io.opentelemetry.api.OpenTelemetry.noop());
}
```

The no-op `OpenTelemetry` keeps the wiring simple while you learn the mechanics. In production, replace `OpenTelemetry.noop()` with a configured SDK instance that exports spans to your collector. The interceptor itself is SDK-agnostic — it depends only on the `opentelemetry-api` artifact, not on any SDK.

Once registered, the interceptor must be added to the `VirtualThreadCommandBus` interceptor chain. You already built the `commandBus` bean in the command-bus chapter — it declares the four existing interceptors as constructor parameters and wires them through `.interceptors(...)`, followed by the command inbox (chapter 11), the dead-letter queue (chapter 14), `.snapshotPolicy(...)` and one `.register(...)` call per aggregate. Do **not** replace that bean — make two surgical edits:

1. Add `OpenTelemetryCommandInterceptor otel` to the parameter list.
2. Prepend `otel` to the existing `.interceptors(...)` call.

Leave every other parameter and builder call exactly as it is. The edited bean looks like this (register calls elided for brevity — keep yours):

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit,
    CircuitBreakerCommandInterceptor circuitBreaker,
    PostgresCommandInbox commandInbox,                // keep
    PostgresDeadLetterQueue deadLetterQueue,          // keep
    PostgresCryptoEngine cryptoEngine,                // keep
    OpenTelemetryCommandInterceptor otel) {           // <-- added parameter
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .commandInbox(commandInbox)                      // keep
      .interceptors(otel, audit, auth, validation, circuitBreaker)  // <-- otel prepended
      .deadLetterQueue(deadLetterQueue)                // keep
      .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))  // keep
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))  // keep
      .register(/* ProductState.TYPE, ProductCommand ... */)              // keep all register(...) calls
      .register(/* OrderState.TYPE, OrderCommand ... */)
      .register(/* CustomerState.TYPE, CustomerCommand ... */)
      .register(/* PaymentState.TYPE, PaymentCommand ... */)
      .register(/* InventoryState.TYPE, InventoryCommand ... */)
      .build();
}
```

Place `otel` first in the interceptor list so the span is open for the entire duration of command processing, including auditing, authorization, and validation; `CommandInterceptorOrdering.ORDER_OPEN_TELEMETRY` is the outermost slot of the framework's canonical order.

### Step 4: Verify StreamRuneHealthIndicator

`StreamRuneAutoConfiguration` registers `StreamRuneHealthIndicator` automatically when a `DataSource` bean and the `HealthIndicator` interface are on the classpath. Its auto-config method is `streamRuneHealthIndicator(DataSource, ObjectProvider<SubscriptionHealthContributor>, ObjectProvider<BackgroundRelayHealthContributor>)` — it has no `EventStore` dependency. No explicit bean declaration is needed.

The indicator performs these checks:

1. Opens a JDBC connection to the `DataSource` and calls `conn.isValid(3)`. If this fails, the overall status is `DOWN` with an `error` detail.
2. While that same connection is open and the database is up, it queries the head of the `event_stream` table directly via SQL (`SELECT global_offset, created_at FROM event_stream ORDER BY global_offset DESC LIMIT 1`) and reports `eventStore.lastGlobalOffset` and `eventStore.lastEventTimestamp` in the details. No `EventStore` bean is involved — the read is plain JDBC on the health-check connection. A probe failure (e.g. the table not yet migrated) is reported as an `eventStore.error` detail but does not flip the overall status.
3. When a `SubscriptionHealthContributor` bean is present, reports each subscription's `state`, `lag`, `errorCount`, and `status`. If any subscription is `DOWN`, the overall health status becomes `DOWN`.
4. Reports every background relay registered with the `BackgroundRelayHealthContributor` as a `relay.<name>` detail with `status`, `started`, `alive`, and `consecutiveFailures`. A relay that was started but whose poll thread has died is `DOWN`, and that turns the overall status `DOWN`: without this check `/actuator/health` would read `UP` while the outbox stopped publishing or dead-lettered commands stopped being replayed. A relay that is running but whose recent poll cycles failed (its database or broker is unreachable) reads `DEGRADED` in its own block and leaves the overall status alone. The framework registers the relays it builds; in the demo those are the outbox relay (`relay.outbox-relay`), the outbox, inbox, dead-letter and saga dead-letter retention sweepers, and the saga compensation-retry sweeper. A relay you build yourself registers itself: the demo's `deadLetterRetryRunner` bean (Chapter 14) replaces the framework's runner and calls `relayHealth.registerDlqRetryRunner(runner)`, so it appears as `relay.dead-letter-retry`.

The indicator is exposed under the bean name `streamRune` at `/actuator/health/streamRune`. The full health response at `/actuator/health` aggregates it alongside Spring's built-in `db` and `diskSpace` indicators.

No additional code is required for this step. Proceed to Step 5 and return here to verify once the application is running.

### Step 5: Create EventExplorerController

Create `EventExplorerController` in the `org.streamrune.ecommerce.spring.controller` package. This controller exposes three endpoints on `/api/events`:

```java
package org.streamrune.ecommerce.spring.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * The demo's window on the event store: the global event list, one stream's history, and a live
 * feed of new events.
 *
 * <p><b>The list and the stream history are ADMIN only.</b> They return each event's payload as the
 * event store reads it, and the store decrypts {@code @Encrypted} fields on the way out. Until a
 * customer's key is erased, these two endpoints therefore show that customer's name, e-mail and
 * address in plain text. They are gated with the same {@code requireRole("ADMIN")} check as the
 * mutating admin endpoints.
 *
 * <p><b>DEMO SHORTCUT, protect this in production.</b> The role is whatever the client sent in
 * {@code X-User-Role} (the gateway stand-in chapter 9 describes), so the check keeps the tutorial
 * honest but protects nothing against a caller who sets the header. A real deployment authenticates
 * the caller, derives the role from that identity, and does not expose raw event payloads outside
 * an operator's tooling.
 *
 * <p><b>The live feed is open and carries no payload.</b> The browser's {@code EventSource} cannot
 * send the role header, and the dashboard only needs to know that something happened to a stream. A
 * frame holds the event's global offset, its type, the stream it belongs to, its version in that
 * stream and its timestamp, never the event itself.
 *
 * <p>The feed reads the global stream from its first event and then polls once a second. Its frames
 * are unnamed (no {@code event:} field), so {@code EventSource.onmessage} receives every one of
 * them.
 */
@RestController
@RequestMapping("/api/events")
public class EventExplorerController {

  private final EventStore eventStore;
  private final ObjectMapper objectMapper;

  public EventExplorerController(EventStore eventStore, ObjectMapper objectMapper) {
    this.eventStore = eventStore;
    this.objectMapper = objectMapper;
  }

  @GetMapping
  public List<Map<String, Object>> listEvents(
      @RequestParam(defaultValue = "0") long offset, @RequestParam(defaultValue = "50") int limit) {
    AdminController.requireRole("ADMIN");
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @GetMapping("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    AdminController.requireRole("ADMIN");
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> sseStream() {
    Sinks.Many<ServerSentEvent<String>> sink = Sinks.many().unicast().onBackpressureBuffer();

    Thread.ofVirtual()
        .name("sse-event-stream")
        .start(
            () -> {
              GlobalOffset offset = GlobalOffset.initial();
              while (sink.currentSubscriberCount() > 0 || !Thread.currentThread().isInterrupted()) {
                try {
                  var events = eventStore.readGlobalStream(offset, 100);
                  for (var env : events) {
                    var data = objectMapper.writeValueAsString(toFrame(env));
                    sink.tryEmitNext(
                        ServerSentEvent.<String>builder()
                            .id(String.valueOf(env.globalOffset().value()))
                            .data(data)
                            .build());
                    offset = env.globalOffset().next();
                  }
                  if (events.isEmpty()) {
                    Thread.sleep(Duration.ofSeconds(1));
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  sink.tryEmitError(e);
                  return;
                }
              }
              sink.tryEmitComplete();
            });

    return sink.asFlux();
  }

  /**
   * What the live feed says about an event: which one, of which stream, and when. No payload — the
   * feed is open to every caller.
   */
  private static Map<String, Object> toFrame(EventEnvelope env) {
    return Map.of(
        "globalOffset", env.globalOffset().value(),
        "streamId", env.streamId().value(),
        "aggregateType", env.aggregateType().value(),
        "aggregateId", env.aggregateId().value(),
        "eventType", env.eventType().name(),
        "version", env.version().value(),
        "timestamp", env.metadata().timestamp().toString());
  }

  private Map<String, Object> toMap(EventEnvelope env) {
    return Map.of(
        "globalOffset", env.globalOffset().value(),
        "streamId", env.streamId().value(),
        "aggregateType", env.aggregateType().value(),
        "aggregateId", env.aggregateId().value(),
        "eventType", env.eventType().name(),
        "timestamp", env.metadata().timestamp().toString(),
        "payload", env.event());
  }
}
```

The three endpoints serve different use cases:

- `GET /api/events?offset=0&limit=50` — paginated read of the global stream, **ADMIN only**. The `offset` is **exclusive** — `readGlobalStream` returns events whose global offset is strictly greater than it. Because offsets start at 1, `offset=0` returns the full history; use the last `globalOffset` you received as the next request's `offset` to page forward. Each response item includes `globalOffset`, `streamId`, `aggregateType`, `aggregateId`, `eventType`, `timestamp`, and the deserialized `payload`.
- `GET /api/events/{aggregateType}/{aggregateId}` — all events for a single stream, returned in append order, **ADMIN only**. A stream is the pair of the aggregate type its decider is registered under and the id the command bus's id extractor returned when the events were appended, written `<aggregateType>:<aggregateId>`. An order placed as `o-1` lives in `order:o-1`, and reserving stock for `p-1` writes to `inventory:p-1` while the product `p-1` stays in `product:p-1`. Both path values are client input and go through the ingress doors (`AggregateType.of`, `AggregateId.of`); a type outside `[a-z][a-z0-9_]{0,31}` is a `400`.
- `GET /api/events/sse` — a persistent SSE connection, **open to every caller and without payloads**. The server starts a virtual thread that polls `readGlobalStream` from `GlobalOffset.initial()` (which is the exclusive offset `0`, so the first poll replays the full history) and emits one SSE frame per event. The `id` field is the global offset; the `data` field is a JSON object with `globalOffset`, `streamId`, `aggregateType`, `aggregateId`, `eventType`, `version` and `timestamp`. There is no `event` field: an unnamed frame is what the browser's `EventSource` hands to `onmessage`, and the frontend in Chapter 16 relies on that. When no new events are present, the thread sleeps one second before polling again.

**Why two of them need ADMIN.** `payload` is `env.event()`: the event as the event store reads it. The store is the crypto-aware one from Chapter 6, and it decrypts `@Encrypted` fields on the way out. So the list and the stream history show a customer's name, e-mail, address and phone **in plain text** for as long as that customer's key exists; only after the key is erased (Chapter 7) do the same fields come back as `[REDACTED]`. Encryption at rest protects the database, not this endpoint. Both methods therefore start with `AdminController.requireRole("ADMIN")`, the check the mutating admin endpoints use (Chapter 14): without the role the answer is `403`, and no event is read.

**Why the feed does not.** The browser opens the feed with `EventSource`, which cannot send request headers, so the feed cannot ask for a role. It stays open instead and says less: `toFrame` builds a frame from the envelope's coordinates and never touches `env.event()`. A dashboard that only needs to know *that* something happened to a stream loses nothing.

> **Demo shortcut — protect this in production.** `requireRole` trusts the `X-User-Role` header, which any client can set to `ADMIN` (the gateway stand-in from Chapter 9). The check keeps the demo honest about *who should* see decrypted events, but it does not stop anyone who adds the header. In a real system an endpoint that returns raw event payloads belongs behind real authentication, with the role derived from the authenticated identity, on an operator-only network path — or it does not exist at all, and operators read events with tooling that has its own access control. The same goes for the open feed: stream ids and event types are metadata, and metadata can be sensitive too (a `customer:<id>` stream with a `CustomerForgotten` event tells a story). Decide per deployment whether an unauthenticated caller may see it.

The SSE endpoint uses `spring-boot-starter-webflux` for the `Flux` return type. The `webflux` dependency is already present in `build.gradle.kts` from an earlier chapter. The virtual thread approach means no Reactor scheduler is consumed while the thread is sleeping — the thread is parked at `Thread.sleep` at zero cost.

The controller injects `com.fasterxml.jackson.databind.ObjectMapper` — the Jackson 2 type. Spring Boot 4 auto-configures Jackson 3 (`tools.jackson.databind.ObjectMapper`), so this parameter resolves to the `legacyObjectMapper()` bean you declared in `StreamRuneConfig` (the framework still uses Jackson 2). If you have not defined that bean yet, the controller will fail to start with a no-such-bean error.

### Step 6: Add Event Explorer REST Endpoints for Global Stream and Per-Stream Access

The paginated global-stream endpoint is useful for admin dashboards that need to display recent activity across all aggregates. The per-stream endpoint is useful for debugging a specific order or customer by reading its exact event history.

Both endpoints are already implemented in `EventExplorerController`. Verify that the controller is picked up by Spring's component scan. Because the ecommerce demo uses a manual `@Configuration` class rather than auto-scan, you may need to verify that Spring detects the `@RestController` annotation. The class lives in `org.streamrune.ecommerce.spring.controller`, which is a sub-package of `org.streamrune.ecommerce.spring`, the application's base package — no additional scan configuration is required.

### Step 6b: Switch On the Framework's Per-Stream SSE Endpoint

`EventExplorerController`'s feed is the demo's own code: one stream of every event, without payloads. The framework ships a second, narrower endpoint — `GET /api/sse/{aggregateType}/{aggregateId}`, the events of **one** aggregate, each with its payload — and it needs no controller and no feeder code from you. It is off by default. Add to `application.yml`:

```yaml
streamrune:
  sse:
    # GET /api/sse/{aggregateType}/{aggregateId}. Off by default; needs an SseAuthorizer bean.
    enabled: true
    # How often the framework's feed reads the global stream: the upper bound on the delay between
    # a commit and its frame. The framework default is 1s.
    polling-interval: 100ms
```

and one bean to `StreamRuneConfig` (with `import org.streamrune.integration.SseAuthorizer;`):

```java
@Bean
public SseAuthorizer sseAuthorizer() {
  return (principal, streamId) -> true;
}
```

That is the whole wiring. With `streamrune.sse.enabled=true` the Spring integration registers three things:

- **`SseController`** — the endpoint. It builds the `StreamId` from the two path segments (an invalid one is a `400`), asks the `SseAuthorizer` whether the caller may read that stream (`403` if not), and subscribes the client. The caller is the request identity from Chapter 9: in the demo's trusted-gateway mode, the `X-User-Id` header.
- **`SseEventPublisher`** — the in-process fan-out the controller subscribes its clients to, one bounded queue per client. A client that cannot keep up is disconnected, not skipped.
- **`SseEventFeed`** — what publishes. One per application instance: a polling subscription on the global stream that starts at the **head** of the stream when the application starts, reads every `streamrune.sse.polling-interval`, and hands each event to the clients of that event's own stream. It keeps its position in memory — no row in the offset store — and stops with the application.

**Do not publish to `SseEventPublisher` yourself.** The feed already publishes every stored event; a subscription of your own that also calls `publish` would write every frame twice. `publish` takes the envelope only and routes it by the envelope's own stream id, so an event cannot reach the clients of another stream.

**Delivery is live, best-effort and at-most-once.** A frame reaches a client only while that client is connected. Nothing is redelivered and `Last-Event-ID` is not honoured: events stored before the client connected, while it was reconnecting, or while the instance was down are never sent to it. Within one stream, frames arrive in version order, up to one polling interval after the commit. Treat a frame as a notification — after every (re)connect read the current state from a query — and put anything that must see every event in a projection. This is the difference from the Event Explorer feed of Step 5, which starts at offset `0` for every client and replays the history.

**A frame carries the payload, so the authorizer matters.** The frame's `id` is the event's global offset and its `data` is the event as the event store reads it — with `@Encrypted` fields decrypted. A client of `/api/sse/customer/c-1` therefore receives that customer's name and e-mail in plain text with every `CustomerRegistered` or `ProfileUpdated` stored while it is connected — if the `SseAuthorizer` lets it in. Without an `SseAuthorizer` bean the framework installs one that denies every stream.

> **Demo shortcut — protect this in production.** The demo's authorizer answers `true` for every caller and every stream, because the demo has no login to check against. A real `SseAuthorizer` compares the authenticated `principal` with the requested `streamId` — a customer may read their own `customer:<id>` stream and the streams of their own orders, an operator more — and the identity it is handed must come from real authentication, not from a header the client sets.

> **The shipped `spring-app` also contains `controller/SseController`**, a subclass of the framework's controller mapped at the same path. It subscribes its clients to the same `SseEventPublisher` and asks the same `SseAuthorizer`, so the frames come from the same `SseEventFeed` with or without it.

> **Quarkus and Micronaut.** The same property and the same `SseAuthorizer` bean switch the endpoint on in `quarkus-app` and `micronaut-app`, and the framework feeds it there too; both apps keep the default polling interval of one second. On Micronaut the frame's `data` is written by Micronaut Serialization, so every domain event (and each record nested in one) needs a serializer: the events live in the framework-agnostic `domain` module, and `MicronautEcommerceApplication` imports them with `@SerdeImport`. Each of the three apps has an `SseLiveE2EIT` that opens a stream over HTTP, sends a command over HTTP and waits for the frame.

### Step 7: Verify — Health, Metrics, and SSE

Start the application and run the following verification commands.

**Health check:**

```bash
curl -s http://localhost:8080/actuator/health | jq .
```

Expected response shape:

```json
{
  "status": "UP",
  "components": {
    "streamRune": {
      "status": "UP",
      "details": {
        "eventStore.lastGlobalOffset": 42,
        "eventStore.lastEventTimestamp": "2026-04-25T09:00:00Z",
        "relay.outbox-relay": {
          "status": "UP",
          "started": true,
          "alive": true,
          "consecutiveFailures": 0
        },
        "relay.dead-letter-retry": {
          "status": "UP",
          "started": true,
          "alive": true,
          "consecutiveFailures": 0
        }
      }
    },
    "db": { "status": "UP" },
    "diskSpace": { "status": "UP" }
  }
}
```

The `details` also hold one `relay.<name>` block for each retention sweeper and for the saga compensation-retry sweeper (`relay.outbox-retention-sweeper`, `relay.inbox-retention-sweeper`, `relay.dead-letter-retention-sweeper`, `relay.saga-dead-letter-retention-sweeper`, and `relay.saga-compensation-retry:` followed by the saga state class), shortened here. `StreamRuneHealthIT` checks this list.

The demo's subscriptions (`order-fulfillment-saga`, `payment-process-manager`, and the `MultiProjectionRunner`) are wired as standalone beans and are **not** register(...)-ed with the auto-configured `SubscriptionHealthContributor`, and neither is the framework's `SseEventFeed`, so no `subscription.<name>` detail blocks appear here. If you register a subscription with the contributor, each one adds a `subscription.<name>` detail with `state`, `lag`, `errorCount`, and `status` keys.

**Metrics — command counter:**

```bash
curl -s http://localhost:8080/actuator/metrics/streamrune.commands.dispatched | jq .
```

The meter name appears in the response because `MicrometerStreamRuneMetrics` registers `streamrune.commands.dispatched` at construction, like most of its meters (a few, such as the per-subscription `streamrune.subscriptions.lag` gauge, appear on their first sample) — so it is always discoverable. However, **the counter stays at 0.0 in the shipped demo** because the hand-rolled `commandBus` bean in `StreamRuneConfig.java` builds the bus with `.interceptors(...).snapshotPolicy(...).register(...)` but never calls `.metrics(...)`. The framework defaults to `StreamRuneMetrics.NOOP` when no metrics collector is wired, so `recordCommandDispatched()` is a no-op regardless of how many orders you place.

**Optional: wire the metrics collector into the command bus.** Inject the `StreamRuneMetrics` bean (the auto-configured `MicrometerStreamRuneMetrics`) and call `.metrics(metrics)` on the builder, so the counter increments:

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
    OpenTelemetryCommandInterceptor otel,
    StreamRuneMetrics metrics) {           // <-- inject the auto-configured bean
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .commandInbox(commandInbox)
      .interceptors(otel, audit, auth, validation, circuitBreaker)
      .deadLetterQueue(deadLetterQueue)
      .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
      .metrics(metrics)                    // <-- wire the collector
      .snapshotPolicy(SnapshotPolicy.everyNEvents(5))
      .register(/* ... keep all register(...) calls ... */)
      .build();
}
```

After adding `.metrics(metrics)`, place an order and re-run the curl — `measurements[0].value` will increment by one per command dispatched. The same `.metrics(metrics)` call can be added to the `MultiProjectionRunner` builder to activate `streamrune.projections.*` counters as well.

**Paginated event list:**

```bash
curl -s -H "X-User-Role: ADMIN" "http://localhost:8080/api/events?offset=0&limit=5" | jq .
```

Each item in the response array includes `globalOffset`, `streamId`, `aggregateType`, `aggregateId`, `eventType`, `timestamp`, and `payload`. Leave the header out and the answer is `403 Forbidden`.

**Per-stream events:**

```bash
curl -s -H "X-User-Role: ADMIN" http://localhost:8080/api/events/order/o-1 | jq .
```

Replace `o-1` with an order ID you have placed (the stream is `order:<orderId>`, so the path carries the type `order` and the id as two segments). The response contains all events for that stream in append order.

Now read a customer's stream the same way, for example the customer from Chapter 6:

```bash
curl -s -H "X-User-Role: ADMIN" http://localhost:8080/api/events/customer/c-1 | jq '.[0].payload'
```

The name, e-mail and address are there in plain text, although the `payload` column of `event_stream` holds ciphertext for them: the event store decrypted the fields for the caller. This is the reason the endpoint asks for the role.

**Live SSE stream:**

```bash
curl -N http://localhost:8080/api/events/sse
```

The `-N` flag disables buffering. Leave the connection open and place an order in a separate terminal:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "o-obs-1", "customerId": "c-1", "lines": [{"productId": "p-1", "quantity": 1, "unitPrice": 9.99}]}'
```

Within one second you should see SSE frames appear in the first terminal, one per event appended. The feed needs no header, and a frame carries no payload:

```
id:17
data:{"aggregateType":"order","eventType":"OrderPlaced","aggregateId":"o-obs-1","streamId":"order:o-obs-1","globalOffset":17,"version":1,"timestamp":"2026-10-04T08:45:09.326626042Z"}
```

**Live SSE stream of one aggregate:**

The framework's endpoint from Step 6b streams one stream, with payloads. Subscribe to an order that does not exist yet:

```bash
curl -N http://localhost:8080/api/sse/order/o-obs-2
```

and place that order in the other terminal:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "o-obs-2", "customerId": "c-1", "lines": [{"productId": "p-1", "quantity": 1, "unitPrice": 9.99}]}'
```

Within the polling interval the `OrderPlaced` event arrives as a frame — the whole event as JSON, and its global offset as the `id`:

```
data:{"orderId":"o-obs-2","customerId":"c-1","lines":[{"productId":"p-1","quantity":1,"unitPrice":{"amount":9.99,"currency":"USD"}}],"total":{"amount":9.99,"currency":"USD"}}
id:14
```

Lines that start with a colon are comments, which an SSE client ignores: the shipped `spring-app`'s `SseController` writes `:connected` when the stream opens, and the framework's controller writes `:keepalive` every `streamrune.sse.keep-alive-interval` (30 seconds by default).

More frames follow as the saga from Chapter 11 drives the order on (`OrderConfirmed`, or `OrderCancelled` when the stock cannot be reserved). The payment and inventory events of the same order do not appear: they belong to other streams. Stop `curl`, start it again, and nothing is replayed — the stream stays silent until the next event of `order:o-obs-2` is stored.

---

## What We Learned

**`MicrometerStreamRuneMetrics`** implements `StreamRuneMetrics` using `io.micrometer.core.instrument.MeterRegistry`. It registers counters, timers, and gauges for commands, events, projections, subscriptions, and the other subsystems, under the names `MetricNames` publishes. The framework's `VirtualThreadCommandBus` and `MultiProjectionRunner` increment these counters at each processing stage **when a `StreamRuneMetrics` collector is wired into their builders (`.metrics(...)`)**. The shipped demo wires its command bus and projection runner by hand without a metrics collector, so the meters are registered and discoverable but stay at zero until you pass the bean in (see the optional step in Step 7 above). `StreamRuneAutoConfiguration` registers the `MicrometerStreamRuneMetrics` bean automatically when a `MeterRegistry` is present; you can override it by declaring your own `StreamRuneMetrics` bean. The meter names follow the `streamrune.*` namespace and are dimensional, so they export cleanly to any Micrometer registry — including Prometheus, once you add `micrometer-registry-prometheus` (the demo ships only the in-memory `SimpleMeterRegistry`).

**`OpenTelemetryCommandInterceptor`** implements `CommandInterceptor` and uses the `opentelemetry-api` facade — it has no SDK dependency and works with any OpenTelemetry SDK implementation. For each command it creates a span named `command/<CommandType>` of kind `INTERNAL`, sets `streamrune.command.type`, `streamrune.command.id`, `streamrune.aggregate.type`, `streamrune.aggregate.id`, and `streamrune.stream.id` as span attributes when the span starts (a failed command's span carries its stream too), and closes the span with `OK` or `ERROR` status. Each span and the scope that makes it current sit on a per-thread stack, because `before()`, `after()`, and `onError()` are called on the same thread that invoked `CommandBus.execute()`, and a command dispatched from inside another command's callback opens a nested span on that thread. `StreamRuneAutoConfiguration` registers the interceptor automatically when an `OpenTelemetry` bean is present.

**`StreamRuneHealthIndicator`** implements Spring Boot's `HealthIndicator` and checks things in sequence: the JDBC `DataSource` connection validity, the head of the `event_stream` table read directly via SQL on that same connection (reported as `eventStore.lastGlobalOffset`/`eventStore.lastEventTimestamp` — there is no `EventStore` dependency), the lag and error count of each registered subscription via `SubscriptionHealthContributor`, and the liveness of each background relay registered with `BackgroundRelayHealthContributor`. If the database is unreachable, the status is `DOWN` immediately. If any subscription reports `DOWN`, or a started relay's poll thread has died, the overall status is also `DOWN`. All checks are exposed as named details so that operators can distinguish a database failure from a subscription lag spike at a glance.

**`EventExplorerController`** provides the `GET /api/events` paginated global stream, `GET /api/events/{aggregateType}/{aggregateId}` per-stream event list, and `GET /api/events/sse` live SSE stream. The first two return event payloads as the event store reads them — decrypted — and answer only to the ADMIN role; that check is a demo shortcut on a client-supplied header, and a real deployment puts such an endpoint behind real authentication. The SSE endpoint is open and carries no payload: it starts a virtual thread that polls `EventStore.readGlobalStream` in a tight loop, sleeping one second when no new events are found, and emits each event as an unnamed `ServerSentEvent<String>` with the global offset as the SSE `id` field and the event's type, stream and version as its data. The `Sinks.Many` unicast sink bridges the imperative polling thread to the reactive `Flux` returned to WebFlux.

**The framework's SSE endpoint** (`GET /api/sse/{aggregateType}/{aggregateId}`, `streamrune.sse.enabled=true`) streams the events of one aggregate with their payloads. The integration serves it and feeds it: an `SseEventFeed` per application instance reads the global stream from its head every `streamrune.sse.polling-interval` and publishes each event through `SseEventPublisher.publish(EventEnvelope)` to the clients of that event's stream. The application writes no feeder; it supplies the `SseAuthorizer` that decides who may open which stream, and without one every stream answers `403`. Delivery is live, best-effort and at-most-once — no replay, no `Last-Event-ID` — so a frame is a notification, and a consumer that must see every event is a projection.

---

## Next Up

The backend is complete. Let's look at the frontend.
