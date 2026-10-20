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
- Create `EventExplorerController`, which exposes three endpoints: a paginated list of all events in the global stream and a per-stream event list, both for an `ADMIN` because they show decrypted payloads, and an open SSE endpoint that announces new events in real time, without their payload.
- Switch on the framework's own SSE endpoint, `GET /api/sse/{aggregateType}/{aggregateId}`, which streams the events of one aggregate. The framework serves it and feeds it; the application adds a property and an `SseAuthorizer` that lets in an `ADMIN` or the customer the stream belongs to, and nobody else.

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
| `streamrune.dlq.pending` | Gauge | Dead-lettered commands not yet exhausted |
| `streamrune.dlq.awaiting_operator` | Gauge | Dead-lettered commands the retry runner never replays on its own (unkeyed ones, keyed ones older than the inbox window): alert above zero; an operator's forced replay or discard (Chapter 14, Step 5) lowers it |
| `streamrune.dlq.stale_replay_blocked` | Counter | Operator retries refused because the entry is older than the inbox window |

The three dead-letter meters come from the demo's own `DeadLetterRetryRunner`, which the bean of Chapter 14 builds with `.metrics(...)`: a runner without it reports nothing.

> **Quarkus and Micronaut.** Both apps report the same meters, the dead-letter ones included: their runners are wired the same way, and each framework integration builds `MicrometerStreamRuneMetrics` once a `MeterRegistry` bean exists. The Quarkus app has `quarkus-micrometer-registry-prometheus`, and its meters are scraped at `/q/metrics` in Prometheus' format (`streamrune_dlq_awaiting_operator`). The Micronaut app has Micronaut Micrometer (`io.micronaut.micrometer:micronaut-micrometer-core`, with its `micronaut-micrometer-annotation` processor): like Actuator, it records into an in-memory `SimpleMeterRegistry` when no other registry is configured, and `/metrics` lists the names, `/metrics/streamrune.dlq.awaiting_operator` one meter's measurements. `StreamRuneMetricsIT` (Spring) and the `ObservabilitySmokeSuiteIT` of the Quarkus and Micronaut apps check that the two dead-letter gauges, a saga meter and a projection meter are listed.

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
4. Reports every background relay registered with the `BackgroundRelayHealthContributor` as a `relay.<name>` detail with `status`, `started`, `alive`, and `consecutiveFailures`. A relay that was started but whose poll thread has died is `DOWN`, and that turns the overall status `DOWN`: without this check `/actuator/health` would read `UP` while the outbox stopped publishing or dead-lettered commands stopped being replayed. A relay that is running but whose recent poll cycles failed (its database is unreachable, say) reads `DEGRADED` in its own block and leaves the overall status alone. A broker outage is not a failed cycle: the outbox relay records each refused publish on its entry, which stays `PENDING`, and reads `UP`; the backlog shows in `streamrune.outbox.pending` (Chapter 13, Step 5b). The framework registers the relays it builds; in the demo those are the outbox relay (`relay.outbox-relay`), the outbox, inbox, dead-letter and saga dead-letter retention sweepers, and the saga compensation-retry sweeper. A relay you build yourself registers itself: the demo's `deadLetterRetryRunner` bean (Chapter 14) replaces the framework's runner and calls `relayHealth.registerDlqRetryRunner(runner)`, so it appears as `relay.dead-letter-retry`.

The indicator is exposed under the bean name `streamRune` at `/actuator/health/streamRune`. The full health response at `/actuator/health` aggregates it alongside Spring's built-in `db` and `diskSpace` indicators.

No additional code is required for this step. Proceed to Step 5 and return here to verify once the application is running.

### Step 5: Create EventExplorerController

The controller of this step exposes three endpoints on `/api/events`: two that read the event history for an `ADMIN`, and a live feed that is open to every caller. What the open feed may say about an event is a decision of its own, shared by the three apps, so it gets a class of its own next to the access rule of Chapter 9. Create `queries/src/main/java/org/streamrune/ecommerce/queries/access/LiveFeedFrame.java`:

```java
package org.streamrune.ecommerce.queries.access;

import java.util.LinkedHashMap;
import java.util.Map;
import org.streamrune.core.EventEnvelope;

/**
 * What the open live feed, {@code GET /api/events/sse}, says about one event: that an event of this
 * type happened to an aggregate of this type, when, and where it stands in the global stream. The
 * three applications build every frame of the feed here.
 *
 * <p><b>A frame names no stream.</b> It carries no stream id, no aggregate id, no version within a
 * stream and no payload. A stream id holds a customer id or an order id, and the feed cannot ask
 * who is calling: a browser's {@code EventSource} sends no {@code X-User-Id} and no {@code
 * X-User-Role}, so every connection is anonymous. What an anonymous caller may not learn is
 * therefore left out for everyone, an {@code ADMIN} included. A caller of the feed learns that a
 * customer registered or that an order was placed, and when; not which customer, and not which
 * order.
 *
 * <p><b>Where the detail is.</b> A view that wants to know what changed reads it from the guarded
 * endpoints, with the caller's two headers: the customer and order reads ({@link
 * OwnerOrAdminAccess}), and for an {@code ADMIN} the event history, {@code GET /api/events}. The
 * frame is the signal to read again; the aggregate type and the event type say which read.
 */
public final class LiveFeedFrame {

  private LiveFeedFrame() {}

  /**
   * The frame of one event, with its fields in the order they are written.
   *
   * @param event an event of the global stream
   * @return {@code globalOffset}, {@code aggregateType}, {@code eventType} and {@code timestamp},
   *     and nothing else
   */
  public static Map<String, Object> of(EventEnvelope event) {
    Map<String, Object> frame = new LinkedHashMap<>();
    frame.put("globalOffset", event.globalOffset().value());
    frame.put("aggregateType", event.aggregateType().value());
    frame.put("eventType", event.eventType().name());
    frame.put("timestamp", event.metadata().timestamp().toString());
    return frame;
  }
}
```

The frame says what the feed tells; the reader says what the feed may cost. A connection of the open feed is anonymous, so everything the app does for one connection it does for anybody, as often as they connect. Two rules follow. A connection starts at the head of the global stream and is sent what is stored while it is open: a caller who could ask for the history would make the app read and decrypt every event of the store. And the app reads the store once for all its connections: one reader polls the global stream and hands each frame to every connected client, and the feed takes a bounded number of connections. The connection that would be one more is refused with an exception of its own. Create `LiveFeedFullException.java` in the same package:

```java
package org.streamrune.ecommerce.queries.access;

/**
 * The open live feed has no place for another connection: thrown by {@link LiveFeed#admit} when
 * {@link LiveFeed#MAX_CONNECTIONS} clients are connected. Each application answers it with {@code
 * 503 Service Unavailable} and a {@code Retry-After} of {@link LiveFeed#RETRY_AFTER_SECONDS}
 * seconds.
 *
 * <p>{@code 503} and not {@code 429}: the limit is the application's, shared by every caller, and
 * says nothing about how many requests this caller sent. The feed cannot tell its callers apart, so
 * it has nobody to count requests for.
 */
public final class LiveFeedFullException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public LiveFeedFullException(int places) {
    super(
        "The live feed serves "
            + places
            + " connections at once and all of them are taken; try again in a few seconds");
  }
}
```

Then create `LiveFeed.java` next to it:

```java
package org.streamrune.ecommerce.queries.access;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;

/**
 * The reader behind the open live feed, {@code GET /api/events/sse}: one thread per application,
 * the same loop in the three applications. It polls the global stream once per {@link
 * #POLLING_INTERVAL} and hands the frame of every event it reads to every connected client. An
 * application takes a {@link Place} for a connection and hands it a {@link Client} that writes to
 * its own kind of response stream.
 *
 * <p><b>What a connection costs.</b> The feed is open to every caller, so what one connection costs
 * the application is what any number of anonymous callers can make it pay. The event store is asked
 * by the one reader: one read of the global stream per polling interval, however many clients are
 * connected, and one read of its head in an interval in which clients joined. An event is read, and
 * its {@code @Encrypted} fields are decrypted, once per application and not once per connection. A
 * connection costs its socket and the frames buffered for it, and the feed has {@link
 * #MAX_CONNECTIONS} places: {@link #admit} refuses the connection that would be one more, and the
 * application answers it {@code 503 Service Unavailable} with {@code Retry-After}.
 *
 * <p><b>A place nobody follows.</b> A place is taken before the answer starts, and its client is
 * registered when the web framework subscribes the response stream. A framework does not always get
 * that far: Quarkus, for one, does not subscribe the stream of a request whose connection was reset
 * before the answer is written. Such a place has no client that could leave, so nothing else would
 * ever free it, and a caller who abandons a hundred requests would leave the feed full until the
 * application restarts. A place that is not followed within {@link #FOLLOW_TIMEOUT} is therefore
 * free again: {@link #admit}, the reader's round and {@link #openConnections} drop the pending
 * places that are older. A client that is registered on a place that was dropped is completed at
 * once, and reconnects. A subscription takes milliseconds, so the timeout is far above what a
 * served connection needs and far below what a caller would have to wait for a place. Taking the
 * place only when the stream is subscribed would need no such rule, but by then the answer has
 * started, and the connection that is one too many could not be refused with a status any more.
 *
 * <p><b>The places add up.</b> The number of places taken is the number of pending places (taken,
 * not followed yet) plus the number of registered clients. The feed keeps these two collections and
 * no counter beside them: a place leaves the first when it is followed or dropped, a client leaves
 * the second when it left, was dropped or was completed, and removing either twice changes nothing.
 * The number can therefore not drift, in either direction.
 *
 * <p><b>When the reader runs.</b> It starts with the first client and ends when the last one has
 * left, so an application nobody watches reads nothing. Nothing is lost by stopping: a connection
 * starts at the head, so a reader that starts again has no position to keep. {@link #close} ends it
 * when the application stops. Whatever ends the reader's thread, an {@code Error} included, its
 * clients are told and the feed knows that it has no reader: the next client starts a new one.
 *
 * <p><b>The feed is live.</b> A connection starts at the head of the global stream. Nothing that
 * was stored before the client connected is sent to it, whatever the request carries, and no
 * connection makes the reader go back. A view that wants what happened earlier reads it from the
 * guarded endpoints: the lists of the read models, and for an {@code ADMIN} the event history,
 * {@code GET /api/events}.
 *
 * <p><b>The opening frame, and why no event after it is missed.</b> A client that has received the
 * comment frame {@code : connected} receives a frame for every event stored from then on, for as
 * long as it stays connected. The order of three steps makes that true:
 *
 * <ol>
 *   <li>{@link Place#follow} registers the client;
 *   <li>the reader, in its next round, takes the clients registered so far and <em>then</em> reads
 *       the head of the global stream, {@code EventStore.lastGlobalOffset()}; that offset, {@code
 *       H}, is where these clients start;
 *   <li>the reader sends them {@code : connected} and from then on hands them every event it reads
 *       whose offset is above {@code H}.
 * </ol>
 *
 * <p>The event store's contract for the head is that an event which becomes readable after {@code
 * lastGlobalOffset()} returned has a greater offset than the one returned. An event stored after
 * the client received {@code : connected} became readable after step 2, so its offset is above
 * {@code H}. The reader's own position is a head it read earlier or the offset of an event it has
 * read, so it is at or below {@code H}; from there it reads the global stream in offset order, as
 * every polling subscription does, so it reaches every event above {@code H}, and by then the
 * client is among the clients it hands frames to, because the client was registered before {@code
 * H} was read. Had the head been read first and the client registered afterwards, an event stored
 * between the two would be above that head and could be read and handed out before the client was
 * there to receive it. An event at or below {@code H} is never sent to the client: it was stored
 * before the client's {@code : connected}. Events stored while a client is away are not sent after
 * it reconnects; the client reads its lists again instead.
 *
 * <p><b>A client that cannot keep up.</b> The reader never waits for a client: a {@link Client}
 * takes a frame without blocking, and the response stream behind it buffers a bounded number of
 * frames for a client that reads slowly. A client whose buffer is full is dropped: its stream
 * fails, its place is free again and it reconnects. A slow or dead client therefore never holds up
 * the reader, which is all the clients share, and a client whose write fails is dropped alone.
 *
 * <p><b>A client that went away.</b> The reader forgets a client within one polling interval of
 * {@link Client#isGone} turning {@code true}. A server that learns of a closed connection only when
 * a write to it fails gets that write from the keep-alive: a comment frame after every {@link
 * #KEEP_ALIVE_INTERVAL} without an event.
 *
 * <p><b>An application that stops.</b> {@link #close} completes every client and returns when the
 * reader has ended, so no open stream holds a shutdown that waits for requests in flight, and no
 * read is under way when the data source closes.
 */
public final class LiveFeed implements AutoCloseable {

  /** How long the reader waits before it asks the store again. */
  public static final Duration POLLING_INTERVAL = Duration.ofSeconds(1);

  /** How long the feed stays without a frame before the reader sends a keep-alive comment. */
  public static final Duration KEEP_ALIVE_INTERVAL = Duration.ofSeconds(5);

  /**
   * How many clients the feed serves at once. A browser tab holds one connection, so a hundred is
   * far more than a demo is shown to, and it bounds what anonymous callers can hold: a hundred
   * sockets and the frames buffered for them.
   */
  public static final int MAX_CONNECTIONS = 100;

  /**
   * The {@code Retry-After} of the answer to a connection the feed has no place for, in seconds. A
   * place is free again within seconds of its client leaving.
   */
  public static final int RETRY_AFTER_SECONDS = 5;

  /**
   * How long a place waits for its client. A framework registers the client of a connection within
   * milliseconds of taking the place; a place that is still pending after this long belongs to a
   * request the framework gave up, and is free again.
   */
  public static final Duration FOLLOW_TIMEOUT = Duration.ofSeconds(5);

  /** The text of the opening comment frame; the line on the wire is {@code : connected}. */
  public static final String CONNECTED_COMMENT = " connected";

  /** The text of the keep-alive comment frame; the line on the wire is {@code : keepalive}. */
  public static final String KEEP_ALIVE_COMMENT = " keepalive";

  /** The name of the reader's thread. */
  public static final String READER_THREAD_NAME = "live-feed-reader";

  private static final int BATCH_SIZE = 100;

  /** How long {@link #close} waits for a read that is under way. */
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

  /**
   * One connected client, as its application's response stream sees it. The reader calls every
   * method from its one thread, and no method may wait for the client: {@link #comment} and {@link
   * #event} hand the frame to a response stream that buffers it, and a stream whose buffer is full
   * fails itself and from then on answers {@link #isGone} with {@code true}.
   */
  public interface Client {

    /** Whether the client disconnected or was dropped; the reader then forgets it. */
    boolean isGone();

    /** Takes a comment frame: a line that starts with a colon and carries no event. */
    void comment(String text) throws Exception;

    /**
     * Takes the frame of one event.
     *
     * @param globalOffset the event's global offset, the frame's {@code id}
     * @param frame the event's {@link LiveFeedFrame}, the frame's {@code data}; one unmodifiable
     *     map for all the clients
     */
    void event(long globalOffset, Map<String, Object> frame) throws Exception;

    /** Ends the stream with an error: the feed sends this client nothing more. */
    void failed(Exception cause);

    /** Ends the stream normally: the feed is closing. */
    void ended();
  }

  /**
   * One of the feed's places, taken by {@link #admit} for one connection. It is pending until
   * {@link #follow} registers its client, and free again when that client has left, was dropped or
   * was completed, or when nobody followed it within the feed's follow timeout.
   */
  public final class Place {

    private final long admittedAt = System.nanoTime();
    private boolean followed;

    private Place() {}

    /**
     * Registers the client and returns at once; the reader serves it from its next round on. On a
     * closed feed, and on a place that was dropped because nobody followed it in time, the client
     * is completed at once.
     *
     * @param client the connected client
     */
    public void follow(Client client) {
      boolean served;
      synchronized (lock) {
        if (followed) {
          throw new IllegalStateException("a place serves one client");
        }
        followed = true;
        // Not pending any more: the place was dropped, and may be somebody else's by now.
        boolean held = pending.remove(this);
        served = held && !closed;
        if (served) {
          followers.add(new Follower(client));
          if (reader == null) {
            reader =
                Thread.ofPlatform()
                    .daemon()
                    .name(READER_THREAD_NAME)
                    .unstarted(LiveFeed.this::read);
            reader.start();
          }
        }
      }
      if (!served) {
        client.ended();
      }
    }
  }

  /** What one step of the reader hands a client. */
  private interface Frame {
    void handTo(Client client) throws Exception;
  }

  /** A registered client. Only the reader's thread reads and writes the two fields. */
  private static final class Follower {

    private static final long NOT_STARTED = -1;

    final Client client;

    /** The offset this client starts after: the head the reader read once it was registered. */
    long after = NOT_STARTED;

    /** Set when a write to the client failed; it is sent nothing more. */
    boolean dropped;

    Follower(Client client) {
      this.client = client;
    }
  }

  private final EventStore eventStore;
  private final Duration pollingInterval;
  private final Duration keepAliveInterval;
  private final int maxConnections;
  private final Duration followTimeout;
  private final AtomicLong storeReads = new AtomicLong();

  /** Guards the four fields below. Never held during a store read or a call to a client. */
  private final Object lock = new Object();

  /** The places taken and not followed yet, oldest first. */
  private final Set<Place> pending = new LinkedHashSet<>();

  /** The registered clients. With {@link #pending}, all the places that are taken. */
  private final List<Follower> followers = new ArrayList<>();

  private Thread reader;
  private boolean closed;

  /**
   * A feed with the intervals and the number of places of the running applications.
   *
   * @param eventStore the store whose global stream the clients follow
   */
  public LiveFeed(EventStore eventStore) {
    this(eventStore, POLLING_INTERVAL, KEEP_ALIVE_INTERVAL, MAX_CONNECTIONS, FOLLOW_TIMEOUT);
  }

  /** A feed with intervals, places and a follow timeout of its own, for a test. */
  public LiveFeed(
      EventStore eventStore,
      Duration pollingInterval,
      Duration keepAliveInterval,
      int maxConnections,
      Duration followTimeout) {
    this.eventStore = eventStore;
    this.pollingInterval = pollingInterval;
    this.keepAliveInterval = keepAliveInterval;
    this.maxConnections = maxConnections;
    this.followTimeout = followTimeout;
  }

  /**
   * Takes a place for one connection. An application calls this before it answers, so a connection
   * the feed has no place for gets a status and not a stream. It never waits and reads nothing, so
   * an event loop may call it.
   *
   * @return the place; hand its client to {@link Place#follow}
   * @throws LiveFeedFullException when every place is taken
   */
  public Place admit() {
    synchronized (lock) {
      dropPlacesNobodyFollowed();
      if (pending.size() + followers.size() >= maxConnections) {
        throw new LiveFeedFullException(maxConnections);
      }
      Place place = new Place();
      pending.add(place);
      return place;
    }
  }

  /**
   * The number of places taken: the pending places and the clients the feed serves. A place nobody
   * followed in time is not counted.
   */
  public int openConnections() {
    synchronized (lock) {
      dropPlacesNobodyFollowed();
      return pending.size() + followers.size();
    }
  }

  /** The number of places that are taken and not followed yet, whatever their age. */
  int pendingPlaces() {
    synchronized (lock) {
      return pending.size();
    }
  }

  /** The number of registered clients. */
  int registeredClients() {
    synchronized (lock) {
      return followers.size();
    }
  }

  /** Whether the reader runs. */
  public boolean isReading() {
    synchronized (lock) {
      return reader != null;
    }
  }

  /** How many times the reader has asked the event store, for its head or for events. */
  public long storeReads() {
    return storeReads.get();
  }

  /**
   * Completes every client and waits until the reader has ended. A client that connects while the
   * feed is closed is completed at once.
   */
  @Override
  public void close() {
    Thread ending;
    synchronized (lock) {
      closed = true;
      ending = reader;
      lock.notifyAll();
    }
    if (ending != null) {
      try {
        ending.join(CLOSE_TIMEOUT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Serves clients again after a {@link #close}: an application that is started a second time. */
  public void open() {
    synchronized (lock) {
      closed = false;
    }
  }

  // --- the reader's thread ---

  private void read() {
    try {
      GlobalOffset position = null;
      long quietSince = System.nanoTime();
      while (true) {
        // The clients registered so far, and only then the head: steps 1 and 2 of the class
        // comment.
        List<Follower> round = clientsOfThisRound();
        if (round.isEmpty()) {
          return;
        }
        List<Follower> joining =
            round.stream().filter(follower -> follower.after == Follower.NOT_STARTED).toList();
        if (position == null || !joining.isEmpty()) {
          GlobalOffset head = head();
          if (position == null) {
            // The head, not the start: the reader never reads the store's history.
            position = head;
          }
          for (Follower follower : joining) {
            follower.after = head.value();
            hand(follower, client -> client.comment(CONNECTED_COMMENT));
          }
        }
        List<EventEnvelope> events = eventsAfter(position);
        for (EventEnvelope event : events) {
          long offset = event.globalOffset().value();
          Map<String, Object> frame = Collections.unmodifiableMap(LiveFeedFrame.of(event));
          for (Follower follower : round) {
            if (offset > follower.after) {
              hand(follower, client -> client.event(offset, frame));
            }
          }
          // readGlobalStream reads the events after the offset it is given.
          position = event.globalOffset();
        }
        if (!events.isEmpty()) {
          quietSince = System.nanoTime();
        } else if (System.nanoTime() - quietSince >= keepAliveInterval.toNanos()) {
          for (Follower follower : round) {
            hand(follower, client -> client.comment(KEEP_ALIVE_COMMENT));
          }
          quietSince = System.nanoTime();
        }
        // A full batch means there is more to read; otherwise the store is asked again after the
        // polling interval, whoever connects in between.
        if (events.size() < BATCH_SIZE) {
          pause();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      endAll(Client::ended);
    } catch (RuntimeException e) {
      // The store cannot be read: every client is told, and reconnects.
      endAll(client -> client.failed(e));
    } finally {
      // Whatever ended this thread, an Error that passes through here included: while the feed
      // still takes it for its reader, no client would start another one and the clients it
      // served would wait for ever. They are told, and the feed has no reader. An Error goes on
      // to the thread's uncaught exception handler.
      List<Follower> left = List.of();
      synchronized (lock) {
        if (reader == Thread.currentThread()) {
          left = takeAll();
        }
      }
      for (Follower follower : left) {
        quietly(
            follower.client,
            client -> client.failed(new IllegalStateException("The feed's reader has ended")));
      }
    }
  }

  /**
   * The clients the reader serves in one round, without the ones that left. Empty when the reader
   * is to end: the feed is closed (its clients are completed here), or no client is left.
   */
  private List<Follower> clientsOfThisRound() {
    List<Follower> registered;
    synchronized (lock) {
      registered = List.copyOf(followers);
    }
    List<Follower> left = registered.stream().filter(f -> f.dropped || f.client.isGone()).toList();
    List<Follower> completed;
    synchronized (lock) {
      dropPlacesNobodyFollowed();
      followers.removeAll(left);
      if (!closed) {
        if (followers.isEmpty()) {
          reader = null;
        }
        return List.copyOf(followers);
      }
      completed = takeAll();
    }
    completed.forEach(follower -> quietly(follower.client, Client::ended));
    return List.of();
  }

  private GlobalOffset head() {
    storeReads.incrementAndGet();
    return eventStore.lastGlobalOffset();
  }

  private List<EventEnvelope> eventsAfter(GlobalOffset position) {
    storeReads.incrementAndGet();
    return eventStore.readGlobalStream(position, BATCH_SIZE);
  }

  /** Hands one frame to one client. A client that cannot take it is dropped, alone. */
  private void hand(Follower follower, Frame frame) {
    if (follower.dropped || follower.client.isGone()) {
      return;
    }
    try {
      frame.handTo(follower.client);
    } catch (Exception e) {
      follower.dropped = true;
      quietly(follower.client, client -> client.failed(e));
    }
  }

  /** Waits one polling interval, or until the feed is closed. */
  private void pause() throws InterruptedException {
    synchronized (lock) {
      if (!closed) {
        lock.wait(Math.max(1, pollingInterval.toMillis()));
      }
    }
  }

  private void endAll(Consumer<Client> ending) {
    List<Follower> all;
    synchronized (lock) {
      all = takeAll();
    }
    all.forEach(follower -> quietly(follower.client, ending));
  }

  /** Forgets every client, frees their places and ends the reader. The caller holds the lock. */
  private List<Follower> takeAll() {
    List<Follower> all = List.copyOf(followers);
    followers.clear();
    reader = null;
    return all;
  }

  /**
   * Frees the pending places that are older than the follow timeout. They are kept oldest first, so
   * the first one that is young enough ends the search. The caller holds the lock.
   */
  private void dropPlacesNobodyFollowed() {
    long now = System.nanoTime();
    Iterator<Place> oldestFirst = pending.iterator();
    while (oldestFirst.hasNext()
        && now - oldestFirst.next().admittedAt >= followTimeout.toNanos()) {
      oldestFirst.remove();
    }
  }

  /** A stream that cannot be ended is one the client has left already. */
  private static void quietly(Client client, Consumer<Client> ending) {
    try {
      ending.accept(client);
    } catch (RuntimeException alreadyOver) {
      // nothing to end
    }
  }
}
```

`admit` takes one of the feed's 100 places or throws `LiveFeedFullException`; `Place.follow` registers the client and starts the reader's thread if it is not running. The reader works in rounds, one per second. A round takes the clients registered so far, reads `eventStore.lastGlobalOffset()` if a client joined (that head is where the new clients start, and they are sent `: connected`), reads `readGlobalStream` once with the offset of the last event it read, and hands every frame to every client that started below it; `readGlobalStream` returns the events *after* the offset it is given, so that offset is the position itself. The order matters: the client is registered first and the head is read second, and the class comment shows why no event stored after a client's `: connected` can then be missed. The `Client` interface is what keeps the class free of Spring: each of the three apps implements its five methods on its own kind of response stream.

Then create `EventExplorerController` in the `org.streamrune.ecommerce.spring.controller` package:

```java
package org.streamrune.ecommerce.spring.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.queries.access.LiveFeed;
import org.streamrune.ecommerce.queries.access.LiveFeedFrame;
import org.streamrune.ecommerce.queries.access.LiveFeedFullException;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * The demo's window on the event store: the global event list, one stream's history, and a live
 * feed of new events.
 *
 * <p><b>The list and the stream history are ADMIN only.</b> They return each event's payload as the
 * event store reads it, and the store decrypts {@code @Encrypted} fields on the way out. Until a
 * customer's key is erased, these two endpoints therefore show that customer's name, e-mail and
 * address in plain text. They are gated with the same {@code requireAdmin} check as the admin
 * endpoints.
 *
 * <p><b>DEMO SHORTCUT, protect this in production.</b> The identity and the role are whatever the
 * client sent in {@code X-User-Id} and {@code X-User-Role} (the gateway stand-in chapter 9
 * describes), so the check keeps the tutorial honest but protects nothing against a caller who sets
 * the two headers. A real deployment authenticates the caller, derives the role from that identity,
 * and does not expose raw event payloads outside an operator's tooling.
 *
 * <p><b>The live feed is open and names no stream.</b> The browser's {@code EventSource} cannot
 * send the two headers, so the feed cannot ask who is calling and every connection is anonymous. A
 * frame is a {@link LiveFeedFrame}: the event's global offset, the type of its aggregate, the event
 * type and the timestamp. It holds no stream id, no aggregate id and no payload, for an {@code
 * ADMIN} as for everyone else, so no caller learns a customer id or an order id from it: the feed
 * says that a customer registered or an order was placed, and when, not which one. A view that
 * wants the detail reads it from the guarded endpoints, with the caller's headers.
 *
 * <p><b>A connection starts at the head.</b> The feed sends a frame for every event stored while
 * the client is connected and nothing of what the store held before, so no caller makes the app
 * read its history. The frames are unnamed (no {@code event:} field), so {@code
 * EventSource.onmessage} receives every one of them.
 *
 * <p><b>One reader, a bounded number of connections.</b> {@link LiveFeed} reads: one reader for the
 * application asks the event store once per polling interval and hands every frame to every
 * connected client, so what the feed costs the store does not grow with the number of connections.
 * The reader runs while a client is connected. The feed has {@link LiveFeed#MAX_CONNECTIONS}
 * places; the connection that would be one more is answered {@code 503 Service Unavailable} with
 * {@code Retry-After} ({@link LiveFeedFullException}).
 */
@RestController
@RequestMapping("/api/events")
public class EventExplorerController implements SmartLifecycle {

  /** Maximum number of frames buffered for a slow feed client before its stream is failed. */
  static final int SLOW_CLIENT_BUFFER = 256;

  private final EventStore eventStore;
  private final ObjectMapper objectMapper;
  private final LiveFeed liveFeed;
  private volatile boolean feedOpen;

  public EventExplorerController(EventStore eventStore, ObjectMapper objectMapper) {
    this.eventStore = eventStore;
    this.objectMapper = objectMapper;
    this.liveFeed = new LiveFeed(eventStore);
  }

  @GetMapping
  public List<Map<String, Object>> listEvents(
      @RequestParam(defaultValue = "0") long offset, @RequestParam(defaultValue = "50") int limit) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @GetMapping("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  /**
   * The live feed: a frame for every event stored while the client is connected, open to every
   * caller. {@link LiveFeed} reads; this method writes what it reads as Server-Sent Events.
   *
   * <p>The place is taken before the answer starts: when the feed has none left, {@link
   * LiveFeed#admit} throws {@link LiveFeedFullException} and {@code GlobalExceptionHandler} answers
   * {@code 503} with {@code Retry-After}. The client is registered when Spring MVC subscribes the
   * {@code Flux}; a place whose stream is never subscribed is free again after {@link
   * LiveFeed#FOLLOW_TIMEOUT}.
   *
   * <p>The feed's reader hands a frame to the sink and goes on to the next client; it never waits
   * for this one. A client that reads too slowly has up to {@link #SLOW_CLIENT_BUFFER} frames
   * buffered; beyond that its stream fails, its place is free again and the client reconnects.
   *
   * <p>Spring MVC writes the buffered frames to the sockets on its async task executor, by default
   * a pool of eight threads. A client whose socket is full holds one of them for as long as the
   * servlet container waits for that write, so a deployment that expects many such clients gives
   * the executor virtual threads ({@code spring.threads.virtual.enabled}).
   */
  @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> sseStream() {
    LiveFeed.Place place = liveFeed.admit();
    return Flux.<ServerSentEvent<String>>create(
            sink -> place.follow(clientOf(sink)), FluxSink.OverflowStrategy.ERROR)
        .onBackpressureBuffer(SLOW_CLIENT_BUFFER);
  }

  /**
   * {@code HEAD} for the live feed: the headers of the feed's answer and nothing else. A framework
   * answers a {@code HEAD} it finds no handler for by running the {@code GET} handler and leaving
   * out the body; for a stream that never ends that would take one of the feed's places and
   * register a client that can never leave. This handler takes no place and reads nothing.
   */
  @RequestMapping(value = "/sse", method = RequestMethod.HEAD)
  public ResponseEntity<Void> sseHead() {
    return ResponseEntity.ok().contentType(MediaType.TEXT_EVENT_STREAM).build();
  }

  /** The client behind one response stream: what the feed's reader sends becomes its frames. */
  private LiveFeed.Client clientOf(FluxSink<ServerSentEvent<String>> sink) {
    return new LiveFeed.Client() {
      @Override
      public boolean isGone() {
        return sink.isCancelled();
      }

      @Override
      public void comment(String text) {
        sink.next(ServerSentEvent.<String>builder().comment(text).build());
      }

      @Override
      public void event(long globalOffset, Map<String, Object> frame) throws Exception {
        sink.next(
            ServerSentEvent.<String>builder()
                .id(String.valueOf(globalOffset))
                .data(objectMapper.writeValueAsString(frame))
                .build());
      }

      @Override
      public void failed(Exception cause) {
        sink.error(cause);
      }

      @Override
      public void ended() {
        sink.complete();
      }
    };
  }

  /** The number of clients connected to the live feed. */
  public int openFeedConnections() {
    return liveFeed.openConnections();
  }

  /** Whether the feed's reader runs: it does while a client is connected. */
  public boolean feedReaderRuns() {
    return liveFeed.isReading();
  }

  /** How many times the feed's reader has asked the event store. */
  public long feedStoreReads() {
    return liveFeed.storeReads();
  }

  // --- SmartLifecycle: the open feeds end before the web server waits for requests in flight ---

  @Override
  public void start() {
    liveFeed.open();
    feedOpen = true;
  }

  /**
   * Ends the open feeds. An open feed is a request that never finishes on its own, and Spring
   * Boot's graceful shutdown waits for the requests in flight before it stops the web server. This
   * bean has the default phase, the last to start and the first to stop: the feeds are completed
   * and the reader has ended before that wait begins, and before the data source closes.
   */
  @Override
  public void stop() {
    liveFeed.close();
    feedOpen = false;
  }

  @Override
  public boolean isRunning() {
    return feedOpen;
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
- `GET /api/events/sse` — a persistent SSE connection, **open to every caller, naming no stream and carrying no payload, and live**. `LiveFeed` registers the connection with the app's one reader, which reads the head of the global stream for it (`lastGlobalOffset()`), sends a `: connected` comment, and hands it what its polls of `readGlobalStream` return from that offset on: one SSE frame per event stored while the client is connected, none for the events before. The `id` field is the global offset; the `data` field is the JSON of a `LiveFeedFrame`, an object with `globalOffset`, `aggregateType`, `eventType` and `timestamp`. There is no `event` field: an unnamed frame is what the browser's `EventSource` hands to `onmessage`, and the frontend in Chapter 16 relies on that. The reader polls once a second, and after five quiet seconds it sends every client a `: keepalive` comment. When 100 connections are open, the next one is answered `503 Service Unavailable` with `Retry-After: 5`.

**Why two of them need ADMIN.** `payload` is `env.event()`: the event as the event store reads it. The store is the crypto-aware one from Chapter 6, and it decrypts `@Encrypted` fields on the way out. So the list and the stream history show a customer's name, e-mail, address and phone **in plain text** for as long as that customer's key exists; only after the key is erased (Chapter 7) do the same fields come back as `[REDACTED]`. Encryption at rest protects the database, not this endpoint. Both methods therefore start with `OwnerOrAdminAccess.requireAdmin(RequestCaller.current())`, the check from Chapter 9 that the admin endpoints use (Chapters 12 and 14): for anyone but an `ADMIN` — the role together with an identity — the answer is `403`, and no event is read.

**Why the feed does not.** The browser opens the feed with `EventSource`, which cannot send request headers, so the feed cannot ask who is calling: every connection is anonymous, an `ADMIN`'s browser included. It stays open instead and says only what an anonymous caller may know. `LiveFeedFrame.of` builds a frame from four of the envelope's coordinates and never touches `env.event()`, `env.streamId()` or `env.aggregateId()`.

**What the feed tells everyone, and what it keeps.** A frame says that an event of a type happened to an aggregate of a type, and when: a caller without any header reads `customer` / `CustomerRegistered` and `order` / `OrderPlaced` off the feed, with their times. It does not say which customer or which order. A stream id is `customer:c-1` or `order:o-1`, and a customer id or an order id in an open feed would tell every caller who exists, who ordered when, who asked for an export and who was forgotten; Chapter 9's `403` keeps exactly that to itself. So the frame leaves out the stream id, the aggregate id and the version within the stream for everyone, rather than for some: the feed has no caller to tell an `ADMIN` from a guest.

**Why the feed starts at the head.** The feed has no caller to refuse, so everything it does for one connection it does for anybody, as often as they connect. A feed that began at offset `0` would hand every connection the whole history: one read and one decryption of every event in the store per connection, and one frame per event for the page to react to. Starting at the head keeps the feed's reading to one small read per second. The feed takes no start position from the request either: no query parameter, and no `Last-Event-ID`. What happened before the connection is read where the caller is known, from the read-model lists and, for an `ADMIN`, from `GET /api/events`; a client that was away reads those again when it reconnects.

**Why one reader, and a bounded number of connections.** A reader per connection makes the store's load a number anonymous callers choose: every open connection is one statement per second against the connection pool the command path uses, and every stored event is read, deserialized and decrypted once per connection. With one reader per app the store is asked once per second and an event is read once, whether one browser tab is open or a hundred; what a connection still costs is its socket and the frames buffered for it, and `MAX_CONNECTIONS` bounds that. The connection after the last place gets `503 Service Unavailable` with `Retry-After: 5`, and not `429`: the limit is the app's and is shared by all callers, and the feed could not count the requests of one caller anyway. `GlobalExceptionHandler` maps the exception; add the imports of `org.springframework.http.HttpHeaders`, `org.springframework.http.MediaType`, `org.streamrune.ecommerce.queries.access.LiveFeed` and `org.streamrune.ecommerce.queries.access.LiveFeedFullException` and this method to it:

```java
@ExceptionHandler(LiveFeedFullException.class)
public ResponseEntity<String> handleLiveFeedFull(LiveFeedFullException e) {
  return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
      .header(HttpHeaders.RETRY_AFTER, String.valueOf(LiveFeed.RETRY_AFTER_SECONDS))
      .contentType(MediaType.TEXT_PLAIN)
      .body(e.getMessage());
}
```

The controller takes the place before it returns the `Flux`, so the refusal is a status and not a stream that ends at once. The client is registered a moment later, when Spring MVC subscribes the `Flux`, and between the two a place is pending. A framework that never subscribes the stream leaves the place without a client that could leave it: Quarkus does that for a request whose connection was reset before the answer is written. `LiveFeed` therefore frees a place nobody followed within `FOLLOW_TIMEOUT`, five seconds, and completes a client that is registered on such a place afterwards. The places taken are always the pending places plus the registered clients; the class keeps those two collections and no counter that could drift. A browser's `EventSource` does not reconnect by itself after an answer that is not `200`; the frontend of Chapter 16 opens a new one with a growing delay. `LiveFeedSharedReaderIT` in each app opens five connections and compares their frames, counts the reader's store reads with one and with eight connections, fills the 100 places and reads the `503` of the next connection, connects a client that never reads beside two that do, and sends five hundred requests whose client is gone at once before it connects a client that is served.

**When the reader runs, and how a place is freed.** The reader starts with the first client and ends when the last one has left: an app nobody watches reads nothing, and since every connection starts at the head, a reader that starts again has no position to keep. Whatever ends the reader's thread, an `Error` included, its clients are told and the next client starts a new reader. Quarkus and Micronaut cancel the response stream the moment the connection closes, and the reader sees `isGone()` in its next round. A servlet container learns of a closed connection only when a write to it fails, so on Spring the `: keepalive` comment is also what finds a client that left during a quiet period: the write fails, the `Flux` is cancelled, the reader forgets the client and its place is free. `LiveFeedAccessIT` in each app connects, disconnects and waits for `openFeedConnections()` to return to zero. On shutdown the controller's `stop()` closes the feed before Spring Boot's graceful shutdown starts waiting for the requests in flight: every client is completed and the reader has ended when `stop()` returns. Without it a single open browser tab holds the shutdown for the whole grace period.

**`HEAD` gets a handler of its own.** Spring answers a `HEAD` request it finds no handler for by running the `GET` handler and leaving out the body. For a stream that never ends that would take one of the feed's places and register a client that can never leave: there is no body whose write could fail. `sseHead` answers with the feed's headers, takes no place and reads nothing; the two other apps have the same handler. `LiveFeedSharedReaderIT` sends a `HEAD` and finds no place taken and no reader running.

**A client that cannot keep up.** The reader hands a frame to each client's `FluxSink` and moves on; it never waits for a socket. A client that reads too slowly has up to 256 frames buffered for it. Beyond that its stream fails, its place is free and it reconnects, at the head like everyone else. Spring MVC writes the buffered frames to the sockets on its async task executor, a pool of eight threads by default; a client whose socket is full holds one of them for as long as the servlet container waits for the write, so a deployment that expects many such clients runs that executor on virtual threads (`spring.threads.virtual.enabled`).

**How a view gets the detail.** A frame is a signal to read again. The frontend of Chapter 16 uses the event type to decide which of its queries are stale and reads them again through the guarded endpoints, with the session's `X-User-Id` and `X-User-Role`: the customer and order reads of Chapter 9 return what that caller may see, and the event history above returns streams and payloads to an `ADMIN`. A client that wants the events of one stream as they happen opens the per-stream endpoint of the next step, which does know its caller.

> **Demo shortcut — protect this in production.** `requireAdmin` trusts the `X-User-Id` and `X-User-Role` headers, which any client can set (the gateway stand-in from Chapter 9). The check keeps the demo honest about *who should* see decrypted events, but it does not stop anyone who adds the two headers. In a real system an endpoint that returns raw event payloads belongs behind real authentication, with the role derived from the authenticated identity, on an operator-only network path — or it does not exist at all, and operators read events with tooling that has its own access control. The same goes for the open feed: event types and their times are metadata, and metadata can be sensitive too (a burst of `OrderPlaced` frames is a sales figure, a `CustomerForgotten` frame says that somebody left). The feed names nobody, but decide per deployment whether an unauthenticated caller may see even that.

The SSE endpoint uses `spring-boot-starter-webflux` for the `Flux` return type. The `webflux` dependency is already present in `build.gradle.kts` from an earlier chapter. `Flux.create` hands the method a `FluxSink`; `clientOf(sink)` adapts it to `LiveFeed.Client`, so `sink.isCancelled()` is how the reader learns that a client left. A client that reads too slowly gets up to 256 frames buffered (`onBackpressureBuffer`) and is then failed, and it reconnects. The reader is a thread of its own, one daemon thread per app named `live-feed-reader`, so no Reactor scheduler and no request thread is held while it waits for its next round.

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
    # How often the framework's feed reads the global stream: the usual delay between a commit and
    # its frame. The framework default is 1s; the smallest value it accepts is 1ms.
    polling-interval: 100ms
```

and decide who may open which stream. A frame of this endpoint carries the event with its `@Encrypted` fields decrypted (more on that below), so the decision is the application's whole part in it. The demo's rule: **an authenticated caller who is `ADMIN`, or the customer the stream belongs to; everyone else is refused.**

The rule is plain Java with no Spring in it, so it goes into the shared `queries` module, next to `OwnerOrAdminAccess` from Chapter 9, where `quarkus-app` and `micronaut-app` use the same class. Create `queries/src/main/java/org/streamrune/ecommerce/queries/access/OwnerOrAdminStreamAccess.java`:

```java
package org.streamrune.ecommerce.queries.access;

import java.util.function.Function;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * Who may open the live event stream of one aggregate: an authenticated caller who holds the ADMIN
 * role, or the customer the stream belongs to. Everyone else is refused, and so is every aggregate
 * type this class does not name.
 */
public final class OwnerOrAdminStreamAccess {

  private final Function<String, OrderView> orderById;

  /**
   * @param orderById the order read model: the order with the given id, or null when the read model
   *     has no row for it
   */
  public OwnerOrAdminStreamAccess(Function<String, OrderView> orderById) {
    if (orderById == null) {
      throw new IllegalArgumentException("orderById is required");
    }
    this.orderById = orderById;
  }

  /** The signature of the framework's SseAuthorizer, so a bean is {@code access::isAuthorized}. */
  public boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId) {
    if (OwnerOrAdminAccess.identityOf(caller) == null) {
      return false;
    }
    AggregateType type = streamId.aggregateType();
    String aggregateId = streamId.aggregateId().value();
    if (CustomerState.TYPE.equals(type)) {
      return OwnerOrAdminAccess.mayAccessCustomer(caller, aggregateId);
    }
    if (OrderState.TYPE.equals(type)) {
      // An ADMIN needs no owner, so the read model is asked only for everyone else.
      return OwnerOrAdminAccess.isAdmin(caller)
          || OwnerOrAdminAccess.mayAccessOrder(caller, orderById.apply(aggregateId));
    }
    if (PaymentState.TYPE.equals(type)
        || InventoryState.TYPE.equals(type)
        || ProductState.TYPE.equals(type)) {
      return OwnerOrAdminAccess.isAdmin(caller);
    }
    return false;
  }
}
```

The framework asks an `SseAuthorizer` one question, `isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId)`, before it subscribes anybody. `caller` is the request context of the request that asks for the stream — the object the framework's request filter built for it in Chapter 9, the same one a command of that request runs under. `caller.userId()` is the request identity — in the demo's trusted-gateway mode the `X-User-Id` header — and `null` when the request has none. A request no context is bound for never reaches the authorizer: the framework answers it `403` itself. The class answers per aggregate type and fails closed:

| Stream | Who may open it | Why |
|--------|-----------------|-----|
| any, without an identity | nobody | A role without a caller is nobody: `X-User-Role: ADMIN` alone opens nothing. |
| `customer:<id>` | `ADMIN`, or the caller whose id is `<id>` | The events carry the customer's name, e-mail, address and phone. |
| `order:<id>` | `ADMIN`, or the customer the order read model names for that order | The read model from Chapter 8 already knows each order's `customerId`; one `OrderProjection.get` is the cheapest honest proof of ownership. |
| `payment:<id>` | `ADMIN` | The amount and the refund or failure reason of one customer's purchase, and no read model links a payment to its customer. |
| `inventory:<id>` | `ADMIN` | A product's stock events name the orders that reserved it — other customers' orders. |
| `product:<id>` | `ADMIN` | Nothing personal, but operational detail the public catalogue does not show: stock adjustments with their free-text reason, the price history. |
| any other type | nobody, `ADMIN` included | A new aggregate stays closed until you add a line for it. |

Mind the `order` line: the read model is eventually consistent. `OrderProjection` writes the row a moment after `POST /api/orders` returns, and until it has, the order has no known owner — the customer is refused exactly as for an order that was never placed, and subscribes once `GET /api/orders/{id}` answers.

Who the caller is and who owns what is `OwnerOrAdminAccess`'s answer, the class from Chapter 9 that the REST routes of customers and orders ask: the SSE endpoint and `GET /api/customers/{id}` cannot disagree about whose data a customer's is. `ADMIN` is therefore decided the way the rest of the demo decides it — the `role` baggage entry of the request context, the one `HeaderUserRoleResolver` resolves the caller's authority from. The context the rule is handed carries that entry next to the caller's id, so `caller` is the only thing the rule reads: no `ScopedValue`, no request-scoped bean, nothing that differs between Spring, Quarkus and Micronaut. `caller.authority()` holds what `HeaderUserRoleResolver` returned for the caller at the request edge — for an identified caller the same role, with its permissions; the rule keeps to the baggage entry so that the demo has one definition of `ADMIN`. A resolver that looks roles up from real authentication writes no baggage entry, and a rule in such a deployment asks `caller.authority().hasRole("ADMIN")` instead.

"The customer the order read model names" is the `customerId` of the order. `POST /api/orders` takes it from the request body, and Chapter 9 ties it to the caller: only that customer, or an `ADMIN`, may place the order. An order's stream therefore opens for the customer who placed it, or for whom an operator placed it.

Now the bean. Add these imports to `StreamRuneConfig`:

```java
import org.streamrune.ecommerce.queries.access.OwnerOrAdminStreamAccess;
import org.streamrune.integration.SseAuthorizer;
```

and the bean method:

```java
@Bean
public SseAuthorizer sseAuthorizer(OrderProjection orderProjection) {
  return new OwnerOrAdminStreamAccess(orderProjection::get)::isAuthorized;
}
```

The rule needs the order read model and nothing else: the caller arrives as the argument of every call.

That is the whole wiring. With `streamrune.sse.enabled=true` the Spring integration registers three things:

- **`SseController`** — the endpoint. It builds the `StreamId` from the two path segments (an invalid one is a `400`), asks the `SseAuthorizer` whether the caller may read that stream, and subscribes the client. A refused caller gets `403 Forbidden` — the same answer whether the request named no caller at all or the wrong one; the framework does not answer `401`. The caller it hands the authorizer is the request context from Chapter 9: in the demo's trusted-gateway mode, `X-User-Id` as its user and `X-User-Role` as its `role` entry.
- **`SseEventPublisher`** — the in-process fan-out the controller subscribes its clients to, one bounded queue per client. A client that cannot keep up is disconnected, not skipped.
- **`SseEventFeed`** — what publishes. One per application instance: a polling subscription on the global stream that starts at the **head** of the stream when the application starts, reads every `streamrune.sse.polling-interval`, and hands each event to the clients of that event's own stream. It keeps its position in memory — no row in the offset store — and stops with the application. It reads, and decrypts, events only while a client is connected to its instance: with nobody subscribed a poll is one read of the head of the global stream, and no event is read or decrypted, however many are stored. With at least one client, on any stream, it reads and decrypts every event stored from then on, whichever stream it belongs to.

**Do not publish to `SseEventPublisher` yourself.** The feed already publishes every stored event; a subscription of your own that also calls `publish` would write every frame twice. `publish` takes the envelope only and routes it by the envelope's own stream id, so an event cannot reach the clients of another stream.

**Delivery is live, best-effort and at-most-once.** A frame reaches a client only while that client is connected. Nothing is redelivered and `Last-Event-ID` is not honoured: events stored before the client connected, while it was reconnecting, or while the instance was down are not sent to it. One exception: an event of its stream stored in the polling interval or so before the client subscribed, which the feed had not passed yet, can still arrive. Within one stream, frames arrive in version order, normally within one polling interval of the commit (longer while a read of the event store fails and is retried). Treat a frame as a notification — after every (re)connect read the current state from a query — and put anything that must see every event in a projection. This is the difference from the Event Explorer feed of Step 5, which starts at offset `0` for every client and replays the history.

**A frame carries the payload, so the authorizer matters.** The frame's `id` is the event's global offset and its `data` is the event as the event store reads it — with `@Encrypted` fields decrypted. A client of `/api/sse/customer/c-1` therefore receives that customer's name and e-mail in plain text with every `CustomerRegistered` or `ProfileUpdated` stored while it is connected — which is why `OwnerOrAdminStreamAccess` lets in customer `c-1` and an `ADMIN` and nobody else. Without an `SseAuthorizer` bean the framework installs one that denies every stream, so forgetting the bean closes the endpoint rather than opening it.

> **Demo shortcut — protect this in production.** The rule is the one a real deployment keeps; what it is handed is not. The demo runs in the trusted-gateway mode without a gateway (Chapter 9), so the caller's id and role are whatever the client wrote into `X-User-Id` and `X-User-Role`: anyone who sends `X-User-Id: c-1` is customer `c-1`, and anyone who adds `X-User-Role: ADMIN` is an operator. Behind real authentication the same class decides on an identity the client cannot choose.

> **Quarkus and Micronaut.** The same property and an `SseAuthorizer` bean built from the same `OwnerOrAdminStreamAccess` switch the endpoint on in `quarkus-app` (`StreamRuneProducer#sseAuthorizer`) and `micronaut-app` (`StreamRuneFactory#sseAuthorizer`), and the framework feeds it there too; both apps keep the default polling interval of one second. The bean is the same one line in all three, because the rule reads the caller from its argument and each integration hands over the request context its own request filter built. The rule may read the database on all three — its one read of the order read model: on Spring it runs on the servlet request thread; on Quarkus the framework's SSE resource method is a blocking one (`@Blocking`), so Quarkus REST runs the request filter and the rule on a worker thread, never on a Vert.x event loop; on Micronaut the framework's `StreamRuneContextFilter` runs on the blocking executor and the controller with it. The apps differ in what a connected client sees when the app stops. On Spring the framework completes every open stream before the web server's graceful shutdown, so the client sees a normal end. Quarkus stops its HTTP server before it fires its shutdown event, and the one earlier point, the shutdown-delay event, exists only in an application built with `quarkus.shutdown.delay-enabled=true` — a build-time property, set in `quarkus-app`'s `application.properties`. With it the framework completes the open streams at the start of the shutdown, ahead of the server; built without it, the server cuts the connections, and with `quarkus.shutdown.timeout` set one connected client holds the shutdown for that whole timeout. On Micronaut the embedded server closes the connections as it stops. An `EventSource` reconnects in every case. On Micronaut the frame's `data` is written by Micronaut Serialization, so every domain event (and each record nested in one) needs a serializer: the events live in the framework-agnostic `domain` module, and `MicronautEcommerceApplication` imports them with `@SerdeImport`. Each of the three apps has an `SseLiveE2EIT` that opens a stream over HTTP, sends a command over HTTP and waits for the frame, and an `SseStreamAccessIT` that walks the table above over HTTP; the table itself is unit-tested in `queries` (`OwnerOrAdminStreamAccessTest`).

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
        "subscription.order-fulfillment-saga": {
          "state": "RUNNING",
          "lag": 0,
          "errorCount": 0,
          "status": "UP",
          "role": "LEADING"
        },
        "subscription.payment-process-manager": {
          "state": "RUNNING",
          "lag": 0,
          "errorCount": 0,
          "status": "UP",
          "role": "LEADING"
        },
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

The `details` also hold one `relay.<name>` block for each retention sweeper, for the saga compensation-retry sweeper and for the saga's timeout runner (`relay.outbox-retention-sweeper`, `relay.inbox-retention-sweeper`, `relay.dead-letter-retention-sweeper`, `relay.saga-dead-letter-retention-sweeper`, and `relay.saga-compensation-retry:` and `relay.saga-timeout:` each followed by the saga state class), shortened here. The timeout runner is the application's (Chapter 11, Step 6); the integration finds the `SagaTimeoutRunner` bean and registers it like the relays it builds itself, so a timeout runner whose poll thread died turns the component `DOWN`. Once Step 6b has switched the per-stream SSE endpoint on, one more block appears, `relay.sse-event-feed`: the framework reports the feed of that endpoint like its other polling threads, `DOWN` when the thread has died, because the endpoint would go on admitting clients while no event reached them. `StreamRuneHealthIT` checks this list.

Four subscriptions are registered, each with its role: `order-fulfillment-saga`, `payment-process-manager`, `order-cancellation` and `order-shipment` — the saga's and those of its three process managers (Chapter 11, Steps 5b, 5c and 6). Each is built with `.healthContributor(...)`, so it registers with the auto-configured `SubscriptionHealthContributor` when it starts and adds a `subscription.<name>` block: `state` (`RUNNING`, `PAUSED`, `STOPPED`), `lag` (the events between its checkpoint and the head of the store), `errorCount`, `status`, and `role`, the subscription's role under its lease. With one backend all four read `LEADING`. With two (Chapter 17) one backend reads `LEADING` and the other `STANDBY`; a standby is `RUNNING`, has no delivery errors and shares the leader's lag — the checkpoint is shared — so it reads `UP` while some backend leads and keeps up, and `DEGRADED` on both once the shared lag crosses `streamrune.subscription.health.lag-threshold` (1000 events unless configured). A listener that keeps failing — the payment process manager on a capture the store refuses, say — still shows first in the application log, as the poll loop's `ERROR` line for every attempt (Chapter 11, Step 5b): the block records it too. A subscription registered with `.healthContributor(...)` wraps its listener itself, so each page the listener throws on adds one to `errorCount`, and the block reads `DEGRADED` from the first failed page; a page the listener returns from sets `errorCount` back to `0`, and the status then follows the other rules again: `DEGRADED` while the lag is at the threshold or the subscription is paused, `DOWN` only when it is stopped or has halted for good (errors alone never make it `DOWN`), otherwise `UP`. A standby delivers no page, so it reports `errorCount` `0` and counts afresh when it leads again. The checkpoint does not move past the failing event, so the block's `lag` also grows with every event stored after it, and crossing the threshold is a second reason for `DEGRADED`, independent of the errors. The demo's `MultiProjectionRunner` is built by hand without a health contributor, so it has no block here (the framework's `SseEventFeed` is not a subscription of that contributor either; it is the `relay.sse-event-feed` block above). `StreamRuneHealthIT` checks the four subscription blocks, and the Quarkus and Micronaut `ObservabilitySmokeSuiteIT` check that the four subscriptions are registered and lead. The role is `UNGATED` for a subscription that takes no lease, and `NOT_REPORTED` for a registration that reports none.

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

**Metrics — forward commands the saga's fence refused:**

The saga runner and the timeout runner of Chapter 11 are built with the metrics bean (`.metrics(...)` in `orderFulfillmentSagaRunner` and `orderFulfillmentTimeoutRunner`), so the saga meters count from the start. One of them is the signal of the race Chapter 11's forward fence closes:

```bash
curl -s http://localhost:8080/actuator/metrics/streamrune.saga.forward_commands_fenced | jq .
```

`streamrune.saga.forward_commands_fenced`, tagged `saga.type`, counts the forward commands of a saga that were refused at their commit because the saga left its forward phase while the step was in flight: a compensation claimed it at its deadline, or it ended. Nothing of a refused command was stored, so a sample needs no repair. The counter is registered on its first sample: until a forward command has been refused, the endpoint answers `404`. With the default fifteen-minute deadline it stays there. With the saga leading on one backend it rarely moves even in Chapter 17's two-backend run, with its deadline of a second and a half: the deadline claim waits for the step in the same process, and the fence refuses a command only when a handover overlaps a step in flight. A rate that keeps climbing in production means deadlines shorter than the steps they bound.

**Metrics — saga steps run again in place:**

```bash
curl -s http://localhost:8080/actuator/metrics/streamrune.saga.delivery_retried | jq .
```

`streamrune.saga.delivery_retried`, tagged `saga.type`, counts the saga steps the runner ran again in place because its write of the saga row lost the compare-and-swap to another writer — a twin delivery, a deadline claim, another backend. The step reads the row that won and routes the event on it, without a backoff and without the subscription redelivering the page; one sample per run again, at most two per event (`streamrune.saga.cas_conflicts` counts the conflicts themselves). It needs no alert. With the saga subscription leading on one backend the writers rarely meet: a few samples around a handover, when a deposed leader may still finish a step the new one runs, or when a deadline falls inside a step. Like the fence counter, it is registered on its first sample.

**Paginated event list:**

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" "http://localhost:8080/api/events?offset=0&limit=5" | jq .
```

Each item in the response array includes `globalOffset`, `streamId`, `aggregateType`, `aggregateId`, `eventType`, `timestamp`, and `payload`. Leave the header out and the answer is `403 Forbidden`.

**Per-stream events:**

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/events/order/o-1 | jq .
```

Replace `o-1` with an order ID you have placed (the stream is `order:<orderId>`, so the path carries the type `order` and the id as two segments). The response contains all events for that stream in append order.

Now read a customer's stream the same way, for example the customer from Chapter 6:

```bash
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/events/customer/c-1 | jq '.[0].payload'
```

The name, e-mail and address are there in plain text, although the `payload` column of `event_stream` holds ciphertext for them: the event store decrypted the fields for the caller. This is the reason the endpoint asks for the role.

**Live SSE stream:**

```bash
curl -N http://localhost:8080/api/events/sse
```

The `-N` flag disables buffering. The first line is `: connected`, a comment: the connection stands at the head of the global stream. Nothing stored earlier follows it, however many events the store holds. Leave the connection open and place an order in a separate terminal:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" \
  -d '{"orderId": "o-obs-1", "customerId": "c-1", "lines": [{"productId": "p-1", "quantity": 1}]}'
```

Within one second you should see SSE frames appear in the first terminal, one per event appended. The feed needs no header, and a frame names no stream and carries no payload:

```
id:17
data:{"globalOffset":17,"aggregateType":"order","eventType":"OrderPlaced","timestamp":"2026-10-04T08:45:09.326626042Z"}
```

The frame says that an order was placed and when. It does not say that the order is `o-obs-1` or that `c-1` placed it: the stream id is not in it. The frames of the payment, the inventory and the order's confirmation follow, each with its own aggregate type and event type. When nothing happens for five seconds the terminal shows `: keepalive`, another comment. Stop `curl` and start it again: the new connection begins with `: connected` and shows none of the frames the first one received.

**Live SSE stream of one aggregate:**

The framework's endpoint from Step 6b streams one stream, with payloads, to an `ADMIN` or to the stream's owner. Start with the callers it turns away. Without an identity:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/sse/customer/c-1
```

```
403
```

As another customer:

```bash
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "X-User-Id: c-2" -H "X-User-Role: CUSTOMER" \
  http://localhost:8080/api/sse/customer/c-1
```

```
403
```

Customer `c-1` (registered in Chapter 6) may open their own stream:

```bash
curl -N -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" http://localhost:8080/api/sse/customer/c-1
```

`curl` prints one line at once:

```
: keepalive
```

That is the stream's opening comment. The endpoint writes it as soon as the client is subscribed, so from this line on every event of `customer:c-1` reaches this terminal. Change the profile in the other terminal:

```bash
curl -s -X PUT http://localhost:8080/api/customers/c-1 \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" \
  -d '{"name": "Alice Smith", "email": "alice.smith@example.com", "address": "123 Main St", "phone": "+1234567890"}'
```

Within the polling interval the `ProfileUpdated` event arrives as a frame below the opening comment — the whole event as JSON, the encrypted fields in plain text, and its global offset as the `id`:

```
: keepalive

data:{"customerId":"c-1","name":"Alice Smith","email":"alice.smith@example.com","address":"123 Main St","phone":"+1234567890"}
id:21

```

That frame is the reason for the two `403`s above. Stop `curl` with Ctrl-C.

An operator may open any stream of the five aggregate types, including one that does not exist yet. Subscribe to an order before it is placed:

```bash
curl -N -H "X-User-Id: ops-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/sse/order/o-obs-2
```

It prints `: keepalive` and waits: the stream is open, though no event of that order exists. Place the order in the other terminal:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" \
  -d '{"orderId": "o-obs-2", "customerId": "c-1", "lines": [{"productId": "p-1", "quantity": 1}]}'
```

Within the polling interval the `OrderPlaced` event arrives. The stored event holds each line's unit price, which the catalogue set when the order was placed: `p-1` at 21.99, the price an `ADMIN` gave it in Chapter 9 (Step 7) after Chapter 5 had raised it to 34.99:

```
: keepalive

data:{"orderId":"o-obs-2","customerId":"c-1","lines":[{"productId":"p-1","quantity":1,"unitPrice":{"amount":21.99,"currency":"USD"}}],"total":{"amount":21.99,"currency":"USD"}}
id:22

```

More frames follow as the saga from Chapter 11 drives the order on (`OrderConfirmed`, or `OrderCancelled` when the stock cannot be reserved). The payment and inventory events of the same order do not appear: they belong to other streams. Stop `curl`, start it again, and nothing is replayed — after its opening `: keepalive` the stream stays silent until the next event of `order:o-obs-2` is stored.

The order now has a row in the order read model that names `c-1` as its customer, so `c-1` may open `order:o-obs-2` as well (`-H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER"`), while `c-2` gets `403` — and so would `c-1` have, had they asked before the order was placed.

Two details of the wire. Lines that start with a colon are comments, which an SSE client ignores. Every stream opens with one, `: keepalive`, written as soon as the client is subscribed: it commits the response, so `curl -i` prints `200` and the headers at once, and it tells the client that an event stored from now on will reach it. The same line is the keepalive the controller writes every `streamrune.sse.keep-alive-interval` (30 seconds by default), which is how a client that vanished is noticed. And authorization is decided once, when the stream opens: a refusal is an immediate `403`, and a caller who loses access keeps the stream they have until the server ends it, after `streamrune.sse.timeout` (5 minutes by default) at the latest; the client that reconnects is asked again.

---

## What We Learned

**`MicrometerStreamRuneMetrics`** implements `StreamRuneMetrics` using `io.micrometer.core.instrument.MeterRegistry`. It registers counters, timers, and gauges for commands, events, projections, subscriptions, and the other subsystems, under the names `MetricNames` publishes. The framework's `VirtualThreadCommandBus` and `MultiProjectionRunner` increment these counters at each processing stage **when a `StreamRuneMetrics` collector is wired into their builders (`.metrics(...)`)**. The shipped demo wires its command bus and projection runner by hand without a metrics collector, so their meters are registered and discoverable but stay at zero until you pass the bean in (see the optional step in Step 7 above); its saga runner and timeout runner get the bean, so the saga meters, `streamrune.saga.forward_commands_fenced` among them, count. `StreamRuneAutoConfiguration` registers the `MicrometerStreamRuneMetrics` bean automatically when a `MeterRegistry` is present; you can override it by declaring your own `StreamRuneMetrics` bean. The meter names follow the `streamrune.*` namespace and are dimensional, so they export cleanly to any Micrometer registry — including Prometheus, once you add `micrometer-registry-prometheus` (the demo ships only the in-memory `SimpleMeterRegistry`).

**`OpenTelemetryCommandInterceptor`** implements `CommandInterceptor` and uses the `opentelemetry-api` facade — it has no SDK dependency and works with any OpenTelemetry SDK implementation. For each command it creates a span named `command/<CommandType>` of kind `INTERNAL`, sets `streamrune.command.type`, `streamrune.command.id`, `streamrune.aggregate.type`, `streamrune.aggregate.id`, and `streamrune.stream.id` as span attributes when the span starts (a failed command's span carries its stream too), and closes the span with `OK` or `ERROR` status. Each span and the scope that makes it current sit on a per-thread stack, because `before()`, `after()`, and `onError()` are called on the same thread that invoked `CommandBus.execute()`, and a command dispatched from inside another command's callback opens a nested span on that thread. `StreamRuneAutoConfiguration` registers the interceptor automatically when an `OpenTelemetry` bean is present.

**`StreamRuneHealthIndicator`** implements Spring Boot's `HealthIndicator` and checks things in sequence: the JDBC `DataSource` connection validity, the head of the `event_stream` table read directly via SQL on that same connection (reported as `eventStore.lastGlobalOffset`/`eventStore.lastEventTimestamp` — there is no `EventStore` dependency), the lag and error count of each registered subscription via `SubscriptionHealthContributor`, and the liveness of each background relay registered with `BackgroundRelayHealthContributor`. If the database is unreachable, the status is `DOWN` immediately. If any subscription reports `DOWN`, or a started relay's poll thread has died, the overall status is also `DOWN`. All checks are exposed as named details so that operators can distinguish a database failure from a subscription lag spike at a glance.

**`EventExplorerController`** provides the `GET /api/events` paginated global stream, `GET /api/events/{aggregateType}/{aggregateId}` per-stream event list, and `GET /api/events/sse` live SSE stream. The first two return event payloads as the event store reads them — decrypted — and answer only to an `ADMIN`; that check is a demo shortcut on two client-supplied headers, and a real deployment puts such an endpoint behind real authentication. The SSE endpoint is open, and because it cannot ask who is calling it names no stream and carries no payload: `LiveFeed` runs one reader per app for at most 100 connections: a connection begins at the head of the global stream, the reader polls `EventStore.readGlobalStream` once a second, and each event stored from then on goes to every connected client as an unnamed `ServerSentEvent<String>` with the global offset as the SSE `id` field and a `LiveFeedFrame` — the global offset, the aggregate type, the event type and the timestamp — as its data. A connection is never served the store's history, and its reader ends when the client leaves or the app stops. A view that wants the detail reads it from the guarded endpoints. `Flux.create` bridges the imperative polling thread to the reactive `Flux` the controller returns.

**The framework's SSE endpoint** (`GET /api/sse/{aggregateType}/{aggregateId}`, `streamrune.sse.enabled=true`) streams the events of one aggregate with their payloads. The integration serves it and feeds it: an `SseEventFeed` per application instance reads the global stream from its head every `streamrune.sse.polling-interval`, while a client is connected to that instance, and publishes each event through `SseEventPublisher.publish(EventEnvelope)` to the clients of that event's stream; with no client connected it reads the head of the stream and no event. The application writes no feeder; it supplies the `SseAuthorizer` that decides who may open which stream, and without one every stream answers `403`. The demo's is `OwnerOrAdminStreamAccess`, one framework-agnostic class the three apps share: an authenticated `ADMIN`, or the customer the stream belongs to — the customer themself for a `customer` stream, the customer the order read model names for an `order` stream — and `403` for everyone else, for every other aggregate type, and for a caller without an identity. The rule is asked once, when a stream opens; the stream then opens with a `: keepalive` comment, written as soon as the client is subscribed. Delivery is live, best-effort and at-most-once — no replay, no `Last-Event-ID` — so a frame is a notification, and a consumer that must see every event is a projection.

---

## Next Up

The backend is complete. Let's look at the frontend.
