# Chapter 13: Inventory Aggregate & Outbox

> **What you'll learn:** How to model an inventory aggregate with three distinct stock pools — available, reserved, and committed — and how StreamRune's transactional outbox pattern reliably forwards a curated, PII-safe set of integration events to RabbitMQ. You will build `InventoryDecider` with full TDD coverage and wire the four outbox beans (`PostgresOutboxStore`, `EcommerceIntegrationEventMapper`, `RabbitMqConfig` publisher, and `streamrune.outbox.enabled=true`) that make end-to-end delivery real. A second Spring Boot application — the `notifications-service` — subscribes to the queue and exposes `GET /received` so you can verify cross-service delivery without touching the e-shop internals.

---

## What We're Building and Why

Every e-commerce system eventually needs to answer a deceptively hard question: how do you tell an external warehouse system, a third-party notification service, or a downstream microservice about something that just happened — reliably, without risking inconsistency?

The naive approach is to publish a message after writing events to the store:

```java
eventStore.append(streamId, events);
messageBus.publish(events); // fires after the DB write
```

This looks correct but has a critical gap. The two operations are not atomic. If the application crashes between the append and the publish, or if the message broker is temporarily unavailable, the event is silently lost. Retrying on the next restart does not help because there is no record that the publish was attempted.

The **transactional outbox** pattern closes this gap. Instead of publishing directly, you write a record to an `outbox_events` table inside the same database transaction as the event store append. Both the event and the outbox entry either commit together or roll back together — guaranteed. A separate **poller** process then reads pending outbox entries and forwards them to the broker, marking each entry as delivered once the broker confirms receipt.

This pattern accepts **at-least-once delivery**: if the process restarts after the commit but before the poller marks the entry delivered, the poller will try again. Consumers must be idempotent (the `notifications-service` deduplicates by `entryId`). In exchange you get a strong guarantee: if the event was committed, the outbox entry exists, and the poller will eventually deliver it — unless an operator deliberately skips it (Chapter 14). In projection terms this is `EXTERNAL_EFFECT` (the delivery modes of Chapter 5): a projection whose job is a side effect writes an outbox row through the handed repository, so the row commits with its checkpoint; a relay you provide delivers it at-least-once, and the receiver dedups. The outbox in this chapter is the event store's own — its rows commit with the event append, and the framework's `OutboxPoller` is the relay.

**What the demo wires end-to-end:** The auto-configured `postgresEventStoreFactory` picks up the `PostgresOutboxStore` and `OutboxEventMapper` beans you expose in this chapter and arranges for every appended event to be screened by the mapper. Only the allowlisted, PII-safe events produce `OutboxEntry` rows — written atomically with the event in the same JDBC transaction. The auto-configured `OutboxPoller` reads those entries and hands them to the `OutboxPublisher` (provided by `RabbitMqConfig`), which publishes to RabbitMQ. A separate `notifications-service` module consumes the queue and exposes `GET /received` (port 8090). The outbox channel is **`STRICT_PER_AGGREGATE`** (the framework default, stated explicitly in the demo): entries of one order, payment or product are delivered in order, and a terminal `FAILED` entry blocks its aggregate until an operator replays or skips it (Chapter 14 adds the admin endpoints for that).

**PII boundary:** Customer events — `CustomerRegistered`, `ProfileUpdated`, `DataExportRequested`, `CustomerForgotten` — never cross the service boundary. `OrderPlaced` and `PaymentInitiated` are also excluded because they carry `customerId`. Only the order lifecycle outcomes, payment results, and product catalog changes are published. This is enforced by `EcommerceIntegrationEventMapper`'s allowlist, not by convention.

StreamRune provides `OutboxStore`, `PostgresOutboxStore`, `OutboxPoller`, `OutboxEntry`, `OutboxEventMapper`, and `OutboxStatus` as first-class building blocks. You focus on the domain model and the integration contract; StreamRune handles the transactional plumbing.

---

## Domain Model

Inventory tracks stock for a single product across three pools:

- **available** — units that can be sold right now.
- **reserved** — units held for a placed order but not yet confirmed (payment pending).
- **committed** — units confirmed for fulfillment; they leave the warehouse when the order ships.

The lifecycle of a unit flows: available → reserved (on `ReserveStock`) → committed (on `ConfirmReservation`) → gone. Cancellations reverse the first step: reserved → available again (on `ReleaseStock`). New stock arrives via `ReceiveShipment`.

### Commands

Create `domain/src/main/java/org/streamrune/ecommerce/domain/inventory/InventoryCommand.java`:

```java
package org.streamrune.ecommerce.domain.inventory;

import org.streamrune.core.Command;

public sealed interface InventoryCommand extends Command {
  record ReserveStock(String productId, String orderId, int quantity) implements InventoryCommand {}

  record ReleaseStock(String productId, String orderId, int quantity) implements InventoryCommand {}

  record ConfirmReservation(String productId, String orderId) implements InventoryCommand {}

  record ReceiveShipment(String productId, int quantity) implements InventoryCommand {}
}
```

### Events

Create `domain/src/main/java/org/streamrune/ecommerce/domain/inventory/InventoryEvent.java`:

```java
package org.streamrune.ecommerce.domain.inventory;

import org.streamrune.core.DomainEvent;

public sealed interface InventoryEvent extends DomainEvent {
  record StockReserved(String productId, String orderId, int quantity, int availableAfter)
      implements InventoryEvent {}

  record StockReleased(String productId, String orderId, int quantity, int availableAfter)
      implements InventoryEvent {}

  record ReservationConfirmed(String productId, String orderId, int quantity)
      implements InventoryEvent {}

  record ShipmentReceived(String productId, int quantity, int availableAfter)
      implements InventoryEvent {}
}
```

Each event records the `availableAfter` pool size at the time of the transition. This makes the event self-describing: a consumer can reconstruct the current availability without replaying the entire stream.

### State

Create `domain/src/main/java/org/streamrune/ecommerce/domain/inventory/InventoryState.java`:

```java
package org.streamrune.ecommerce.domain.inventory;

import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record InventoryState(String productId, int available, int reserved, int committed)
    implements AggregateState {
  /**
   * The aggregate type the inventory decider is registered under: streams are
   * inventory:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("inventory");

  public InventoryState() {
    this(null, 0, 0, 0);
  }
}
```

The zero-argument constructor satisfies `AggregateState`; StreamRune calls it when no prior events exist for the stream.

---

## Step by Step

### Step 1: Write the Tests First

Create `commands/src/test/java/org/streamrune/ecommerce/commands/inventory/InventoryDeciderTest.java`. `DeciderFixture` from `streamrune-test` removes all boilerplate: `given` supplies prior events, `when` issues a command, and `expectEvents` / `expectState` / `expectException` assert the outcome.

```java
package org.streamrune.ecommerce.commands.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;
import org.streamrune.test.DeciderFixture;

class InventoryDeciderTest {

  private final DeciderFixture<InventoryCommand, InventoryState, InventoryEvent> fixture =
      DeciderFixture.of(new InventoryDecider());

  private InventoryEvent.ShipmentReceived received() {
    return new InventoryEvent.ShipmentReceived("p-1", 100, 100);
  }

  @Test
  void receiveShipment() {
    fixture
        .given()
        .when(new InventoryCommand.ReceiveShipment("p-1", 100))
        .expectEvents(received())
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(100);
              assertThat(s.reserved()).isEqualTo(0);
            });
  }

  @Test
  void reserveStock() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 10))
        .expectEvents(new InventoryEvent.StockReserved("p-1", "o-1", 10, 90))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(90);
              assertThat(s.reserved()).isEqualTo(10);
            });
  }

  @Test
  void reserveStock_insufficient_throws() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 200))
        .expectException(DomainException.class);
  }

  @Test
  void releaseStock() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 10, 90))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1", 10))
        .expectEvents(new InventoryEvent.StockReleased("p-1", "o-1", 10, 100))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(100);
              assertThat(s.reserved()).isEqualTo(0);
            });
  }

  @Test
  void confirmReservation() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 10, 90))
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectEvents(new InventoryEvent.ReservationConfirmed("p-1", "o-1", 10))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(0);
              assertThat(s.committed()).isEqualTo(10);
            });
  }
}
```

Run the tests. They will all fail because `InventoryDecider` does not exist yet. That is expected.

### Step 2: Implement `InventoryDecider`

Create `commands/src/main/java/org/streamrune/ecommerce/commands/inventory/InventoryDecider.java`:

```java
package org.streamrune.ecommerce.commands.inventory;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;

public class InventoryDecider implements Decider<InventoryCommand, InventoryState, InventoryEvent> {

  @Override
  public InventoryState initialState() {
    return new InventoryState();
  }

  @Override
  public List<InventoryEvent> decide(InventoryCommand cmd, InventoryState state) {
    return switch (cmd) {
      case InventoryCommand.ReserveStock c -> {
        if (state.available() < c.quantity())
          throw new DomainException(
              "Insufficient stock for "
                  + c.productId()
                  + ": available="
                  + state.available()
                  + ", requested="
                  + c.quantity());
        int newAvailable = state.available() - c.quantity();
        yield List.of(
            new InventoryEvent.StockReserved(
                c.productId(), c.orderId(), c.quantity(), newAvailable));
      }

      case InventoryCommand.ReleaseStock c -> {
        int newAvailable = state.available() + c.quantity();
        yield List.of(
            new InventoryEvent.StockReleased(
                c.productId(), c.orderId(), c.quantity(), newAvailable));
      }

      case InventoryCommand.ConfirmReservation c ->
          List.of(
              new InventoryEvent.ReservationConfirmed(
                  c.productId(), c.orderId(), state.reserved()));

      case InventoryCommand.ReceiveShipment c -> {
        int newAvailable = state.available() + c.quantity();
        yield List.of(
            new InventoryEvent.ShipmentReceived(c.productId(), c.quantity(), newAvailable));
      }
    };
  }

  @Override
  public InventoryState evolve(InventoryState state, InventoryEvent evt) {
    return switch (evt) {
      case InventoryEvent.StockReserved e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() + e.quantity(),
              state.committed());
      case InventoryEvent.StockReleased e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() - e.quantity(),
              state.committed());
      case InventoryEvent.ReservationConfirmed e ->
          new InventoryState(
              state.productId(),
              state.available(),
              state.reserved() - e.quantity(),
              state.committed() + e.quantity());
      case InventoryEvent.ShipmentReceived e ->
          new InventoryState(
              e.productId(), e.availableAfter(), state.reserved(), state.committed());
    };
  }
}
```

A few things worth noting. `ReserveStock` is the only command that can fail: it checks `state.available()` before emitting an event. All other commands are unconditional — there is no business rule that prevents releasing more than was reserved, because the event stream itself is the source of truth and such a scenario indicates a bug upstream. `ConfirmReservation` reads `state.reserved()` rather than the command's quantity because by the time the command is processed the reservation quantity is already recorded in state; this avoids a stale-quantity bug if the command is replayed.

Run the tests. All five should pass.

### Step 3: Register Inventory Events in the Event Type Registry

Open `StreamRuneConfig.java` and add the four inventory event types to the `SimpleEventTypeRegistry` inside the `eventTypeRegistry` bean:

```java
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
```

### Step 4: Register the Inventory Decider in the Command Bus

In the same file, add the `InventoryCommand` block to the `VirtualThreadCommandBus` builder. The inventory decider registers under its own aggregate type, `InventoryState.TYPE` (`inventory`), so `ReserveStock` for `p-1` writes to `inventory:p-1` while the product's events live in `product:p-1` — one stream per product and per type, no hand-made prefix. The framework refuses two registrations of one command type and two overlapping command roots with different types at startup.

```java
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
```

> Two aggregate types may share an id value without sharing a stream: `ReserveStock` for `productId = "p-1"` writes to `inventory:p-1`, and the `ProductCreated`, `PriceUpdated` and `StockAdjusted` events of the product `p-1` stay in `product:p-1`. The type is part of the stream's identity — the event store keeps it in its own `aggregate_type` column, next to `aggregate_id` — so mixing the two kinds of events in one stream cannot happen by construction.

> The inventory id is the product id itself, so `AggregateId.of`'s 255-character bound (chapter 2) applies to it as is: a product id over the bound, or one with a control character, makes an inventory request answer `400`. An order cannot name such a product id: the `OrderCommand.OrderLine` constructor refuses it when the order is placed (chapter 8), with the same bound. If the saga meets such an id anyway, in an `OrderPlaced` event that never crossed that check, its reservation is refused here and the saga compensates the order (chapter 11).

### Step 4b: Add `InventoryProjection` and register it in `MultiProjectionRunner`

Create `projections/src/main/java/org/streamrune/ecommerce/projections/InventoryProjection.java` (source follows the same pattern as `ProductProjection` and `OrderProjection` — copy the full implementation from the real demo at that path). Like the other projections, it extends `org.streamrune.core.projection.BaseProjection` (introduced in the projections chapter) and persists the `org.streamrune.ecommerce.queries.dto.InventoryView` DTO in the `queries` module, so make sure both already exist before you compile.

Register the bean in `StreamRuneConfig`:

```java
@Bean
public InventoryProjection inventoryProjection(ProjectionRepository repo) {
    return new InventoryProjection(repo);
}
```

Update `MultiProjectionRunner` to include the inventory projection:

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
    CacheInvalidator cacheInvalidator) {

  var runner =
      MultiProjectionRunner.builder()
          .eventStore(eventStore)
          .offsetStore(offsetStore)
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
```

All four projections are now wired. The runner assigns each its own virtual thread and its own offset namespace.

`InventoryProjection` is a **counter** (`existing.reserved() + e.quantity()`). That is exactly why it must be `TRANSACTIONAL_LOCAL`: under at-least-once a crash between the write and the checkpoint would reserve the stock twice.

### Step 5: Wire the Outbox Beans

Add three beans to `StreamRuneConfig.java`. The `PostgresOutboxStore` must share the same `DataSource` as the event store — that is what makes the outbox insert and the event append atomic. The `OutboxEventMapper` determines which events are forwarded and in what shape. Both beans are discovered by the auto-configured `postgresEventStoreFactory`.

```java
import java.time.Duration;
import javax.sql.DataSource;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.ecommerce.commands.integration.EcommerceIntegrationEventMapper;
import org.streamrune.postgres.PostgresOutboxStore;

/**
 * Outbox store bean: needed by both the auto-configured postgresEventStoreFactory (so outbox
 * entries are written inside the same append transaction) and the auto-configured OutboxPoller.
 *
 * The claim lease (2 minutes) must strictly exceed the RabbitMQ publisher's in-flight horizon
 * (below): 30s publish-block budget + 30s confirm timeout = 60s horizon, plus
 * the framework's recommended 30s margin = 90s minimum. OutboxPoller refuses to boot when the
 * lease does not exceed the horizon, so this is a hard requirement, not a tuning choice.
 *
 * STRICT_PER_AGGREGATE is the default; it is written out so the choice is visible. The demo's
 * outbox carries order lifecycle, payment outcomes and stock changes — consumers that must not
 * see n+1 before n — so a FAILED entry blocks its aggregate until an operator replays or skips
 * it. The alternative, AVAILABILITY_FIRST, lets later entries flow past a FAILED one and is only
 * right for telemetry-like consumers that are idempotent AND order-tolerant.
 */
@Bean
public PostgresOutboxStore outboxStore(DataSource ds) {
    return new PostgresOutboxStore(
        ds, Duration.ofMinutes(2), OutboxOrderingMode.STRICT_PER_AGGREGATE);
}

/**
 * Curated, PII-safe integration event mapper. Only allowlisted, non-PII events
 * are forwarded to the broker. All CustomerEvent.* and InventoryEvent.* types,
 * plus OrderPlaced and PaymentInitiated, are explicitly excluded.
 */
@Bean
public OutboxEventMapper outboxEventMapper() {
    return new EcommerceIntegrationEventMapper();
}
```

The `PostgresOutboxStore` constructor takes the claim lease (two minutes here — the maximum time a poller holds an `IN_PROGRESS` entry before another instance may reclaim it) and the ordering mode. Strict mode also makes `streamId` mandatory: every `OutboxEntry` the mapper returns must name the stream whose order the consumer depends on — `EcommerceIntegrationEventMapper` passes `envelope.streamId()`, the event's own stream (`order:<id>`, `payment:<id>`, `product:<id>`) — and a `null` stream fails the append with `OutboxOrderingViolationException`. The `OutboxEventMapper` is `EcommerceIntegrationEventMapper`, which lives in the `commands` module at `org.streamrune.ecommerce.commands.integration`. Its `toOutbox(EventEnvelope)` method returns an empty list for excluded events and a single-entry list (with a compact integration DTO) for allowed ones.

You also need to enable the outbox and tell the app where the broker is, in `application.yml`. Add the `rabbitmq` block under the existing `spring:` key and the `outbox` block under the existing `streamrune:` key:

```yaml
spring:
  rabbitmq:
    host: ${RABBITMQ_HOST:localhost}
    port: 5672
    username: ${RABBITMQ_USERNAME:guest}
    password: ${RABBITMQ_PASSWORD:guest}

streamrune:
  outbox:
    enabled: true
    retention-max-age: 7d
    skipped-retention-max-age: 30d
```

The publisher's connection (Step 5b) reads the broker address from these `spring.rabbitmq.*` properties. The defaults reach a broker on `localhost`; under Docker Compose (Step 7) the backend sets `RABBITMQ_HOST` to the broker's service name.

`streamrune.outbox.enabled=true` activates the auto-configured `OutboxPoller`. Without it the poller bean is not created and delivered entries accumulate indefinitely. `retention-max-age: 7d` tells the auto-configured `OutboxRetentionSweeper` to delete `DELIVERED` entries older than seven days, preventing unbounded table growth. `skipped-retention-max-age: 30d` keeps the audit rows of operator skips (Chapter 14) for thirty days; `FAILED` entries are never pruned — they block their aggregate until someone resolves them.

### Step 5a-pre: Add Build Dependencies for RabbitMQ

Before wiring `RabbitMqConfig`, add the two RabbitMQ-related dependencies to `spring-app/build.gradle.kts`. The `streamrune-postgres` artifact (for `PostgresOutboxStore`) is already present; add the new lines if they are not there:

```kotlin
// spring-app/build.gradle.kts — inside the dependencies { } block
implementation("org.streamrune:streamrune-postgres:$sr")       // already present (outbox store + event store)
implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr") // adds RabbitMqOutboxPublisher
implementation("org.springframework.boot:spring-boot-starter-amqp") // adds Spring AMQP, @RabbitListener and the RabbitMQ Java client
```

Without `streamrune-rabbitmq-outbox` and `spring-boot-starter-amqp`, Step 5b's `RabbitMqConfig.java` imports will fail to resolve at compile time.

### Step 5b: Wire the RabbitMQ Publisher

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/RabbitMqConfig.java`. This declares the RabbitMQ topology and provides the `OutboxPublisher` bean that the auto-configured `OutboxPoller` uses to actually deliver messages:

```java
package org.streamrune.ecommerce.spring.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

@Configuration(proxyBeanMethods = false)
public class RabbitMqConfig {

  public static final String EXCHANGE   = "streamrune.events";
  public static final String QUEUE      = "streamrune.integration";
  public static final String ROUTING_KEY = "integration.event";

  /** Client-provided name of the publisher's connection, shown in the RabbitMQ management UI. */
  public static final String OUTBOX_CONNECTION_NAME = "streamrune-outbox-publisher";

  /** Integration event exchange. The outbox publisher publishes here. */
  @Bean
  public TopicExchange integrationExchange() {
    return new TopicExchange(EXCHANGE, true, false);
  }

  /** Queue that notifications-service (and any other consumer) subscribes to. */
  @Bean
  public Queue integrationQueue() {
    return new Queue(QUEUE, true);
  }

  /** Binding: routing key "integration.#" routes to streamrune.integration. */
  @Bean
  public Binding integrationBinding(TopicExchange integrationExchange, Queue integrationQueue) {
    return BindingBuilder.bind(integrationQueue).to(integrationExchange).with("integration.#");
  }

  /**
   * The outbox publisher's own AMQP connection: the plain RabbitMQ Java client with automatic
   * recovery on, not Spring AMQP's ConnectionFactory (see the text below the listing). Host,
   * port, credentials and virtual host come from the spring.rabbitmq.* properties. Spring closes
   * the connection on shutdown, after the outbox poller that uses it has stopped. Gated on
   * streamrune.outbox.enabled=true so no connection is opened when no broker is available.
   */
  @Bean(destroyMethod = "close")
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public Connection outboxRabbitConnection(RabbitProperties rabbit)
      throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbit.determineHost());
    factory.setPort(rabbit.determinePort());
    factory.setUsername(rabbit.determineUsername());
    factory.setPassword(rabbit.determinePassword());
    String virtualHost = rabbit.determineVirtualHost();
    if (virtualHost != null) {
      factory.setVirtualHost(virtualHost);
    }
    // The client's default, written out because this is the whole point of a separate connection.
    factory.setAutomaticRecoveryEnabled(true);
    return factory.newConnection(OUTBOX_CONNECTION_NAME);
  }

  /**
   * OutboxPublisher backed by RabbitMQ, on a channel only it uses; build() puts the channel
   * into confirm mode. The confirm timeout (30 s) plus the publisher's default 30 s
   * publish-block budget give a 60 s in-flight horizon, comfortably below the 2-minute claim
   * lease (see `outboxStore` above), so a broker timeout causes the poller to retry rather than
   * lose the message and a lease reclaim can never race a still-in-flight publish of the same
   * entry.
   */
  @Bean
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public OutboxPublisher outboxPublisher(Connection outboxRabbitConnection) throws IOException {
    Channel channel = outboxRabbitConnection.createChannel();
    // Declared here as well as by the beans above: Spring AMQP declares those only when its own
    // connection is first opened, which can be after the first publish, and a publish to a
    // missing exchange makes the broker close this channel for good. The declarations are
    // idempotent, and the client's topology recovery repeats them after a reconnect.
    channel.exchangeDeclare(EXCHANGE, "topic", /* durable= */ true);
    channel.queueDeclare(
        QUEUE,
        /* durable= */ true,
        /* exclusive= */ false,
        /* autoDelete= */ false,
        /* arguments= */ null);
    channel.queueBind(QUEUE, EXCHANGE, "integration.#");
    return RabbitMqOutboxPublisher.builder()
        .channel(channel)
        .exchange(EXCHANGE)
        .routingKey(ROUTING_KEY)
        .confirmTimeout(Duration.ofSeconds(30))
        .build();
  }
}
```

The `TopicExchange` is `streamrune.events` (durable, not auto-delete). The queue is `streamrune.integration` (durable). The binding pattern `integration.#` routes any routing key prefixed with `integration.` (or exactly `integration`) to that queue — so the routing key `integration.event` used by the publisher lands there. The `notifications-service` subscribes to the same queue. Spring AMQP declares the three topology beans, but lazily: only when its own connection is first opened. Nothing in this app opens that connection before the outbox's first publish, and a publish to an exchange that does not exist makes the broker close the publisher's channel for good. So `outboxPublisher` declares the same exchange, queue and binding on its own channel before it builds the publisher — with the same arguments as the beans, so the declarations agree — exactly as the Quarkus and Micronaut apps do. `RabbitMqConfigTopologyIT` runs the two bean methods against a fresh broker and expects the first publish to be confirmed and waiting in the queue.

**Why the publisher gets its own connection.** It is tempting to write `connectionFactory.createConnection().createChannel(false)` with the Spring AMQP `ConnectionFactory` that `spring-boot-starter-amqp` already provides. Don't. Spring AMQP switches the client's automatic recovery off and hands out channels behind a caching proxy. When the broker restarts or the network drops, the proxy quietly opens a fresh channel on the next call — without confirm mode and without the confirm and return listeners `RabbitMqOutboxPublisher.build()` registered on the old one. Publishes on that channel still reach the queue, but the broker never confirms them: every entry times out, stays claimed for the 2-minute lease and is published again, over and over, and no entry is ever marked `DELIVERED` until the application restarts.

The plain RabbitMQ Java client with automatic recovery (its default, set explicitly above) reconnects by itself every 5 seconds and reopens the *same* channel object with confirm mode and both listeners restored, so the publisher keeps working. While the broker is down a publish fails before anything is handed off, and the relay tries the entry again on its next poll. A publish already sent when the connection drops gets no confirm: it waits out the confirm timeout, stays claimed until the lease expires, and is then published again — the consumer may see it twice, which is why the `notifications-service` deduplicates by entry id. The Quarkus and Micronaut apps open their publisher channel the same way (`RabbitMqProducer`, `RabbitMqFactory`). `OutboxBrokerRestartIT` in `spring-app` proves it: it stops and restarts the broker application, writes an outbox entry while the broker is down, and expects that entry to reach `DELIVERED` — which the relay records only after the broker confirms the publish.

### Step 6: Wire `InventoryCommandController`

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/InventoryCommandController.java`. This is the REST entry point for the only inventory command that operators trigger directly — receiving a new shipment. Stock reservations and releases are driven by the saga, not by direct HTTP calls.

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@RestController
@RequestMapping("/api/inventory")
public class InventoryCommandController {
  private final VirtualThreadCommandBus commandBus;

  public InventoryCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping("/{productId}/receive")
  public ResponseEntity<Void> receiveShipment(
      @PathVariable String productId, @Valid @RequestBody ReceiveShipmentRequest req) {
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, req.quantity()));
    return ResponseEntity.ok().build();
  }

  public record ReceiveShipmentRequest(@Positive int quantity) {}
}
```

### Step 7: Create the `notifications-service` Module and Wire docker-compose

The `notifications-service` is a separate, headless Spring Boot application. It subscribes to the `streamrune.integration` queue via `@RabbitListener` and exposes `GET /received` (port 8090) that returns the last 200 received integration events in memory. It runs independently of the e-shop — no access to the e-shop's database, no awareness of the event store. That decoupling is the whole point of the outbox pattern.

**7a. Register the module in `settings.gradle.kts`.** Add it to the `include(...)` call alongside the other modules:

```kotlin
include(
    "domain",
    "commands",
    "queries",
    "projections",
    "spring-app",
    "notifications-service"   // <-- add this line
)
```

**7b. Create `notifications-service/build.gradle.kts`:**

```kotlin
plugins {
    java
    alias(libs.plugins.spring.dependency.management)
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-amqp")

    testImplementation(libs.spring.boot.starter.test)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
}
```

**7c. Create the four Java source files** under `notifications-service/src/main/java/org/streamrune/ecommerce/notifications/`:

`NotificationsApplication.java`:

```java
package org.streamrune.ecommerce.notifications;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NotificationsApplication {
  public static void main(String[] args) {
    SpringApplication.run(NotificationsApplication.class, args);
  }
}
```

`RabbitConfig.java` — declares the same exchange/queue/binding as the e-shop so the consumer connects to the right topology:

```java
package org.streamrune.ecommerce.notifications;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

  @Bean
  public TopicExchange integrationExchange() {
    return new TopicExchange("streamrune.events", true, false);
  }

  @Bean
  public Queue integrationQueue(
      @Value("${app.integration.queue:streamrune.integration}") String queueName) {
    return new Queue(queueName, true);
  }

  @Bean
  public Binding integrationBinding(Queue integrationQueue, TopicExchange integrationExchange) {
    return BindingBuilder.bind(integrationQueue).to(integrationExchange).with("integration.#");
  }
}
```

`IntegrationEventListener.java` — listens to the queue, deduplicates by `entryId`, and keeps the last 200 messages:

```java
package org.streamrune.ecommerce.notifications;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class IntegrationEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationEventListener.class);
  private static final int CAP = 200;

  public record Received(String type, String body, String entryId, Instant receivedAt) {}

  private final Deque<Received> received = new ArrayDeque<>();
  private final Set<String> seenEntryIds = new HashSet<>();

  @RabbitListener(queues = "${app.integration.queue:streamrune.integration}")
  public void onMessage(Message msg) {
    var props = msg.getMessageProperties();

    // Publisher sets AMQP standard `type` property for the payload type.
    String type = props.getType();
    if (type == null || type.isBlank()) {
      type = "unknown";
    }

    // Publisher sets AMQP standard `messageId` property to entry.id().value()
    // and also mirrors it in header X-Outbox-Entry-Id. Prefer messageId.
    String entryId = props.getMessageId();
    if (entryId == null || entryId.isBlank()) {
      Object headerVal = props.getHeaders().get("X-Outbox-Entry-Id");
      entryId = headerVal != null ? headerVal.toString() : "unknown";
    }

    String body = new String(msg.getBody(), java.nio.charset.StandardCharsets.UTF_8);

    synchronized (this) {
      if (seenEntryIds.contains(entryId)) {
        LOG.debug("Duplicate integration event suppressed entryId={}", entryId);
        return;
      }
      seenEntryIds.add(entryId);
      received.addFirst(new Received(type, body, entryId, Instant.now()));
      while (received.size() > CAP) {
        Received oldest = received.removeLast();
        seenEntryIds.remove(oldest.entryId());
      }
    }

    LOG.info("Received integration event type={} entryId={} body={}", type, entryId, body);
  }

  public synchronized List<Received> recent() {
    return Collections.unmodifiableList(new ArrayList<>(received));
  }
}
```

`ReceivedController.java`:

```java
package org.streamrune.ecommerce.notifications;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReceivedController {

  private final IntegrationEventListener listener;

  public ReceivedController(IntegrationEventListener listener) {
    this.listener = listener;
  }

  @GetMapping("/received")
  public List<IntegrationEventListener.Received> received() {
    return listener.recent();
  }
}
```

**7d. Create `notifications-service/src/main/resources/application.yml`:**

```yaml
spring:
  application:
    name: notifications-service
  rabbitmq:
    host: ${RABBITMQ_HOST:localhost}
    port: ${RABBITMQ_PORT:5672}
    username: ${RABBITMQ_USERNAME:guest}
    password: ${RABBITMQ_PASSWORD:guest}

server:
  port: 8090

app:
  integration:
    queue: streamrune.integration
```

**7e. Create `notifications-service/Dockerfile`.** The Dockerfile copies a pre-built JAR, so build the JAR locally before running `docker compose build`:

```dockerfile
# Pre-built JAR approach: run ./gradlew :notifications-service:bootJar first,
# then docker compose build copies the result.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY notifications-service/build/libs/notifications-service-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
```

**7f. Add two new services to `docker-compose.yml`:**

```yaml
rabbitmq:
  image: rabbitmq:3-management
  ports:
    - "5672:5672"
    - "15672:15672"
  healthcheck:
    test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
    interval: 5s
    timeout: 10s
    retries: 10

notifications-service:
  build:
    context: .
    dockerfile: notifications-service/Dockerfile
  ports:
    - "8090:8090"
  environment:
    RABBITMQ_HOST: rabbitmq
    RABBITMQ_USERNAME: guest
    RABBITMQ_PASSWORD: guest
  depends_on:
    rabbitmq:
      condition: service_healthy
```

Port 5672 is the AMQP port; 15672 exposes the management UI at `http://localhost:15672` (guest/guest). Update the `backend` service too: add `RABBITMQ_HOST: rabbitmq`, `RABBITMQ_USERNAME: guest` and `RABBITMQ_PASSWORD: guest` to its `environment`, and `rabbitmq: condition: service_healthy` to its `depends_on`. Without `RABBITMQ_HOST` the backend's outbox publisher looks for the broker on `localhost` inside its own container and cannot connect.

Before running `docker compose up`, build the notifications JAR:

```bash
./gradlew :notifications-service:bootJar
docker compose up
```

The `notifications-service` deduplicates messages by `entryId` (the AMQP `messageId` property, set by `RabbitMqOutboxPublisher` to `entry.id().value()`). If the broker delivers a message twice — because the poller restarted after publishing but before marking it `DELIVERED` — the service discards the duplicate silently.

### Step 8: Verify End-to-End Delivery

Start the full stack. Build the notifications-service JAR first (the Dockerfile copies a pre-built jar — see Step 7e):

```bash
./gradlew :notifications-service:bootJar
docker compose up
```

Wait for all services to be healthy. `SeedDataRunner` issues `ReceiveShipment` at startup for the seed products, so inventory events are already written by the time you reach the next step.

**8a. Place and confirm an order.** Confirming an order emits `OrderConfirmed`, which `EcommerceIntegrationEventMapper` includes in the outbox. Place and confirm an order:

```bash
# Place an order (adapt IDs to your seed data)
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"o-test-1","customerId":"c-1","lines":[{"productId":"prod-widget","quantity":1,"unitPrice":29.99}]}'

# The saga automatically progresses through payment and inventory;
# once PaymentCaptured fires, OrderConfirmed follows automatically.
```

**8b. Check the notifications service.** Within a couple of seconds the poller claims the outbox entry and publishes it to RabbitMQ. The `notifications-service` receives it and stores it in memory:

```bash
curl http://localhost:8090/received
```

You should see a JSON array containing an entry whose `type` is `"OrderConfirmed"` and whose `body` contains the `orderId`. The response is newest-first.

**8c. Confirm customer events are NOT forwarded.** Register a new customer and verify no notification arrives:

```bash
curl -s -X POST http://localhost:8080/api/customers \
  -H "Content-Type: application/json" \
  -d '{"customerId":"c-pii-test","name":"Eve","email":"eve@example.com"}'

# Wait a moment, then check — the count should not increase
curl http://localhost:8090/received | python3 -m json.tool | grep -c '"type"'
```

The count does not change. `CustomerRegistered` is excluded by `EcommerceIntegrationEventMapper` (the `default -> List.of()` branch), so no outbox entry is written and no message is published. Customer PII never leaves the e-shop's database.

**8d. Optional — RabbitMQ management UI.** Visit `http://localhost:15672` (guest/guest). The `streamrune.events` exchange and `streamrune.integration` queue are visible under Exchanges and Queues. The queue's message rate chart shows the delivery spike when orders are confirmed. Under Connections, the one named `streamrune-outbox-publisher` is the outbox publisher's own connection from Step 5b.

### When delivery fails: replay or skip

An entry that the broker rejects on every attempt (an unroutable message, a payload the consumer's schema refuses) exhausts the retry ladder — `streamrune.outbox.retry-max-attempts`, 10 by default; transport outages never count — and becomes `FAILED`. On this strict channel that entry is still the **head** of its stream: the order's later events stay `PENDING` and are not delivered, the relay logs `stream BLOCKED`, and two gauges show it — `streamrune.outbox.blocked_aggregates` (how many) and `streamrune.outbox.blockage_age_seconds` (for how long; alert on this one).

There are exactly two ways out, both through `OutboxFailedReplayer`:

- **Replay** — `replayer.replay(entryId)` puts the same row back to `PENDING` at its original position, so it is delivered *before* the entries that were waiting behind it. Use it once the cause is fixed.
- **Skip** — `replayer.skip(entryId, operator, reason)` marks the row `SKIPPED` with who, when and why, and the aggregate's later entries are claimable on the next poll. **A skip is not a delivery:** the notifications service will never see that event and will receive the later ones as if it had. If a consumer's state depends on the skipped change, emit a corrective domain event through the normal command path.

Nothing else releases a blocked stream: `OutboxStore.delete` refuses a `FAILED` row, and retention never prunes one. Chapter 14 exposes both operations on the admin API.

---

## What We Learned

**`OutboxStore`** is the core interface. `PostgresOutboxStore` implements it using the `outbox_events` table. The auto-configured `postgresEventStoreFactory` discovers the `PostgresOutboxStore` and `OutboxEventMapper` beans, and arranges for every event append to call `mapper.toOutbox(envelope)`. If the mapper returns entries, they are inserted in the same JDBC transaction as the event append — so they commit (or roll back) together.

**`OutboxEventMapper`** is the PII boundary. `EcommerceIntegrationEventMapper.toOutbox(EventEnvelope)` returns an empty list for excluded event types (`CustomerEvent.*`, `InventoryEvent.*`, `OrderPlaced`, `PaymentInitiated`) and a list with one compact integration DTO for allowed types (`OrderConfirmed`, `OrderShipped`, `OrderDelivered`, `OrderCancelled`, `PaymentCaptured`, `PaymentRefunded`, `PaymentFailed`, and the four `ProductEvent` variants). The DTO records carry only non-PII business fields — there is no `customerId`, no name, no email, no address.

**`OutboxEntry`** is the record the poller reads. It carries an `id` (`OutboxEntryId` — a record wrapping a `String` value, used as the idempotency key), `payloadType` (e.g. `"OrderConfirmed"`), a JSON `payload` containing the integration DTO, a `streamId` ordering key — the event's own stream (mandatory on the demo's strict channel), and an `OutboxStatus`.

**`OutboxStatus`** has five values: `PENDING` (awaiting delivery), `IN_PROGRESS` (claimed by the poller — the competing-consumer lease), `DELIVERED` (broker confirmed), `FAILED` (all retries exhausted; terminal until an operator replays or skips it, never pruned), and `SKIPPED` (an operator's audited decision that a `FAILED` entry will never be delivered).

**`OutboxPoller`** runs on its own virtual thread, auto-configured when `streamrune.outbox.enabled=true`. It wakes on a configured interval, atomically claims a batch via `FOR UPDATE SKIP LOCKED`, and invokes the `OutboxPublisher` for each entry. On success it marks the entry `DELIVERED`. On failure it follows a retry policy (exponential backoff) and only marks `FAILED` once `maxAttempts` is exhausted.

**Ordering mode.** The outbox channel's ordering is a typed choice (`OutboxOrderingMode`), strict by default; strict mode makes `streamId` mandatory.

**Replay or skip.** A `FAILED` entry blocks its aggregate until replayed (in place) or skipped (audited); a skip is not a delivery.

**`RabbitMqOutboxPublisher`** is the `OutboxPublisher` implementation that sends AMQP messages. It puts its channel into confirm mode at startup; the confirm timeout (30 s) plus its default 30 s publish-block budget give a 60 s in-flight horizon, well below the claim lease (2 min), so a broker timeout causes the poller to retry without losing the entry — and `OutboxPoller` refuses to boot in the first place if the lease does not strictly exceed that horizon. The channel must stay in confirm mode for the publisher's whole life, so it comes from a dedicated, automatically recovering RabbitMQ Java client connection, never from Spring AMQP's caching `ConnectionFactory`, whose recreated channels lose confirm mode after a reconnect.

**notifications-service** is a separate Spring Boot application. It has no access to the e-shop's database — it only sees what arrives on the `streamrune.integration` queue. This is the key decoupling: the e-shop publishes integration events; downstream consumers are fully independent services. The notifications service deduplicates by `entryId` to handle at-least-once delivery.

**Transactional outbox pattern** trades direct publish for durability. The invariant: if an event was committed, its outbox entry exists, and the poller eventually delivers it — unless an operator deliberately skips it, an audited decision that is not a delivery. This is the correct foundation for integrating with any system you do not own.

**At-least-once delivery** is a consequence. Consumers must handle duplicates — the `notifications-service` does so by tracking seen `entryId` values.

---

## Next Up

What happens when a projection fails to process an event? Next: dead letter queue.
