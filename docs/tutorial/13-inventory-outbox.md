# Chapter 13: Inventory Aggregate & Outbox

> **What you'll learn:** How to model an inventory aggregate with three distinct stock pools — available, reserved, and committed — and how StreamRune's transactional outbox pattern reliably forwards a curated, PII-safe set of integration events to RabbitMQ. You will build `InventoryDecider` with full TDD coverage and wire the four outbox beans (`PostgresOutboxStore`, `EcommerceIntegrationEventMapper`, `RabbitMqConfig` publisher, and `streamrune.outbox.enabled=true`) that make end-to-end delivery real. A second Spring Boot application — the `notifications-service` — subscribes to the queue and exposes `GET /received`, for an `ADMIN`, so you can verify cross-service delivery without touching the e-shop internals.

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

**What the demo wires end-to-end:** The auto-configured `postgresEventStoreFactory` picks up the `PostgresOutboxStore` and `OutboxEventMapper` beans you expose in this chapter and arranges for every appended event to be screened by the mapper. Only the allowlisted, PII-safe events produce `OutboxEntry` rows — written atomically with the event in the same JDBC transaction. The auto-configured `OutboxPoller` reads those entries and hands them to the `OutboxPublisher` (provided by `RabbitMqConfig`), which publishes to RabbitMQ. A separate `notifications-service` module consumes the queue and exposes `GET /received` (port 8090) to an `ADMIN`. The outbox channel is **`STRICT_PER_AGGREGATE`** (the framework default, stated explicitly in the demo): entries of one order, payment or product are delivered in order, and a terminal `FAILED` entry blocks its aggregate until an operator replays or skips it (Chapter 14 adds the admin endpoints for that).

**PII boundary:** Customer events — `CustomerRegistered`, `ProfileUpdated`, `DataExportRequested`, `CustomerForgotten` — never cross the service boundary. `OrderPlaced` and `PaymentInitiated` are also excluded because they carry `customerId`. Only the order lifecycle outcomes, payment results, and product catalog changes are published. This is enforced by `EcommerceIntegrationEventMapper`'s allowlist, not by convention.

StreamRune provides `OutboxStore`, `PostgresOutboxStore`, `OutboxPoller`, `OutboxEntry`, `OutboxEventMapper`, and `OutboxStatus` as first-class building blocks. You focus on the domain model and the integration contract; StreamRune handles the transactional plumbing.

---

## Domain Model

Inventory tracks stock for a single product across three pools:

- **available** — units that can be sold right now.
- **reserved** — units held for an order between its payment and its shipment, per order: each order has its own reservation at the product.
- **committed** — units confirmed for fulfillment; they leave the warehouse when the order ships.

The lifecycle of a unit flows: available → reserved (on `ReserveStock`) → committed (on `ConfirmReservation`) → gone. Cancellations reverse the first step: reserved → available again (on `ReleaseStock`). New stock arrives via `ReceiveShipment`. The lifecycle is per order: a release or a confirmation acts on the reservation of the order it names, at the product it names, and stores nothing when that order holds none. So a release that runs twice releases once, a release after the shipment committed the stock changes nothing, and two orders at one product are released and confirmed apart. The fulfillment saga of Chapter 11 sends `ReserveStock` for every line of an order; the order cancellation process manager sends `ReleaseStock` for every line of a cancelled order, and the order shipment process manager sends `ConfirmReservation` for every line of a shipped one (Chapter 11, Step 5c).

### Commands

Create `domain/src/main/java/org/streamrune/ecommerce/domain/inventory/InventoryCommand.java`:

```java
package org.streamrune.ecommerce.domain.inventory;

import org.streamrune.core.Command;

public sealed interface InventoryCommand extends Command {
  record ReserveStock(String productId, String orderId, int quantity) implements InventoryCommand {}

  record ReleaseStock(String productId, String orderId) implements InventoryCommand {}

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

`ReleaseStock` and `ConfirmReservation` name the order and no quantity: the quantity is the one the order reserved, which the aggregate knows. Each event records the `availableAfter` pool size at the time of the transition. This makes the event self-describing: a consumer can reconstruct the current availability without replaying the entire stream.

### State

Create `domain/src/main/java/org/streamrune/ecommerce/domain/inventory/InventoryState.java`:

```java
package org.streamrune.ecommerce.domain.inventory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.OptionalInt;
import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record InventoryState(
    String productId, int available, int reserved, int committed, Map<String, Integer> reservations)
    implements AggregateState {
  /**
   * The aggregate type the inventory decider is registered under: streams are
   * inventory:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("inventory");

  /**
   * {@code reservations} holds the open reservation of each order at this product, by order id: an
   * entry is added by {@code StockReserved} and removed by {@code StockReleased} or {@code
   * ReservationConfirmed}. The state is snapshotted with the map.
   */
  @JsonCreator
  public InventoryState(
      @JsonProperty("productId") String productId,
      @JsonProperty("available") int available,
      @JsonProperty("reserved") int reserved,
      @JsonProperty("committed") int committed,
      @JsonProperty("reservations") Map<String, Integer> reservations) {
    this.productId = productId;
    this.available = available;
    this.reserved = reserved;
    this.committed = committed;
    this.reservations = reservations == null ? Map.of() : Map.copyOf(reservations);
  }

  public InventoryState() {
    this(null, 0, 0, 0, Map.of());
  }

  /** The quantity the order holds reserved at this product, or none. */
  public OptionalInt reservationOf(String orderId) {
    Integer quantity = reservations.get(orderId);
    return quantity == null ? OptionalInt.empty() : OptionalInt.of(quantity);
  }
}
```

The zero-argument constructor satisfies `AggregateState`; StreamRune calls it when no prior events exist for the stream.

`reservations` holds the open reservation of each order at this product, by order id. `StockReserved` adds an entry; `StockReleased` and `ReservationConfirmed` remove it, so the map holds only the orders in flight for the product. The bus snapshots the state with the map (chapter 8): the canonical constructor is the one Jackson uses (`@JsonCreator`, one `@JsonProperty` per component), and it reads a state written without `reservations` as an empty map. A snapshot that lost the map would make every release and confirmation after it store nothing, with no error, and strand the stock. `reservationOf(orderId)` is the quantity an order holds, or none.

---

## Step by Step

### Step 1: Write the Tests First

Create `commands/src/test/java/org/streamrune/ecommerce/commands/inventory/InventoryDeciderTest.java`. `DeciderFixture` from `streamrune-test` removes all boilerplate: `given` supplies prior events, `when` issues a command, and `expectEvents` / `expectState` / `expectException` assert the outcome.

```java
package org.streamrune.ecommerce.commands.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;
import org.streamrune.test.DeciderFixture;

class InventoryDeciderTest {

  private final DeciderFixture<InventoryCommand, InventoryState, InventoryEvent> fixture =
      DeciderFixture.of(new InventoryDecider());

  private InventoryEvent.ShipmentReceived received() {
    return new InventoryEvent.ShipmentReceived("p-1", 10, 10);
  }

  @Test
  void receiveShipment() {
    fixture
        .given()
        .when(new InventoryCommand.ReceiveShipment("p-1", 100))
        .expectEvents(new InventoryEvent.ShipmentReceived("p-1", 100, 100))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(100);
              assertThat(s.reserved()).isEqualTo(0);
            });
  }

  @Test
  void reserveStock_recordsTheOrdersReservation() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 3))
        .expectEvents(new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(7);
              assertThat(s.reserved()).isEqualTo(3);
              assertThat(s.reservationOf("o-1")).isEqualTo(OptionalInt.of(3));
            });
  }

  @Test
  void reserveStock_twiceForOneOrder_storesNothingTheSecondTime() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 3))
        .expectNoEvents();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void reserveStock_aNonPositiveQuantity_isRefused(int quantity) {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", quantity))
        .expectException(DomainException.class);
  }

  @Test
  void reserveStock_insufficient_throws() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 200))
        .expectException(DomainException.class);
  }

  @Test
  void releaseStock_releasesWhatTheOrderHolds() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectEvents(new InventoryEvent.StockReleased("p-1", "o-1", 3, 10))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(10);
              assertThat(s.reserved()).isEqualTo(0);
              assertThat(s.reservations()).isEmpty();
            });
  }

  @Test
  void releaseStock_withoutAReservation_storesNothing() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void releaseStock_afterTheReservationWasConfirmed_storesNothing() {
    fixture
        .given(
            received(),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 7),
            new InventoryEvent.ReservationConfirmed("p-1", "o-1", 3))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void releaseStock_twice_releasesOnce() {
    fixture
        .given(
            received(),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 7),
            new InventoryEvent.StockReleased("p-1", "o-1", 3, 10))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void confirmReservation_afterTheReservationWasReleased_storesNothing() {
    fixture
        .given(
            received(),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 7),
            new InventoryEvent.StockReleased("p-1", "o-1", 3, 10))
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void reserveStock_afterTheOrdersReservationWasReleased_reservesAgain() {
    fixture
        .given(
            received(),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 7),
            new InventoryEvent.StockReleased("p-1", "o-1", 3, 10))
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 3))
        .expectEvents(new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(3);
              assertThat(s.reservationOf("o-1")).isEqualTo(OptionalInt.of(3));
            });
  }

  @Test
  void confirmReservation_commitsTheOrdersQuantity() {
    fixture
        .given(
            new InventoryEvent.ShipmentReceived("p-1", 20, 20),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 17),
            new InventoryEvent.StockReserved("p-1", "o-2", 5, 12))
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectEvents(new InventoryEvent.ReservationConfirmed("p-1", "o-1", 3))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(5);
              assertThat(s.committed()).isEqualTo(3);
            });
  }

  @Test
  void confirmReservation_withoutAReservation_storesNothing() {
    fixture
        .given(received())
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void twoOrdersAtOneProduct_areReleasedAndConfirmedApart() {
    fixture
        .given(
            new InventoryEvent.ShipmentReceived("p-1", 20, 20),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 17),
            new InventoryEvent.StockReserved("p-1", "o-2", 5, 12))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-2"))
        .expectEvents(new InventoryEvent.StockReleased("p-1", "o-2", 5, 17))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(3);
              assertThat(s.reservationOf("o-1")).isEqualTo(OptionalInt.of(3));
              assertThat(s.reservationOf("o-2")).isEqualTo(OptionalInt.empty());
            });
  }
}
```

Run the tests. They will all fail because `InventoryDecider` does not exist yet. That is expected.

### Step 2: Implement `InventoryDecider`

Create `commands/src/main/java/org/streamrune/ecommerce/commands/inventory/InventoryDecider.java`:

```java
package org.streamrune.ecommerce.commands.inventory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;

public class InventoryDecider implements Decider<InventoryCommand, InventoryState, InventoryEvent> {

  @Override
  public InventoryState initialState() {
    return new InventoryState();
  }

  /**
   * Stock is held per order: a second reservation of one order at one product stores nothing, and a
   * release or a confirmation acts on this order's reservation alone — nothing when it holds none.
   */
  @Override
  public List<InventoryEvent> decide(InventoryCommand cmd, InventoryState state) {
    return switch (cmd) {
      case InventoryCommand.ReserveStock c -> {
        if (c.quantity() <= 0)
          throw new DomainException("Reservation quantity must be positive, got " + c.quantity());
        if (state.reservationOf(c.orderId()).isPresent()) yield List.of();
        if (state.available() < c.quantity())
          throw new DomainException(
              "Insufficient stock for "
                  + c.productId()
                  + ": available="
                  + state.available()
                  + ", requested="
                  + c.quantity());
        yield List.of(
            new InventoryEvent.StockReserved(
                c.productId(), c.orderId(), c.quantity(), state.available() - c.quantity()));
      }

      case InventoryCommand.ReleaseStock c -> {
        OptionalInt held = state.reservationOf(c.orderId());
        if (held.isEmpty()) yield List.of();
        yield List.of(
            new InventoryEvent.StockReleased(
                c.productId(), c.orderId(), held.getAsInt(), state.available() + held.getAsInt()));
      }

      case InventoryCommand.ConfirmReservation c -> {
        OptionalInt held = state.reservationOf(c.orderId());
        if (held.isEmpty()) yield List.of();
        yield List.of(
            new InventoryEvent.ReservationConfirmed(c.productId(), c.orderId(), held.getAsInt()));
      }

      case InventoryCommand.ReceiveShipment c ->
          List.of(
              new InventoryEvent.ShipmentReceived(
                  c.productId(), c.quantity(), state.available() + c.quantity()));
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
              state.committed(),
              with(state.reservations(), e.orderId(), e.quantity()));
      case InventoryEvent.StockReleased e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() - e.quantity(),
              state.committed(),
              without(state.reservations(), e.orderId()));
      case InventoryEvent.ReservationConfirmed e ->
          new InventoryState(
              state.productId(),
              state.available(),
              state.reserved() - e.quantity(),
              state.committed() + e.quantity(),
              without(state.reservations(), e.orderId()));
      case InventoryEvent.ShipmentReceived e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved(),
              state.committed(),
              state.reservations());
    };
  }

  private static Map<String, Integer> with(
      Map<String, Integer> reservations, String orderId, int quantity) {
    var next = new HashMap<>(reservations);
    next.put(orderId, quantity);
    return next;
  }

  private static Map<String, Integer> without(Map<String, Integer> reservations, String orderId) {
    var next = new HashMap<>(reservations);
    next.remove(orderId);
    return next;
  }
}
```

A few things worth noting. `ReserveStock` is the only command that can fail: it refuses a quantity that is not positive and checks `state.available()` before emitting an event. A second reservation of one order at one product stores nothing, so a redelivered `ReserveStock` reserves once. `ReleaseStock` and `ConfirmReservation` act on `state.reservationOf(orderId)`, the quantity this order reserved at this product: they never take a quantity from the command and never read the pool-wide `reserved` counter, so one order cannot release or commit the units another order holds. Both store nothing when the order holds no reservation, which is what makes them safe to send whether or not the reservation step ran, and safe to send twice.

Run the tests. They should all pass.

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

Create `projections/src/main/java/org/streamrune/ecommerce/projections/InventoryProjection.java` (source follows the same pattern as `ProductProjection` and `OrderProjection` — copy the full implementation from the real demo at that path). Like the other projections, it extends `org.streamrune.core.projection.BaseProjection` (introduced in the projections chapter) and persists the `org.streamrune.ecommerce.queries.dto.InventoryView` DTO in the `queries` module, so make sure both already exist before you compile. The projection applies each event's own quantity: `StockReserved` and `StockReleased` carry the order's quantity, and `ReservationConfirmed` carries the quantity that order reserved, not the product's whole reserved pool, so the counters stay right while several orders hold stock at one product.

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

The publisher's connection (Step 5b) reads the broker address from these `spring.rabbitmq.*` properties, and opens on the first publish, not at startup. The defaults reach a broker on `localhost`; under Docker Compose (Step 7) the backend sets `RABBITMQ_HOST` to the broker's service name.

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

The publisher class of Step 5b lives in the shared `commands` module, next to `EcommerceIntegrationEventMapper`, because the Quarkus and Micronaut apps wire the same class. Add its three dependencies to `commands/build.gradle.kts`:

```kotlin
// commands/build.gradle.kts — inside the dependencies { } block
implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr")
api(libs.rabbitmq.client)
implementation(libs.slf4j.api)
```

### Step 5b: Wire the RabbitMQ Publisher

The outbox exists so that a command does not depend on the broker: its integration events are written in the command's own transaction, and the relay delivers them later. The application's startup should not depend on the broker either. A `RabbitMqOutboxPublisher` publishes on a channel it is given when it is built — and a channel needs an open connection — so the demo builds it on the first publish, not at startup. Create `commands/src/main/java/org/streamrune/ecommerce/commands/integration/LazyRabbitMqOutboxPublisher.java`:

```java
package org.streamrune.ecommerce.commands.integration;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * The outbox's RabbitMQ publisher, connected on its first publish instead of at startup, so the
 * application starts and serves commands while the broker is down. The three apps wire it as their
 * {@link OutboxPublisher}.
 *
 * <p>A command's integration events are written to the outbox in the command's own transaction; the
 * relay ({@code OutboxPoller}) hands them to this publisher later. Nothing about a command depends
 * on the broker, so neither does the application's startup: the constructor opens nothing. The
 * first {@link #publishBatch} opens the connection, opens the publisher's channel, declares the
 * topology on it and builds a {@link RabbitMqOutboxPublisher} on that channel, which publishes this
 * batch and every later one.
 *
 * <p>While the broker cannot be reached, every entry of a batch fails with a {@link
 * BrokerUnreachableException} and nothing is handed to a broker. {@link #classifyFailure} reports
 * it as {@link FailureKind#TRANSPORT}: the relay keeps the entries {@code PENDING} with the error
 * in {@code last_error}, counts no attempt against the retry ladder and tries again on a backoff
 * that grows towards its 60-second cap. The relay logs one {@code WARN} per poll cycle that names
 * the error, and {@code streamrune.outbox.pending} holds the backlog. Once the broker answers, the
 * next attempt connects and the entries are delivered in order, each stream's entries one after the
 * other. This publisher logs one {@code INFO} line when it connects.
 *
 * <p>Once connected, the connection stays open for the life of the application. It is an
 * amqp-client connection with automatic recovery (the {@link ConnectionFactory}'s default): after a
 * broker restart or a network failure the client reconnects by itself and reopens the same channel
 * in confirm mode, with the confirm and return listeners the publisher registered, so a later
 * outage needs nothing from this class. Automatic recovery covers a connection that was established
 * once; a first connection that fails is this class's to retry, on the relay's next attempt.
 *
 * <p>The in-flight horizon is the one the {@link RabbitMqOutboxPublisher} reports once built — its
 * {@link RabbitMqOutboxPublisher#DEFAULT_PUBLISH_BLOCK_BUDGET default publish-block budget} plus
 * the confirm timeout — and is known before the first connection, because the relay reads it when
 * it is built, to check the claim lease against it.
 *
 * <p>The relay calls a publisher from its one poll thread; {@link #close} may come from another
 * thread, at shutdown.
 */
public final class LazyRabbitMqOutboxPublisher implements OutboxPublisher, AutoCloseable {

  /** Declares the exchanges, queues and bindings the publisher's channel publishes to. */
  @FunctionalInterface
  public interface Topology {

    /**
     * Declares the topology on {@code channel}, before the first publish. The declarations must be
     * idempotent: the client's topology recovery repeats them after every reconnect.
     *
     * @param channel the publisher's channel
     * @throws IOException if a declaration fails
     */
    void declare(Channel channel) throws IOException;
  }

  /**
   * The broker could not be reached, so nothing of the batch was published: the connection, the
   * publisher's channel or a topology declaration failed, or the publisher is closed. The cause is
   * the client's own exception, e.g. {@code java.net.ConnectException: Connection refused}.
   */
  public static final class BrokerUnreachableException extends IOException {

    BrokerUnreachableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(LazyRabbitMqOutboxPublisher.class);

  /** The outcome of a batch this publisher did not start: nothing reached a broker. */
  private static final BatchResult NOTHING_ATTEMPTED = new BatchResult(Set.of(), Map.of());

  private final ConnectionFactory connectionFactory;
  private final String connectionName;
  private final Topology topology;
  private final String exchange;
  private final String routingKey;
  private final Duration confirmTimeout;

  /** Held while a connection is opened, so that one poll thread at a time opens one. */
  private final ReentrantLock connecting = new ReentrantLock();

  private volatile Connection connection;
  private volatile RabbitMqOutboxPublisher delegate;
  private volatile boolean closed;

  /**
   * Creates the publisher; nothing is opened until the first publish.
   *
   * @param connectionFactory the factory of the publisher's own connection: host, port, credentials
   *     and virtual host set, automatic recovery on
   * @param connectionName the client-provided connection name the broker's management UI shows
   * @param topology declares, on the publisher's channel, what it publishes to
   * @param exchange the exchange every entry is published to
   * @param routingKey the routing key every entry is published with
   * @param confirmTimeout how long a publish waits for the broker's confirm
   */
  public LazyRabbitMqOutboxPublisher(
      ConnectionFactory connectionFactory,
      String connectionName,
      Topology topology,
      String exchange,
      String routingKey,
      Duration confirmTimeout) {
    this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.connectionName = Objects.requireNonNull(connectionName, "connectionName");
    this.topology = Objects.requireNonNull(topology, "topology");
    this.exchange = Objects.requireNonNull(exchange, "exchange");
    this.routingKey = Objects.requireNonNull(routingKey, "routingKey");
    this.confirmTimeout = Objects.requireNonNull(confirmTimeout, "confirmTimeout");
  }

  @Override
  public void publish(OutboxEntry entry) throws Exception {
    connected().publish(entry);
  }

  @Override
  public BatchResult publishBatch(List<OutboxEntry> entries) {
    return publishBatch(entries, null);
  }

  /**
   * Publishes the batch through the {@link RabbitMqOutboxPublisher}, connecting first when there is
   * no connection yet. A connection that cannot be opened fails every entry with the same {@link
   * BrokerUnreachableException}. When the thread is interrupted (the relay stops) or the deadline
   * has passed before a connection is opened, no entry is attempted and none is in either bucket,
   * as the {@link OutboxPublisher} contract asks of entries a batch never reached.
   */
  @Override
  public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
    if (entries.isEmpty()) {
      return NOTHING_ATTEMPTED;
    }
    RabbitMqOutboxPublisher publisher = delegate;
    if (publisher == null) {
      if (Thread.currentThread().isInterrupted()
          || (deadline != null && !Instant.now().isBefore(deadline))) {
        return NOTHING_ATTEMPTED;
      }
      try {
        publisher = connected();
      } catch (BrokerUnreachableException unreachable) {
        if (Thread.currentThread().isInterrupted()) {
          // The relay is stopping: an interrupted connect is no evidence about the broker.
          return NOTHING_ATTEMPTED;
        }
        Map<OutboxEntryId, Exception> failed = LinkedHashMap.newLinkedHashMap(entries.size());
        for (OutboxEntry entry : entries) {
          failed.put(entry.id(), unreachable);
        }
        return new BatchResult(Set.of(), failed);
      }
    }
    return publisher.publishBatch(entries, deadline);
  }

  /**
   * {@link FailureKind#TRANSPORT} for a {@link BrokerUnreachableException} (nothing was handed to a
   * broker); every other failure comes from the {@link RabbitMqOutboxPublisher} and is classified
   * by it.
   */
  @Override
  public FailureKind classifyFailure(Exception failure) {
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 50; depth++, cause = cause.getCause()) {
      if (cause instanceof BrokerUnreachableException) {
        return FailureKind.TRANSPORT;
      }
    }
    RabbitMqOutboxPublisher publisher = delegate;
    return publisher != null ? publisher.classifyFailure(failure) : FailureKind.TRANSPORT;
  }

  /**
   * The {@link RabbitMqOutboxPublisher}'s horizon: its default publish-block budget plus the
   * confirm timeout.
   */
  @Override
  public Duration inFlightHorizon() {
    return RabbitMqOutboxPublisher.DEFAULT_PUBLISH_BLOCK_BUDGET.plus(confirmTimeout);
  }

  /** Whether the publisher has connected to the broker. */
  public boolean isConnected() {
    return delegate != null;
  }

  /**
   * Closes the connection, if one was opened; afterwards every publish fails with a {@link
   * BrokerUnreachableException}. The application calls it at shutdown, after the relay stopped.
   */
  @Override
  public void close() {
    closed = true;
    Connection opened = connection;
    if (opened != null) {
      closeQuietly(opened);
    }
  }

  /** The publisher on an open connection, connecting first when there is none yet. */
  private RabbitMqOutboxPublisher connected() throws BrokerUnreachableException {
    RabbitMqOutboxPublisher publisher = delegate;
    if (publisher != null) {
      return publisher;
    }
    connecting.lock();
    try {
      if (delegate != null) {
        return delegate;
      }
      if (closed) {
        throw new BrokerUnreachableException("the outbox publisher is closed", null);
      }
      Connection opened = null;
      try {
        opened = connectionFactory.newConnection(connectionName);
        Channel channel = opened.createChannel();
        if (channel == null) {
          throw new IOException("the connection has no channel left for the outbox publisher");
        }
        topology.declare(channel);
        publisher =
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange(exchange)
                .routingKey(routingKey)
                .confirmTimeout(confirmTimeout)
                .build();
      } catch (IOException | TimeoutException | RuntimeException e) {
        if (opened != null) {
          closeQuietly(opened);
        }
        throw new BrokerUnreachableException(
            "RabbitMQ is not reachable at " + address() + ", nothing was published: " + describe(e),
            e);
      }
      // Published before closed is read, and close() sets closed before it reads the connection:
      // whichever of the two runs second sees the other's write, so a connection opened while the
      // publisher was being closed is closed here or there.
      connection = opened;
      if (closed) {
        closeQuietly(opened);
        throw new BrokerUnreachableException("the outbox publisher is closed", null);
      }
      delegate = publisher;
      LOG.info("Outbox publisher connected to RabbitMQ at {}", address());
      return publisher;
    } finally {
      connecting.unlock();
    }
  }

  /** The failure and, when it wraps one, the cause the client reported it for. */
  private static String describe(Exception failure) {
    Throwable root = failure;
    for (int depth = 0; root.getCause() != null && root.getCause() != root && depth < 50; depth++) {
      root = root.getCause();
    }
    return root == failure ? failure.toString() : failure + " (" + root + ")";
  }

  private String address() {
    return connectionFactory.getHost() + ":" + connectionFactory.getPort();
  }

  private static void closeQuietly(Connection opened) {
    try {
      opened.close();
    } catch (IOException | RuntimeException e) {
      LOG.debug("Closing the outbox publisher's RabbitMQ connection failed", e);
    }
  }
}
```

Until it has connected, the publisher fails every entry of a batch with its own `BrokerUnreachableException`, before anything reaches a broker, and `classifyFailure` reports that as `TRANSPORT`: the relay keeps the entries `PENDING`, records the error in `last_error`, counts no attempt against the retry ladder (`streamrune.outbox.retry-max-attempts`) and tries again on a backoff that grows from one second towards the relay's 60-second cap. Once the broker answers, the next attempt connects, declares the topology and delivers the entries. Every later failure comes from the `RabbitMqOutboxPublisher`, which classifies its own. The relay reads `inFlightHorizon()` when it is built, before any connection, to check the claim lease against it, so the class reports the horizon the `RabbitMqOutboxPublisher` would: its default 30-second publish-block budget plus the confirm timeout.

Then create `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/RabbitMqConfig.java`. This declares the RabbitMQ topology and provides the `OutboxPublisher` bean that the auto-configured `OutboxPoller` uses to actually deliver messages:

```java
package org.streamrune.ecommerce.spring.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * RabbitMQ topology and outbox publisher for the e-commerce demo.
 *
 * <p>Declares the {@code streamrune.events} topic exchange, the {@code streamrune.integration}
 * queue, and a binding with routing key {@code integration.#} so any routing key prefixed with
 * {@code integration.} (or exactly {@code integration}) lands in the queue. The notifications
 * service consumer subscribes to {@code streamrune.integration}. Spring AMQP declares these three
 * beans when its own connection is first opened; the outbox publisher declares the same topology on
 * its own channel ({@link #declareTopology}), because its first publish may come before that.
 *
 * <p>The {@link OutboxPublisher} bean is consumed by the auto-configured {@link
 * org.streamrune.runtime.OutboxPoller} once {@code streamrune.outbox.enabled=true} is set in
 * application.yml. It publishes on its own AMQP connection, not on Spring AMQP's (see {@link
 * #outboxPublisher}), and opens that connection on its first publish: the application starts and
 * serves commands while the broker is down.
 */
@Configuration(proxyBeanMethods = false)
public class RabbitMqConfig {

  /** Integration event exchange. The outbox publisher publishes here. */
  public static final String EXCHANGE = "streamrune.events";

  /** Queue that notifications-service (and any other consumer) subscribes to. */
  public static final String QUEUE = "streamrune.integration";

  /**
   * Routing key used when publishing outbox entries. The topic binding pattern {@code
   * integration.#} routes this key to the {@link #integrationQueue()}.
   */
  public static final String ROUTING_KEY = "integration.event";

  /** Client-provided name of the publisher's connection, shown in the RabbitMQ management UI. */
  public static final String OUTBOX_CONNECTION_NAME = "streamrune-outbox-publisher";

  @Bean
  public TopicExchange integrationExchange() {
    // durable=true, auto-delete=false
    return new TopicExchange(EXCHANGE, true, false);
  }

  @Bean
  public Queue integrationQueue() {
    // durable=true so messages survive broker restarts
    return new Queue(QUEUE, true);
  }

  @Bean
  public Binding integrationBinding(TopicExchange integrationExchange, Queue integrationQueue) {
    // "integration.#" matches "integration" and "integration.<anything>"
    return BindingBuilder.bind(integrationQueue).to(integrationExchange).with("integration.#");
  }

  /**
   * The outbox publisher: a {@link LazyRabbitMqOutboxPublisher} that opens its own AMQP connection,
   * with the plain RabbitMQ Java client and automatic recovery switched on, on its first publish,
   * and then publishes through a {@link RabbitMqOutboxPublisher} on a channel only it uses.
   *
   * <p>Nothing is opened at startup, so the application starts and serves commands while the broker
   * is down. A command's integration events wait in the outbox: each publish attempt fails with a
   * transport failure, which the relay retries on a growing backoff without counting it against the
   * entry, and the entries are delivered once the broker answers. The relay's {@code WARN} line and
   * {@code streamrune.outbox.pending} show the outage, and the publisher logs {@code Outbox
   * publisher connected to RabbitMQ at <host>:<port>} when it connects.
   *
   * <p>The publisher is deliberately not given a channel from Spring AMQP's {@code
   * ConnectionFactory}. Spring AMQP turns the client's automatic recovery off and, after the
   * connection drops, hands out a fresh channel behind the same proxy, without confirm mode and
   * without the publisher's listeners. Publishes on that channel still reach the queue, but no
   * confirm ever comes back: every entry times out, stays claimed for the lease and is published
   * again, over and over, until the application restarts.
   *
   * <p>With automatic recovery the client reconnects by itself after a broker restart or a network
   * failure (every 5 seconds by default) and reopens the same channel object with confirm mode and
   * the confirm and return listeners restored, so the publisher keeps working without a restart.
   * While the connection is down, a publish fails before anything is handed to the broker and the
   * relay tries the entry again later. A publish already sent when the connection drops gets no
   * confirm: it waits out the confirm timeout, stays claimed until the lease expires and is then
   * published again, so the consumer may receive it twice (it deduplicates by entry id).
   *
   * <p>Host, port, credentials and virtual host come from the same {@code spring.rabbitmq.*}
   * properties Spring AMQP uses. TLS settings under {@code spring.rabbitmq.ssl} are not applied
   * here; a TLS deployment configures the factory itself.
   *
   * <p>The 30-second confirm timeout, combined with the publisher's default 30-second {@code
   * publishBlockBudget}, gives an in-flight horizon (see {@link
   * RabbitMqOutboxPublisher#inFlightHorizon()}) of 60 seconds; {@link StreamRuneConfig#outboxStore}
   * sets the outbox claim lease to 2 minutes so it strictly exceeds that horizon plus the
   * framework's recommended margin. A broker timeout therefore makes the poller retry rather than
   * lose the message, and a lease reclaim can never race a still-in-flight publish of the same
   * entry.
   *
   * <p>Spring closes the publisher, and with it the connection, on shutdown, after the outbox
   * poller that uses it has stopped. Gated on {@code streamrune.outbox.enabled=true}, which the
   * relay needs.
   */
  @Bean(destroyMethod = "close")
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public LazyRabbitMqOutboxPublisher outboxPublisher(RabbitProperties rabbit) {
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
    return new LazyRabbitMqOutboxPublisher(
        factory,
        OUTBOX_CONNECTION_NAME,
        RabbitMqConfig::declareTopology,
        EXCHANGE,
        ROUTING_KEY,
        Duration.ofSeconds(30));
  }

  /**
   * Declares the exchange, the queue and the binding on the publisher's channel, with the same
   * arguments as the beans above, before its first publish. Spring AMQP declares those beans
   * lazily, when its own connection is first opened, and nothing in this application opens it
   * before the outbox's first publish. A publish to an exchange that does not exist makes the
   * broker close the channel, and a channel closed that way is not reopened by automatic recovery:
   * every later publish would fail. The declarations are idempotent, and the client's topology
   * recovery repeats them after a reconnect, so a broker that comes back without its definitions
   * gets them again. The Quarkus and Micronaut apps declare the topology the same way.
   */
  static void declareTopology(Channel channel) throws IOException {
    channel.exchangeDeclare(EXCHANGE, "topic", /* durable= */ true);
    channel.queueDeclare(
        QUEUE,
        /* durable= */ true,
        /* exclusive= */ false,
        /* autoDelete= */ false,
        /* arguments= */ null);
    channel.queueBind(QUEUE, EXCHANGE, "integration.#");
  }
}
```

The `TopicExchange` is `streamrune.events` (durable, not auto-delete). The queue is `streamrune.integration` (durable). The binding pattern `integration.#` routes any routing key prefixed with `integration.` (or exactly `integration`) to that queue — so the routing key `integration.event` used by the publisher lands there. The `notifications-service` subscribes to the same queue. Spring AMQP declares the three topology beans, but lazily: only when its own connection is first opened. Nothing in this app opens that connection before the outbox's first publish, and a publish to an exchange that does not exist makes the broker close the publisher's channel for good. So the publisher declares the same exchange, queue and binding on its own channel before its first publish (`declareTopology`) — with the same arguments as the beans, so the declarations agree — exactly as the Quarkus and Micronaut apps do. `RabbitMqConfigTopologyIT` runs the bean method against a fresh broker and expects the first publish to be confirmed and waiting in the queue.

**Why the publisher gets its own connection.** It is tempting to write `connectionFactory.createConnection().createChannel(false)` with the Spring AMQP `ConnectionFactory` that `spring-boot-starter-amqp` already provides. Don't. Spring AMQP switches the client's automatic recovery off and hands out channels behind a caching proxy. When the broker restarts or the network drops, the proxy quietly opens a fresh channel on the next call — without confirm mode and without the confirm and return listeners `RabbitMqOutboxPublisher.build()` registered on the old one. Publishes on that channel still reach the queue, but the broker never confirms them: every entry times out, stays claimed for the 2-minute lease and is published again, over and over, and no entry is ever marked `DELIVERED` until the application restarts.

The plain RabbitMQ Java client with automatic recovery (its default, set explicitly above) reconnects by itself every 5 seconds and reopens the *same* channel object with confirm mode and both listeners restored, so the publisher keeps working. While the broker is down a publish fails before anything is handed off, and the relay tries the entry again on its backoff. A publish already sent when the connection drops gets no confirm: it waits out the confirm timeout, stays claimed until the lease expires, and is then published again — the consumer may see it twice, which is why the `notifications-service` deduplicates by entry id. The Quarkus and Micronaut apps wire the same `LazyRabbitMqOutboxPublisher` (`RabbitMqProducer`, `RabbitMqFactory`). `OutboxBrokerRestartIT` in `spring-app` proves the recovery: it stops and restarts the broker application, writes an outbox entry while the broker is down, and expects that entry to reach `DELIVERED` — which the relay records only after the broker confirms the publish.

**The application starts and serves commands while the broker is down, and drains the outbox when it returns.** Nothing at startup connects to RabbitMQ, so the app comes up without it and accepts every command; the integration events wait in `outbox_events`. You can watch it happen:

- `GET /api/admin/outbox` (as an `ADMIN`) lists the waiting entries: `status` `PENDING`, `attempts` `0`, and `lastError` naming the broker, e.g. `RabbitMQ is not reachable at localhost:5672, nothing was published: java.net.ConnectException: Connection refused`.
- The relay logs one `WARN` per poll cycle: `Outbox delivery hit a broker-wide TRANSPORT failure for 3 of 3 claimed entries (...) retry deferred by up to ... — escalating toward the 60s cap while the failures last: RabbitMQ is not reachable at ...`.
- `streamrune.outbox.pending` (`/actuator/metrics/streamrune.outbox.pending`) counts the backlog, and `/actuator/health` stays `UP`: the `relay.outbox-relay` block (Chapter 15) reads `UP`, because a refused publish fails an entry, not the relay's poll cycle.
- When the broker answers, the publisher logs `Outbox publisher connected to RabbitMQ at localhost:5672`, the entries are delivered in order, and `GET /api/admin/outbox` empties. An entry that waited longest waits up to the 60-second cap for its next attempt.

A broker that goes away after the publisher connected shows the same way, with the client's own error in `lastError` (the client reconnects by itself). `OutboxBrokerDownStartupIT` in each app proves the startup case: it starts the app with a broker port nothing listens on, expects `UP` health, a command answered `200` and its entry `PENDING` with the refusal recorded and no attempt counted, then starts RabbitMQ on that port and expects the entry `DELIVERED` and on the queue once.

Spring Boot adds a health check of its own for RabbitMQ with `spring-boot-starter-amqp`. It opens Spring AMQP's connection, which the app does not publish on, and would answer `DOWN` for the whole app while the broker is down — taking an app that serves out of a load balancer's rotation. Switch it off in `application.yml`; the outbox's own signals above show the outage:

```yaml
management:
  health:
    rabbit:
      enabled: false
```


### Step 6: Wire `InventoryCommandController`

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/InventoryCommandController.java`. This is the REST entry point for the only inventory command that operators trigger directly — receiving a new shipment. Stock reservations and releases are driven by the saga, not by direct HTTP calls.

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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

  @PostMapping("/{productId}/receive")
  public ResponseEntity<Void> receiveShipment(
      @PathVariable String productId, @Valid @RequestBody ReceiveShipmentRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, req.quantity()));
    return ResponseEntity.ok().build();
  }

  public record ReceiveShipmentRequest(@Positive int quantity) {}
}
```

The handler's first line is the guard Chapter 9 gave the product writes: `OwnerOrAdminAccess.requireAdmin` answers `403` to everyone but an `ADMIN`, the role together with an identity. The stock a shipment adds is the stock every customer's order reserves from, so the route is an operator's:

```bash
curl -i -X POST http://localhost:8080/api/inventory/p-1/receive \
  -H 'Content-Type: application/json' -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN' \
  -d '{"quantity": 50}'
# → 200 OK; 403 without the two headers
```

`ReceiveShipment` itself carries no `@RequireRole`: the seed data of Chapter 17 and a dead-letter replay (Chapter 14) dispatch it through the command bus without a request.

### Step 7: Create the `notifications-service` Module and Wire docker-compose

The `notifications-service` is a separate, headless Spring Boot application. It subscribes to the `streamrune.integration` queue via `@RabbitListener` and exposes two endpoints on port 8090: `GET /received`, which returns the last 200 received integration events from memory to an `ADMIN`, and `GET /health`, an open `200` without a body. It runs independently of the e-shop — no access to the e-shop's database, no awareness of the event store. That decoupling is the whole point of the outbox pattern.

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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * The service's two HTTP endpoints: what it received, for an operator, and whether it runs, for a
 * probe.
 *
 * <p><b>{@code GET /received} is ADMIN only.</b> It returns the body of every integration event the
 * service holds, and a body names an order, a payment or a product and can carry the reason a
 * customer typed when cancelling. The rule is the e-shop's definition of an {@code ADMIN}: the
 * {@code ADMIN} role together with an identity. Everyone else gets {@code 403}, a caller without
 * {@code X-User-Id} first of all.
 *
 * <p><b>DEMO SHORTCUT, the gateway stand-in the tutorial's chapter 9 describes.</b> The identity
 * and the role are whatever the client sent in {@code X-User-Id} and {@code X-User-Role}. The
 * service shares no code with the e-shop, so it reads the two headers itself. A real deployment
 * authenticates the caller and derives the role from that identity.
 *
 * <p><b>{@code GET /health} is open and carries no body.</b> A {@code 200} says the service runs; a
 * probe needs no headers and learns nothing else.
 */
@RestController
public class ReceivedController {

  private static final String ADMIN = "ADMIN";

  private final IntegrationEventListener listener;

  public ReceivedController(IntegrationEventListener listener) {
    this.listener = listener;
  }

  @GetMapping("/received")
  public List<IntegrationEventListener.Received> received(
      @RequestHeader(value = "X-User-Id", required = false) String userId,
      @RequestHeader(value = "X-User-Role", required = false) String role) {
    if (!isAdmin(userId, role)) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Required role: ADMIN");
    }
    return listener.recent();
  }

  @GetMapping("/health")
  public ResponseEntity<Void> health() {
    return ResponseEntity.ok().build();
  }

  /** An {@code ADMIN} is the role, spelled exactly, together with an identity. */
  private static boolean isAdmin(String userId, String role) {
    return userId != null && !userId.isBlank() && ADMIN.equals(role);
  }
}
```

`GET /received` answers an `ADMIN` only. A body it shows names an order, a payment or a product, and an `OrderCancelled` body carries the reason the customer gave, so the endpoint follows Chapter 9's definition: the `ADMIN` role together with an identity, `403` for everyone else. The service shares no code with the e-shop and reads the two headers itself; like every header check of the demo it is the gateway stand-in, not authentication. `GET /health` is the open endpoint: a `200` without a body, which is all a probe needs and all a caller without headers learns from this service.

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
    test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
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

Port 5672 is the AMQP port; 15672 exposes the management UI at `http://localhost:15672` (guest/guest). The health check is `check_port_connectivity` rather than `ping`: `ping` succeeds as soon as the broker's Erlang node runs, several seconds before port 5672 accepts connections, and the services that wait for `service_healthy` connect the moment the check passes. Update the `backend` service too: add `RABBITMQ_HOST: rabbitmq`, `RABBITMQ_USERNAME: guest` and `RABBITMQ_PASSWORD: guest` to its `environment`, and `rabbitmq: condition: service_healthy` to its `depends_on`. The backend starts without the broker (Step 5b); waiting for it only lets the seed data's integration events go out at once instead of on the relay's backoff. Without `RABBITMQ_HOST` the backend's outbox publisher looks for the broker on `localhost` inside its own container and never connects: the backend serves, and its entries wait `PENDING`.

Before running `docker compose up`, build the two JARs the images copy — the backend's and the notifications service's:

```bash
./gradlew :spring-app:bootJar :notifications-service:bootJar
docker compose up
```

The repository's `.dockerignore` keeps every `build/` directory out of the Docker build context except the two the Dockerfiles copy from, `spring-app/build/libs` and `notifications-service/build/libs`. If you write your own, re-include both (`!spring-app/build/libs`, `!notifications-service/build/libs`), or the `COPY` of a JAR fails with *file not found*.

The `notifications-service` deduplicates messages by `entryId` (the AMQP `messageId` property, set by `RabbitMqOutboxPublisher` to `entry.id().value()`). If the broker delivers a message twice — because the poller restarted after publishing but before marking it `DELIVERED` — the service discards the duplicate silently.

### Step 8: Verify End-to-End Delivery

Start the full stack. Build both JARs first (the Dockerfiles copy pre-built jars — see Step 7e):

```bash
./gradlew :spring-app:bootJar :notifications-service:bootJar
docker compose up
```

Wait for all services to be healthy. `SeedDataRunner` issues `ReceiveShipment` at startup for the seed products, so inventory events are already written by the time you reach the next step.

**8a. Place and confirm an order.** Confirming an order emits `OrderConfirmed`, which `EcommerceIntegrationEventMapper` includes in the outbox. Place and confirm an order:

```bash
# Place an order (adapt IDs to your seed data)
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" \
  -d '{"orderId":"o-test-1","customerId":"c-1","lines":[{"productId":"prod-widget","quantity":1}]}'

# The saga automatically progresses through payment and inventory;
# once PaymentCaptured fires, OrderConfirmed follows automatically.
```

**8b. Check the notifications service.** Within a couple of seconds the poller claims the outbox entry and publishes it to RabbitMQ. The `notifications-service` receives it and stores it in memory:

```bash
curl -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8090/received
```

You should see a JSON array containing an entry whose `type` is `"OrderConfirmed"` and whose `body` contains the `orderId`. The response is newest-first. Without the two headers the answer is `403`: the bodies name orders, and the service shows them to an `ADMIN` only. `curl -i http://localhost:8090/health` answers `200` with an empty body to anyone.

**8c. Confirm customer events are NOT forwarded.** Register a new customer and verify no notification arrives:

```bash
curl -s -X POST http://localhost:8080/api/customers \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-pii-test" -H "X-User-Role: CUSTOMER" \
  -d '{"customerId":"c-pii-test","name":"Eve","email":"eve@example.com"}'

# Wait a moment, then check — the count should not increase
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8090/received | python3 -m json.tool | grep -c '"type"'
```

The count does not change. `CustomerRegistered` is excluded by `EcommerceIntegrationEventMapper` (the `default -> List.of()` branch), so no outbox entry is written and no message is published. Customer PII never leaves the e-shop's database.

**8d. Optional — RabbitMQ management UI.** Visit `http://localhost:15672` (guest/guest). The `streamrune.events` exchange and `streamrune.integration` queue are visible under Exchanges and Queues. The queue's message rate chart shows the delivery spike when orders are confirmed. Under Connections, the one named `streamrune-outbox-publisher` is the outbox publisher's own connection from Step 5b.

**8e. Optional — stop the broker.** The shop keeps working without RabbitMQ:

```bash
docker compose stop rabbitmq
# Place and confirm another order as in 8a: the requests answer as before.
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8080/api/admin/outbox
docker compose start rabbitmq
```

While the broker is stopped, the order's `OrderConfirmed` entry is listed `PENDING` with the client's error in `lastError`, the backend logs the relay's `WARN` each poll cycle, and `/actuator/health` stays `UP` (Step 5b). Once the broker is back, the client reconnects, the relay's next attempt delivers the entry — within its backoff, at most a minute — and `/received` on the notifications service lists it.

### When delivery fails: replay or skip

An entry that the broker rejects on every attempt (an unroutable message, a payload the consumer's schema refuses) exhausts the retry ladder — `streamrune.outbox.retry-max-attempts`, 10 by default; transport outages never count — and becomes `FAILED`. On this strict channel that entry is still the **head** of its stream: the order's later events stay `PENDING` and are not delivered, the relay logs `stream BLOCKED`, and two gauges show it — `streamrune.outbox.blocked_aggregates` (how many) and `streamrune.outbox.blockage_age_seconds` (for how long; alert on this one).

There are exactly two ways out, both through `OutboxFailedReplayer`:

- **Replay** — `replayer.replay(entryId)` puts the same row back to `PENDING` at its original position, so it is delivered *before* the entries that were waiting behind it. Use it once the cause is fixed.
- **Skip** — `replayer.skip(entryId, operator, reason)` marks the row `SKIPPED` with who, when and why, and the aggregate's later entries are claimable on the next poll. **A skip is not a delivery:** the notifications service will never see that event and will receive the later ones as if it had. If a consumer's state depends on the skipped change, emit a corrective domain event through the normal command path.

Nothing else releases a blocked stream: `OutboxStore.delete` refuses a `FAILED` row, and retention never prunes one. Chapter 14 exposes both operations on the admin API.

---

## What We Learned

**`OutboxStore`** is the core interface. `PostgresOutboxStore` implements it using the `outbox_events` table. The auto-configured `postgresEventStoreFactory` discovers the `PostgresOutboxStore` and `OutboxEventMapper` beans, and arranges for every event append to call `mapper.toOutbox(envelope)`. If the mapper returns entries, they are inserted in the same JDBC transaction as the event append — so they commit (or roll back) together.

**`OutboxEventMapper`** is the PII boundary. `EcommerceIntegrationEventMapper.toOutbox(EventEnvelope)` returns an empty list for excluded event types (`CustomerEvent.*`, `InventoryEvent.*`, `OrderPlaced`, `PaymentInitiated`) and a list with one compact integration DTO for allowed types (`OrderConfirmed`, `OrderShipped`, `OrderDelivered`, `OrderCancelled`, `PaymentCaptured`, `PaymentRefunded`, `PaymentVoided`, `PaymentFailed`, and the four `ProductEvent` variants). The DTO records carry only non-PII business fields — there is no `customerId`, no name, no email, no address.

**`OutboxEntry`** is the record the poller reads. It carries an `id` (`OutboxEntryId` — a record wrapping a `String` value, used as the idempotency key), `payloadType` (e.g. `"OrderConfirmed"`), a JSON `payload` containing the integration DTO, a `streamId` ordering key — the event's own stream (mandatory on the demo's strict channel), and an `OutboxStatus`.

**`OutboxStatus`** has five values: `PENDING` (awaiting delivery), `IN_PROGRESS` (claimed by the poller — the competing-consumer lease), `DELIVERED` (broker confirmed), `FAILED` (all retries exhausted; terminal until an operator replays or skips it, never pruned), and `SKIPPED` (an operator's audited decision that a `FAILED` entry will never be delivered).

**`OutboxPoller`** runs on its own virtual thread, auto-configured when `streamrune.outbox.enabled=true`. It wakes on a configured interval, atomically claims a batch via `FOR UPDATE SKIP LOCKED`, and invokes the `OutboxPublisher` for each entry. On success it marks the entry `DELIVERED`. On failure it follows a retry policy (exponential backoff) and only marks `FAILED` once `maxAttempts` is exhausted.

**Ordering mode.** The outbox channel's ordering is a typed choice (`OutboxOrderingMode`), strict by default; strict mode makes `streamId` mandatory.

**Replay or skip.** A `FAILED` entry blocks its aggregate until replayed (in place) or skipped (audited); a skip is not a delivery.

**`RabbitMqOutboxPublisher`** is the `OutboxPublisher` implementation that sends AMQP messages. It puts its channel into confirm mode when it is built; the confirm timeout (30 s) plus its default 30 s publish-block budget give a 60 s in-flight horizon, well below the claim lease (2 min), so a broker timeout causes the poller to retry without losing the entry — and `OutboxPoller` refuses to boot in the first place if the lease does not strictly exceed that horizon. The channel must stay in confirm mode for the publisher's whole life, so it comes from a dedicated, automatically recovering RabbitMQ Java client connection, never from Spring AMQP's caching `ConnectionFactory`, whose recreated channels lose confirm mode after a reconnect.

**`LazyRabbitMqOutboxPublisher`** opens that connection and builds the `RabbitMqOutboxPublisher` on the first publish, so the application starts and serves commands while the broker is down. Until the broker answers, every publish fails as a transport failure: the entries stay `PENDING` with the error in `lastError`, no attempt is counted, the relay retries on a backoff capped at 60 s, and the outbox drains once the broker is back. `streamrune.outbox.pending`, the relay's `WARN` line and `GET /api/admin/outbox` show the backlog; health stays `UP`.

**notifications-service** is a separate Spring Boot application. It has no access to the e-shop's database — it only sees what arrives on the `streamrune.integration` queue. This is the key decoupling: the e-shop publishes integration events; downstream consumers are fully independent services. The notifications service deduplicates by `entryId` to handle at-least-once delivery.

**Transactional outbox pattern** trades direct publish for durability. The invariant: if an event was committed, its outbox entry exists, and the poller eventually delivers it — unless an operator deliberately skips it, an audited decision that is not a delivery. This is the correct foundation for integrating with any system you do not own.

**At-least-once delivery** is a consequence. Consumers must handle duplicates — the `notifications-service` does so by tracking seen `entryId` values.

---

## Next Up

What happens when a projection fails to process an event? Next: dead letter queue.
