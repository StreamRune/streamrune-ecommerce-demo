# Chapter 8 — Order Aggregate & Snapshots

> **What you'll learn:**
> - How to model an aggregate that moves through a strict lifecycle with a state machine
> - How to enforce lifecycle rules in `decide` so invalid transitions are rejected at the domain boundary
> - What snapshots are, why they exist, and when the overhead is worth paying
> - How to configure `SnapshotPolicy.everyNEvents(N)` on the `VirtualThreadCommandBus`
> - How to wire and test the `OrderDecider` and verify that snapshots are stored after enough events

---

## What We're Building and Why

Every e-commerce system has an `Order`. An order is not just a bag of data — it has a **lifecycle**. It starts as `CREATED` when a customer places it. It becomes `CONFIRMED` when a merchant accepts it. It becomes `SHIPPED` when it leaves the warehouse. It becomes `DELIVERED` when a courier hands it over. At any point before delivery it can be `CANCELLED`.

Each state transition is one-way. An order cannot move backwards from `CONFIRMED` to `CREATED`. An order cannot be shipped before it is confirmed. An order cannot be cancelled after it is delivered. These rules are not UI-level validations — they are domain invariants that must be enforced regardless of how a command arrives. The `decide` function is the right place to enforce them.

### The Snapshot Problem

Every time the `CommandBus` processes a command for an existing aggregate, it must reconstruct the current state. It does this by reading the entire event stream from the event store and calling `evolve` once per event.

For most aggregates this is fast: `Product` typically has a handful of events — a `ProductCreated`, a few `PriceUpdated`, maybe a `StockAdjusted`. The replay cost is negligible.

Orders are different. A busy order might accumulate dozens of events as it moves through fulfilment, receives partial shipment updates, gets a hold placed on it, and finally delivers. In a high-throughput system, replaying fifty events for every command becomes measurably slow.

**Snapshots** solve this. After a configurable number of events, StreamRune serialises the current `AggregateState` to the event store as a snapshot record. On the next command, the `CommandBus` loads the most recent snapshot as the starting state and replays only the events that arrived *after* the snapshot was taken. If the snapshot is recent, the replay window is small.

The domain logic changes nothing. The `Decider` continues to receive a fully-reconstructed `OrderState` — it does not know whether that state came from a full replay or a snapshot-plus-tail replay. Snapshots are purely an infrastructure optimisation.

You activate them with a single call in the `CommandBus` builder:

```java
.snapshotPolicy(SnapshotPolicy.everyNEvents(5))
```

That is the only line you need to write. The rest of this chapter focuses on building the aggregate that benefits from it.

The threshold counts events **per aggregate stream**, not across the whole system. With `everyNEvents(5)`, a snapshot is written once an individual order accumulates five events since its last snapshot. The happy-path lifecycle is only four events (placed, confirmed, shipped, delivered), so a plain happy-path order never reaches the threshold; you need at least one extra event — for example a cancellation-then-replacement scenario, or by adding a second order that shares the same order ID namespace to accumulate a fifth. Orders that stop well below the threshold never get a snapshot, and that is fine: the saving only matters for streams that grow long.

---

## Domain Model

### OrderStatus

The lifecycle is expressed as a plain enum. There are no methods on the enum — the state machine logic lives in `decide`.

Create `domain/src/main/java/org/streamrune/ecommerce/domain/order/OrderStatus.java`:

```java
package org.streamrune.ecommerce.domain.order;

public enum OrderStatus {
  CREATED,
  CONFIRMED,
  SHIPPED,
  DELIVERED,
  CANCELLED
}
```

The five values map directly to the five transitions in the lifecycle. Having them in an enum lets you use `switch` exhaustively in both `decide` and `evolve`, and the compiler will warn if you add a status without handling it.

### OrderCommand

Create `domain/src/main/java/org/streamrune/ecommerce/domain/order/OrderCommand.java`:

```java
package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.Command;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface OrderCommand extends Command {
  /**
   * Places an order. The constructor checks the order id's length, so every app that builds a
   * {@code PlaceOrder} applies the same rule: a client-supplied order id must also become a valid
   * saga id. The order stream takes any id {@code AggregateId.of} accepts, up to 255 characters,
   * but the fulfillment saga that {@code OrderPlaced} starts is identified by {@code "fulfillment-"
   * + orderId}, and a {@code SagaId} holds 255 characters at most. Refused only there, the order
   * would already be placed and the saga could never start: its first event would be quarantined as
   * poison and the order would stay {@code CREATED} for good. Refused here, the order endpoint
   * answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>A blank order id or one with a control character is refused where the command bus builds the
   * stream id with {@code AggregateId.of}, before anything is written, so the constructor adds only
   * the length rule. The payment id {@code "pay-" + orderId} is shorter by eight characters than
   * the saga id and fits whenever that does.
   */
  record PlaceOrder(String orderId, String customerId, List<OrderLine> lines)
      implements OrderCommand {

    /** The prefix of the saga id the order starts: {@code "fulfillment-" + orderId}. */
    public static final String SAGA_ID_PREFIX = "fulfillment-";

    /** The longest order id whose saga id {@code "fulfillment-" + orderId} fits 255 characters. */
    public static final int MAX_ORDER_ID_LENGTH =
        IdConstraints.MAX_LENGTH - SAGA_ID_PREFIX.length();

    public PlaceOrder {
      if (orderId != null && orderId.length() > MAX_ORDER_ID_LENGTH) {
        throw new IllegalArgumentException(
            "orderId must be at most "
                + MAX_ORDER_ID_LENGTH
                + " characters, got "
                + orderId.length());
      }
    }
  }

  record ConfirmOrder(String orderId) implements OrderCommand {}

  record ShipOrder(String orderId) implements OrderCommand {}

  record DeliverOrder(String orderId) implements OrderCommand {}

  record CancelOrder(String orderId, String reason) implements OrderCommand {}

  /**
   * One line of an order to place. The constructor checks the product id, so every app that builds
   * a {@code PlaceOrder} applies the same rule: a client-supplied product id must become a valid
   * inventory id. The saga reserves stock on the inventory aggregate whose id is the product id,
   * and the inventory extractor builds that id with {@code AggregateId.of}, which refuses a control
   * character or an id longer than 255 characters. Refused only there, the order would already be
   * placed and paid for, and the saga would refund and cancel it. Refused here, the order endpoint
   * answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>{@link OrderEvent.OrderLine}, the line an {@code OrderPlaced} event records, has no such
   * rule: it is rebuilt from the event log and from saga state, where a rule would make a stored
   * order unreadable instead of stopping a write.
   */
  record OrderLine(String productId, int quantity, Money unitPrice) {

    /** The longest product id: it is the inventory aggregate id, so the id bound applies as is. */
    public static final int MAX_PRODUCT_ID_LENGTH = IdConstraints.MAX_LENGTH;

    public OrderLine {
      if (productId == null || productId.isBlank()) {
        throw new IllegalArgumentException("productId is required");
      }
      if (productId.length() > MAX_PRODUCT_ID_LENGTH) {
        throw new IllegalArgumentException(
            "productId must be at most "
                + MAX_PRODUCT_ID_LENGTH
                + " characters, got "
                + productId.length());
      }
      IdConstraints.requireNoControlCharacters(productId, "productId");
    }
  }
}
```

`PlaceOrder` carries `OrderLine` records from the command layer. Notice that `OrderCommand.OrderLine` and `OrderEvent.OrderLine` are separate types even though they have the same shape — a command's line is an intent; an event's line is a recorded fact. The decider maps one to the other explicitly rather than sharing a mutable DTO.

The two types differ in one rule. The constructor of `OrderCommand.OrderLine` checks the product id. The id must not be blank, must not contain a control character (C0 `U+0000`–`U+001F`, DEL `U+007F` or C1 `U+0080`–`U+009F`), and must be at most 255 characters. Otherwise the constructor throws an `IllegalArgumentException`, and the message names the rule without echoing the id. The order endpoints of all three apps build their lines with this constructor, so they apply the same rule and cannot drift apart. The `IllegalArgumentException` handler from chapter 1 answers `400 Bad Request` before anything is written.

The product id gets this check because it later becomes the id of another aggregate. In chapter 11 the payment saga reserves stock for each line, and in chapter 13 the inventory extractor routes that reservation to the `inventory` aggregate whose id is the product id. The extractor builds that id with `AggregateId.of`, which refuses a control character or an id longer than 255 characters (chapter 2), so the product id is bound by the same rule. Without the check here, such an id would be refused only when the saga reserves stock. By then the saga has already captured the payment, so it would refund it and cancel the order. `IdConstraints` is the framework class that `AggregateId.of` uses for its charset rule. `OrderEvent.OrderLine` has no such rule, because it is rebuilt from the event log, and a rule there would make a stored order unreadable instead of stopping a write.

`PlaceOrder` has a rule of its own, on the order id. The order stream takes any id that `AggregateId.of` accepts, up to 255 characters, but `OrderPlaced` starts the fulfillment saga of chapter 11, and that saga is identified by `"fulfillment-" + orderId`. A `SagaId` holds 255 characters at most, and the twelve-character prefix leaves 243 for the order id. The constructor of `PlaceOrder` throws an `IllegalArgumentException` for a longer order id (`MAX_ORDER_ID_LENGTH`), and the message names the rule without echoing the id. The order endpoints build the command with this constructor too, so they answer `400 Bad Request` and write nothing. Without the check, an order id of 244 to 255 characters would be placed and its saga could never start: the saga runner would quarantine `OrderPlaced` as a poison event, and the order would stay `CREATED` with no payment and no confirmation. The constructor leaves a blank order id and one with a control character to the command bus, where the order id extractor refuses both with `AggregateId.of` before anything is written (chapter 2). `SAGA_ID_PREFIX` is the prefix the saga builds and matches its ids with, so the bound and the saga cannot drift apart. The payment id `"pay-" + orderId` is eight characters shorter than the saga id, so it fits whenever the saga id does.

### OrderEvent

Create `domain/src/main/java/org/streamrune/ecommerce/domain/order/OrderEvent.java`:

```java
package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface OrderEvent extends DomainEvent {
  record OrderPlaced(String orderId, String customerId, List<OrderLine> lines, Money total)
      implements OrderEvent {}

  record OrderConfirmed(String orderId) implements OrderEvent {}

  record OrderShipped(String orderId) implements OrderEvent {}

  record OrderDelivered(String orderId) implements OrderEvent {}

  record OrderCancelled(String orderId, String reason) implements OrderEvent {}

  record OrderLine(String productId, int quantity, Money unitPrice) {}
}
```

`OrderPlaced` includes the computed `total` so any downstream projection can display the order value without re-running the multiplication. Compute in the decider, record in the event.

### OrderState

Create `domain/src/main/java/org/streamrune/ecommerce/domain/order/OrderState.java`:

```java
package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;
import org.streamrune.ecommerce.domain.common.Money;

public record OrderState(
    String orderId,
    String customerId,
    List<OrderEvent.OrderLine> lines,
    Money total,
    OrderStatus status)
    implements AggregateState {
  /** The aggregate type the order decider is registered under: streams are order:<orderId>. */
  public static final AggregateType TYPE = AggregateType.of("order");

  public OrderState() {
    this(null, null, List.of(), null, OrderStatus.CREATED);
  }

  public OrderState withStatus(OrderStatus newStatus) {
    return new OrderState(orderId, customerId, lines, total, newStatus);
  }
}
```

Two things worth noting. First, the no-arg constructor establishes the initial state: no data, status `CREATED`. `decide` for `PlaceOrder` will read this initial state and immediately produce an `OrderPlaced` event; the `evolve` call that follows populates the real field values. Second, `withStatus` is a convenience method that copies all fields except `status`. The `evolve` method uses it for every transition after `OrderPlaced`.

---

## Step by Step

### Step 1 — Write the tests first

The `DeciderFixture` gives you a TDD workflow with no boilerplate. Write the test class before you write `OrderDecider`.

Create `commands/src/test/java/org/streamrune/ecommerce/commands/order/OrderDeciderTest.java`:

```java
package org.streamrune.ecommerce.commands.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.*;
import org.streamrune.test.DeciderFixture;

class OrderDeciderTest {

  private final DeciderFixture<OrderCommand, OrderState, OrderEvent> fixture =
      DeciderFixture.of(new OrderDecider());

  private static final Money TEN_USD = new Money(BigDecimal.TEN, "USD");

  private OrderEvent.OrderPlaced placed() {
    return new OrderEvent.OrderPlaced(
        "o-1",
        "c-1",
        List.of(new OrderEvent.OrderLine("p-1", 2, TEN_USD)),
        new Money(BigDecimal.valueOf(20), "USD"));
  }

  @Test
  void placeOrder_emitsOrderPlaced() {
    fixture
        .given()
        .when(
            new OrderCommand.PlaceOrder(
                "o-1", "c-1", List.of(new OrderCommand.OrderLine("p-1", 2, TEN_USD))))
        .expectState(
            s -> {
              assertThat(s.status()).isEqualTo(OrderStatus.CREATED);
              assertThat(s.total().amount()).isEqualByComparingTo(BigDecimal.valueOf(20));
            });
  }

  @Test
  void confirmOrder_fromCreated() {
    fixture
        .given(placed())
        .when(new OrderCommand.ConfirmOrder("o-1"))
        .expectEvents(new OrderEvent.OrderConfirmed("o-1"))
        .expectState(s -> assertThat(s.status()).isEqualTo(OrderStatus.CONFIRMED));
  }

  @Test
  void confirmOrder_notCreated_throws() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"))
        .when(new OrderCommand.ConfirmOrder("o-1"))
        .expectException(DomainException.class);
  }

  @Test
  void shipOrder_fromConfirmed() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"))
        .when(new OrderCommand.ShipOrder("o-1"))
        .expectEvents(new OrderEvent.OrderShipped("o-1"));
  }

  @Test
  void shipOrder_notConfirmed_throws() {
    fixture
        .given(placed())
        .when(new OrderCommand.ShipOrder("o-1"))
        .expectException(DomainException.class);
  }

  @Test
  void deliverOrder_fromShipped() {
    fixture
        .given(placed(), new OrderEvent.OrderConfirmed("o-1"), new OrderEvent.OrderShipped("o-1"))
        .when(new OrderCommand.DeliverOrder("o-1"))
        .expectEvents(new OrderEvent.OrderDelivered("o-1"));
  }

  @Test
  void cancelOrder_fromCreated() {
    fixture
        .given(placed())
        .when(new OrderCommand.CancelOrder("o-1", "Changed mind"))
        .expectEvents(new OrderEvent.OrderCancelled("o-1", "Changed mind"));
  }

  @Test
  void cancelOrder_delivered_throws() {
    fixture
        .given(
            placed(),
            new OrderEvent.OrderConfirmed("o-1"),
            new OrderEvent.OrderShipped("o-1"),
            new OrderEvent.OrderDelivered("o-1"))
        .when(new OrderCommand.CancelOrder("o-1", "Too late"))
        .expectException(DomainException.class);
  }
}
```

Run the tests now — they will all fail because `OrderDecider` does not exist yet. That is expected and correct: the tests describe the contract, the implementation will satisfy it.

```bash
./gradlew :commands:test
```

### Step 2 — Implement OrderDecider

Create `commands/src/main/java/org/streamrune/ecommerce/commands/order/OrderDecider.java`:

```java
package org.streamrune.ecommerce.commands.order;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.*;

public class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {

  @Override
  public OrderState initialState() {
    return new OrderState();
  }

  @Override
  public List<OrderEvent> decide(OrderCommand cmd, OrderState state) {
    return switch (cmd) {
      case OrderCommand.PlaceOrder c -> {
        List<OrderEvent.OrderLine> eventLines =
            c.lines().stream()
                .map(l -> new OrderEvent.OrderLine(l.productId(), l.quantity(), l.unitPrice()))
                .toList();
        Money total =
            c.lines().stream()
                .map(l -> l.unitPrice().multiply(l.quantity()))
                .reduce(new Money(java.math.BigDecimal.ZERO, "USD"), Money::add);
        yield List.of(new OrderEvent.OrderPlaced(c.orderId(), c.customerId(), eventLines, total));
      }

      case OrderCommand.ConfirmOrder c -> {
        if (state.status() != OrderStatus.CREATED)
          throw new DomainException(
              "Can only confirm CREATED orders, current status: " + state.status());
        yield List.of(new OrderEvent.OrderConfirmed(c.orderId()));
      }

      case OrderCommand.ShipOrder c -> {
        if (state.status() != OrderStatus.CONFIRMED)
          throw new DomainException(
              "Can only ship CONFIRMED orders, current status: " + state.status());
        yield List.of(new OrderEvent.OrderShipped(c.orderId()));
      }

      case OrderCommand.DeliverOrder c -> {
        if (state.status() != OrderStatus.SHIPPED)
          throw new DomainException(
              "Can only deliver SHIPPED orders, current status: " + state.status());
        yield List.of(new OrderEvent.OrderDelivered(c.orderId()));
      }

      case OrderCommand.CancelOrder c -> {
        if (state.status() == OrderStatus.DELIVERED)
          throw new DomainException("Cannot cancel DELIVERED orders");
        yield List.of(new OrderEvent.OrderCancelled(c.orderId(), c.reason()));
      }
    };
  }

  @Override
  public OrderState evolve(OrderState state, OrderEvent evt) {
    return switch (evt) {
      case OrderEvent.OrderPlaced e ->
          new OrderState(e.orderId(), e.customerId(), e.lines(), e.total(), OrderStatus.CREATED);
      case OrderEvent.OrderConfirmed e -> state.withStatus(OrderStatus.CONFIRMED);
      case OrderEvent.OrderShipped e -> state.withStatus(OrderStatus.SHIPPED);
      case OrderEvent.OrderDelivered e -> state.withStatus(OrderStatus.DELIVERED);
      case OrderEvent.OrderCancelled e -> state.withStatus(OrderStatus.CANCELLED);
    };
  }
}
```

The `decide` cases enforce the state machine: each case checks `state.status()` before accepting the command. The `evolve` cases are simpler — they update state unconditionally because a stored event is already a fact; the validity check happened before it was written.

Run the tests again:

```bash
./gradlew :commands:test
```

All eight tests should pass. If `shipOrder_notConfirmed_throws` fails, check that the guard in `ShipOrder` checks for `CONFIRMED` (not `CREATED`).

### Step 3 — Configure SnapshotPolicy in the CommandBus

Open `spring-app/src/main/java/org/streamrune/ecommerce/spring/config/StreamRuneConfig.java` and locate the `VirtualThreadCommandBus` builder. Add `.snapshotPolicy(SnapshotPolicy.everyNEvents(5))`:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation) {
  // more interceptors added in later chapters
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .interceptors(validation)
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
      // ... other registrations
      .build();
}
```

`SnapshotPolicy.everyNEvents(5)` tells the `CommandBus` to write a snapshot once an individual aggregate stream reaches five events since its last snapshot. The count is **per stream**, not a global event total. You can tune this number. Lower values mean more snapshot writes but shorter replay windows; higher values reduce write overhead at the cost of longer worst-case replays. Production systems often set this to a larger value; five is a reasonable starting point and aligns with the verification scenario in Step 6.

The snapshot policy is global for the bus — it applies to every registered aggregate. There is currently no per-aggregate override: `register` takes only the aggregate type, the command type, the aggregate id extractor, and the decider.

Add the import at the top of `StreamRuneConfig.java`:

```java
import org.streamrune.core.SnapshotPolicy;
```

### Step 4 — Register the Order decider

Still in `StreamRuneConfig.java`, register `OrderDecider` in the `commandBus` builder alongside `ProductDecider`:

```java
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
```

The first argument to `register` is the aggregate type `OrderState.TYPE`; the third is the id extractor — a `Function<C, AggregateId>` that maps a command to the id of the aggregate instance it targets. Every `OrderCommand` carries an `orderId` field, so every variant returns `c.orderId()`. The `CommandBus` combines the type and that id into the stream, `order:order-1`, and loads it from the event store before calling `decide`.

### Step 4a — Register Order events in the EventTypeRegistry

Just as you did for products (chapter 2) and customers (chapter 6, Step 5), add the five Order event types to the `SimpleEventTypeRegistry` inside the `eventTypeRegistry` bean. **This is required** — the auto-configured event store throws `UnknownEventTypeException` when it tries to serialise (write) or deserialise (read) an event type that is not registered, so without these entries the very first `POST /api/orders` fails at runtime even though everything compiles:

```java
.registerEvent("OrderPlaced",    OrderEvent.OrderPlaced.class)
.registerEvent("OrderConfirmed", OrderEvent.OrderConfirmed.class)
.registerEvent("OrderShipped",   OrderEvent.OrderShipped.class)
.registerEvent("OrderDelivered", OrderEvent.OrderDelivered.class)
.registerEvent("OrderCancelled", OrderEvent.OrderCancelled.class)
```

Add `import org.streamrune.ecommerce.domain.order.OrderEvent;` to `StreamRuneConfig.java` (or use the fully-qualified form as earlier chapters do).

### Step 5 — Add the REST controller

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/OrderCommandController.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@RestController
@RequestMapping("/api/orders")
public class OrderCommandController {

  private final VirtualThreadCommandBus commandBus;

  public OrderCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> placeOrder(@Valid @RequestBody PlaceOrderRequest req) {
    List<OrderCommand.OrderLine> lines =
        req.lines().stream()
            .map(
                l ->
                    new OrderCommand.OrderLine(
                        l.productId(), l.quantity(), new Money(l.unitPrice(), "USD")))
            .toList();
    commandBus.execute(new OrderCommand.PlaceOrder(req.orderId(), req.customerId(), lines));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/confirm")
  public ResponseEntity<Void> confirmOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.ConfirmOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/ship")
  public ResponseEntity<Void> shipOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.ShipOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/deliver")
  public ResponseEntity<Void> deliverOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.DeliverOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/cancel")
  public ResponseEntity<Void> cancelOrder(
      @PathVariable String id, @Valid @RequestBody CancelOrderRequest req) {
    commandBus.execute(new OrderCommand.CancelOrder(id, req.reason()));
    return ResponseEntity.ok().build();
  }

  public record PlaceOrderRequest(
      @NotBlank String orderId,
      @NotBlank String customerId,
      @NotEmpty List<@Valid OrderLineRequest> lines) {}

  public record OrderLineRequest(
      @NotBlank String productId,
      @NotNull @Positive int quantity,
      @NotNull @Positive BigDecimal unitPrice) {}

  public record CancelOrderRequest(String reason) {}
}
```

Each endpoint maps a simple HTTP request to an `OrderCommand` and delegates entirely to the `CommandBus`. No business logic belongs here. The `@Valid` annotation on `PlaceOrderRequest` and `CancelOrderRequest` triggers the `BeanValidationInterceptor` wired in chapter 3, so a malformed request is rejected before it reaches `OrderDecider`. `@NotBlank` does not catch a control character in a product id. The `new OrderCommand.OrderLine(...)` call in `placeOrder` does: it throws before `commandBus.execute` runs, so the request answers `400`. The Quarkus and Micronaut order controllers declare their request records without validation annotations. They build their lines with the same constructor, so the same product ids are refused there.

### Step 5a — Create `OrderView`

Create `queries/src/main/java/org/streamrune/ecommerce/queries/dto/OrderView.java`:

```java
package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import java.util.List;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;

public record OrderView(
    String orderId,
    String customerId,
    List<OrderLineView> lines,
    Money total,
    OrderStatus status,
    Instant createdAt,
    Instant updatedAt) {
  public record OrderLineView(String productId, int quantity, Money unitPrice) {}
}
```

### Step 5b — Create `OrderProjection`

Create `projections/src/main/java/org/streamrune/ecommerce/projections/OrderProjection.java`:

```java
package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.order.*;
import org.streamrune.ecommerce.queries.dto.OrderView;

@org.streamrune.core.ProjectionConfig(
    name = "orders",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class OrderProjection extends BaseProjection {

  public OrderProjection(ProjectionRepository repository) {
    super(repository, "orders");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      if (envelope.event() instanceof OrderEvent evt) {
        switch (evt) {
          case OrderEvent.OrderPlaced e ->
              save(
                  e.orderId(),
                  new OrderView(
                      e.orderId(),
                      e.customerId(),
                      e.lines().stream()
                          .map(l -> new OrderView.OrderLineView(l.productId(), l.quantity(), l.unitPrice()))
                          .toList(),
                      e.total(),
                      OrderStatus.CREATED,
                      envelope.metadata().timestamp(),
                      envelope.metadata().timestamp()));
          case OrderEvent.OrderConfirmed e ->
              updateStatus(e.orderId(), OrderStatus.CONFIRMED, envelope.metadata().timestamp());
          case OrderEvent.OrderShipped e ->
              updateStatus(e.orderId(), OrderStatus.SHIPPED, envelope.metadata().timestamp());
          case OrderEvent.OrderDelivered e ->
              updateStatus(e.orderId(), OrderStatus.DELIVERED, envelope.metadata().timestamp());
          case OrderEvent.OrderCancelled e ->
              updateStatus(e.orderId(), OrderStatus.CANCELLED, envelope.metadata().timestamp());
        }
      }
    }
  }

  private void updateStatus(String id, OrderStatus status, Instant timestamp) {
    findById(id, OrderView.class)
        .ifPresent(
            existing ->
                save(
                    id,
                    new OrderView(
                        existing.orderId(), existing.customerId(), existing.lines(),
                        existing.total(), status, existing.createdAt(), timestamp)));
  }

  public OrderView get(String orderId) {
    return findById(orderId, OrderView.class).orElse(null);
  }

  public List<OrderView> listAll() {
    return repository.findAll(projectionName(), OrderView.class);
  }

  public List<OrderView> listByCustomer(String customerId) {
    return repository.findAll(projectionName(), OrderView.class).stream()
        .filter(o -> customerId.equals(o.customerId()))
        .toList();
  }
}
```

Register the bean in `StreamRuneConfig`:

```java
@Bean
public OrderProjection orderProjection(ProjectionRepository repo) {
    return new OrderProjection(repo);
}
```

And add `"orders"` to the `MultiProjectionRunner`:

```java
@Bean(destroyMethod = "close")
public MultiProjectionRunner projectionRunner(
    EventStore eventStore,
    OffsetStore offsetStore,
    JdbcProjectionRepository projectionRepository,
    ProductProjection productProjection,
    CustomerProjection customerProjection,
    OrderProjection orderProjection,
    CacheInvalidator cacheInvalidator) {

  var runner =
      MultiProjectionRunner.builder()
          .eventStore(eventStore)
          .offsetStore(offsetStore)
          .atomicProcessor(projectionRepository)
          .register(
              "products",
              new CacheAwareProjection(productProjection, cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .register(
              "customers",
              new CacheAwareProjection(customerProjection, cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .register(
              "orders",
              new CacheAwareProjection(orderProjection, cacheInvalidator),
              ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          // Ch13: .register("inventory", new CacheAwareProjection(inventoryProjection, cacheInvalidator), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
          .build();
  runner.start();
  return runner;
}
```

### Step 5c — Create `OrderQueryController`

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/OrderQueryController.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.dto.OrderView;

@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {

  private final OrderProjection orderProjection;

  public OrderQueryController(OrderProjection orderProjection) {
    this.orderProjection = orderProjection;
  }

  @GetMapping("/{id}")
  public ResponseEntity<OrderView> getOrder(@PathVariable String id) {
    OrderView order = orderProjection.get(id);
    return order != null ? ResponseEntity.ok(order) : ResponseEntity.notFound().build();
  }

  @GetMapping
  public List<OrderView> listOrders(@RequestParam(required = false) String customerId) {
    if (customerId != null) {
      return orderProjection.listByCustomer(customerId);
    }
    return orderProjection.listAll();
  }
}
```

### Step 6 — Verify orders and snapshots

Start the application:

```bash
./gradlew :spring-app:bootRun
```

**Place an order:**

```bash
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "orderId": "order-1",
    "customerId": "c-1",
    "lines": [
      {"productId": "p-1", "quantity": 2, "unitPrice": 29.99}
    ]
  }'
```

**Confirm it:**

```bash
curl -X POST http://localhost:8080/api/orders/order-1/confirm
```

**Verify the state machine guards work — try shipping an unconfirmed order:**

Place a second order and immediately try to ship it without confirming:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "orderId": "order-2",
    "customerId": "c-1",
    "lines": [{"productId": "p-1", "quantity": 1, "unitPrice": 9.99}]
  }'

curl -X POST http://localhost:8080/api/orders/order-2/ship
```

The second `curl` returns `400 Bad Request` with a message like `Can only ship CONFIRMED orders, current status: CREATED`. The state machine guard fired in `decide` before any event was written.

**Verify that a bad product id is refused when the order is placed:**

```bash
curl -i -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "orderId": "order-3",
    "customerId": "c-1",
    "lines": [{"productId": "p-1\nforged", "quantity": 1, "unitPrice": 9.99}]
  }'
```

The JSON `\n` puts a line feed into the product id. The response is `400 Bad Request` with the body `productId must not contain control characters (U+0000-U+001F, U+007F, U+0080-U+009F)`. The `OrderCommand.OrderLine` constructor refused the line before the command reached the bus, so `order-3` has no events.

> **Heads up for chapter 9:** this `/ship` call succeeds in reaching `decide` only because authorization is not wired yet. In the next chapter you will add `@RequireRole("ADMIN")` to `ShipOrder` and `DeliverOrder` and an authorization interceptor that runs *before* `decide`. After that, this same unauthenticated call is rejected by authorization (an `AuthorizationException`) before the lifecycle guard ever runs, so you will need an ADMIN role on the request to see the `400` from the state machine.

**Trigger snapshot creation:**

The threshold is counted **per order stream**, so to see a snapshot you have to drive a single order to five events. The basic happy-path lifecycle produces only four events (placed, confirmed, shipped, delivered), so it does **not** cross the `everyNEvents(5)` threshold on its own. You need a fifth event on the same stream.

The quickest path: take `order-1` through the full lifecycle (4 events), then use a **fresh order** that you place, confirm, ship, and also cancel before delivery — place → confirm → ship → cancel gives 4 events on the new stream, still not enough. The simplest single-stream five-event scenario is: place → confirm → cancel (3 events), which does not reach 5 either.

**The practical approach for this demo**: because the basic domain's lifecycle produces at most four events per order (deliver is terminal; cancel after confirm/ship is also terminal), the `everyNEvents(5)` threshold is intentionally set above the simple lifecycle. It is designed to protect orders that accumulate extra events in later chapters — for example saga retries or payment-failure compensation events (Chapters 11–12). To watch the threshold fire right now, **temporarily lower the value to 4** in `StreamRuneConfig.java`, restart, and walk `order-1` through the full lifecycle:

```bash
# Temporarily: .snapshotPolicy(SnapshotPolicy.everyNEvents(4)) in StreamRuneConfig.java, then restart
# order-1 already has: OrderPlaced, OrderConfirmed (2 events)
curl -X POST http://localhost:8080/api/orders/order-1/ship
# 3 events
curl -X POST http://localhost:8080/api/orders/order-1/deliver
# 4 events on the order-1 stream -> snapshot written
```

Set the value back to 5 after the observation.

**Inspect the snapshot table:**

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT aggregate_type, aggregate_id, version, created_at FROM snapshot_store ORDER BY created_at DESC LIMIT 5;"
```

You should see a row `order | order-1 | …` once it crosses the threshold. The `version` column records the event offset at which the snapshot was taken. (Orders that stopped below the threshold — like `order-2` above — never appear here, because the threshold is per stream, not a system-wide total.)

To confirm the snapshot is being used on reads:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT aggregate_type, aggregate_id, COUNT(*) AS event_count FROM event_stream GROUP BY aggregate_type, aggregate_id;"
```

Compare `event_count` against `version` in `snapshot_store`. When `event_count` equals `version`, the `CommandBus` loaded the snapshot and replayed zero trailing events. When `event_count` is larger than `version`, it replayed `event_count - version` events on top of the snapshot.

---

## What We Learned

- **State machine validation in `decide`** is the correct place to enforce lifecycle rules. The guard `if (state.status() != OrderStatus.CONFIRMED) throw new DomainException(...)` runs before any event is written, so an invalid transition is rejected cleanly at the domain boundary. No invalid state ever reaches the event store.

- **`SnapshotPolicy`** is configured once on the `VirtualThreadCommandBus` and applies transparently to every registered aggregate. `SnapshotPolicy.everyNEvents(5)` means the bus writes a snapshot once an individual stream reaches five events since its last snapshot — the count is per stream, not a global total. The decider is unaware: it always receives a fully-reconstructed `AggregateState`, whether that state was assembled from a full replay or from a snapshot plus a short tail.

- **Snapshot storage** uses the same underlying event store. The snapshot record includes the serialised `AggregateState` and the `version` (event offset) at the time of the snapshot. On the next command, the bus reads the snapshot, deserialises the state, then reads only the events with an offset greater than `version` and applies them via `evolve`.

- **Aggregate reconstruction from a snapshot** means the cost of `evolve` is bounded. In the worst case you replay `N - 1` events (one less than the snapshot interval, so 4 with the demo's `everyNEvents(5)`). In the best case you replay zero. A lower `N` in `everyNEvents(N)` reduces the replay window at the cost of more snapshot writes — tune it for your access patterns.

- **`AggregateState` must be serialisable.** `OrderState` is a plain record with primitive fields, a `List`, a `Money` value object, and an enum — all of which serialise cleanly to JSON. If you use custom types in your state record, ensure they have a no-arg constructor or a suitable Jackson deserialiser.

- **`OrderCommand.OrderLine` and `OrderEvent.OrderLine` are separate types.** This duplication is intentional. A command carries intent; an event records a fact. If the command shape changes (for example, a promotion field is added), the event shape can stay stable. The decider mapping between them is the explicit translation layer. Only the command's line checks its product id, because it is built from client input. The event's line is rebuilt from the log, where a check could only make a stored order unreadable.

---

## Next Up

Our order commands are open to everyone. Next, we'll add role-based authorization.
