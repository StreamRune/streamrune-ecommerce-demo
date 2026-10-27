# Chapter 13: Inventory Read Model & Outbox

> **What you'll learn:** How the inventory's read model counts the stock of the three pools Chapter 11's inventory aggregate keeps — available, reserved, and committed — and how StreamRune's transactional outbox pattern reliably forwards a curated, PII-safe set of integration events to RabbitMQ. You will add `InventoryProjection` and wire the four outbox beans (`PostgresOutboxStore`, `EcommerceIntegrationEventMapper`, `RabbitMqConfig` publisher, and `streamrune.outbox.enabled=true`) that make end-to-end delivery real. A second Spring Boot application — the `notifications-service` — subscribes to the queue and exposes `GET /received`, for an `ADMIN`, so you can verify cross-service delivery without touching the e-shop internals.

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

Inventory tracks stock for a single product across three pools — **available**, **reserved** (per order, between its payment and its shipment) and **committed** (confirmed for an order that ships). Chapter 11 built the aggregate — its commands, events, state and decider, and the `POST /api/inventory/{productId}/receive` route — because the fulfillment saga reserves stock and the two process managers release and commit it. This chapter adds what the shop shows of it, the inventory read model, and then the outbox.

---

## Step by Step

### Step 1: Add `InventoryProjection` and register it in `MultiProjectionRunner`

The read model is one row per product with the three counters. Create `queries/src/main/java/org/streamrune/ecommerce/queries/dto/InventoryView.java`:

```java
package org.streamrune.ecommerce.queries.dto;

public record InventoryView(String productId, int available, int reserved, int committed) {}
```

and `projections/src/main/java/org/streamrune/ecommerce/projections/InventoryProjection.java`, which follows the pattern of `ProductProjection` and `OrderProjection`:

```java
package org.streamrune.ecommerce.projections;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.inventory.*;
import org.streamrune.ecommerce.queries.dto.InventoryView;

public class InventoryProjection extends BaseProjection {

  public InventoryProjection(ProjectionRepository repository) {
    super(repository, "inventory");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      if (envelope.event() instanceof InventoryEvent evt) {
        switch (evt) {
          case InventoryEvent.StockReserved e ->
              findById(e.productId(), InventoryView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new InventoryView(
                                  e.productId(),
                                  e.availableAfter(),
                                  existing.reserved() + e.quantity(),
                                  existing.committed())));
          case InventoryEvent.StockReleased e ->
              findById(e.productId(), InventoryView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new InventoryView(
                                  e.productId(),
                                  e.availableAfter(),
                                  existing.reserved() - e.quantity(),
                                  existing.committed())));
          case InventoryEvent.ReservationConfirmed e ->
              findById(e.productId(), InventoryView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new InventoryView(
                                  e.productId(),
                                  existing.available(),
                                  existing.reserved() - e.quantity(),
                                  existing.committed() + e.quantity())));
          case InventoryEvent.ShipmentReceived e ->
              save(
                  e.productId(),
                  new InventoryView(
                      e.productId(),
                      e.availableAfter(),
                      findById(e.productId(), InventoryView.class)
                          .map(InventoryView::reserved)
                          .orElse(0),
                      findById(e.productId(), InventoryView.class)
                          .map(InventoryView::committed)
                          .orElse(0)));
        }
      }
    }
  }

  public InventoryView get(String productId) {
    return findById(productId, InventoryView.class).orElse(null);
  }

  public List<InventoryView> listAll() {
    return repository.findAll(projectionName(), InventoryView.class);
  }
}
```

The projection applies each event's own quantity: `StockReserved` and `StockReleased` carry the order's quantity, and `ReservationConfirmed` carries the quantity that order reserved, not the product's whole reserved pool, so the counters stay right while several orders hold stock at one product.

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

### Step 2: Wire the Outbox Beans

The outbox needs two beans in `StreamRuneConfig.java`, the store and the mapper. The `PostgresOutboxStore` must share the same `DataSource` as the event store — that is what makes the outbox insert and the event append atomic. The `OutboxEventMapper` determines which events are forwarded and in what shape. Both beans are discovered by the auto-configured `postgresEventStoreFactory`.

The mapper is the PII boundary, so it is a class of its own in the shared `commands` module, which the Quarkus and Micronaut apps use too. Create `commands/src/main/java/org/streamrune/ecommerce/commands/integration/EcommerceIntegrationEventMapper.java`:

```java
package org.streamrune.ecommerce.commands.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;

/**
 * Curated, PII-safe outbox mapper for the e-commerce demo.
 *
 * <p>ONLY the allowlisted integration events below are published to the broker. All {@code
 * CustomerEvent.*} and {@code InventoryEvent.*} types — and {@code OrderPlaced} (carries {@code
 * customerId}) and {@code PaymentInitiated} — are explicitly excluded. Each published entry
 * contains a small, hand-crafted integration DTO with ONLY non-PII business fields. The payment
 * outcomes published are {@code PaymentCaptured}, {@code PaymentRefunded}, {@code PaymentVoided}
 * and {@code PaymentFailed}.
 *
 * <p>Every entry carries a {@code streamId} — the stream the event was appended to ({@code
 * order:<id>}, {@code payment:<id>}, {@code product:<id>}): the demo's outbox channel is {@code
 * STRICT_PER_AGGREGATE}, where a {@code null} stream fails the append with {@code
 * OutboxOrderingViolationException}.
 */
public class EcommerceIntegrationEventMapper implements OutboxEventMapper {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public List<OutboxEntry> toOutbox(EventEnvelope envelope) {
    String eventId = envelope.metadata().eventId().value();
    StreamId stream = envelope.streamId();
    return switch (envelope.event()) {
      // --- Order lifecycle (OrderPlaced excluded — carries customerId) ---
      case OrderEvent.OrderConfirmed e ->
          List.of(entry(eventId, new OrderConfirmedDto(e.orderId()), "OrderConfirmed", stream));
      case OrderEvent.OrderShipped e ->
          List.of(entry(eventId, new OrderShippedDto(e.orderId()), "OrderShipped", stream));
      case OrderEvent.OrderDelivered e ->
          List.of(entry(eventId, new OrderDeliveredDto(e.orderId()), "OrderDelivered", stream));
      case OrderEvent.OrderCancelled e ->
          List.of(
              entry(
                  eventId,
                  new OrderCancelledDto(e.orderId(), e.reason()),
                  "OrderCancelled",
                  stream));

      // --- Payment outcomes (PaymentInitiated excluded) ---
      case PaymentEvent.PaymentCaptured e ->
          List.of(
              entry(
                  eventId,
                  new PaymentCapturedDto(e.paymentId(), e.orderId()),
                  "PaymentCaptured",
                  stream));
      case PaymentEvent.PaymentRefunded e ->
          List.of(
              entry(
                  eventId,
                  new PaymentRefundedDto(e.paymentId(), e.reason()),
                  "PaymentRefunded",
                  stream));
      case PaymentEvent.PaymentVoided e ->
          List.of(
              entry(
                  eventId,
                  new PaymentVoidedDto(e.paymentId(), e.reason()),
                  "PaymentVoided",
                  stream));
      case PaymentEvent.PaymentFailed e ->
          List.of(
              entry(
                  eventId,
                  new PaymentFailedDto(e.paymentId(), e.reason()),
                  "PaymentFailed",
                  stream));

      // --- Product catalog ---
      case ProductEvent.ProductCreated e ->
          List.of(
              entry(
                  eventId,
                  new ProductCreatedDto(
                      e.productId(),
                      e.name(),
                      e.category(),
                      e.price().amount(),
                      e.price().currency(),
                      e.stock()),
                  "ProductCreated",
                  stream));
      case ProductEvent.PriceUpdated e ->
          List.of(
              entry(
                  eventId,
                  new PriceUpdatedDto(
                      e.productId(),
                      e.previousPrice().amount(),
                      e.previousPrice().currency(),
                      e.newPrice().amount(),
                      e.newPrice().currency()),
                  "PriceUpdated",
                  stream));
      case ProductEvent.StockAdjusted e ->
          List.of(
              entry(
                  eventId,
                  new StockAdjustedDto(e.productId(), e.previousStock(), e.newStock(), e.reason()),
                  "StockAdjusted",
                  stream));
      case ProductEvent.ProductDiscontinued e ->
          List.of(
              entry(
                  eventId,
                  new ProductDiscontinuedDto(e.productId()),
                  "ProductDiscontinued",
                  stream));

      // --- Everything else: CustomerEvent.*, InventoryEvent.*, OrderPlaced,
      //                       PaymentInitiated — excluded to prevent PII leakage ---
      default -> List.of();
    };
  }

  private OutboxEntry entry(String eventId, Object dto, String payloadType, StreamId stream) {
    try {
      return OutboxEntry.pending(
          OutboxEntryId.of(eventId), OBJECT_MAPPER.writeValueAsString(dto), payloadType, stream);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize integration event DTO: " + payloadType, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Integration DTOs — contain only non-PII business fields
  // ---------------------------------------------------------------------------

  record OrderConfirmedDto(String orderId) {}

  record OrderShippedDto(String orderId) {}

  record OrderDeliveredDto(String orderId) {}

  record OrderCancelledDto(String orderId, String reason) {}

  record PaymentCapturedDto(String paymentId, String orderId) {}

  record PaymentRefundedDto(String paymentId, String reason) {}

  record PaymentVoidedDto(String paymentId, String reason) {}

  record PaymentFailedDto(String paymentId, String reason) {}

  record ProductCreatedDto(
      String productId,
      String name,
      String category,
      BigDecimal price,
      String currency,
      int stock) {}

  record PriceUpdatedDto(
      String productId,
      BigDecimal previousPrice,
      String previousCurrency,
      BigDecimal newPrice,
      String newCurrency) {}

  record StockAdjustedDto(String productId, int previousStock, int newStock, String reason) {}

  record ProductDiscontinuedDto(String productId) {}
}
```

Then the two beans (imports `org.streamrune.core.outbox.OutboxEventMapper`, `org.streamrune.core.outbox.OutboxOrderingMode` and `org.streamrune.ecommerce.commands.integration.EcommerceIntegrationEventMapper`; `PostgresOutboxStore` comes with `org.streamrune.postgres.*`):

```java
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
```

The `PostgresOutboxStore` constructor takes the claim lease (two minutes here — the maximum time a poller holds an `IN_PROGRESS` entry before another instance may reclaim it) and the ordering mode. Strict mode also makes `streamId` mandatory: every `OutboxEntry` the mapper returns must name the stream whose order the consumer depends on — `EcommerceIntegrationEventMapper` passes `envelope.streamId()`, the event's own stream (`order:<id>`, `payment:<id>`, `product:<id>`) — and a `null` stream fails the append with `OutboxOrderingViolationException`. The `OutboxEventMapper` is `EcommerceIntegrationEventMapper`, which lives in the `commands` module at `org.streamrune.ecommerce.commands.integration`. Its `toOutbox(EventEnvelope)` method returns an empty list for excluded events and a single-entry list (with a compact integration DTO) for allowed ones.

The whole class at the end of this chapter:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 13</summary>

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

You also need to enable the outbox and tell the app where the broker is, in `spring-app/src/main/resources/application.yml`. Add the `rabbitmq` block under the existing `spring:` key and the `outbox` block under the existing `streamrune:` key:

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

The publisher's connection (Step 4) reads the broker address from these `spring.rabbitmq.*` properties, and opens on the first publish, not at startup. The defaults reach a broker on `localhost`; under Docker Compose (Step 5) the backend sets `RABBITMQ_HOST` to the broker's service name.

`streamrune.outbox.enabled=true` activates the auto-configured `OutboxPoller`. Without it the poller bean is not created and delivered entries accumulate indefinitely. `retention-max-age: 7d` tells the auto-configured `OutboxRetentionSweeper` to delete `DELIVERED` entries older than seven days, preventing unbounded table growth. `skipped-retention-max-age: 30d` keeps the audit rows of operator skips (Chapter 14) for thirty days; `FAILED` entries are never pruned — they block their aggregate until someone resolves them.

### Step 3: Add Build Dependencies for RabbitMQ

Before wiring `RabbitMqConfig`, add the two RabbitMQ-related dependencies to `spring-app/build.gradle.kts`. The `streamrune-postgres` artifact (for `PostgresOutboxStore`) is already present; add the new lines if they are not there:

```kotlin
// spring-app/build.gradle.kts — inside the dependencies { } block
implementation("org.streamrune:streamrune-postgres:$sr")       // already present (outbox store + event store)
implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr") // adds RabbitMqOutboxPublisher
implementation("org.springframework.boot:spring-boot-starter-amqp") // adds Spring AMQP, @RabbitListener and the RabbitMQ Java client
```

Without `streamrune-rabbitmq-outbox` and `spring-boot-starter-amqp`, Step 4's `RabbitMqConfig.java` imports will fail to resolve at compile time.

The publisher class of Step 4 lives in the shared `commands` module, next to `EcommerceIntegrationEventMapper`, because the Quarkus and Micronaut apps wire the same class. Add its three dependencies to `commands/build.gradle.kts`. The RabbitMQ Java client is an `api` dependency: the class takes the app's `ConnectionFactory` and hands its topology a `Channel`, so the client is part of what the module offers its users:

```kotlin
// commands/build.gradle.kts — inside the dependencies { } block
implementation("org.streamrune:streamrune-rabbitmq-outbox:$sr")
api(libs.rabbitmq.client)
implementation(libs.slf4j.api)
```

### Step 4: Wire the RabbitMQ Publisher

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
 * <p><b>The factory bounds the connect.</b> The relay's publish deadline bounds nothing while the
 * connection opens: the attempt blocks for the factory's connection timeout plus its handshake
 * timeout ({@link #connectBound()}). The amqp-client's defaults, 60 s and 10 s, would let a broker
 * whose packets are dropped (a firewall, a dead address behind a stable name) hold the relay about
 * 70 s per poll cycle, with the claimed batch {@code IN_PROGRESS} — out of {@code
 * streamrune.outbox.pending} and the outbox listing of {@code PENDING} entries. The apps therefore
 * set {@link #CONNECTION_TIMEOUT} and {@link #HANDSHAKE_TIMEOUT}, 10 s each. A refused connection
 * (no broker listening) fails at once.
 *
 * <p>The in-flight horizon is the one the {@link RabbitMqOutboxPublisher} reports once built — its
 * {@link RabbitMqOutboxPublisher#DEFAULT_PUBLISH_BLOCK_BUDGET default publish-block budget} plus
 * the confirm timeout — and is known before the first connection, because the relay reads it when
 * it is built, to check the claim lease against it. The connect is not part of it: nothing is
 * handed to the broker while the connection opens, and the {@link RabbitMqOutboxPublisher} checks
 * the relay's deadline before each publish, so a connect that ends past the deadline publishes
 * nothing and the relay returns the batch to {@code PENDING}; an entry it does publish still has
 * the whole horizon of its claim lease left.
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
   * The publisher could not get a channel ready, so nothing of the batch was published: the
   * connection could not be opened, or, on an open connection, the publisher's channel, a topology
   * declaration or confirm mode failed; or the publisher is closed. The message names the step that
   * failed, and the cause is the client's own exception, e.g. {@code java.net.ConnectException:
   * Connection refused}.
   */
  public static final class BrokerUnreachableException extends IOException {

    BrokerUnreachableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * How long the publisher's connection may take to open, the TCP connect, unless the application
   * configures its own: what the three apps set on the factory they hand over.
   */
  public static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(10);

  /** How long the AMQP handshake may take once the TCP connection is open; the apps set it too. */
  public static final Duration HANDSHAKE_TIMEOUT = Duration.ofSeconds(10);

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

  /**
   * The longest a connection attempt can hold the relay: the factory's connection timeout plus its
   * handshake timeout.
   */
  public Duration connectBound() {
    return Duration.ofMillis(connectionFactory.getConnectionTimeout())
        .plusMillis(connectionFactory.getHandshakeTimeout());
  }

  /**
   * Whether the publisher has opened its connection and is not closed. The connection may be
   * recovering from a broker restart at the moment.
   */
  public boolean isConnected() {
    return delegate != null && !closed;
  }

  /**
   * Closes the connection, if one was opened. A publish afterwards hands nothing to the broker:
   * with no connection opened yet it fails with a {@link BrokerUnreachableException}; on the closed
   * connection's channel the {@link RabbitMqOutboxPublisher} fails it as a transport failure. The
   * application calls it at shutdown, after the relay stopped.
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
      String step = "open a channel";
      try {
        opened = connectionFactory.newConnection(connectionName);
        Channel channel = opened.createChannel();
        if (channel == null) {
          throw new IOException("the connection has no channel left for the outbox publisher");
        }
        step = "declare the topology";
        topology.declare(channel);
        step = "put its channel into confirm mode";
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
            opened == null
                ? "RabbitMQ is not reachable at "
                    + address()
                    + ", nothing was published: "
                    + describe(e)
                : "RabbitMQ at "
                    + address()
                    + " is connected, but the outbox publisher could not "
                    + step
                    + ", nothing was published: "
                    + describe(e),
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

Until it has connected, the publisher fails every entry of a batch with its own `BrokerUnreachableException`, before anything reaches a broker — its message says whether the broker could not be reached or answered and refused a step, such as a topology declaration — and `classifyFailure` reports that as `TRANSPORT`: the relay keeps the entries `PENDING`, records the error in `last_error`, counts no attempt against the retry ladder (`streamrune.outbox.retry-max-attempts`) and tries again on a backoff that grows from one second towards the relay's 60-second cap. Once the broker answers, the next attempt connects, declares the topology and delivers the entries. Every later failure comes from the `RabbitMqOutboxPublisher`, which classifies its own. The relay reads `inFlightHorizon()` when it is built, before any connection, to check the claim lease against it, so the class reports the horizon the `RabbitMqOutboxPublisher` would: its default 30-second publish-block budget plus the confirm timeout. The connect is not part of it: nothing is handed to the broker while the connection opens, and the `RabbitMqOutboxPublisher` checks the relay's deadline before each publish, so a connect that ends past the deadline publishes nothing.

The relay's deadline does not bound the connect itself, so the connection factory must: the apps set a 10-second connection timeout and a 10-second handshake timeout (`CONNECTION_TIMEOUT`, `HANDSHAKE_TIMEOUT`; Spring takes `spring.rabbitmq.connection-timeout` when it is set). A refused connection — no broker listening — fails at once. A broker whose packets are dropped, behind a firewall or at a dead address, would otherwise hold the relay for the client's own 60 seconds plus 10 on every poll cycle, with the claimed entries `IN_PROGRESS` and out of the `PENDING` listing and the backlog gauge below.

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
   * <p>The connection attempt is bounded: {@code spring.rabbitmq.connection-timeout} when it is
   * set, otherwise {@link LazyRabbitMqOutboxPublisher#CONNECTION_TIMEOUT} (10 s), plus a 10 s
   * handshake timeout. The relay's publish deadline bounds nothing while the publisher connects,
   * and the client's own 60-second connection timeout would let a broker whose packets are dropped
   * hold the relay about 70 seconds per poll cycle, with the claimed entries {@code IN_PROGRESS}.
   *
   * <p>The factory takes {@code spring.rabbitmq.host}, {@code port}, {@code username}, {@code
   * password}, {@code virtual-host} and {@code connection-timeout}, the properties Spring AMQP
   * uses. TLS settings under {@code spring.rabbitmq.ssl} are not applied here; a TLS deployment
   * configures the factory itself.
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
    Duration connectionTimeout = rabbit.getConnectionTimeout();
    factory.setConnectionTimeout(
        (int)
            (connectionTimeout != null
                    ? connectionTimeout
                    : LazyRabbitMqOutboxPublisher.CONNECTION_TIMEOUT)
                .toMillis());
    factory.setHandshakeTimeout((int) LazyRabbitMqOutboxPublisher.HANDSHAKE_TIMEOUT.toMillis());
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

- The `outbox_events` table holds the waiting entries — `status` `PENDING`, `attempts` `0`, and `last_error` naming the broker, e.g. `RabbitMQ is not reachable at localhost:5672, nothing was published: java.net.ConnectException: Connection refused` (Step 6e reads it; Chapter 14 adds `GET /api/admin/outbox` for it).
- The relay logs one `WARN` per poll cycle: `Outbox delivery hit a broker-wide TRANSPORT failure for 3 of 3 claimed entries (...) retry deferred by up to ... — escalating toward the 60s cap while the failures last: RabbitMQ is not reachable at ...`.
- `streamrune.outbox.pending` (`/actuator/metrics/streamrune.outbox.pending`) counts the backlog, and `/actuator/health` stays `UP`: the `relay.outbox-relay` block (Chapter 15) reads `UP`, because a refused publish fails an entry, not the relay's poll cycle.
- When the broker answers, the publisher logs `Outbox publisher connected to RabbitMQ at localhost:5672`, the entries are delivered in order, and their rows read `DELIVERED`. An entry that waited longest waits up to the 60-second cap for its next attempt.

A broker that goes away after the publisher connected shows the same way, with the client's own error in `lastError` (the client reconnects by itself). `OutboxBrokerDownStartupIT` in each app proves the startup case: it starts the app with a broker port nothing listens on, expects `UP` health, a command answered `200` and its entry `PENDING` with the refusal recorded and no attempt counted, then starts RabbitMQ on that port and expects the entry `DELIVERED` and on the queue once.

Spring Boot adds a health check of its own for RabbitMQ with `spring-boot-starter-amqp`. It opens Spring AMQP's connection, which the app does not publish on, and would answer `DOWN` for the whole app while the broker is down — taking an app that serves out of a load balancer's rotation. Switch it off in `spring-app/src/main/resources/application.yml`; the outbox's own signals above show the outage:

```yaml
management:
  health:
    rabbit:
      enabled: false
```

### Step 5: Create the `notifications-service` Module and Wire docker-compose

The `notifications-service` is a separate, headless Spring Boot application. It subscribes to the `streamrune.integration` queue via `@RabbitListener` and exposes two endpoints on port 8090: `GET /received`, which returns the last 200 received integration events from memory to an `ADMIN`, and `GET /health`, an open `200` without a body. It runs independently of the e-shop — no access to the e-shop's database, no awareness of the event store. That decoupling is the whole point of the outbox pattern.

**5a. Register the module in `settings.gradle.kts`.** Add it to the `include(...)` call alongside the other modules:

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

**5b. Create `notifications-service/build.gradle.kts`:**

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

**5c. Create the four Java source files** under `notifications-service/src/main/java/org/streamrune/ecommerce/notifications/`:

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

**5d. Create `notifications-service/src/main/resources/application.yml`:**

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

**5e. Create `notifications-service/Dockerfile`.** The Dockerfile copies a pre-built JAR, so build the JAR locally before running `docker compose build`:

```dockerfile
# Pre-built JAR approach: run ./gradlew :notifications-service:bootJar locally first,
# then docker compose build copies the result. This avoids needing the
# StreamRune framework source inside the Docker build context.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY notifications-service/build/libs/notifications-service-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
```

**5f. Add the broker, the backend and the notifications service to the Compose file.** The backend runs from the JAR that `bootJar` builds, in a container of its own.

Create `Dockerfile.backend` in the project root:

```dockerfile
# Pre-built JAR approach: run ./gradlew :spring-app:bootJar locally first,
# then docker compose build copies the result. This avoids needing the
# StreamRune framework source inside the Docker build context.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY spring-app/build/libs/spring-app-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
```

and replace `docker-compose.yml` with the stack of this chapter:

```yaml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: streamrune_ecommerce
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10

  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      # check_port_connectivity, not ping: ping answers as soon as the Erlang node runs, seconds
      # before the AMQP listener accepts connections, and the backend and the notifications
      # service connect the moment this check passes.
      test: ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"]
      interval: 5s
      timeout: 10s
      retries: 10

  backend:
    build:
      context: .
      dockerfile: Dockerfile.backend
    ports:
      - "8080:8080"
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/streamrune_ecommerce
      SPRING_DATASOURCE_USERNAME: postgres
      SPRING_DATASOURCE_PASSWORD: postgres
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_USERNAME: guest
      RABBITMQ_PASSWORD: guest
      APP_SAGA_FULFILLMENT_TIMEOUT: ${APP_SAGA_FULFILLMENT_TIMEOUT:-15m}
      APP_SAGA_TIMEOUT_POLL_INTERVAL: ${APP_SAGA_TIMEOUT_POLL_INTERVAL:-10s}
    # The backend starts without the broker: its outbox publisher connects on the first publish.
    # Waiting for the broker lets the first integration events go out at once.
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy

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

volumes:
  pgdata:
```

Port 5672 is the AMQP port; 15672 exposes the management UI at `http://localhost:15672` (guest/guest). The health check is `check_port_connectivity` rather than `ping`: `ping` succeeds as soon as the broker's Erlang node runs, several seconds before port 5672 accepts connections, and the services that wait for `service_healthy` connect the moment the check passes. The `backend` service reaches the database and the broker by their service names: `SPRING_DATASOURCE_URL` points at `postgres`, and `RABBITMQ_HOST` at `rabbitmq` — without it the backend's outbox publisher looks for the broker on `localhost` inside its own container and never connects: the backend serves, and its entries wait `PENDING`. The `APP_SAGA_*` variables set the saga's deadline and the timeout runner's poll interval of Chapter 11, here to their defaults. The backend starts without the broker (Step 4); waiting for it only lets the first integration events go out at once instead of on the relay's backoff.

The images copy the two JARs that `bootJar` builds, the backend's and the notifications service's, so Step 6 builds them before it starts the stack.

Your project needs no `.dockerignore` for these `COPY` lines. The demo's repository has one that keeps every `build/` directory out of the Docker build context except the two the Dockerfiles copy from, `spring-app/build/libs` and `notifications-service/build/libs`; if you add one, re-include both (`!spring-app/build/libs`, `!notifications-service/build/libs`), or the `COPY` of a JAR fails with *file not found*.

The `notifications-service` deduplicates messages by `entryId` (the AMQP `messageId` property, set by `RabbitMqOutboxPublisher` to `entry.id().value()`). If the broker delivers a message twice — because the poller restarted after publishing but before marking it `DELIVERED` — the service discards the duplicate silently.

The entry id tells a delivery apart from another delivery of the same entry, not one fact from another fact. One order can publish `OrderCancelled` twice, as two entries: a customer who cancels while the saga still fulfills the order stores one, and the saga's own compensation stores the second ten to twenty seconds later (Chapter 11); a cancellation sent again stores one more each time, since the order decider appends `OrderCancelled` for a cancelled order too, so that every cancellation runs the undo. A consumer that acts on a cancellation — a refund at a payment provider, an e-mail to the customer — keys its effect by the order id, or it acts twice. The notifications service only records what arrives.

### Step 6: Verify End-to-End Delivery

Start the full stack. Stop the `./gradlew :spring-app:bootRun` of the earlier chapters first: the backend container takes port 8080. Build both JARs (the Dockerfiles copy pre-built jars — see Step 5e), then start everything in the background:

```bash
./gradlew :spring-app:bootJar :notifications-service:bootJar
docker compose up -d --build
```

`docker compose ps` lists the four services of this chapter's file; wait until `postgres` and `rabbitmq` read `healthy` and the backend has started (`docker compose logs -f backend` shows `Started SpringEcommerceApplication`). The database is the one the earlier chapters filled, so `p-1`, its stock from Chapter 11 and the customer `c-1` are there.

**6a. Place and confirm an order.** Confirming an order emits `OrderConfirmed`, which `EcommerceIntegrationEventMapper` includes in the outbox. Place and confirm an order:

```bash
# Place an order of the product p-1
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-1" -H "X-User-Role: CUSTOMER" \
  -d '{"orderId":"o-test-1","customerId":"c-1","lines":[{"productId":"p-1","quantity":1}]}'

# The saga automatically progresses through payment and inventory;
# once PaymentCaptured fires, OrderConfirmed follows automatically.
```

**6b. Check the notifications service.** Within a couple of seconds the poller claims the outbox entry and publishes it to RabbitMQ. The `notifications-service` receives it and stores it in memory:

```bash
curl -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8090/received
```

You should see a JSON array containing an entry whose `type` is `"OrderConfirmed"` and whose `body` contains the `orderId`. The response is newest-first. Without the two headers the answer is `403`: the bodies name orders, and the service shows them to an `ADMIN` only. `curl -i http://localhost:8090/health` answers `200` with an empty body to anyone.

**6c. Confirm customer events are NOT forwarded.** Register a new customer and verify no notification arrives:

```bash
curl -s -X POST http://localhost:8080/api/customers \
  -H "Content-Type: application/json" \
  -H "X-User-Id: c-pii-test" -H "X-User-Role: CUSTOMER" \
  -d '{"customerId":"c-pii-test","name":"Eve","email":"eve@example.com"}'

# Wait a moment, then check — the count should not increase
curl -s -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN" http://localhost:8090/received | python3 -m json.tool | grep -c '"type"'
```

The count does not change. `CustomerRegistered` is excluded by `EcommerceIntegrationEventMapper` (the `default -> List.of()` branch), so no outbox entry is written and no message is published. Customer PII never leaves the e-shop's database.

**6d. Optional — RabbitMQ management UI.** Visit `http://localhost:15672` (guest/guest). The `streamrune.events` exchange and `streamrune.integration` queue are visible under Exchanges and Queues. The queue's message rate chart shows the delivery spike when orders are confirmed. Under Connections, the one named `streamrune-outbox-publisher` is the outbox publisher's own connection from Step 4.

**6e. Optional — stop the broker.** The shop keeps working without RabbitMQ:

```bash
docker compose stop rabbitmq
# Place and confirm another order as in 6a: the requests answer as before.
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT payload_type, status, attempts, last_error FROM outbox_events WHERE status <> 'DELIVERED';"
docker compose start rabbitmq
```

While the broker is stopped, the order's `OrderConfirmed` entry is listed `PENDING` with the client's error in `lastError`, the backend logs the relay's `WARN` each poll cycle, and `/actuator/health` stays `UP` (Step 4). Once the broker is back, the client reconnects, the relay's next attempt delivers the entry — within its backoff, at most a minute — and `/received` on the notifications service lists it.

**6f. Back to `bootRun`.** The later chapters change the backend's code, and a container runs the JAR it was built from. Stop the backend's container and run the application from Gradle again, against the database, the broker and the notifications service that keep running in Docker:

```bash
docker compose stop backend
./gradlew :spring-app:bootRun
```

To run the backend in Docker instead, build its JAR again after each change and recreate the container: `./gradlew :spring-app:bootJar` and `docker compose up -d --build backend`.

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

**`LazyRabbitMqOutboxPublisher`** opens that connection and builds the `RabbitMqOutboxPublisher` on the first publish, so the application starts and serves commands while the broker is down. Until the broker answers, every publish fails as a transport failure: the entries stay `PENDING` with the error in `lastError`, no attempt is counted, the relay retries on a backoff capped at 60 s, and the outbox drains once the broker is back. `streamrune.outbox.pending`, the relay's `WARN` line and the `outbox_events` table show the backlog; health stays `UP`.

**notifications-service** is a separate Spring Boot application. It has no access to the e-shop's database — it only sees what arrives on the `streamrune.integration` queue. This is the key decoupling: the e-shop publishes integration events; downstream consumers are fully independent services. The notifications service deduplicates by `entryId` to handle at-least-once delivery.

**Transactional outbox pattern** trades direct publish for durability. The invariant: if an event was committed, its outbox entry exists, and the poller eventually delivers it — unless an operator deliberately skips it, an audited decision that is not a delivery. This is the correct foundation for integrating with any system you do not own.

**At-least-once delivery** is a consequence. Consumers must handle duplicates — the `notifications-service` does so by tracking seen `entryId` values.

---

## Next Up

What happens when a projection fails to process an event? Next: dead letter queue.
