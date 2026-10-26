# Chapter 10 — Audit Logging

> **What you'll learn:**
> - How to annotate commands with `@Auditable` to opt them into audit recording
> - How `AuditCommandInterceptor` captures who ran a command, what it was, when it ran, and whether it succeeded
> - How `PostgresAuditStore` persists entries to an append-only `audit_log` table
> - How `AuditingQueryBus` transparently logs sensitive queries without touching query handlers
> - How to expose audit data through a REST endpoint and a `ComplianceReportQuery`

---

## What We're Building and Why

Every production e-commerce system needs an audit trail. Regulators ask "who changed this order?" Support teams ask "why did this payment fail?" Security teams ask "which admin accounts were active last Tuesday?" Without an audit log, answering these questions means replaying events and guessing from timestamps. With one, every command execution is a first-class record.

StreamRune's audit system has three parts that work together.

**Command auditing** is handled by `AuditCommandInterceptor`, a `CommandInterceptor` that runs for every command. In `before()` it captures the caller's identity from `StreamRuneContext`. In `after()` it writes an `AuditEntry` with the outcome, the aggregate targeted, and the number of events produced. In `onError()` it writes a `FAILURE` entry with the error message. This happens regardless of whether the command succeeded or failed — an attempted unauthorized action is exactly the kind of thing an audit log should capture.

**Query auditing** is selective. Most queries — list orders, get a product — are routine read traffic and do not need to be recorded. Sensitive queries do. You signal this by placing `@Auditable` on the query class. `AuditingQueryBus` is a decorator that wraps your existing query bus. When it receives a query, it checks for `@Auditable` using a cached reflection lookup. If the annotation is absent, the query passes through untouched. If it is present, the bus records the dispatch to `AuditStore` both on success and on failure. This is the same pattern as the interceptor: it wraps, it delegates, it records.

**Storage** is handled by `PostgresAuditStore`, which writes to an `audit_log` table created by Flyway. The table is append-only — entries are never updated or deleted. The same table is used for both command and query audit entries; queries carry no aggregate type or id and `eventCount=0`.

---

## Step by Step

### Step 1 — Add @Auditable to order commands

The `@Auditable` annotation on commands is not actually what drives command auditing — `AuditCommandInterceptor` records every command regardless. Where `@Auditable` matters is on **queries** (Step 4). However, commands in the demo are already annotated as a documentation signal that these records are audit-significant. Open `domain/src/main/java/org/streamrune/ecommerce/domain/order/OrderCommand.java` and confirm it matches the source (no changes needed here — the annotation is on the query side):

```java
package org.streamrune.ecommerce.domain.order;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.streamrune.core.Command;
import org.streamrune.core.RequireRole;
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
   *
   * <p>The lines are not empty and name each product once: stock is reserved per order and product,
   * so two lines of one product would be reserved once. The message never echoes a product id.
   */
  record PlaceOrder(String orderId, String customerId, List<OrderLine> lines)
      implements OrderCommand {

    /** The prefix of the saga id the order starts: {@code "fulfillment-" + orderId}. */
    public static final String SAGA_ID_PREFIX = "fulfillment-";

    /** The longest order id whose saga id {@code "fulfillment-" + orderId} fits 255 characters. */
    public static final int MAX_ORDER_ID_LENGTH =
        IdConstraints.MAX_LENGTH - SAGA_ID_PREFIX.length();

    public PlaceOrder {
      requireValidOrderId(orderId);
      if (lines == null || lines.isEmpty()) {
        throw new IllegalArgumentException("lines must not be empty");
      }
      Set<String> products = new HashSet<>();
      for (OrderLine line : lines) {
        if (line == null) {
          throw new IllegalArgumentException("lines must not hold null");
        }
        if (!products.add(line.productId())) {
          throw new IllegalArgumentException("lines must name each product once");
        }
      }
      lines = List.copyOf(lines);
    }

    /**
     * The length rule of an order id: at most {@value #MAX_ORDER_ID_LENGTH} characters, so that the
     * saga id fits. A blank id or one with a control character is the command bus's to refuse. The
     * message never echoes the id.
     */
    public static void requireValidOrderId(String orderId) {
      if (orderId != null && orderId.length() > MAX_ORDER_ID_LENGTH) {
        throw new IllegalArgumentException(
            "orderId must be at most "
                + MAX_ORDER_ID_LENGTH
                + " characters, got "
                + orderId.length());
      }
    }
  }

  /**
   * Confirms a placed order. The fulfillment saga sends it once the payment is captured and every
   * line reserved; the order decider's guard refuses it from anybody else.
   */
  record ConfirmOrder(String orderId) implements OrderCommand {}

  @RequireRole("ADMIN")
  record ShipOrder(String orderId) implements OrderCommand {}

  @RequireRole("ADMIN")
  record DeliverOrder(String orderId) implements OrderCommand {}

  record CancelOrder(String orderId, String reason) implements OrderCommand {}

  /**
   * One line of an order to place. The constructor checks the product id, so every app that builds
   * a {@code PlaceOrder} applies the same rule: a client-supplied product id must become a valid
   * inventory id. The saga reserves stock on the inventory aggregate whose id is the product id,
   * and the inventory extractor builds that id with {@code AggregateId.of}, which refuses a control
   * character or an id longer than 255 characters. Refused only there, the order would already be
   * placed and paid for, and the saga would cancel it and the cancellation refund it. Refused here,
   * the order endpoint answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>The order endpoints take a product and a quantity from the client and the unit price from
   * the product catalogue.
   *
   * <p>{@link OrderEvent.OrderLine}, the line an {@code OrderPlaced} event records, has no such
   * rule: it is rebuilt from the event log and from saga state, where a rule would make a stored
   * order unreadable instead of stopping a write.
   */
  record OrderLine(String productId, int quantity, Money unitPrice) {

    /** The longest product id: it is the inventory aggregate id, so the id bound applies as is. */
    public static final int MAX_PRODUCT_ID_LENGTH = IdConstraints.MAX_LENGTH;

    public OrderLine {
      requireValidProductId(productId);
      if (quantity <= 0) {
        throw new IllegalArgumentException("quantity must be positive, got " + quantity);
      }
      if (unitPrice == null || unitPrice.amount() == null || unitPrice.amount().signum() <= 0) {
        throw new IllegalArgumentException("unitPrice must be positive");
      }
    }

    /**
     * The rule a product id of an order line follows: present, at most {@value
     * #MAX_PRODUCT_ID_LENGTH} characters, no control character. The message never echoes the id.
     */
    public static void requireValidProductId(String productId) {
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

Order commands do not carry `@Auditable` themselves because `AuditCommandInterceptor` already records every command. The annotation is reserved for queries, where auditing must be selective.

### Step 2 — The PostgresAuditStore bean

The audit interceptor writes through the `PostgresAuditStore` bean that `StreamRuneConfig` declares since Chapter 7, where `ForgetSubjectService` records every forget with it:

```java
@Bean
public PostgresAuditStore auditStore(DataSource ds) {
  return new PostgresAuditStore(ds);
}
```

`PostgresAuditStore` implements the `AuditStore` interface from `streamrune-core`. Its `save(AuditEntry)` method executes a single `INSERT` into the `audit_log` table. The table carries an `id BIGSERIAL` surrogate primary key plus the columns `command_id`, `command_type`, `aggregate_type`, `aggregate_id`, `user_id`, `occurred_at`, `outcome`, `error_message`, `event_count`, and `correlation_id`. `aggregate_type` is the registered type of the command's aggregate (`NULL` for query-origin and subject entries); `idx_audit_aggregate` indexes `(aggregate_type, aggregate_id)`. The framework's `V001__streamrune_baseline.sql` creates the table already in this final shape — including the `correlation_id` column and a nullable `command_id`, so query-origin entries can leave it unset. The event store factory applied that baseline on the application's first start (Chapter 1), so the table is already there.

### Step 3 — Create AuditCommandInterceptor bean and add it to the interceptor chain

Still in `StreamRuneConfig.java`, add a bean for the interceptor:

```java
@Bean
public AuditCommandInterceptor auditInterceptor(PostgresAuditStore store) {
  return new AuditCommandInterceptor(store);
}
```

Then wire it into the `VirtualThreadCommandBus` builder, in front of the two interceptors of chapters 3 and 9 (`AuditCommandInterceptor` comes with `org.streamrune.runtime.*`). The command bus bean now reads:

```java
@Bean
public VirtualThreadCommandBus commandBus(
    EventStore store,
    BeanValidationInterceptor validation,
    AnnotationAuthorizationInterceptor auth,
    AuditCommandInterceptor audit) {
  return VirtualThreadCommandBus.builder()
      .eventStore(store)
      .interceptors(audit, auth, validation)
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
      .build();
}
```

Interceptor ordering matters here: `audit` goes first, in front of `auth`. The bus delivers `onError()` only to the interceptors whose `before()` completed. Behind the authorization interceptor, the audit interceptor would never see a refused command: authorization throws before audit's `before()` runs, and the attempt leaves no trace. In front, audit's `before()` has already run when authorization refuses, so its `onError()` records the attempt as a `FAILURE` entry with the caller's id and the refusal message. A command Bean Validation rejects is recorded the same way. An attempt to do something you may not do is exactly what an audit log exists to capture. This is the framework's canonical order (`CommandInterceptorOrdering` in `streamrune-runtime`); the Quarkus and Micronaut apps get it from their integrations, which sort the chain they assemble, and all three apps check it with the same test (`shipOrderWithoutAdminRoleReturns403AndIsAuditedAsAFailure`).

`AuditCommandInterceptor` captures the caller's user ID in `before()` by reading `StreamRuneContext.CURRENT`, and the bound `CorrelationId` with it, so each audit row is tied back to the originating business flow (the correlation id is `null` when no request context was bound). For a command a saga dispatches, and for a command that continues a saga's event (the payment process manager of Chapter 11), that id is the saga's own, `streamrune:saga:<sagaId>`. The request filter refuses the `streamrune:` prefix from a client (Chapter 9), so a row that carries it was written by a command that ran with that saga's origin bound. It pushes both onto a per-thread stack, so they remain available in `after()` and `onError()`, which run on the same thread after the `ScopedValue` scope may have moved. It is a stack rather than a single slot because a process manager can dispatch a nested command on the same thread before the outer command's callback runs. In `after()`, it writes a `SUCCESS` entry with the event count from `ctx.result()`, or a `VETOED` entry when a later interceptor's `before()` returned `false` and the command did not run. In `onError()`, it writes a `FAILURE` entry with the exception message and `eventCount=0`. Each callback takes its frame off the stack before writing and removes the `ThreadLocal` once the stack is empty, so a pooled thread carries nothing over.

### Step 4 — Wrap the `queryBus` bean with `AuditingQueryBus`

Chapter 5 registered the query handlers on the `cachingQueryBus` bean, and the controllers have injected it as their `QueryBus`. Wrap it: the `queryBus` bean is an `AuditingQueryBus` around the caching bus, and the `cachingQueryBus` bean and its handler registrations are untouched. With two `QueryBus` beans in the context, Spring hands `ProductQueryController`'s `QueryBus queryBus` parameter the bean whose name matches it, `queryBus`.

Add the `queryBus` bean to `StreamRuneConfig`:

```java
@Bean
public QueryBus queryBus(CachingQueryBus cachingBus, PostgresAuditStore auditStore) {
  return new AuditingQueryBus(cachingBus, auditStore);
}
```

(The parameter name is arbitrary — Spring injects the `CachingQueryBus` bean by type — but the real demo names it `cachingBus`, so we match that here.)

`AuditingQueryBus` comes with `org.streamrune.runtime.*`. The whole class at the end of this chapter:

<details>
<summary><code>StreamRuneConfig.java</code> at the end of Chapter 10</summary>

```java
package org.streamrune.ecommerce.spring.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.*;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.commands.customer.CustomerDecider;
import org.streamrune.ecommerce.commands.order.OrderDecider;
import org.streamrune.ecommerce.commands.order.OrderPricing;
import org.streamrune.ecommerce.commands.product.ProductDecider;
import org.streamrune.ecommerce.commands.upcaster.ProductCreatedUpcaster;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
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
        // Aggregate states, by simple class name: the bus snapshots a state under that name and
        // refuses to write a snapshot for a state the registry does not resolve.
        .registerState("ProductState", ProductState.class)
        .registerState("OrderState", OrderState.class)
        .registerState("CustomerState", CustomerState.class)
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
  public HeaderUserRoleResolver userRoleResolver() {
    return new HeaderUserRoleResolver();
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
  public VirtualThreadCommandBus commandBus(
      EventStore store,
      BeanValidationInterceptor validation,
      AnnotationAuthorizationInterceptor auth,
      AuditCommandInterceptor audit) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .interceptors(audit, auth, validation)
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

  /** Prices every order from the product catalogue, as the product streams hold it. */
  @Bean
  public OrderPricing orderPricing(EventStore eventStore) {
    return new OrderPricing(eventStore);
  }
}
```

</details>

The wrapping order is `AuditingQueryBus → CachingQueryBus → SimpleQueryBus`. Dispatched queries hit `AuditingQueryBus` first — every `@Auditable` query dispatch is recorded before being forwarded to the caching bus. If the result is already in the cache, the caching bus returns it immediately and no handler is called. This means cache hits **are** recorded in the audit log (the dispatch happened), but the handler was never reached. If you prefer to audit only cache misses, swap the order — but the real demo wraps `CachingQueryBus` inside `AuditingQueryBus` as shown above.

`AuditingQueryBus` checks each incoming query for `@Auditable` using `Class.isAnnotationPresent`. The result is cached in a `ConcurrentHashMap<Class<?>, Boolean>` so the reflection call only happens once per query type. Queries without the annotation are passed straight to the delegate.

### Step 5 — Create AuditController

Create `spring-app/src/main/java/org/streamrune/ecommerce/spring/controller/AuditController.java`:

```java
package org.streamrune.ecommerce.spring.controller;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;

/**
 * The command audit log, for the {@code ADMIN} role ({@code 403} otherwise): a row names the user
 * who sent a command and the aggregate it was sent to, for every customer.
 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

  private final DataSource dataSource;

  public AuditController(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @GetMapping("/commands")
  public List<Map<String, Object>> commandAuditLog(@RequestParam(defaultValue = "100") int limit) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    var results = new ArrayList<Map<String, Object>>();
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT command_id, command_type, aggregate_id, user_id, occurred_at, outcome, "
                    + "error_message, event_count FROM audit_log ORDER BY occurred_at DESC LIMIT ?")) {
      ps.setInt(1, limit);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          results.add(
              Map.of(
                  "commandId", rs.getString("command_id"),
                  "commandType", rs.getString("command_type"),
                  "aggregateId",
                      rs.getString("aggregate_id") != null ? rs.getString("aggregate_id") : "",
                  "userId", rs.getString("user_id") != null ? rs.getString("user_id") : "",
                  "occurredAt", rs.getTimestamp("occurred_at").toInstant().toString(),
                  "outcome", rs.getString("outcome"),
                  "eventCount", rs.getInt("event_count")));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to query audit log", e);
    }
    return results;
  }
}
```

This controller queries `audit_log` directly via JDBC rather than through a query handler. The endpoint is intentionally read-only and paginated — the `limit` parameter prevents runaway result sets. In a production system you would add filtering by date range, user ID, or outcome.

Its first line is the guard: the log says who sent which command to which aggregate, for every customer, so it is for operators. `@RequireRole` guards commands, and this is a read, so the controller calls `OwnerOrAdminAccess.requireAdmin` from Chapter 9 with the caller `RequestCaller` hands it, and a request from anyone but an `ADMIN` is answered `403`. The identity and the role are the `X-User-Id` and `X-User-Role` headers — the demo shortcut from Chapter 9, which a real deployment replaces with authentication.

### Step 6 — Create ComplianceReportQuery

Create `queries/src/main/java/org/streamrune/ecommerce/queries/query/ComplianceReportQuery.java`:

```java
package org.streamrune.ecommerce.queries.query;

import java.time.Instant;
import org.streamrune.core.audit.Auditable;

@Auditable
public record ComplianceReportQuery(Instant from, Instant to)
    implements org.streamrune.core.Query<org.streamrune.core.audit.ComplianceReport> {}
```

Notice that the record implements `Query<R>`. That is what makes it dispatchable through `AuditingQueryBus`, whose signature is `<R> R dispatch(Query<R> query)` — a bare record with no `implements` clause could never reach the bus. The result type is `org.streamrune.core.audit.ComplianceReport`, an aggregated report record from `streamrune-core`.

Both the `Query` interface and the `ComplianceReport` result type are fully qualified on purpose. `streamrune-core` already ships an unrelated **interface** named `org.streamrune.core.audit.ComplianceReportQuery` (a service interface for generating reports — nothing to do with this demo record). A wildcard import of `org.streamrune.core.audit.*` would pull that interface into scope and clash with the record you are defining here, so the real demo fully qualifies both `org.streamrune.core.Query` and `org.streamrune.core.audit.ComplianceReport` instead of importing them.

This query represents a compliance report request for a given time window. The `@Auditable` annotation tells `AuditingQueryBus` to record every dispatch — including who requested the report, when, and whether it succeeded. Compliance reports are themselves a sensitive operation that should be auditable: if a regulator asks "who pulled a compliance report last quarter?", you need this trail.

In the real demo this record is a standalone illustration of the `@Auditable` opt-in: no handler is registered for it in `StreamRuneConfig.java`, and nothing dispatches it. It compiles and demonstrates the annotation contract, but it is not wired into any flow. Registering a handler — one that queries `audit_log` filtered by `occurred_at BETWEEN ? AND ?` and returns a structured report — is an optional extension you could add, not something the demo actually does. The important thing demonstrated here is the annotation contract: placing `@Auditable` on a dispatchable query record is the complete opt-in mechanism.

### Step 7 — Verify

Start the application:

```bash
./gradlew :spring-app:bootRun
```

**Dispatch a few commands:**

```bash
# Place an order
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER' \
  -d '{
    "orderId": "order-audit-1",
    "customerId": "customer-1",
    "lines": [{"productId": "p-1", "quantity": 2}]
  }'

# Attempt to ship as CUSTOMER (will fail authorization)
curl -v -X POST http://localhost:8080/api/orders/order-audit-1/ship \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER'

# Cancel it
curl -X POST http://localhost:8080/api/orders/order-audit-1/cancel \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: customer-1' \
  -H 'X-User-Role: CUSTOMER' \
  -d '{"reason": "changed my mind"}'
```

**Query the audit log:**

```bash
curl -s http://localhost:8080/api/audit/commands \
  -H 'X-User-Id: admin-1' -H 'X-User-Role: ADMIN' | jq .
```

You should see the three entries of these calls first, newest first, and below them the older rows: the `GDPR_FORGET` rows that `ForgetSubjectService` wrote in Chapters 7 and 9. The refused ship attempt is one of them: the audit interceptor runs before the authorization interceptor, so the refusal is recorded as a `FAILURE` with the caller's id:

```json
[
  {
    "commandId": "...",
    "commandType": "CancelOrder",
    "aggregateId": "order-audit-1",
    "userId": "customer-1",
    "occurredAt": "2026-04-25T...",
    "outcome": "SUCCESS",
    "eventCount": 1
  },
  {
    "commandId": "...",
    "commandType": "ShipOrder",
    "aggregateId": "order-audit-1",
    "userId": "customer-1",
    "occurredAt": "2026-04-25T...",
    "outcome": "FAILURE",
    "eventCount": 0
  },
  {
    "commandId": "...",
    "commandType": "PlaceOrder",
    "aggregateId": "order-audit-1",
    "userId": "customer-1",
    "occurredAt": "2026-04-25T...",
    "outcome": "SUCCESS",
    "eventCount": 1
  }
]
```

Each entry records the command type, the aggregate it targeted, the caller's identity, and the number of events produced. A `PlaceOrder` that creates one `OrderPlaced` event shows `eventCount: 1`. The row of a failed command also holds the exception message in `error_message`, which this endpoint leaves out. Read it from the table:

```bash
docker compose exec postgres psql -U postgres -d streamrune_ecommerce \
  -c "SELECT command_type, user_id, outcome, error_message FROM audit_log WHERE aggregate_id = 'order-audit-1' ORDER BY id"
```

The refused attempt reads `ShipOrder | customer-1 | FAILURE | Required role: ADMIN`. The `error_message` is the exception's message in stored form: each run of line breaks or other control characters becomes one space, and anything past 2048 characters is cut off. This matters because an exception message can echo client input, such as a reused idempotency key. Stored as thrown, a line break in that input could forge a row for anyone reading the table, and a NUL would make the insert fail. The exception your controller receives is not changed.

**What the log does not hold.** The audit interceptor sits in the command bus, so it records what reaches the bus. A `@RequireRole` refusal does, as the `FAILURE` row above shows. A refusal by the access rule of Chapter 9 does not: `OwnerOrAdminAccess` is asked in the controller, as the first line of the handler, and a caller it refuses never dispatches a command. A refused `POST /api/customers/{id}/forget` or `PUT /api/products/{id}/price` answers `403` and leaves no row here. A deployment that must record refused attempts on those routes logs them where they are refused, at the HTTP edge.

The `userId` in each entry is simply what `X-User-Id` said, because the demo runs in the trusted-gateway mode (Chapter 9) with your `curl` standing in for the gateway. An audit trail is only as trustworthy as its identity source: in production that is an authenticated principal, or a gateway that authenticates the caller and sets the header itself. The role you sent in `X-User-Role` is not part of the audit entry; it does travel into each produced event's metadata as the `role` baggage entry, exactly as unverified as the header it came from. (The framework copies that header into the baggage only in the trusted-gateway mode, where a real gateway would set it; in any other mode the entry is absent.)

---

## What We Learned

- **`@Auditable`** is a `@Retention(RUNTIME)` annotation from `streamrune-core` that you place on query classes to opt them into audit recording. Command auditing is unconditional — every command is recorded regardless of whether the command class carries any annotation.

- **`AuditStore`** is a single-method interface from `streamrune-core`: `void save(AuditEntry entry)`. It is an append-only write-only sink. The runtime provides `PostgresAuditStore` as the production implementation.

- **`AuditEntry`** is an immutable record with ten components: `commandId` (nullable — `null` for query-origin entries), `commandType`, `aggregateType` (nullable — `null` for query-origin entries and for the GDPR subject entries, whose `aggregateId` is a subject hash), `aggregateId`, `userId` (nullable for anonymous requests), `occurredAt`, `outcome` (`SUCCESS`, `FAILURE`, or `VETOED` for a command an interceptor's `before()` declined to run), `errorMessage` (nullable; the exception message of a failure, or the name of the vetoing interceptor), `eventCount` (always 0 for failures and queries), and `correlationId` (nullable — the bound `StreamRuneContext` correlation token, or `null` when no request context was bound). Both command and query audit entries are stored in this same shape.

- **`AuditCommandInterceptor`** is a `CommandInterceptor` that intercepts every command execution. It captures the caller's user ID and the bound `CorrelationId` in `before()` on a per-thread stack, records both on the entry, writes a `SUCCESS` (or `VETOED`) entry in `after()`, and writes a `FAILURE` entry in `onError()`, storing the exception message through `LogSanitizer.sanitizeFreeText`. Interceptor position in the chain determines which failures are recorded: in front of authorization and validation, as here, it records the commands they refuse.

- **`AuditingQueryBus`** is a `QueryBus` decorator. It wraps any existing bus and adds selective auditing: only queries annotated with `@Auditable` are recorded. The annotation check uses a `ConcurrentHashMap` cache so reflection is invoked at most once per query type. The real demo wraps `CachingQueryBus` inside `AuditingQueryBus` (`AuditingQueryBus → CachingQueryBus → SimpleQueryBus`), meaning every `@Auditable` dispatch is logged — including cache hits. The `cachingQueryBus` bean and its handler registrations are untouched by this change; only the outer `queryBus` bean is replaced.

- **`ComplianceReportQuery`** is a `Query<ComplianceReport>` record that carries `@Auditable`. It demonstrates the opt-in contract: implementing `Query<R>` makes it dispatchable, and annotating it with `@Auditable` is all the bus needs to record the dispatch. In the real demo it is a standalone illustration — no handler is registered for it and nothing dispatches it; wiring a handler is an optional extension, not part of the demo's runtime flow. (Note: fully qualify the result type — `streamrune-core` has an unrelated `ComplianceReportQuery` interface in the same `org.streamrune.core.audit` package that a wildcard import would clash with.)

### Bonus: `AuditProjection` — event-level audit log

In addition to the command audit interceptor, the demo ships an `AuditProjection` (`projections/src/main/java/org/streamrune/ecommerce/projections/AuditProjection.java`) that builds an in-memory event audit trail. Unlike `AuditCommandInterceptor`, which records at the command dispatch layer, `AuditProjection` subscribes to the event store and records every `EventEnvelope` it processes via `MultiProjectionRunner`.

Each entry is an `AuditEntry` record capturing `eventId`, `aggregateType` and `aggregateId` (the envelope's typed parts — the projection never parses the stream id), `userId`, `commandId`, `correlationId`, `eventType`, the raw event object, and the event `timestamp`. For an event a saga caused, the `correlationId` is the saga's `streamrune:saga:<sagaId>`; for an event a request caused, it is whatever the request carried. Note that this is a **demo-local** `AuditEntry` in package `org.streamrune.ecommerce.projections` — a different type from the framework's `org.streamrune.core.audit.AuditEntry` used by the command interceptor earlier in this chapter. Same name, different package and shape; do not conflate the two. Entries are stored in a `CopyOnWriteArrayList`, making them thread-safe for concurrent reads without locking, beside a concurrent set of the event ids already recorded.

`AuditProjection` declares `@ProjectionConfig(name = "audit", deliveryMode = AT_LEAST_ONCE_IDEMPOTENT)` (the delivery modes are introduced in Chapter 5). It keeps its entries in memory, so they can never share the runner's checkpoint transaction, and the runner hands it no repository at all. A batch can be delivered twice even without a crash — a checkpoint commit that fails after `process` returned (a lost connection, a failed `COMMIT`) is retried from the unchanged checkpoint, and a dead-letter replay re-applies a range — so the projection dedups by `eventId`: an event whose id is already in the set is skipped. Appending blindly would break the guarantee the projection declares.

The projection exposes a `listByAggregateType(String aggregateType)` method that filters entries by aggregate type. If you want to expose this through a REST endpoint, inject `AuditProjection` into a controller and call `listByAggregateType("order")`. Note that `AuditProjection` is not currently registered in the Spring app's `MultiProjectionRunner` (the Quarkus and Micronaut apps discover it through its annotation) — it is provided as an extension point for an audit read model. To activate it in the Spring app, declare an `AuditProjection` bean and register it like the other four projections, but with its own mode: `.register("audit", auditProjection, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)`. It may share the runner with the four `TRANSACTIONAL_LOCAL` read models.

---

## Next Up

Time for the most complex feature: the Payment aggregate and saga orchestration.
