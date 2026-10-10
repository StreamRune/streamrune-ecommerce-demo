package org.streamrune.ecommerce.quarkus.config;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.commands.customer.CustomerDecider;
import org.streamrune.ecommerce.commands.integration.EcommerceIntegrationEventMapper;
import org.streamrune.ecommerce.commands.inventory.InventoryDecider;
import org.streamrune.ecommerce.commands.order.OrderDecider;
import org.streamrune.ecommerce.commands.payment.PaymentDecider;
import org.streamrune.ecommerce.commands.product.ProductDecider;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.commands.upcaster.ProductCreatedUpcaster;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.projections.AuditProjection;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.projections.CustomerSubjectDataPurger;
import org.streamrune.ecommerce.projections.InventoryProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.projections.ProductProjection;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminStreamAccess;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.postgres.PostgresDeadLetterQueue;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.quarkus.StreamRuneQuarkusProperties;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

/**
 * Application-specific StreamRune beans. Infrastructure (command bus, projection repository,
 * projection runner, lifecycle, and the event store) comes from the streamrune-quarkus
 * integration's default producers — this class contributes what the framework cannot know: the
 * event type registry, the upcasters, the decider registrations, the projections, and the outbox /
 * crypto beans that the auto-configured event store factory picks up.
 */
@ApplicationScoped
public class StreamRuneProducer {

  /**
   * Binds the framework's {@code streamrune.*} {@link StreamRuneQuarkusProperties}
   * {@code @ConfigMapping}.
   *
   * <p>Workaround for a composite-build gap: {@code streamrune-quarkus} ships a Jandex index +
   * {@code beans.xml}, so Quarkus discovers the {@code @ConfigMapping} interface (as a synthetic
   * bean) and auto-registers the {@code @Provider} {@link
   * org.streamrune.quarkus.StreamRuneRequestFilter}, which injects it. But under this repo's Gradle
   * composite build ({@code includeBuild("../streamrune")}) the mapping is never registered as a
   * runtime config mapping, so the filter fails to resolve it at boot with {@code SRCFG00027: Could
   * not find a mapping for org.streamrune.quarkus.StreamRuneQuarkusProperties}. This alternative
   * producer binds the mapping against a throwaway {@link SmallRyeConfig} whose single source
   * <b>delegates every lookup to the live Quarkus config</b> ({@link
   * SmallRyeConfig#getConfigValue}). Every value therefore comes from Quarkus's own resolution —
   * identical to what a {@code @ConfigProperty} injection sees — so e.g. {@code
   * properties.outbox().enabled()} always agrees with the {@code streamrune.outbox.enabled} that
   * {@code RabbitMqProducer} reads (rebuilding the config from copied sources + profiles instead
   * resolves profile/ordinal precedence differently and can disagree, silently disabling the
   * OutboxPoller). Wins over the failing synthetic bean via {@code @Alternative} +
   * {@code @Priority}; harmless when StreamRune is consumed as a published jar.
   */
  @Produces
  @Singleton
  @Alternative
  @Priority(1)
  public StreamRuneQuarkusProperties streamRuneQuarkusProperties() {
    SmallRyeConfig quarkusConfig = (SmallRyeConfig) ConfigProvider.getConfig();
    ConfigSource delegating =
        new ConfigSource() {
          @Override
          public Set<String> getPropertyNames() {
            Set<String> names = new HashSet<>();
            quarkusConfig.getPropertyNames().forEach(names::add);
            return names;
          }

          @Override
          public String getValue(String propertyName) {
            var value = quarkusConfig.getConfigValue(propertyName);
            return value == null ? null : value.getValue();
          }

          @Override
          public String getName() {
            return "streamrune-quarkus-properties-delegate";
          }

          @Override
          public int getOrdinal() {
            return 100;
          }
        };
    return new SmallRyeConfigBuilder()
        .withSources(delegating)
        // Register the same Quarkus Duration converter the runtime uses.
        // StreamRuneQuarkusProperties
        // has several Duration-typed properties (lockTimeout, circuitBreakerCooldown, the
        // *retention
        // max-age settings). Raw SmallRye only parses ISO-8601 ("PT5S"); the live Quarkus config
        // additionally accepts the relaxed form ("5s", "30m"). Without this converter a relaxed
        // streamrune.* duration would bind here differently than everywhere else in the app (or
        // fail
        // the @ConfigMapping build outright), silently diverging this throwaway config from
        // Quarkus's
        // own resolution — the exact hazard this delegating producer exists to avoid.
        .withConverter(
            Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
        .withMapping(StreamRuneQuarkusProperties.class)
        .build()
        .getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  /**
   * Per-subject AES-256 crypto engine backing the {@code @Encrypted} fields on {@code
   * CustomerEvent}. Wired into the event store below so PII is encrypted at rest in {@code
   * event_stream.payload}; deleting a subject's key (the forget flow) crypto-shreds that subject.
   * Its {@code encryption_keys}, {@code forgotten_subjects} and {@code erased_key_generations}
   * tables come from the framework's crypto migration series, which the event store factory applies
   * at startup next to its own ({@code streamrune.event-store.schema.auto-initialize}).
   *
   * <p>Exposed as both {@link PostgresCryptoEngine} (the forget service needs its concrete API) and
   * {@link CryptoEngine} (what {@code CryptoShreddingModule} consumes). The {@code @Singleton}
   * declared type is {@link PostgresCryptoEngine}; CDI also satisfies {@link CryptoEngine}
   * injection points from this same bean because the producer's bean types include the interface.
   */
  @Produces
  @Singleton
  public PostgresCryptoEngine cryptoEngine(javax.sql.DataSource ds) {
    return PostgresCryptoEngine.builder().dataSource(ds).build();
  }

  /**
   * Outbox store: persists outbox entries in the same transaction as domain events. Consumed by
   * both the event store (in-tx writes) and the auto-configured {@link
   * org.streamrune.runtime.OutboxPoller} (polling + claiming pending entries).
   *
   * <p>The claim lease (2 minutes) must strictly exceed {@link RabbitMqProducer#outboxPublisher}'s
   * in-flight horizon: its default 30s {@code publishBlockBudget} plus its 30s {@code
   * confirmTimeout} give a 60s horizon, plus the framework's recommended 30s margin — 90s minimum.
   * {@code OutboxPoller.Builder.build()} now refuses to boot when the lease does not exceed the
   * horizon.
   *
   * <p>STRICT_PER_AGGREGATE (the default, stated here so the choice is visible): the demo's outbox
   * carries order lifecycle, payment outcomes and stock changes — consumers that must not see n+1
   * before n. A terminal FAILED entry therefore blocks its aggregate until an operator replays or
   * skips it from the admin API (tutorial ch. 14). The mapper sets aggregateId on every entry,
   * which strict mode requires.
   */
  @Produces
  @Singleton
  public PostgresOutboxStore outboxStore(javax.sql.DataSource ds) {
    return new PostgresOutboxStore(
        ds, Duration.ofMinutes(2), OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  /**
   * Curated, PII-safe integration event mapper. Only allowlisted, non-PII events are forwarded to
   * the broker. Consumed by the event store alongside the outbox store.
   */
  @Produces
  @Singleton
  public OutboxEventMapper outboxEventMapper() {
    return new EcommerceIntegrationEventMapper();
  }

  @Produces
  @Singleton
  public org.streamrune.core.UserRoleResolver userRoleResolver() {
    return new HeaderUserRoleResolver();
  }

  /**
   * Decides who may open {@code GET /api/sse/{aggregateType}/{aggregateId}}: an authenticated
   * {@code ADMIN}, or the customer the stream belongs to (see {@link OwnerOrAdminStreamAccess} for
   * the rule per aggregate type). A frame carries the event with {@code @Encrypted} fields
   * decrypted, so the application's part is this decision. Without an {@link SseAuthorizer} bean
   * the framework installs one that refuses every stream once {@code streamrune.sse.enabled=true}.
   *
   * <p>The rule reads the caller's role from the request context. On Quarkus the framework's
   * request filter stores it in the request-scoped {@link StreamRuneRequestContextHolder} before
   * the resource method runs, and the framework's SSE resource calls the authorizer inside that
   * method, where the request scope is active; {@code StreamRuneContext.CURRENT} is the fallback
   * the controllers of this app use too.
   *
   * <p>That resource method is a blocking one ({@code @Blocking}), so Quarkus REST runs the request
   * filter and the method, and with it this rule, on a worker thread, never on a Vert.x event loop.
   * The rule may therefore read the database, and it does so only for an order stream opened by a
   * caller who is not an {@code ADMIN}: one primary-key read of the order read model.
   */
  @Produces
  @Singleton
  public SseAuthorizer sseAuthorizer(
      StreamRuneRequestContextHolder requestContext, OrderProjection orderProjection) {
    OwnerOrAdminStreamAccess access =
        new OwnerOrAdminStreamAccess(
            () -> {
              StreamRuneContext.RequestContext context = requestContext.context();
              if (context == null && StreamRuneContext.CURRENT.isBound()) {
                context = StreamRuneContext.CURRENT.get();
              }
              return context;
            },
            orderProjection::get);
    return access::isAuthorized;
  }

  @Produces
  @Singleton
  public EventTypeRegistry eventTypeRegistry() {
    return SimpleEventTypeRegistry.builder()
        // Product events
        .registerEvent("ProductCreated", ProductEvent.ProductCreated.class)
        .registerEvent("PriceUpdated", ProductEvent.PriceUpdated.class)
        .registerEvent("StockAdjusted", ProductEvent.StockAdjusted.class)
        .registerEvent("ProductDiscontinued", ProductEvent.ProductDiscontinued.class)
        // Order events
        .registerEvent("OrderPlaced", OrderEvent.OrderPlaced.class)
        .registerEvent("OrderConfirmed", OrderEvent.OrderConfirmed.class)
        .registerEvent("OrderShipped", OrderEvent.OrderShipped.class)
        .registerEvent("OrderDelivered", OrderEvent.OrderDelivered.class)
        .registerEvent("OrderCancelled", OrderEvent.OrderCancelled.class)
        // Customer events — their @Encrypted fields are crypto-shredded via the engine above.
        .registerEvent("CustomerRegistered", CustomerEvent.CustomerRegistered.class)
        .registerEvent("ProfileUpdated", CustomerEvent.ProfileUpdated.class)
        .registerEvent("DataExportRequested", CustomerEvent.DataExportRequested.class)
        .registerEvent("CustomerForgotten", CustomerEvent.CustomerForgotten.class)
        // Payment events
        .registerEvent("PaymentInitiated", PaymentEvent.PaymentInitiated.class)
        .registerEvent("PaymentCaptured", PaymentEvent.PaymentCaptured.class)
        .registerEvent("PaymentRefunded", PaymentEvent.PaymentRefunded.class)
        .registerEvent("PaymentFailed", PaymentEvent.PaymentFailed.class)
        // Inventory events
        .registerEvent("StockReserved", InventoryEvent.StockReserved.class)
        .registerEvent("StockReleased", InventoryEvent.StockReleased.class)
        .registerEvent("ReservationConfirmed", InventoryEvent.ReservationConfirmed.class)
        .registerEvent("ShipmentReceived", InventoryEvent.ShipmentReceived.class)
        // Aggregate state types — required so the command bus's snapshot policy
        // (everyNEvents, on by default in the Quarkus auto-config) can persist and
        // rehydrate snapshots. Without these, every snapshot save fails best-effort with
        // "state type 'XxxState' does not resolve in the EventTypeRegistry"; the saga's
        // multi-event order streams trip the threshold and flood the log. Mirrors the
        // framework's own SimpleEventTypeRegistry example (registerEvent + registerState).
        .registerState("ProductState", ProductState.class)
        .registerState("OrderState", OrderState.class)
        .registerState("CustomerState", CustomerState.class)
        .registerState("PaymentState", PaymentState.class)
        .registerState("InventoryState", InventoryState.class)
        .build();
  }

  /**
   * Upcasts {@code ProductCreated} v1→v2 (adds the {@code category} field). Exposed as an {@link
   * EventUpcaster} bean so the auto-configured {@code postgresEventStoreFactory} picks it up via
   * {@code @All List<EventUpcaster>} and applies it on read — mirrors the Spring demo's {@code
   * productCreatedUpcaster} bean.
   */
  @Produces
  @Singleton
  public EventUpcaster productCreatedUpcaster() {
    return new ProductCreatedUpcaster();
  }

  @Produces
  @Singleton
  public DeciderRegistration<ProductCommand, ProductState, ProductEvent>
      productDeciderRegistration() {
    return new DeciderRegistration<>(
        ProductState.TYPE,
        ProductCommand.class,
        cmd ->
            AggregateId.of(
                switch (cmd) {
                  case ProductCommand.CreateProduct c -> c.productId();
                  case ProductCommand.AdjustStock c -> c.productId();
                  case ProductCommand.UpdatePrice c -> c.productId();
                  case ProductCommand.DiscontinueProduct c -> c.productId();
                }),
        new ProductDecider());
  }

  @Produces
  @Singleton
  public DeciderRegistration<OrderCommand, OrderState, OrderEvent> orderDeciderRegistration() {
    return new DeciderRegistration<>(
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
        new OrderDecider());
  }

  @Produces
  @Singleton
  public DeciderRegistration<CustomerCommand, CustomerState, CustomerEvent>
      customerDeciderRegistration() {
    return new DeciderRegistration<>(
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
        new CustomerDecider());
  }

  @Produces
  @Singleton
  public DeciderRegistration<PaymentCommand, PaymentState, PaymentEvent>
      paymentDeciderRegistration() {
    return new DeciderRegistration<>(
        PaymentState.TYPE,
        PaymentCommand.class,
        cmd ->
            AggregateId.of(
                switch (cmd) {
                  case PaymentCommand.InitiatePayment c -> c.paymentId();
                  case PaymentCommand.CapturePayment c -> c.paymentId();
                  case PaymentCommand.RefundPayment c -> c.paymentId();
                  case PaymentCommand.FailPayment c -> c.paymentId();
                }),
        new PaymentDecider());
  }

  @Produces
  @Singleton
  public DeciderRegistration<InventoryCommand, InventoryState, InventoryEvent>
      inventoryDeciderRegistration() {
    return new DeciderRegistration<>(
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
        new InventoryDecider());
  }

  // Delivery: the four PostgreSQL read models declare TRANSACTIONAL_LOCAL; the framework's
  // ProjectionProducer selects the single AtomicBatchProcessor bean (the default
  // JdbcProjectionRepository, the same object these projections write through) because they demand
  // it and because leadership is on — never because a DataSource exists. AuditProjection declares
  // AT_LEAST_ONCE_IDEMPOTENT in the shared module and is handed no repository.

  /**
   * Product read model. {@link ProductProjection} (shared module) carries
   * {@code @ProjectionConfig(name = "products", deliveryMode = TRANSACTIONAL_LOCAL)} already; this
   * subclass exists so the Quarkus producer below owns the bean and the discovery reads the
   * annotation from a class in this module — mirrors the other Discoverable* wrappers.
   */
  @Produces
  @Singleton
  public DiscoverableProductProjection productProjection(ProjectionRepository repo) {
    return new DiscoverableProductProjection(repo);
  }

  @org.streamrune.core.ProjectionConfig(
      name = "products",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class DiscoverableProductProjection extends ProductProjection {
    public DiscoverableProductProjection(ProjectionRepository repository) {
      super(repository);
    }
  }

  /**
   * Order read model. {@link OrderProjection} (shared module) carries {@code @ProjectionConfig(name
   * = "orders", deliveryMode = TRANSACTIONAL_LOCAL)} already; this subclass exists so the Quarkus
   * producer below owns the bean and the discovery reads the annotation from a class in this module
   * — mirrors the other Discoverable* wrappers.
   */
  @Produces
  @Singleton
  public DiscoverableOrderProjection orderProjection(ProjectionRepository repo) {
    return new DiscoverableOrderProjection(repo);
  }

  @org.streamrune.core.ProjectionConfig(
      name = "orders",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class DiscoverableOrderProjection extends OrderProjection {
    public DiscoverableOrderProjection(ProjectionRepository repository) {
      super(repository);
    }
  }

  /**
   * Customer read model, registered as the CONTINUOUS {@code customers} projection.
   *
   * <p>{@link CustomerProjection} (shared module) carries no {@code @ProjectionConfig}, so the
   * library's discovery-based projection runner would not pick it up. The annotated subclass below
   * makes it discoverable without touching the shared module — the runner reads the annotation from
   * the bean's class, and the inherited {@code process}/{@code projectionName()} ("customers") do
   * the work. The customer GET endpoint reads this projection; the forget purger deletes its row.
   */
  @Produces
  @Singleton
  public DiscoverableCustomerProjection customerProjection(ProjectionRepository repo) {
    return new DiscoverableCustomerProjection(repo);
  }

  @org.streamrune.core.ProjectionConfig(
      name = "customers",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class DiscoverableCustomerProjection extends CustomerProjection {
    public DiscoverableCustomerProjection(ProjectionRepository repository) {
      super(repository);
    }
  }

  /**
   * Inventory read model. {@link InventoryProjection} (shared module) carries no
   * {@code @ProjectionConfig}, so the library's discovery-based projection runner would not pick it
   * up. The annotated subclass makes it discoverable without touching the shared module — safe for
   * the Spring app, which uses an explicit {@link org.streamrune.runtime.MultiProjectionRunner} and
   * never relies on annotation-based discovery.
   */
  @Produces
  @Singleton
  public DiscoverableInventoryProjection inventoryProjection(ProjectionRepository repo) {
    return new DiscoverableInventoryProjection(repo);
  }

  @org.streamrune.core.ProjectionConfig(
      name = "inventory",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static final class DiscoverableInventoryProjection extends InventoryProjection {
    public DiscoverableInventoryProjection(ProjectionRepository repository) {
      super(repository);
    }
  }

  @Produces
  @Singleton
  public AuditProjection auditProjection() {
    return new AuditProjection();
  }

  /**
   * Read-model purger for the customer subject. {@link ForgetSubjectService} runs it after the
   * encryption key is shredded, deleting the {@code customers_view} row so the forget removes the
   * plaintext PII the projection captured (crypto-shredding alone only kills the ciphertext).
   */
  @Produces
  @Singleton
  public CustomerSubjectDataPurger customerSubjectDataPurger(ProjectionRepository repo) {
    return new CustomerSubjectDataPurger(repo);
  }

  /**
   * GDPR Article 17 orchestrator. The customer forget endpoint calls {@code forget(customerId,
   * requester)} after emitting {@code ForgetCustomer}: step 1 deletes the subject's key
   * (crypto-shred + terminal-erasure tombstone), step 2 runs every registered purger to remove the
   * subject's read-model rows. The audit store records each outcome in {@code audit_log}.
   *
   * <p>Overrides the library default {@code forgetSubjectService} producer (which collects every
   * {@code SubjectDataPurger} bean) so the forget's engine, audit store and purger are named here,
   * explicitly.
   */
  @Produces
  @Singleton
  public ForgetSubjectService forgetSubjectService(
      PostgresCryptoEngine cryptoEngine,
      PostgresAuditStore auditStore,
      CustomerSubjectDataPurger customerPurger) {
    return ForgetSubjectService.builder()
        .cryptoEngine(cryptoEngine)
        .auditStore(auditStore)
        .purgers(List.of(customerPurger))
        .build();
  }

  /**
   * Concrete {@link PostgresAuditStore} for the forget service's audit trail. The library produces
   * a {@code PostgresAuditStore} as the default {@link org.streamrune.core.audit.AuditStore}; this
   * exposes it under its concrete type so {@link #forgetSubjectService} can inject it directly,
   * matching the Spring config's explicit {@code auditStore} bean.
   */
  @Produces
  @Singleton
  public PostgresAuditStore auditStore(javax.sql.DataSource ds) {
    return new PostgresAuditStore(ds);
  }

  /**
   * PostgreSQL-backed dead-letter queue. Stores failed commands so operators can inspect and retry
   * them. Consumed by the overriding {@link #deadLetterRetryRunner} producer below and by the DLQ
   * admin endpoints (QB3). Mirrors the Spring {@code deadLetterQueue} bean.
   */
  @Produces
  @Singleton
  public PostgresDeadLetterQueue deadLetterQueue(javax.sql.DataSource ds) {
    return new PostgresDeadLetterQueue(ds);
  }

  /**
   * Replaces the framework's {@code @DefaultBean} {@link DeadLetterRetryRunner} with the demo's own
   * retry policy (5 attempts, 60 s initial backoff, 60 s poll). It registers the same five sealed
   * command roots the decider registrations above route, so a dead-lettered command of any
   * aggregate resolves: a dead-letter entry names the CONCRETE command class, and {@code
   * registerCommand} expands a sealed root into every command it permits. In a GraalVM native image
   * that expansion reads {@code getPermittedSubclasses()}, which the image answers only for a root
   * registered for reflection: the five roots are {@code {"type": ...}} entries in {@code
   * META-INF/native-image/org.streamrune.ecommerce/quarkus-app/reachability-metadata.json}
   * (registering their records alone is not enough, and neither is {@code @RegisterForReflection}
   * on the roots). A root the image cannot expand makes {@code registerCommand} refuse to start the
   * app instead of registering the root alone.
   *
   * <p>Because the application supplies this bean, the framework's {@code StreamRuneLifecycle}
   * neither builds its own runner nor starts this one: {@link EcommerceSubscriptionLifecycle}
   * starts it on {@code StartupEvent} and closes it on {@code ShutdownEvent}. {@code @Singleton},
   * so the runner that polls is the one the admin endpoint replays through.
   *
   * <p>The framework's command bus writes each entry with {@link
   * DeadLetterRetryRunner#createObjectMapper(CryptoEngine)}, which encrypts the {@code @Encrypted}
   * fields of {@code RegisterCustomer} and {@code UpdateProfile} under the customer's key. The
   * runner reads them with the same kind of mapper, so the fields decrypt to the original values.
   * The application's {@code ObjectMapper} has no crypto module: it would read the ciphertext into
   * the command and replay it as the customer's data. If the customer was forgotten while the entry
   * waited, the fields decrypt to {@code [REDACTED]} and the runner does not replay the command.
   */
  @Produces
  @Singleton
  public DeadLetterRetryRunner deadLetterRetryRunner(
      DeadLetterQueue dlq, CommandBus commandBus, PostgresCryptoEngine cryptoEngine) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
        .registerCommand(ProductCommand.class)
        .registerCommand(OrderCommand.class)
        .registerCommand(CustomerCommand.class)
        .registerCommand(PaymentCommand.class)
        .registerCommand(InventoryCommand.class)
        .policy(new DeadLetterRetryPolicy(5, Duration.ofSeconds(60), 2.0, false))
        .pollInterval(Duration.ofSeconds(60))
        .build();
  }

  /**
   * The order-fulfillment saga runner, produced as a bean so that the framework finds it. For every
   * {@code SagaRunner} bean the Quarkus integration starts a compensation retry sweeper, which
   * re-drives a compensation that failed transiently and faults it once {@code
   * streamrune.saga.compensation-retry-give-up-after} (1 hour by default) has passed — {@code POST
   * /api/admin/sagas/faulted/resume} resumes such a saga — and it applies {@code
   * streamrune.inbox.retention-max-age} to the runner's key-age guard for compensations resumed by
   * a live event. {@link EcommerceSubscriptionLifecycle} feeds this runner from the event stream,
   * and the {@link #orderFulfillmentSagaDeadLetterReplayer replayer} feeds it quarantined events.
   *
   * <p>The framework's saga stores are {@code null} when {@code streamrune.saga.enabled=false}; the
   * demo's orders are confirmed only by this saga, so it refuses to start without them.
   */
  @Produces
  @Singleton
  public SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner(
      SagaStore sagaStore, SagaDeadLetterStore sagaDeadLetterStore, CommandBus commandBus) {
    if (sagaStore == null || sagaDeadLetterStore == null) {
      throw new IllegalStateException(
          "The order-fulfillment saga needs the saga stores: keep streamrune.saga.enabled=true");
    }
    return SagaRunner.<OrderFulfillmentState>builder()
        .orchestrator(new OrderFulfillmentSaga())
        .sagaStore(sagaStore)
        .commandBus(commandBus)
        // Poison events (routing/decider failures) are quarantined here instead of blocking the
        // subscription.
        .sagaDeadLetterStore(sagaDeadLetterStore)
        .build();
  }

  /**
   * Operator-triggered recovery of FAULTED order-fulfillment sagas, behind {@code
   * AdminController}'s saga endpoints: it drains a saga's quarantined events ({@code replayAll},
   * {@code replay}), resumes a compensation the compensation retry sweeper gave up on ({@code
   * resumeFaulted}) and abandons an entry ({@code discard}), through the same runner the live
   * subscription uses.
   *
   * <p>{@code inboxRetentionMaxAge} is the deployment's {@code streamrune.inbox.retention-max-age}.
   * It arms the replayer's two key-age refusals: once a replay's first attempt, or a faulted
   * compensation's episode, is older than that window, the command-inbox keys that deduplicate its
   * commands may already be pruned, and re-running it could charge or refund a customer twice. Such
   * a replay answers {@code STALE_REDRIVE_BLOCKED} or {@code STALE_COMPENSATION_BLOCKED} until an
   * operator who has reconciled what already executed repeats it with {@code force}. Without the
   * window both refusals are inert. {@code metrics} records every outcome when Micrometer is on the
   * classpath.
   */
  @Produces
  @Singleton
  public SagaDeadLetterReplayer orderFulfillmentSagaDeadLetterReplayer(
      SagaDeadLetterStore sagaDeadLetterStore,
      SagaStore sagaStore,
      EventStore eventStore,
      SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner,
      StreamRuneQuarkusProperties properties,
      Instance<StreamRuneMetrics> metrics) {
    return SagaDeadLetterReplayer.builder()
        .sagaDeadLetterStore(sagaDeadLetterStore)
        .sagaStore(sagaStore)
        .eventStore(eventStore)
        .sagaRunner(orderFulfillmentSagaRunner)
        .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
        .metrics(metrics.isResolvable() ? metrics.get() : null)
        .build();
  }
}
