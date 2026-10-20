package org.streamrune.ecommerce.micronaut.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
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
import org.streamrune.ecommerce.projections.CustomerSubjectDataPurger;
import org.streamrune.ecommerce.projections.InventoryProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.projections.ProductProjection;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminStreamAccess;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.micronaut.StreamRuneMicronautProperties;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.postgres.PostgresDeadLetterQueue;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.postgres.PostgresSnapshotStorePurger;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

/**
 * Application-specific StreamRune beans. Infrastructure (event store, command bus, projection
 * runner, lifecycle) comes from the streamrune-micronaut integration's defaults — they yield to
 * user beans via {@code @Requires(missingBeans = ...)}. This factory contributes what the framework
 * cannot know: the event type registry, the decider registrations, the projection repository, and
 * the projections.
 */
@Factory
public class StreamRuneFactory {

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
   * <p>The framework's SSE controller hands the rule the request context its {@code
   * StreamRuneContextFilter} built for the subscribing request: the caller's id and the {@code
   * role} entry the rule decides by are both in it, so the wiring is the rule and the order read
   * model. That filter runs on the blocking executor and the controller with it, so the rule may
   * read the database.
   */
  @Singleton
  public SseAuthorizer sseAuthorizer(OrderProjection orderProjection) {
    return new OwnerOrAdminStreamAccess(orderProjection::get)::isAuthorized;
  }

  /**
   * The customer and order read models as one caller may see them: an authenticated {@code ADMIN}
   * or the owner, the rule of {@code OwnerOrAdminAccess} that the SSE endpoint above applies too.
   * The customer and order controllers answer every read from it.
   */
  @Singleton
  public OwnerOrAdminQueries ownerOrAdminQueries(
      CustomerViewProjection customerProjection, OrderProjection orderProjection) {
    return new OwnerOrAdminQueries(customerProjection, orderProjection);
  }

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
        // Customer events — CustomerRegistered/ProfileUpdated carry @Encrypted PII fields, so they
        // must be registered for the wired PostgresCryptoEngine to encrypt them at rest.
        .registerEvent("CustomerRegistered", CustomerEvent.CustomerRegistered.class)
        .registerEvent("ProfileUpdated", CustomerEvent.ProfileUpdated.class)
        .registerEvent("DataExportRequested", CustomerEvent.DataExportRequested.class)
        .registerEvent("CustomerForgotten", CustomerEvent.CustomerForgotten.class)
        // Payment events
        .registerEvent("PaymentInitiated", PaymentEvent.PaymentInitiated.class)
        .registerEvent("PaymentCaptured", PaymentEvent.PaymentCaptured.class)
        .registerEvent("PaymentRefunded", PaymentEvent.PaymentRefunded.class)
        .registerEvent("PaymentFailed", PaymentEvent.PaymentFailed.class)
        .registerEvent("PaymentVoided", PaymentEvent.PaymentVoided.class)
        // Inventory events
        .registerEvent("StockReserved", InventoryEvent.StockReserved.class)
        .registerEvent("StockReleased", InventoryEvent.StockReleased.class)
        .registerEvent("ReservationConfirmed", InventoryEvent.ReservationConfirmed.class)
        .registerEvent("ShipmentReceived", InventoryEvent.ShipmentReceived.class)
        // Aggregate state types — required so the command bus's snapshot policy can persist and
        // rehydrate snapshots. Without these, every snapshot save fails best-effort with
        // "state type 'XxxState' does not resolve in the EventTypeRegistry" and floods the log.
        .registerState("ProductState", ProductState.class)
        .registerState("OrderState", OrderState.class)
        .registerState("CustomerState", CustomerState.class)
        .registerState("PaymentState", PaymentState.class)
        .registerState("InventoryState", InventoryState.class)
        .build();
  }

  /**
   * Exposes the product-created upcaster so the library-default {@code postgresEventStoreFactory}
   * collects it via {@code List<EventUpcaster>} and applies it on read — mirrors the Spring and
   * Quarkus demos so all three apps upcast {@code ProductCreated} v1 events identically.
   */
  @Singleton
  public EventUpcaster productCreatedUpcaster() {
    return new ProductCreatedUpcaster();
  }

  /**
   * Per-subject AES-256 crypto engine backing the {@code @Encrypted} fields on {@code
   * CustomerEvent}. Wired into the event store below so PII is encrypted at rest in {@code
   * event_stream.payload}; deleting a subject's key (the forget flow) crypto-shreds that subject.
   * Its {@code encryption_keys}, {@code forgotten_subjects} and {@code erased_key_generations}
   * tables come from the framework's crypto migration series, which the event store factory applies
   * at startup next to its own ({@code streamrune.event-store.schema.auto-initialize}).
   *
   * <p>Declared directly here rather than via the framework's {@code StreamRuneCryptoFactory}
   * (which gates on {@code streamrune.crypto.postgres.enabled} and wraps the engine in a cache) so
   * the demo holds the concrete {@link PostgresCryptoEngine} type — matching the Spring app and
   * letting {@link #eventStore} and {@link #forgetSubjectService} share one un-cached engine
   * instance whose key writes are immediately visible to the direct-JDBC E2E assertions.
   */
  @Singleton
  public PostgresCryptoEngine cryptoEngine(DataSource ds) {
    return PostgresCryptoEngine.builder().dataSource(ds).build();
  }

  /**
   * Transactional outbox store. Persists outbox entries in the same transaction as domain events.
   * Consumed by the event store (in-tx writes) and the auto-configured {@link
   * org.streamrune.runtime.OutboxPoller} (polling + claiming pending entries).
   *
   * <p>The claim lease (2 minutes) must strictly exceed {@link RabbitMqFactory#outboxPublisher}'s
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
  @Singleton
  public PostgresOutboxStore outboxStore(DataSource ds) {
    return new PostgresOutboxStore(
        ds, Duration.ofMinutes(2), OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  /**
   * Curated, PII-safe integration event mapper. Only allowlisted, non-PII events are forwarded to
   * the broker. Consumed by the event store alongside the outbox store.
   */
  @Singleton
  public OutboxEventMapper outboxEventMapper() {
    return new EcommerceIntegrationEventMapper();
  }

  /**
   * Default {@link org.streamrune.core.audit.AuditStore} for the demo. The framework's module
   * provides a {@code @Secondary} {@link PostgresAuditStore}; declaring a concrete bean here makes
   * it the primary store and gives {@link #forgetSubjectService} the exact type the Spring app
   * uses.
   */
  @Singleton
  public PostgresAuditStore auditStore(DataSource ds) {
    return new PostgresAuditStore(ds);
  }

  /**
   * Read-model purger for the customer subject. {@link ForgetSubjectService} runs it after the
   * encryption key is shredded, deleting the {@code customers_view} row so the forget removes the
   * plaintext PII the projection captured (crypto-shredding alone only kills the ciphertext).
   */
  @Singleton
  public CustomerSubjectDataPurger customerSubjectDataPurger(ProjectionRepository repo) {
    return new CustomerSubjectDataPurger(repo);
  }

  /**
   * GDPR Article 17 orchestrator. {@code CustomerCommandController}'s forget endpoint calls {@code
   * forget(customerId, requester)} after emitting {@code ForgetCustomer}: step 1 deletes the
   * subject's key (crypto-shred + terminal-erasure tombstone), step 2 runs every registered purger
   * to remove the subject's read-model rows. The audit store records each outcome in {@code
   * audit_log}.
   *
   * <p>This overrides the framework module's {@code @Secondary} {@link ForgetSubjectService} (which
   * collects every {@code SubjectDataPurger} bean) so the forget's engine, audit store and {@link
   * CustomerSubjectDataPurger} are named here, explicitly.
   */
  @Singleton
  public ForgetSubjectService forgetSubjectService(
      PostgresCryptoEngine cryptoEngine,
      PostgresAuditStore auditStore,
      CustomerSubjectDataPurger customerPurger,
      DataSource dataSource) {
    return ForgetSubjectService.builder()
        .cryptoEngine(cryptoEngine)
        .auditStore(auditStore)
        .purgers(List.of(customerPurger, customerSnapshotPurger(dataSource)))
        .build();
  }

  /**
   * Deletes the snapshots of a forgotten customer. A snapshot holds the aggregate's state as plain
   * JSON, and {@code CustomerState} carries the customer's name, email, address and phone in the
   * clear (the encryption lives on the events), so the key shred alone leaves them readable in
   * {@code snapshot_store}. The customer's stream is {@code customer:<subject id>}.
   */
  static PostgresSnapshotStorePurger customerSnapshotPurger(DataSource dataSource) {
    return new PostgresSnapshotStorePurger(
        dataSource,
        subject -> List.of(StreamId.of(CustomerState.TYPE, AggregateId.of(subject.value()))));
  }

  /**
   * Declared to return the CONCRETE {@link JdbcProjectionRepository} type, not the {@link
   * ProjectionRepository} interface — Micronaut's compile-time bean introspection exposes a
   * {@code @Factory} method's bean only under its declared return type's own hierarchy, so an
   * interface-typed declaration would hide {@link
   * org.streamrune.core.projection.AtomicBatchProcessor}, which {@code JdbcProjectionRepository}
   * also implements. The framework's {@code
   * org.streamrune.micronaut.ProjectionFactory#multiProjectionRunner} collects every {@code
   * AtomicBatchProcessor} bean and selects the single one because the four projections declare
   * {@code TRANSACTIONAL_LOCAL} and leadership is on ({@code
   * streamrune.subscription.single-active-consumer.enabled} defaults to true; the rule is {@code
   * org.streamrune.integration.ProjectionProcessorResolution}); had this stayed interface-typed,
   * the list would be empty and startup would fail naming the four projections — loudly, instead of
   * running them without the checkpoint transaction. The same object is what the four projections
   * write through, so the write-target identity check verifies them. Mirrors how the framework's
   * own {@code StreamRuneMicronautModule#jdbcProjectionRepository} default producer is declared.
   */
  @Singleton
  public JdbcProjectionRepository projectionRepository(DataSource ds) {
    return new JdbcProjectionRepository(ds);
  }

  @Singleton
  public org.streamrune.core.projection.OffsetStore offsetStore(DataSource ds) {
    return new org.streamrune.postgres.PostgresOffsetStore(ds);
  }

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
                  case PaymentCommand.FailPayment c -> c.paymentId();
                  case PaymentCommand.CancelPayment c -> c.paymentId();
                }),
        new PaymentDecider());
  }

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

  /**
   * Product read model. {@link ProductProjection} (shared module) carries
   * {@code @ProjectionConfig(name = "products", deliveryMode = TRANSACTIONAL_LOCAL)} already;
   * {@link DiscoverableProductProjection} repeats it on a class of this module, which this factory
   * produces — mirrors the {@link DiscoverableInventoryProjection} pattern; safe for the Spring
   * app, which uses an explicit {@link org.streamrune.runtime.MultiProjectionRunner}.
   */
  @Singleton
  public DiscoverableProductProjection productProjection(ProjectionRepository repo) {
    return new DiscoverableProductProjection(repo);
  }

  /**
   * Order read model. {@link OrderProjection} (shared module) carries {@code @ProjectionConfig(name
   * = "orders", deliveryMode = TRANSACTIONAL_LOCAL)} already; {@link DiscoverableOrderProjection}
   * repeats it on a class of this module, which this factory produces — mirrors the other
   * Discoverable* wrappers.
   */
  @Singleton
  public DiscoverableOrderProjection orderProjection(ProjectionRepository repo) {
    return new DiscoverableOrderProjection(repo);
  }

  /**
   * The customer read model, wired via {@link CustomerViewProjection} so the
   * {@code @ProjectionConfig} annotation makes it discoverable by the Micronaut projection runner
   * under the name {@code "customers"}.
   */
  @Singleton
  public CustomerViewProjection customerProjection(ProjectionRepository repo) {
    return new CustomerViewProjection(repo);
  }

  /**
   * Inventory read model. {@link InventoryProjection} (shared module) carries no
   * {@code @ProjectionConfig}, so the library's discovery-based projection runner would not pick it
   * up. {@link DiscoverableInventoryProjection} adds the annotation without touching the shared
   * module — safe for the Spring app, which uses an explicit {@link
   * org.streamrune.runtime.MultiProjectionRunner} and never relies on annotation-based discovery.
   */
  @Singleton
  public DiscoverableInventoryProjection inventoryProjection(ProjectionRepository repo) {
    return new DiscoverableInventoryProjection(repo);
  }

  @Singleton
  public AuditProjection auditProjection() {
    return new AuditProjection();
  }

  /**
   * Jackson {@link ObjectMapper} for the admin, saga and event-explorer controllers, which turn
   * framework records into maps. Micronaut Serialization (micronaut-serde-jackson) does not expose
   * a {@code com.fasterxml.jackson.databind.ObjectMapper} bean by default — it provides its own
   * {@code io.micronaut.serde.ObjectMapper} abstraction — so we produce one here. It has no crypto
   * module, so the dead-letter runner does not use it (see {@link #deadLetterRetryRunner}).
   */
  @Singleton
  public ObjectMapper jacksonObjectMapper() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  /**
   * PostgreSQL-backed dead-letter queue. Stores failed commands so operators can inspect and retry
   * them. Consumed by {@link #deadLetterRetryRunner} below and by the DLQ admin endpoints. Mirrors
   * the Spring {@code deadLetterQueue} bean.
   */
  @Singleton
  public PostgresDeadLetterQueue deadLetterQueue(DataSource ds) {
    return new PostgresDeadLetterQueue(ds);
  }

  /**
   * Overrides the framework's {@code @Secondary} {@link DeadLetterRetryRunner} with the demo's own
   * retry policy (5 attempts, 60 s initial backoff). It registers the same five sealed command
   * roots the decider registrations above route, so a dead-lettered command of any aggregate
   * resolves: a dead-letter entry names the CONCRETE command class, and {@code registerCommand}
   * expands a sealed root into every command it permits. In a GraalVM native image that expansion
   * reads {@code getPermittedSubclasses()}, which the image answers only for a root registered for
   * reflection: the roots are {@code {"type": ...}} entries in {@code
   * META-INF/native-image/org.streamrune.ecommerce/micronaut-app/reachability-metadata.json}
   * (registering their records alone is not enough, and neither is naming the roots in a Micronaut
   * {@code @TypeHint}). A root the image cannot expand makes {@code registerCommand} refuse to
   * start the app instead of registering the root alone.
   *
   * <p>A plain {@code @Singleton} (no {@code @Secondary}) replaces the default: the framework's
   * runner carries {@code @Requires(missingBeans = DeadLetterRetryRunner.class)}, so it is not
   * built, and the framework's {@code StreamRuneLifecycle} starts only the runner it built itself.
   * {@link EcommerceSubscriptionLifecycle} starts this one on {@link StartupEvent} and closes it on
   * shutdown.
   *
   * <p>The framework's command bus writes each entry with {@link
   * DeadLetterRetryRunner#createObjectMapper(org.streamrune.core.crypto.CryptoEngine)}, which
   * encrypts the {@code @Encrypted} fields of {@code RegisterCustomer} and {@code UpdateProfile}
   * under the customer's key. The runner reads them with the same kind of mapper, so the fields
   * decrypt to the original values. {@link #jacksonObjectMapper} has no crypto module: it would
   * read the ciphertext into the command and replay it as the customer's data. If the customer was
   * forgotten while the entry waited, the fields decrypt to {@code [REDACTED]} and the runner does
   * not replay the command.
   *
   * <p>{@code OPERATOR_ONLY}: on its schedule the runner replays only the entries the command inbox
   * can deduplicate, those of commands that ran under an idempotency key. An entry of an unkeyed
   * command waits for an operator ({@code POST /api/admin/dead-letters/{id}/retry}), because its
   * caller may already have retried it successfully. {@code metrics}, when a {@code
   * StreamRuneMetrics} bean exists, counts those entries ({@code streamrune.dlq.awaiting_operator})
   * and the operator retries refused as older than the inbox window ({@code
   * streamrune.dlq.stale_replay_blocked}). It looks for due entries every {@code
   * app.dead-letter.poll-interval} (60 seconds unless configured).
   */
  @Singleton
  public DeadLetterRetryRunner deadLetterRetryRunner(
      DeadLetterQueue dlq,
      org.streamrune.core.CommandBus commandBus,
      PostgresCryptoEngine cryptoEngine,
      StreamRuneMicronautProperties properties,
      @Nullable StreamRuneMetrics metrics,
      @Value("${app.dead-letter.poll-interval:60s}") Duration pollInterval) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(commandBus)
        .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
        .registerCommand(ProductCommand.class)
        .registerCommand(OrderCommand.class)
        .registerCommand(CustomerCommand.class)
        .registerCommand(PaymentCommand.class)
        .registerCommand(InventoryCommand.class)
        .policy(
            new DeadLetterRetryPolicy(
                5,
                Duration.ofSeconds(60),
                2.0,
                false,
                DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY))
        // The window the inbox sweeper prunes with: a keyed entry older than it is refused.
        .inboxRetentionMaxAge(properties.inboxRetentionMaxAge())
        .metrics(metrics)
        .pollInterval(pollInterval)
        .build();
  }

  /**
   * The order-fulfillment saga, with its deadline: {@code app.saga.fulfillment-timeout} (15 minutes
   * unless configured). The saga runner and the timeout runner below share this one instance.
   */
  @Singleton
  public OrderFulfillmentSaga orderFulfillmentSaga(
      @Value("${app.saga.fulfillment-timeout:15m}") Duration fulfillmentTimeout) {
    return new OrderFulfillmentSaga(fulfillmentTimeout);
  }

  /**
   * The order-fulfillment saga runner, a bean so that the framework finds it. For every {@code
   * SagaRunner} bean the Micronaut integration starts a compensation retry sweeper, which re-drives
   * a compensation that failed transiently and faults it once {@code
   * streamrune.saga.compensation-retry-give-up-after} (1 hour by default) has passed — {@code POST
   * /api/admin/sagas/faulted/resume} resumes such a saga — it applies {@code
   * streamrune.inbox.retention-max-age} to the runner's key-age guard for compensations resumed by
   * a live event, and it applies {@code streamrune.saga.forward-fence} ({@code auto} unless
   * configured, which is on while single-active-consumer leadership is on) to the runner's forward
   * fence. {@link EcommerceSubscriptionLifecycle} feeds this runner from the event stream, and the
   * {@link #orderFulfillmentSagaDeadLetterReplayer replayer} feeds it quarantined events. {@code
   * metrics} counts, among the saga meters, the forward commands the fence refuses ({@code
   * streamrune.saga.forward_commands_fenced}) and the steps run again in place after a lost write
   * of the saga row ({@code streamrune.saga.delivery_retried}) when a {@code StreamRuneMetrics}
   * bean exists.
   *
   * <p>{@code leadership} is the integration's single-active-consumer leadership ({@code
   * streamrune.subscription.single-active-consumer.enabled}, on unless configured; absent when it
   * is off). The runner carries a lease over it, {@code "saga:" + saga type}: with more than one
   * backend on the database, one of them delivers the saga's events, claims its deadlines and
   * retries its compensations, and the others stand by and take over when it stops or dies. The
   * lease is taken by the saga subscription {@link EcommerceSubscriptionLifecycle} builds with
   * {@code sagaRunner(...)}; without leadership every backend delivers.
   */
  @Singleton
  public SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner(
      OrderFulfillmentSaga orderFulfillmentSaga,
      SagaStore sagaStore,
      SagaDeadLetterStore sagaDeadLetterStore,
      org.streamrune.core.CommandBus commandBus,
      @Nullable StreamRuneMetrics metrics,
      @Nullable SubscriptionLeadership leadership) {
    return SagaRunner.<OrderFulfillmentState>builder()
        .orchestrator(orderFulfillmentSaga)
        .sagaStore(sagaStore)
        .commandBus(commandBus)
        // Poison events (routing/decider failures) are quarantined here instead of blocking the
        // subscription.
        .sagaDeadLetterStore(sagaDeadLetterStore)
        .metrics(metrics)
        // One backend delivers this saga type; the others stand by.
        .leadership(leadership)
        .build();
  }

  /**
   * Compensates a fulfillment that has not ended by its deadline ({@link
   * OrderFulfillmentSaga#timeout()}): every {@code app.saga.timeout-poll-interval} (10 seconds
   * unless configured) it claims each saga past its deadline and dispatches its compensation, which
   * is the order's cancellation whatever the state.
   *
   * <p>It is built from {@link #orderFulfillmentSagaRunner}, shares that runner's step lock and
   * follows its lease: it claims deadlines only while this backend leads the saga type through the
   * saga subscription, so it claims where the saga's steps run, and in this process a claim at a
   * saga's deadline waits for a delivery that is dispatching the saga's forward commands. Around a
   * handover two backends can overlap for a moment — a backend that dies keeps its lease until the
   * lease expires, and a deposed one may finish its page; there the saga runner's forward fence
   * keeps the order: a forward command is stored before the claim, and its compensation has to
   * cover it, or it is refused at its commit. {@code inboxRetentionMaxAge} is the deployment's
   * {@code streamrune.inbox.retention-max-age}: a compensation this runner resumes after its keys
   * may have been pruned is faulted instead of dispatched again. {@link
   * EcommerceSubscriptionLifecycle} starts it with the saga subscription and stops it on shutdown.
   */
  @Singleton
  public SagaTimeoutRunner<OrderFulfillmentState> orderFulfillmentTimeoutRunner(
      OrderFulfillmentSaga orderFulfillmentSaga,
      SagaStore sagaStore,
      org.streamrune.core.CommandBus commandBus,
      SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner,
      StreamRuneMicronautProperties properties,
      @Nullable StreamRuneMetrics metrics,
      @Value("${app.saga.timeout-poll-interval:10s}") Duration pollInterval) {
    return SagaTimeoutRunner.<OrderFulfillmentState>builder()
        .decider(orderFulfillmentSaga)
        .sagaStore(sagaStore)
        .commandBus(commandBus)
        // The saga runner stores its rows under the state class; the sweep selects by it.
        .sagaType(SagaType.fromClass(OrderFulfillmentState.class))
        .sagaRunner(orderFulfillmentSagaRunner)
        .pollInterval(pollInterval)
        .metrics(metrics)
        .inboxRetentionMaxAge(properties.inboxRetentionMaxAge())
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
   * window both refusals are inert. {@code metrics} records every outcome when a {@code
   * StreamRuneMetrics} bean exists.
   */
  @Singleton
  public SagaDeadLetterReplayer orderFulfillmentSagaDeadLetterReplayer(
      SagaDeadLetterStore sagaDeadLetterStore,
      SagaStore sagaStore,
      EventStore eventStore,
      SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner,
      StreamRuneMicronautProperties properties,
      @Nullable StreamRuneMetrics metrics) {
    return SagaDeadLetterReplayer.builder()
        .sagaDeadLetterStore(sagaDeadLetterStore)
        .sagaStore(sagaStore)
        .eventStore(eventStore)
        .sagaRunner(orderFulfillmentSagaRunner)
        .inboxRetentionMaxAge(properties.inboxRetentionMaxAge())
        .metrics(metrics)
        .build();
  }

  /** Prices every order from the product catalogue, as the product streams hold it. */
  @Singleton
  public OrderPricing orderPricing(EventStore eventStore) {
    return new OrderPricing(eventStore);
  }

  /**
   * Undoes the money and stock of every cancelled order; {@link EcommerceSubscriptionLifecycle}
   * feeds it from the event stream under the lease {@code order-cancellation}.
   */
  @Singleton
  public OrderCancellationProcessManager orderCancellationProcessManager(
      org.streamrune.core.CommandBus commandBus) {
    return new OrderCancellationProcessManager(commandBus);
  }

  /**
   * Commits the stock of every shipped order; {@link EcommerceSubscriptionLifecycle} feeds it from
   * the event stream under the lease {@code order-shipment}.
   */
  @Singleton
  public OrderShipmentProcessManager orderShipmentProcessManager(
      org.streamrune.core.CommandBus commandBus) {
    return new OrderShipmentProcessManager(commandBus);
  }
}
