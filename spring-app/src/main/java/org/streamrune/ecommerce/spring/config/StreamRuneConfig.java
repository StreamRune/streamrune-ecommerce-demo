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
import org.springframework.context.annotation.ImportRuntimeHints;
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
import org.streamrune.ecommerce.queries.access.OwnerOrAdminStreamAccess;
import org.streamrune.ecommerce.queries.query.ListProducts;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.postgres.*;
import org.streamrune.runtime.*;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.spring.StreamRuneProperties;

@Configuration(proxyBeanMethods = false)
@ImportRuntimeHints(EcommerceNativeHints.class)
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
   * <p>A command that fails on infrastructure (a store or network error, not a business rejection)
   * is recorded in {@link #deadLetterQueue} before the error reaches the caller, and {@link
   * #deadLetterRetryRunner} replays it later. The bus writes the command as JSON with {@link
   * DeadLetterRetryRunner#createObjectMapper(org.streamrune.core.crypto.CryptoEngine)}, which
   * encrypts the {@code @Encrypted} fields of {@code RegisterCustomer} and {@code UpdateProfile}
   * under the customer's key: the queued copy of the customer's personal data is ciphertext and is
   * erased with the customer. {@link #legacyObjectMapper} has no crypto module and would store it
   * in plaintext.
   *
   * <p>The bus takes the {@link AggregateLocker} bean, {@link #aggregateLocker}, and {@code
   * streamrune.lock-timeout} (5 s by default), as the bus the framework builds does: a command
   * waits that long for its aggregate's lock on each attempt before the bus gives up with a {@link
   * LockException}, which the app answers {@code 503} ({@link GlobalExceptionHandler}).
   *
   * <p>{@code app.chaos.saga-command-delay-max} (default {@code 0ms}) adds a {@link
   * SagaCommandDelayInterceptor} innermost when it is above zero: a demo-only knob that delays
   * every command a saga dispatches, so that a saga's deadline can pass while its forward command
   * is in flight. The two-backend Compose profile and {@code TwoInstanceSagaIT} turn it on.
   */
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
      AggregateLocker locker,
      StreamRuneProperties properties,
      @Value("${app.chaos.saga-command-delay-max:0ms}") Duration sagaCommandDelayMax) {
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(locker)
            .lockTimeout(properties.lockTimeout())
            // Required for saga command dispatch: SagaCommandDispatch calls
            // commandBus.execute(command, IdempotencyKey), which throws IllegalStateException
            // unless the bus has a commandInbox wired in.
            .commandInbox(commandInbox)
            .interceptors(audit, auth, validation, circuitBreaker)
            .deadLetterQueue(deadLetterQueue)
            .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
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
                new InventoryDecider());
    if (sagaCommandDelayMax.isPositive()) {
      var sagaCommandDelay = new SagaCommandDelayInterceptor(sagaCommandDelayMax);
      bus.interceptors(sagaCommandDelay);
    }
    return bus.build();
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

  // Dead Letter Queue
  @Bean
  public PostgresDeadLetterQueue deadLetterQueue(DataSource ds) {
    return new PostgresDeadLetterQueue(ds);
  }

  /**
   * Replays dead-lettered commands. Registers the same five sealed command roots the command bus
   * routes to deciders, so any command of the five aggregates resolves: a dead-letter entry names
   * the CONCRETE command class, and {@code registerCommand} expands a sealed root into every
   * command it permits. In a GraalVM native image that expansion reads {@code
   * getPermittedSubclasses()}, which works only for a root registered for reflection — {@link
   * EcommerceNativeHints} registers the roots through {@code
   * StreamRuneRuntimeHints.registerDomainPackages}. A root the image cannot expand makes {@code
   * registerCommand} refuse to start the app instead of registering the root alone.
   *
   * <p>The runner reads the entries with the same kind of mapper the {@link #commandBus} writes
   * them with, so the {@code @Encrypted} fields decrypt to the original values. A plain mapper
   * would read the ciphertext into the command and replay it as the customer's data. If the
   * customer was forgotten while the entry waited, the fields decrypt to {@code [REDACTED]} and the
   * runner does not replay the command.
   *
   * <p>This bean replaces the framework's runner, so the framework does not register it for
   * liveness; the method does, with the framework's {@link BackgroundRelayHealthContributor}. The
   * {@code streamRune} health component then reports it as {@code relay.dead-letter-retry} and
   * turns {@code DOWN} if the runner's poll thread dies, instead of reporting {@code UP} while
   * dead-letter entries stop being replayed.
   *
   * <p>The runner keeps the default policy, which replays on its schedule only the entries the
   * command inbox can deduplicate: those of commands that ran under an idempotency key. An entry of
   * an unkeyed command waits for an operator ({@code POST /api/admin/dead-letters/{id}/retry}),
   * because its caller may already have retried it successfully. {@code metrics} counts those
   * entries ({@code streamrune.dlq.awaiting_operator}) and the operator retries refused as older
   * than the inbox window ({@code streamrune.dlq.stale_replay_blocked}). It looks for due entries
   * every {@code app.dead-letter.poll-interval} (60 seconds unless configured).
   */
  @Bean
  public DeadLetterRetryRunner deadLetterRetryRunner(
      PostgresDeadLetterQueue dlq,
      VirtualThreadCommandBus commandBus,
      PostgresCryptoEngine cryptoEngine,
      BackgroundRelayHealthContributor relayHealth,
      org.streamrune.spring.StreamRuneProperties properties,
      ObjectProvider<StreamRuneMetrics> metrics,
      @Value("${app.dead-letter.poll-interval:60s}") Duration pollInterval) {
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(commandBus)
            .objectMapper(DeadLetterRetryRunner.createObjectMapper(cryptoEngine))
            .registerCommand(ProductCommand.class)
            .registerCommand(OrderCommand.class)
            .registerCommand(CustomerCommand.class)
            .registerCommand(PaymentCommand.class)
            .registerCommand(InventoryCommand.class)
            // The window the inbox sweeper prunes with: a keyed entry older than it is refused.
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
            .pollInterval(pollInterval)
            .build();
    relayHealth.registerDlqRetryRunner(runner);
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
   * Decides who may open {@code GET /api/sse/{aggregateType}/{aggregateId}}: an authenticated
   * {@code ADMIN}, or the customer the stream belongs to (see {@link OwnerOrAdminStreamAccess} for
   * the rule per aggregate type). With {@code streamrune.sse.enabled=true} the framework serves the
   * endpoint and feeds it; a frame carries the event with {@code @Encrypted} fields decrypted, so
   * the application's part is this decision. Without an {@link SseAuthorizer} bean the framework
   * installs one that refuses every stream.
   *
   * <p>The framework's SSE controller hands the rule the request context its {@code
   * ScopedValueFilter} built for the subscribing request: the caller's id and the {@code role}
   * entry the rule decides by are both in it, so the wiring is the rule and the order read model.
   */
  @Bean
  public SseAuthorizer sseAuthorizer(OrderProjection orderProjection) {
    return new OwnerOrAdminStreamAccess(orderProjection::get)::isAuthorized;
  }

  /**
   * The customer and order read models as one caller may see them: an authenticated {@code ADMIN}
   * or the owner, the rule of {@code OwnerOrAdminAccess} that the SSE endpoint above applies too.
   * The customer and order controllers answer every read from it.
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
   * streamrune.saga.compensation-retry-give-up-after} (1 hour by default) has passed — {@code POST
   * /api/admin/sagas/faulted/resume} resumes such a saga — it applies {@code
   * streamrune.inbox.retention-max-age} to the runner's key-age guard for compensations resumed by
   * a live event, and it applies {@code streamrune.saga.forward-fence} ({@code auto} unless
   * configured, which is on while single-active-consumer leadership is on) to the runner's forward
   * fence. {@code metrics} counts, among the saga meters, the forward commands the fence refuses
   * ({@code streamrune.saga.forward_commands_fenced}) and the steps run again in place after a lost
   * write of the saga row ({@code streamrune.saga.delivery_retried}).
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
   * Operator-triggered recovery of FAULTED order-fulfillment sagas, behind {@link
   * AdminController}'s saga endpoints. It drains a saga's quarantined events ({@code replayAll},
   * {@code replay}), resumes a compensation the auto-configured compensation retry sweeper gave up
   * on ({@code resumeFaulted}) and abandons an entry ({@code discard}). Nothing runs it on a
   * schedule: a poison event is a deterministic orchestrator bug that resolves only once a fix is
   * deployed. It feeds events through the same {@code orderFulfillmentSagaRunner} the live
   * subscription uses, so a replayed event takes the identical path, poison guard included.
   *
   * <p>{@code inboxRetentionMaxAge} is the deployment's {@code streamrune.inbox.retention-max-age}
   * (7 days unless configured). It arms the replayer's two key-age refusals: once a replay's first
   * attempt, or a faulted compensation's episode, is older than that window, the command-inbox keys
   * that deduplicate its commands may already be pruned, and re-running it could charge or refund a
   * customer twice. Such a replay answers {@code STALE_REDRIVE_BLOCKED} or {@code
   * STALE_COMPENSATION_BLOCKED} instead, until an operator who has reconciled what already executed
   * repeats it with {@code force}. Without the window both refusals are inert, and the forward path
   * has no other key-age guard. {@code metrics} records every outcome ({@code
   * streamrune.saga.replayed}, {@code streamrune.saga.resume_faulted}).
   */
  @Bean
  public SagaDeadLetterReplayer orderFulfillmentSagaDeadLetterReplayer(
      PostgresSagaDeadLetterStore sagaDeadLetterStore,
      PostgresSagaStore sagaStore,
      EventStore eventStore,
      SagaRunner<?> orderFulfillmentSagaRunner,
      StreamRuneProperties properties,
      ObjectProvider<StreamRuneMetrics> metrics) {
    return SagaDeadLetterReplayer.builder()
        .sagaDeadLetterStore(sagaDeadLetterStore)
        .sagaStore(sagaStore)
        .eventStore(eventStore)
        .sagaRunner(orderFulfillmentSagaRunner)
        .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
        .metrics(metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP))
        .build();
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
