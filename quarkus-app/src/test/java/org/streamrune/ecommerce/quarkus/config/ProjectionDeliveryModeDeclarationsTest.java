package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.ecommerce.projections.AuditProjection;
import org.streamrune.integration.ProjectionRoutes;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.quarkus.ProjectionProducer;
import org.streamrune.quarkus.StreamRuneQuarkusProperties;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The delivery modes the Quarkus app's discovered projections declare, read from the classes
 * exactly as the framework's discovery reads them, and the startup check applied to the demo's own
 * classes over a repository that belongs to no read-model store: once through the runner builder,
 * and once through the framework's discovery entry point (annotation read, routing to a read-model
 * store or the subscription checkpoint, startup check), with the CDI collections stood in by plain
 * lists and the default read-model store by an in-memory one.
 */
class ProjectionDeliveryModeDeclarationsTest {

  private static ProjectionDeliveryMode declared(Class<?> projection) {
    return projection.getAnnotation(ProjectionConfig.class).deliveryMode();
  }

  @Test
  void everyPostgresProjectionDeclaresTransactionalLocal_andTheAuditLogAtLeastOnce() {
    assertThat(
            Map.of(
                "products", declared(StreamRuneProducer.DiscoverableProductProjection.class),
                "orders", declared(StreamRuneProducer.DiscoverableOrderProjection.class),
                "customers", declared(StreamRuneProducer.DiscoverableCustomerProjection.class),
                "inventory", declared(StreamRuneProducer.DiscoverableInventoryProjection.class)))
        .allSatisfy(
            (name, mode) -> assertThat(mode).isEqualTo(ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    assertThat(declared(AuditProjection.class))
        .isEqualTo(ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
  }

  /**
   * The read-model store discovery routes the four to: an in-memory repository that is also its own
   * checkpoints and its own processor, as the PostgreSQL store's JdbcProjectionRepository and
   * readmodel_checkpoint are. Named {@code postgres}, as the default store is.
   */
  private static ReadModelStore store(InMemoryProjectionRepository repository) {
    return ReadModelStore.of("postgres", repository, repository, repository);
  }

  /** The subscription checkpoint discovery routes an unrouted at-least-once projection to. */
  private static CheckpointTarget subscriptionCheckpoint() {
    return new CheckpointTarget(
        "subscription_checkpoint",
        new InMemoryOffsetStore(),
        AtomicBatchProcessor.nonAtomicAtLeastOnce());
  }

  /**
   * The mode the demo declares, applied to the demo's own class over the WRONG repository: refused.
   */
  @Test
  void aDiscoverableProjectionOverAnotherRepository_isRefused() {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    var orders = new StreamRuneProducer.DiscoverableOrderProjection(other);
    assertThatThrownBy(
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(
                        new InMemoryEventStore(
                            new StreamRuneProducer().eventTypeRegistry(),
                            new InMemoryCryptoEngine()))
                    .register(
                        ProjectionName.of("orders"),
                        orders,
                        ProjectionErrorStrategy.HALT,
                        declared(orders.getClass()),
                        store(processor).checkpointTarget())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'orders' declares TRANSACTIONAL_LOCAL and writes to")
        .hasMessageContaining(String.valueOf(other.writeTargetIdentity()))
        .hasMessageContaining(String.valueOf(processor.writeTargetIdentity()));
  }

  @Test
  void discovery_buildsTheDemoShape_overOneReadModelStore() {
    var processor = new InMemoryProjectionRepository();
    assertThat(discover(processor, demoProjections(processor, processor, "none"))).isNotNull();
  }

  /**
   * Each of the four, built over a repository that belongs to no read-model store, is refused at
   * discovery: routing cannot give it a transaction with its checkpoint. An
   * AT_LEAST_ONCE_IDEMPOTENT declaration would be routed to the subscription checkpoint instead, so
   * this fails if one of the four declarations is flipped.
   */
  @ParameterizedTest
  @ValueSource(strings = {"products", "orders", "customers", "inventory"})
  void discovery_refusesEachOfTheFour_overAnotherRepository(String name) {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    assertThatThrownBy(() -> discover(processor, demoProjections(processor, other, name)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "'"
                + name
                + "' (TRANSACTIONAL_LOCAL): its writeTarget() belongs to no read-model store")
        .hasMessageContaining("'postgres'");
  }

  /**
   * The demo's discovered projections: the one named {@code misplaced} is built over {@code other}.
   */
  private static List<Projection> demoProjections(
      ProjectionRepository processor, ProjectionRepository other, String misplaced) {
    return List.of(
        new StreamRuneProducer.DiscoverableProductProjection(
            "products".equals(misplaced) ? other : processor),
        new StreamRuneProducer.DiscoverableOrderProjection(
            "orders".equals(misplaced) ? other : processor),
        new StreamRuneProducer.DiscoverableCustomerProjection(
            "customers".equals(misplaced) ? other : processor),
        new StreamRuneProducer.DiscoverableInventoryProjection(
            "inventory".equals(misplaced) ? other : processor),
        new AuditProjection());
  }

  /**
   * The framework's CONTINUOUS discovery, with {@code processor}'s store as the single read-model
   * store.
   */
  private static MultiProjectionRunner discover(
      InMemoryProjectionRepository processor, List<Projection> projections) {
    var producer = new ProjectionProducer();
    // The routes are a bean of their own: they route every registration (and refuse a projection
    // over a repository no store owns); the runner is then built from them.
    ProjectionRoutes routes =
        producer.projectionRoutes(
            instance(projections),
            emptyConfig(),
            new ReadModelStores(() -> Optional.of(store(processor)), List::of),
            subscriptionCheckpoint(),
            instance(List.of()));
    return producer.multiProjectionRunner(
        instance(projections),
        new InMemoryEventStore(
            new StreamRuneProducer().eventTypeRegistry(), new InMemoryCryptoEngine()),
        instance(List.of(routes)),
        instance(List.of()),
        instance(List.of()),
        instance(List.of()),
        instance(List.of()),
        instance(List.of()),
        properties(),
        emptyConfig(),
        StreamRuneTracing.NOOP);
  }

  /** A CDI Instance over a fixed list of beans: only what discovery calls is answered. */
  @SuppressWarnings("unchecked")
  private static <T> Instance<T> instance(List<T> beans) {
    return (Instance<T>)
        Proxy.newProxyInstance(
            ProjectionDeliveryModeDeclarationsTest.class.getClassLoader(),
            new Class<?>[] {Instance.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "iterator" -> beans.iterator();
                  case "spliterator" -> beans.spliterator();
                  case "stream" -> beans.stream();
                  case "isUnsatisfied" -> beans.isEmpty();
                  case "isAmbiguous" -> beans.size() > 1;
                  case "isResolvable" -> beans.size() == 1;
                  case "get" -> beans.getFirst();
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" -> "Instance" + beans;
                  default ->
                      throw new UnsupportedOperationException("Instance." + method.getName());
                });
  }

  /** The framework's properties with the polling defaults; nothing else is read by discovery. */
  private static StreamRuneQuarkusProperties properties() {
    return (StreamRuneQuarkusProperties)
        Proxy.newProxyInstance(
            ProjectionDeliveryModeDeclarationsTest.class.getClassLoader(),
            new Class<?>[] {StreamRuneQuarkusProperties.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "pollingInterval" -> java.time.Duration.ofSeconds(5);
                  case "pollingJitter" -> java.time.Duration.ofSeconds(1);
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" -> "properties";
                  default ->
                      throw new UnsupportedOperationException(
                          "StreamRuneQuarkusProperties." + method.getName());
                });
  }

  /** A config with no streamrune.* overrides: every annotation value stands. */
  private static Config emptyConfig() {
    return (Config)
        Proxy.newProxyInstance(
            ProjectionDeliveryModeDeclarationsTest.class.getClassLoader(),
            new Class<?>[] {Config.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getOptionalValue" -> Optional.empty();
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" -> "emptyConfig";
                  default -> throw new UnsupportedOperationException("Config." + method.getName());
                });
  }
}
