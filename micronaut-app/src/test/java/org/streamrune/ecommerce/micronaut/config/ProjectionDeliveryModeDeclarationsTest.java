package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micronaut.context.BeanContext;
import io.micronaut.context.env.Environment;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.streamrune.micronaut.ProjectionFactory;
import org.streamrune.micronaut.StreamRuneMicronautProperties;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The delivery modes the Micronaut app's discovered projections declare, read from the classes
 * exactly as the framework's discovery reads them, and the startup check applied to the demo's own
 * classes over a repository that belongs to no read-model store: once through the runner builder,
 * and once through the framework's discovery entry point (annotation read, routing to a read-model
 * store or the subscription checkpoint, startup check), with the bean collections passed as plain
 * lists, the default read-model store stood in by an in-memory one, and an environment that answers
 * no {@code streamrune.projections.*} override.
 */
class ProjectionDeliveryModeDeclarationsTest {

  private static ProjectionDeliveryMode declared(Class<?> projection) {
    return projection.getAnnotation(ProjectionConfig.class).deliveryMode();
  }

  @Test
  void everyPostgresProjectionDeclaresTransactionalLocal_andTheAuditLogAtLeastOnce() {
    assertThat(
            Map.of(
                "products", declared(DiscoverableProductProjection.class),
                "orders", declared(DiscoverableOrderProjection.class),
                "customers", declared(CustomerViewProjection.class),
                "inventory", declared(DiscoverableInventoryProjection.class)))
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
    var orders = new DiscoverableOrderProjection(other);
    assertThatThrownBy(
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(
                        new InMemoryEventStore(
                            new StreamRuneFactory().eventTypeRegistry(),
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
    try (var runner = discover(processor, demoProjections(processor, processor, "none"))) {
      assertThat(runner).isNotNull();
    }
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
        new DiscoverableProductProjection("products".equals(misplaced) ? other : processor),
        new DiscoverableOrderProjection("orders".equals(misplaced) ? other : processor),
        new CustomerViewProjection("customers".equals(misplaced) ? other : processor),
        new DiscoverableInventoryProjection("inventory".equals(misplaced) ? other : processor),
        new AuditProjection());
  }

  /**
   * The framework's CONTINUOUS discovery, with {@code processor}'s store as the single read-model
   * store bean and no leadership bean.
   */
  private static MultiProjectionRunner discover(
      InMemoryProjectionRepository processor, List<Projection> projections) {
    return new ProjectionFactory()
        .multiProjectionRunner(
            projections,
            new InMemoryEventStore(
                new StreamRuneFactory().eventTypeRegistry(), new InMemoryCryptoEngine()),
            beanContextWith(store(processor)),
            subscriptionCheckpoint(),
            List.of(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            emptyEnvironment(),
            StreamRuneTracing.NOOP);
  }

  /**
   * A bean context that answers only what discovery asks it: the read-model store beans, which the
   * framework collects with {@code getBeansOfType}.
   */
  private static BeanContext beanContextWith(ReadModelStore store) {
    return (BeanContext)
        Proxy.newProxyInstance(
            ProjectionDeliveryModeDeclarationsTest.class.getClassLoader(),
            new Class<?>[] {BeanContext.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getBeansOfType" ->
                      args[0] == ReadModelStore.class ? List.of(store) : List.of();
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" -> "beanContextWith(" + store.name() + ")";
                  default ->
                      throw new UnsupportedOperationException("BeanContext." + method.getName());
                });
  }

  /** An environment with no streamrune.* overrides: every annotation value stands. */
  private static Environment emptyEnvironment() {
    return (Environment)
        Proxy.newProxyInstance(
            ProjectionDeliveryModeDeclarationsTest.class.getClassLoader(),
            new Class<?>[] {Environment.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getProperty" -> Optional.empty();
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" -> "emptyEnvironment";
                  default ->
                      throw new UnsupportedOperationException("Environment." + method.getName());
                });
  }
}
