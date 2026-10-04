package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.projections.AuditProjection;
import org.streamrune.micronaut.ProjectionFactory;
import org.streamrune.micronaut.StreamRuneMicronautProperties;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The delivery modes the Micronaut app's discovered projections declare, read from the classes
 * exactly as the framework's discovery reads them, and the startup check applied to the demo's own
 * classes over a repository that is not the processor: once through the runner builder, and once
 * through the framework's discovery entry point (annotation read, processor resolution, startup
 * check), with the bean collections passed as plain lists and an environment that answers no {@code
 * streamrune.projections.*} override.
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
                    .eventStore(new InMemoryEventStore())
                    .offsetStore(processor)
                    .atomicProcessor(processor)
                    .register("orders", orders, declared(orders.getClass()))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'orders' declares TRANSACTIONAL_LOCAL and writes to")
        .hasMessageContaining(String.valueOf(other.writeTargetIdentity()))
        .hasMessageContaining(String.valueOf(processor.writeTargetIdentity()));
  }

  @Test
  void discovery_buildsTheDemoShape_overOneProcessor() {
    var processor = new InMemoryProjectionRepository();
    try (var runner = discover(processor, demoProjections(processor, processor, "none"))) {
      assertThat(runner).isNotNull();
    }
  }

  /**
   * Each of the four, built over a repository that is not the processor, is refused at discovery
   * and named with both identities. An AT_LEAST_ONCE_IDEMPOTENT declaration would be accepted over
   * any repository, so this fails if one of the four declarations is flipped.
   */
  @ParameterizedTest
  @ValueSource(strings = {"products", "orders", "customers", "inventory"})
  void discovery_refusesEachOfTheFour_overAnotherRepository(String name) {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    assertThatThrownBy(() -> discover(processor, demoProjections(processor, other, name)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'" + name + "' declares TRANSACTIONAL_LOCAL and writes to")
        .hasMessageContaining(String.valueOf(other.writeTargetIdentity()))
        .hasMessageContaining(String.valueOf(processor.writeTargetIdentity()));
  }

  /**
   * The demo's discovered projections: the one named {@code misplaced} is built over {@code other}.
   */
  private static List<Projection> demoProjections(
      ProjectionRepository processor, ProjectionRepository other, String misplaced) {
    return List.of(
        new DiscoverableProductProjection(misplaced.equals("products") ? other : processor),
        new DiscoverableOrderProjection(misplaced.equals("orders") ? other : processor),
        new CustomerViewProjection(misplaced.equals("customers") ? other : processor),
        new DiscoverableInventoryProjection(misplaced.equals("inventory") ? other : processor),
        new AuditProjection());
  }

  /**
   * The framework's CONTINUOUS discovery, with the processor as the single AtomicBatchProcessor
   * bean and no leadership bean.
   */
  private static MultiProjectionRunner discover(
      InMemoryProjectionRepository processor, List<Projection> projections) {
    return new ProjectionFactory()
        .multiProjectionRunner(
            projections,
            new InMemoryEventStore(),
            processor,
            List.of(),
            List.<AtomicBatchProcessor>of(processor),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            emptyEnvironment());
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
