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
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.projections.AuditProjection;
import org.streamrune.quarkus.ProjectionProducer;
import org.streamrune.quarkus.StreamRuneQuarkusProperties;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The delivery modes the Quarkus app's discovered projections declare, read from the classes
 * exactly as the framework's discovery reads them, and the startup check applied to the demo's own
 * classes over a repository that is not the processor: once through the runner builder, and once
 * through the framework's discovery entry point (annotation read, processor resolution, startup
 * check), with the CDI collections stood in by plain lists.
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
    assertThat(discover(processor, demoProjections(processor, processor, "none"))).isNotNull();
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
        new StreamRuneProducer.DiscoverableProductProjection(
            misplaced.equals("products") ? other : processor),
        new StreamRuneProducer.DiscoverableOrderProjection(
            misplaced.equals("orders") ? other : processor),
        new StreamRuneProducer.DiscoverableCustomerProjection(
            misplaced.equals("customers") ? other : processor),
        new StreamRuneProducer.DiscoverableInventoryProjection(
            misplaced.equals("inventory") ? other : processor),
        new AuditProjection());
  }

  /**
   * The framework's CONTINUOUS discovery, with the processor as the single AtomicBatchProcessor
   * bean.
   */
  private static MultiProjectionRunner discover(
      InMemoryProjectionRepository processor, List<Projection> projections) {
    return new ProjectionProducer()
        .multiProjectionRunner(
            instance(projections),
            new InMemoryEventStore(),
            processor,
            instance(List.of()),
            instance(List.<AtomicBatchProcessor>of(processor)),
            instance(List.of()),
            instance(List.of()),
            instance(List.of()),
            instance(List.of()),
            properties(),
            emptyConfig());
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
                  case "pollingIntervalMs" -> 5000L;
                  case "pollingJitterMs" -> 1000L;
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
