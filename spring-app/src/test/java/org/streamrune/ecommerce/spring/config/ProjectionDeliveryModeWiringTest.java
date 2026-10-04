package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.projections.InventoryProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.projections.ProductProjection;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.runtime.CacheAwareProjection;
import org.streamrune.runtime.CacheInvalidator;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The Spring app wires its MultiProjectionRunner by hand (auto-discovery is off), so the delivery
 * mode rules are pinned on the same builder shape StreamRuneConfig#projectionRunner uses, over
 * in-memory stores, and on the real StreamRuneConfig#projectionRunner bean method, over
 * JdbcProjectionRepository objects that never open a connection.
 */
class ProjectionDeliveryModeWiringTest {

  record Ping(int n) implements DomainEvent {}

  /**
   * A BaseProjection over ANOTHER store, declared at-least-once: it runs beside the transactional
   * four and its rows land in its own store, never in the processor's.
   */
  static final class AuditMemProjection extends BaseProjection {
    AuditMemProjection(InMemoryProjectionRepository memory) {
      super(memory, "audit_mem");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (var e : batch) {
        save(String.valueOf(e.globalOffset().value()), e.eventType().name());
      }
    }
  }

  private static CacheInvalidator invalidator() {
    // CachingQueryBus's only constructor is package-private; the demo builds it through the
    // builder and takes its invalidator from cacheInvalidator(), as StreamRuneConfig does.
    return org.streamrune.runtime.CachingQueryBus.builder()
        .delegate(new org.streamrune.runtime.SimpleQueryBus())
        .build()
        .cacheInvalidator();
  }

  /** A DataSource for a repository that the test never lets reach the database. */
  private static DataSource unusedDataSource() {
    return (DataSource)
        Proxy.newProxyInstance(
            ProjectionDeliveryModeWiringTest.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" ->
                      "unusedDataSource@" + Integer.toHexString(System.identityHashCode(proxy));
                  default ->
                      throw new UnsupportedOperationException(
                          "no database in this test: DataSource." + method.getName());
                });
  }

  private static MultiProjectionRunner.Builder demoShape(
      InMemoryProjectionRepository processor, InMemoryEventStore events) {
    var invalidator = invalidator();
    return MultiProjectionRunner.builder()
        .eventStore(events)
        .offsetStore(processor)
        .atomicProcessor(processor)
        .batchSize(10)
        .register(
            "products",
            new CacheAwareProjection(new ProductProjection(processor), invalidator),
            TRANSACTIONAL_LOCAL)
        .register(
            "orders",
            new CacheAwareProjection(new OrderProjection(processor), invalidator),
            TRANSACTIONAL_LOCAL)
        .register(
            "customers",
            new CacheAwareProjection(new CustomerProjection(processor), invalidator),
            TRANSACTIONAL_LOCAL)
        .register(
            "inventory",
            new CacheAwareProjection(new InventoryProjection(processor), invalidator),
            TRANSACTIONAL_LOCAL);
  }

  @Test
  void theDemoShape_buildsOverOneInMemoryProcessor() {
    assertThat(demoShape(new InMemoryProjectionRepository(), new InMemoryEventStore()).build())
        .isNotNull();
  }

  @Test
  void aTransactionalProjectionOverAnotherRepository_isRefused_namingBoth() {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    assertThatThrownBy(
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(new InMemoryEventStore())
                    .offsetStore(processor)
                    .atomicProcessor(processor)
                    .register("orders", new OrderProjection(other), TRANSACTIONAL_LOCAL)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'orders' declares TRANSACTIONAL_LOCAL and writes to")
        .hasMessageContaining(InMemoryProjectionRepository.class.getName())
        .hasMessageContaining(String.valueOf(other.writeTargetIdentity()))
        .hasMessageContaining(String.valueOf(processor.writeTargetIdentity()))
        .hasMessageContaining("Pass the repository it writes to as the processor");
  }

  /**
   * The real bean method registers each of the four TRANSACTIONAL_LOCAL: built over a repository
   * that is not the processor, that one projection is refused. An AT_LEAST_ONCE_IDEMPOTENT
   * registration would be accepted over any repository, so this fails if one of the four is
   * flipped.
   */
  @ParameterizedTest
  @ValueSource(strings = {"products", "orders", "customers", "inventory"})
  void theProjectionRunnerBean_refusesEachOfTheFour_overAnotherRepository(String name) {
    var processor = new JdbcProjectionRepository(unusedDataSource());
    var other = new JdbcProjectionRepository(unusedDataSource());
    assertThatThrownBy(
            () ->
                new StreamRuneConfig()
                    .projectionRunner(
                        new InMemoryEventStore(),
                        new InMemoryOffsetStore(),
                        processor,
                        new ProductProjection(name.equals("products") ? other : processor),
                        new OrderProjection(name.equals("orders") ? other : processor),
                        new CustomerProjection(name.equals("customers") ? other : processor),
                        new InventoryProjection(name.equals("inventory") ? other : processor),
                        invalidator()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'" + name + "' declares TRANSACTIONAL_LOCAL and writes to")
        .hasMessageContaining(String.valueOf(other.writeTargetIdentity()))
        .hasMessageContaining(String.valueOf(processor.writeTargetIdentity()));
  }

  @Test
  void anAtLeastOnceBaseProjectionOverMemory_runsBesideTheTransactionalFour_andWritesOnlyToMemory()
      throws Exception {
    var processor = new InMemoryProjectionRepository();
    var memory = new InMemoryProjectionRepository();
    var events = new InMemoryEventStore();
    events.append(
        StreamId.of(ProductState.TYPE, AggregateId.of("p-1")),
        List.of(
            new EventEnvelope(
                GlobalOffset.initial(),
                StreamId.of(ProductState.TYPE, AggregateId.of("p-1")),
                new Version(1),
                new EventType("Ping"),
                new Ping(1),
                new EventMetadata(
                    IdGenerator.generateEventId(),
                    IdGenerator.generateCommandId(),
                    null,
                    null,
                    CorrelationId.of("c"),
                    null,
                    null,
                    Instant.now()))),
        new Version(0));
    var runner =
        demoShape(processor, events)
            .register("audit_mem", new AuditMemProjection(memory), AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(
              () -> memory.findById(ProjectionName.of("audit_mem"), "1", String.class).isPresent());
    } finally {
      runner.close();
    }
    assertThat(processor.hasReadModels(ProjectionName.of("audit_mem")))
        .as("nothing the at-least-once projection writes lands in the processor's store")
        .isFalse();
    assertThat(processor.committedOffset(ProjectionName.of("audit_mem")))
        .isEqualTo(GlobalOffset.of(1));
  }
}
