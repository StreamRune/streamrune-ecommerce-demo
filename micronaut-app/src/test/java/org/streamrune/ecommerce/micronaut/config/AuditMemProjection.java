package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * A BaseProjection over ANOTHER store, declared at-least-once, discovered in the real app beside
 * the four TRANSACTIONAL_LOCAL PostgreSQL projections: its rows land in its own in-memory store,
 * never in the processor's database. Only the {@code test} environment (which {@code MicronautTest}
 * activates) creates the bean. The repository is a plain field, not a bean: a bean of that class
 * would be exposed under every type it implements, AtomicBatchProcessor included, and become a
 * second processor candidate.
 */
@Singleton
@Requires(env = "test")
@ProjectionConfig(
    name = "audit_mem",
    deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
public class AuditMemProjection extends BaseProjection {
  public static final InMemoryProjectionRepository MEMORY = new InMemoryProjectionRepository();

  public AuditMemProjection() {
    super(MEMORY, "audit_mem");
  }

  @Override
  public void process(List<EventEnvelope> batch) {
    for (var e : batch) {
      save(String.valueOf(e.globalOffset().value()), e.eventType().name());
    }
  }
}
