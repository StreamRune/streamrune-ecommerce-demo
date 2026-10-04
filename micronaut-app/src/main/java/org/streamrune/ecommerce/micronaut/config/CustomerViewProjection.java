package org.streamrune.ecommerce.micronaut.config;

import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.projections.CustomerProjection;

/**
 * Micronaut-discoverable customer projection.
 *
 * <p>The framework-agnostic {@link CustomerProjection} in the shared {@code projections} module
 * carries no Micronaut annotations, and the Micronaut {@code ProjectionFactory} only registers
 * {@link Projection} beans annotated with {@link ProjectionConfig}. This thin subclass adds the
 * annotation (name {@code "customers"}, matching {@code CustomerProjection.projectionName()}) so
 * the customer read model is wired into the {@code MultiProjectionRunner} alongside products and
 * orders. No behaviour is added — all event handling is inherited. The bean itself is produced by
 * {@link StreamRuneFactory#customerProjection} (mirroring the product/order projection beans).
 *
 * <p>It declares {@code TRANSACTIONAL_LOCAL}: it writes through the {@code
 * JdbcProjectionRepository} bean {@link StreamRuneFactory#projectionRepository} produces, which is
 * also the runner's processor, so its read-model writes and its checkpoint commit in one
 * transaction.
 */
@ProjectionConfig(name = "customers", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class CustomerViewProjection extends CustomerProjection {

  public CustomerViewProjection(ProjectionRepository repository) {
    super(repository);
  }
}
