package org.streamrune.ecommerce.micronaut.config;

import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.projections.InventoryProjection;

/**
 * Micronaut-discoverable inventory projection.
 *
 * <p>The framework-agnostic {@link InventoryProjection} in the shared {@code projections} module
 * carries no Micronaut annotations, and the Micronaut {@code ProjectionFactory} only registers
 * {@link Projection} beans annotated with {@link ProjectionConfig}. This thin subclass adds the
 * annotation (name {@code "inventory"}, matching {@code InventoryProjection.projectionName()}) so
 * the inventory read model is wired into the {@code MultiProjectionRunner} alongside products,
 * orders, and customers. No behaviour is added — all event handling is inherited. The bean itself
 * is produced by {@link StreamRuneFactory#inventoryProjection} (mirroring the customer projection
 * bean). Safe for the Spring app, which uses an explicit {@code MultiProjectionRunner} and never
 * relies on annotation-based discovery.
 *
 * <p>It declares {@code TRANSACTIONAL_LOCAL}: it writes through the {@code
 * JdbcProjectionRepository} bean {@link StreamRuneFactory#projectionRepository} produces, which is
 * also the runner's processor, so its read-model writes and its checkpoint commit in one
 * transaction.
 */
@ProjectionConfig(name = "inventory", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class DiscoverableInventoryProjection extends InventoryProjection {

  public DiscoverableInventoryProjection(ProjectionRepository repository) {
    super(repository);
  }
}
