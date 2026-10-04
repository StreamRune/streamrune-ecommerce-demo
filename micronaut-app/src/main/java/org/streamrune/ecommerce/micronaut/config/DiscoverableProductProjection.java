package org.streamrune.ecommerce.micronaut.config;

import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.projections.ProductProjection;

/**
 * Micronaut-discoverable product projection.
 *
 * <p>The framework-agnostic {@link ProductProjection} in the shared {@code projections} module
 * carries no Micronaut annotations, and the Micronaut {@code ProjectionFactory} only registers
 * {@link Projection} beans annotated with {@link ProjectionConfig}. This thin subclass carries the
 * annotation (name {@code "products"}, matching {@code ProductProjection.projectionName()} — the
 * same declaration the shared class carries) on a class of this module, so the product read model
 * is wired into the {@code MultiProjectionRunner}. No behaviour is added — all event handling is
 * inherited. The bean is produced by {@link StreamRuneFactory#productProjection}. Safe for the
 * Spring app, which uses an explicit {@code MultiProjectionRunner} and never relies on
 * annotation-based discovery.
 *
 * <p>It declares {@code TRANSACTIONAL_LOCAL}: it writes through the {@code
 * JdbcProjectionRepository} bean {@link StreamRuneFactory#projectionRepository} produces, which is
 * also the runner's processor, so its read-model writes and its checkpoint commit in one
 * transaction.
 */
@ProjectionConfig(name = "products", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class DiscoverableProductProjection extends ProductProjection {

  public DiscoverableProductProjection(ProjectionRepository repository) {
    super(repository);
  }
}
