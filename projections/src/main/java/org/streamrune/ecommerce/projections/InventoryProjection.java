package org.streamrune.ecommerce.projections;

import java.util.List;
import java.util.function.UnaryOperator;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.queries.dto.InventoryView;

/**
 * The operators' inventory view: one row per product of the catalogue, with the units available,
 * reserved and committed. The row is opened, with nothing held, when the product is created, so a
 * product without stock is listed too; the inventory's events move its counts. Like {@code
 * ProductProjection}, it changes only a row whose product it has seen created: the inventory
 * endpoints refuse a product the catalogue does not hold, so its {@code ProductCreated} comes first
 * in the global stream. A refused reservation changes no count.
 */
public class InventoryProjection extends BaseProjection {

  public InventoryProjection(ProjectionRepository repository) {
    super(repository, "inventory");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      switch (envelope.event()) {
        case ProductEvent.ProductCreated e ->
            save(e.productId(), new InventoryView(e.productId(), e.name(), 0, 0, 0));
        case InventoryEvent.StockReserved e ->
            update(
                e.productId(),
                v ->
                    new InventoryView(
                        v.productId(),
                        v.productName(),
                        e.availableAfter(),
                        v.reserved() + e.quantity(),
                        v.committed()));
        case InventoryEvent.StockReleased e ->
            update(
                e.productId(),
                v ->
                    new InventoryView(
                        v.productId(),
                        v.productName(),
                        e.availableAfter(),
                        v.reserved() - e.quantity(),
                        v.committed()));
        case InventoryEvent.ReservationConfirmed e ->
            update(
                e.productId(),
                v ->
                    new InventoryView(
                        v.productId(),
                        v.productName(),
                        v.available(),
                        v.reserved() - e.quantity(),
                        v.committed() + e.quantity()));
        case InventoryEvent.ShipmentReceived e ->
            update(
                e.productId(),
                v ->
                    new InventoryView(
                        v.productId(),
                        v.productName(),
                        e.availableAfter(),
                        v.reserved(),
                        v.committed()));
        case InventoryEvent.InventoryAdjusted e ->
            update(
                e.productId(),
                v ->
                    new InventoryView(
                        v.productId(),
                        v.productName(),
                        e.availableAfter(),
                        v.reserved(),
                        v.committed()));
        default -> {}
      }
    }
  }

  /** Rewrites the row of a product the projection has seen created. */
  private void update(String productId, UnaryOperator<InventoryView> change) {
    findById(productId, InventoryView.class).ifPresent(v -> save(productId, change.apply(v)));
  }

  public InventoryView get(String productId) {
    return findById(productId, InventoryView.class).orElse(null);
  }

  public List<InventoryView> listAll() {
    return repository.findAll(projectionName(), InventoryView.class);
  }
}
