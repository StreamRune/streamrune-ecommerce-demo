package org.streamrune.ecommerce.projections;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.queries.dto.InventoryView;

/**
 * The operators' inventory view: one row per product of the catalogue, with the units available,
 * reserved and committed. The row is opened, with nothing held, when the product is created, so a
 * product without stock is listed too; the inventory's events move its counts.
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
        case InventoryEvent.StockReserved e -> {
          InventoryView v = current(e.productId());
          save(
              e.productId(),
              new InventoryView(
                  e.productId(),
                  v.productName(),
                  e.availableAfter(),
                  v.reserved() + e.quantity(),
                  v.committed()));
        }
        case InventoryEvent.StockReleased e -> {
          InventoryView v = current(e.productId());
          save(
              e.productId(),
              new InventoryView(
                  e.productId(),
                  v.productName(),
                  e.availableAfter(),
                  v.reserved() - e.quantity(),
                  v.committed()));
        }
        case InventoryEvent.ReservationConfirmed e -> {
          InventoryView v = current(e.productId());
          save(
              e.productId(),
              new InventoryView(
                  e.productId(),
                  v.productName(),
                  v.available(),
                  v.reserved() - e.quantity(),
                  v.committed() + e.quantity()));
        }
        case InventoryEvent.ShipmentReceived e -> {
          InventoryView v = current(e.productId());
          save(
              e.productId(),
              new InventoryView(
                  e.productId(), v.productName(), e.availableAfter(), v.reserved(), v.committed()));
        }
        case InventoryEvent.InventoryAdjusted e -> {
          InventoryView v = current(e.productId());
          save(
              e.productId(),
              new InventoryView(
                  e.productId(), v.productName(), e.availableAfter(), v.reserved(), v.committed()));
        }
        default -> {}
      }
    }
  }

  /** The product's row, or an empty one when the projection has not seen the product created. */
  private InventoryView current(String productId) {
    return findById(productId, InventoryView.class)
        .orElseGet(() -> new InventoryView(productId, null, 0, 0, 0));
  }

  public InventoryView get(String productId) {
    return findById(productId, InventoryView.class).orElse(null);
  }

  public List<InventoryView> listAll() {
    return repository.findAll(projectionName(), InventoryView.class);
  }
}
