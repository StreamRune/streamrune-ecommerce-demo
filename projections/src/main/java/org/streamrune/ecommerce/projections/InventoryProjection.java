package org.streamrune.ecommerce.projections;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.inventory.*;
import org.streamrune.ecommerce.queries.dto.InventoryView;

public class InventoryProjection extends BaseProjection {

  public InventoryProjection(ProjectionRepository repository) {
    super(repository, "inventory");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      if (envelope.event() instanceof InventoryEvent evt) {
        switch (evt) {
          case InventoryEvent.StockReserved e ->
              findById(e.productId(), InventoryView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new InventoryView(
                                  e.productId(),
                                  e.availableAfter(),
                                  existing.reserved() + e.quantity(),
                                  existing.committed())));
          case InventoryEvent.StockReleased e ->
              findById(e.productId(), InventoryView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new InventoryView(
                                  e.productId(),
                                  e.availableAfter(),
                                  existing.reserved() - e.quantity(),
                                  existing.committed())));
          case InventoryEvent.ReservationConfirmed e ->
              findById(e.productId(), InventoryView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new InventoryView(
                                  e.productId(),
                                  existing.available(),
                                  existing.reserved() - e.quantity(),
                                  existing.committed() + e.quantity())));
          case InventoryEvent.ShipmentReceived e ->
              save(
                  e.productId(),
                  new InventoryView(
                      e.productId(),
                      e.availableAfter(),
                      findById(e.productId(), InventoryView.class)
                          .map(InventoryView::reserved)
                          .orElse(0),
                      findById(e.productId(), InventoryView.class)
                          .map(InventoryView::committed)
                          .orElse(0)));
        }
      }
    }
  }

  public InventoryView get(String productId) {
    return findById(productId, InventoryView.class).orElse(null);
  }

  public List<InventoryView> listAll() {
    return repository.findAll(projectionName(), InventoryView.class);
  }
}
