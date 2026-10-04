package org.streamrune.ecommerce.domain.inventory;

import org.streamrune.core.DomainEvent;

public sealed interface InventoryEvent extends DomainEvent {
  record StockReserved(String productId, String orderId, int quantity, int availableAfter)
      implements InventoryEvent {}

  record StockReleased(String productId, String orderId, int quantity, int availableAfter)
      implements InventoryEvent {}

  record ReservationConfirmed(String productId, String orderId, int quantity)
      implements InventoryEvent {}

  record ShipmentReceived(String productId, int quantity, int availableAfter)
      implements InventoryEvent {}
}
