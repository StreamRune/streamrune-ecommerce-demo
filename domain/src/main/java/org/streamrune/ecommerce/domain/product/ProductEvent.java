package org.streamrune.ecommerce.domain.product;

import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface ProductEvent extends DomainEvent {
  record ProductCreated(
      String productId, String name, String description, String category, Money price, int stock)
      implements ProductEvent {}

  record PriceUpdated(String productId, Money previousPrice, Money newPrice)
      implements ProductEvent {}

  record StockAdjusted(String productId, int previousStock, int newStock, String reason)
      implements ProductEvent {}

  record ProductDiscontinued(String productId) implements ProductEvent {}
}
