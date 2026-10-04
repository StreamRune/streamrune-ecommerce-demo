package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface OrderEvent extends DomainEvent {
  record OrderPlaced(String orderId, String customerId, List<OrderLine> lines, Money total)
      implements OrderEvent {}

  record OrderConfirmed(String orderId) implements OrderEvent {}

  record OrderShipped(String orderId) implements OrderEvent {}

  record OrderDelivered(String orderId) implements OrderEvent {}

  record OrderCancelled(String orderId, String reason) implements OrderEvent {}

  record OrderLine(String productId, int quantity, Money unitPrice) {}
}
