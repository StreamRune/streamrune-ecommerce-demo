package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface OrderEvent extends DomainEvent {
  record OrderPlaced(String orderId, String customerId, List<OrderLine> lines, Money total)
      implements OrderEvent {}

  record OrderConfirmed(String orderId) implements OrderEvent {}

  /** {@code lines} are the order's lines: the shipment commits the stock reserved for each. */
  record OrderShipped(String orderId, List<OrderLine> lines) implements OrderEvent {
    public OrderShipped {
      lines = lines == null ? List.of() : List.copyOf(lines);
    }
  }

  record OrderDelivered(String orderId) implements OrderEvent {}

  /**
   * {@code lines} are the order's lines, empty for an id nobody placed: the undo of a cancellation
   * acts on the stock reserved for each line.
   */
  record OrderCancelled(String orderId, String reason, List<OrderLine> lines)
      implements OrderEvent {
    public OrderCancelled {
      lines = lines == null ? List.of() : List.copyOf(lines);
    }
  }

  record OrderLine(String productId, int quantity, Money unitPrice) {}
}
