package org.streamrune.ecommerce.domain.order;

import java.util.List;
import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;
import org.streamrune.ecommerce.domain.common.Money;

public record OrderState(
    String orderId,
    String customerId,
    List<OrderEvent.OrderLine> lines,
    Money total,
    OrderStatus status)
    implements AggregateState {
  /** The aggregate type the order decider is registered under: streams are order:<orderId>. */
  public static final AggregateType TYPE = AggregateType.of("order");

  public OrderState() {
    this(null, null, List.of(), null, OrderStatus.CREATED);
  }

  public OrderState withStatus(OrderStatus newStatus) {
    return new OrderState(orderId, customerId, lines, total, newStatus);
  }
}
