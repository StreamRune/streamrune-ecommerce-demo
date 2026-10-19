package org.streamrune.ecommerce.commands.order;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.*;

public class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {

  @Override
  public OrderState initialState() {
    return new OrderState();
  }

  @Override
  public List<OrderEvent> decide(OrderCommand cmd, OrderState state) {
    return switch (cmd) {
      case OrderCommand.PlaceOrder c -> {
        // An order id is placed once. Without this check a second PlaceOrder would append another
        // OrderPlaced, and evolve would replace the order's lines and total and reset a confirmed
        // or shipped order to CREATED. An id cancelled before anybody placed it is not placed
        // afterwards: its stream already says CANCELLED.
        if (state.orderId() != null || state.status() == OrderStatus.CANCELLED)
          throw new DomainException("Order already exists: " + c.orderId());
        List<OrderEvent.OrderLine> eventLines =
            c.lines().stream()
                .map(l -> new OrderEvent.OrderLine(l.productId(), l.quantity(), l.unitPrice()))
                .toList();
        Money total =
            c.lines().stream()
                .map(l -> l.unitPrice().multiply(l.quantity()))
                .reduce(new Money(java.math.BigDecimal.ZERO, "USD"), Money::add);
        yield List.of(new OrderEvent.OrderPlaced(c.orderId(), c.customerId(), eventLines, total));
      }

      case OrderCommand.ConfirmOrder c -> {
        requirePlaced(state, c.orderId());
        if (state.status() != OrderStatus.CREATED)
          throw new DomainException(
              "Can only confirm CREATED orders, current status: " + state.status());
        yield List.of(new OrderEvent.OrderConfirmed(c.orderId()));
      }

      case OrderCommand.ShipOrder c -> {
        requirePlaced(state, c.orderId());
        if (state.status() != OrderStatus.CONFIRMED)
          throw new DomainException(
              "Can only ship CONFIRMED orders, current status: " + state.status());
        yield List.of(new OrderEvent.OrderShipped(c.orderId(), state.lines()));
      }

      case OrderCommand.DeliverOrder c -> {
        requirePlaced(state, c.orderId());
        if (state.status() != OrderStatus.SHIPPED)
          throw new DomainException(
              "Can only deliver SHIPPED orders, current status: " + state.status());
        yield List.of(new OrderEvent.OrderDelivered(c.orderId()));
      }

      case OrderCommand.CancelOrder c -> {
        // No requirePlaced here: a cancellation of an id nobody placed is recorded and undoes
        // nothing, and a compensation command the decider refuses ends the saga FAILED instead of
        // COMPENSATED. A shipped or delivered order is not cancelled. A cancelled order is
        // cancelled again: every OrderCancelled starts the undo of the order's money and stock,
        // and a reservation stored after an earlier undo ran is released by the undo of the next
        // one.
        if (state.status() == OrderStatus.SHIPPED || state.status() == OrderStatus.DELIVERED)
          throw new DomainException("Cannot cancel " + state.status() + " orders");
        yield List.of(new OrderEvent.OrderCancelled(c.orderId(), c.reason(), state.lines()));
      }
    };
  }

  /**
   * An order that was never placed is not confirmed, shipped or delivered. Its empty state reads
   * {@code CREATED}, so without this check {@code ConfirmOrder} would open a stream for an id
   * nobody placed.
   */
  private static void requirePlaced(OrderState state, String orderId) {
    if (state.orderId() == null) throw new DomainException("Order not found: " + orderId);
  }

  @Override
  public OrderState evolve(OrderState state, OrderEvent evt) {
    return switch (evt) {
      case OrderEvent.OrderPlaced e ->
          new OrderState(e.orderId(), e.customerId(), e.lines(), e.total(), OrderStatus.CREATED);
      case OrderEvent.OrderConfirmed e -> state.withStatus(OrderStatus.CONFIRMED);
      case OrderEvent.OrderShipped e -> state.withStatus(OrderStatus.SHIPPED);
      case OrderEvent.OrderDelivered e -> state.withStatus(OrderStatus.DELIVERED);
      case OrderEvent.OrderCancelled e -> state.withStatus(OrderStatus.CANCELLED);
    };
  }
}
