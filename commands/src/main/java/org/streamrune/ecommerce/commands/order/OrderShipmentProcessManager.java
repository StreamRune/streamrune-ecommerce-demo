package org.streamrune.ecommerce.commands.order;

import java.util.List;
import java.util.Objects;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;

/**
 * Commits the stock of a shipped order: for each line of {@code OrderShipped} it sends {@code
 * ConfirmReservation}, which turns the order's reservation at that product into committed stock.
 * Each command continues the event under a key derived from the event's id and the line's index, so
 * a redelivery stores nothing twice; a confirmation of a reservation that is not there stores
 * nothing. An order that is shipped is never cancelled, so its reservation is never released.
 */
public final class OrderShipmentProcessManager implements EventListener {

  /** The name of the subscription that feeds it, and of that subscription's lease. */
  public static final String SUBSCRIPTION_NAME = "order-shipment";

  private final CommandBus commandBus;

  public OrderShipmentProcessManager(CommandBus commandBus) {
    this.commandBus = Objects.requireNonNull(commandBus, "commandBus is required");
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    for (EventEnvelope envelope : events) {
      if (envelope.event() instanceof OrderEvent.OrderShipped shipped) {
        String eventId = envelope.metadata().eventId().value();
        List<OrderEvent.OrderLine> lines = shipped.lines();
        StreamRuneContext.continuing(envelope)
            .run(
                () -> {
                  for (int i = 0; i < lines.size(); i++) {
                    commandBus.execute(
                        new InventoryCommand.ConfirmReservation(
                            lines.get(i).productId(), shipped.orderId()),
                        confirmKey(eventId, i));
                  }
                });
      }
    }
  }

  /** The confirmation's key: one per shipment event and line, the same on every delivery. */
  static IdempotencyKey confirmKey(String eventId, int line) {
    return new IdempotencyKey("order-ship:" + eventId + ":line:" + line);
  }
}
