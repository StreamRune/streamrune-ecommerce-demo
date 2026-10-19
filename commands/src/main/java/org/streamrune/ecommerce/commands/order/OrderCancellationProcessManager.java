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
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentIds;

/**
 * Undoes an order's money and stock for every {@code OrderCancelled}, whoever cancelled the order:
 * its customer, an {@code ADMIN}, or the fulfillment saga's compensation. For each line it sends
 * {@code ReleaseStock}, then {@code CancelPayment} for the order's payment. Every command is safe
 * whether or not the step it undoes ran: a release with nothing reserved, and a cancellation of a
 * payment that is voided, refunded or failed, store nothing.
 *
 * <p>Each command continues the event ({@link StreamRuneContext#continuing}) under a key derived
 * from the event's id and the line's index: a redelivery of the event is answered from the command
 * inbox, and another {@code OrderCancelled} of the same order is a new event with new keys, whose
 * undo stores only what the earlier one left. A cancellation of an order nobody placed carries no
 * lines, and there is nothing to undo.
 *
 * <p>A failure leaves {@link #onEvents}: the subscription keeps its checkpoint and delivers the
 * event again. None of the commands is refused by its decider in a consistent store, so a refusal
 * is a fault and shows on the subscription, at {@code ERROR}, until it is fixed.
 */
public final class OrderCancellationProcessManager implements EventListener {

  /** The name of the subscription that feeds it, and of that subscription's lease. */
  public static final String SUBSCRIPTION_NAME = "order-cancellation";

  private final CommandBus commandBus;

  public OrderCancellationProcessManager(CommandBus commandBus) {
    this.commandBus = Objects.requireNonNull(commandBus, "commandBus is required");
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    for (EventEnvelope envelope : events) {
      if (envelope.event() instanceof OrderEvent.OrderCancelled cancelled
          && !cancelled.lines().isEmpty()) {
        undo(envelope, cancelled);
      }
    }
  }

  private void undo(EventEnvelope envelope, OrderEvent.OrderCancelled cancelled) {
    String eventId = envelope.metadata().eventId().value();
    List<OrderEvent.OrderLine> lines = cancelled.lines();
    StreamRuneContext.continuing(envelope)
        .run(
            () -> {
              for (int i = 0; i < lines.size(); i++) {
                commandBus.execute(
                    new InventoryCommand.ReleaseStock(
                        lines.get(i).productId(), cancelled.orderId()),
                    releaseKey(eventId, i));
              }
              commandBus.execute(
                  new PaymentCommand.CancelPayment(
                      PaymentIds.forOrder(cancelled.orderId()), cancelled.reason()),
                  paymentKey(eventId));
            });
  }

  /** The release's key: one per cancellation event and line, the same on every delivery. */
  static IdempotencyKey releaseKey(String eventId, int line) {
    return new IdempotencyKey("order-cancel:" + eventId + ":line:" + line);
  }

  /** The payment cancellation's key: one per cancellation event. */
  static IdempotencyKey paymentKey(String eventId) {
    return new IdempotencyKey("order-cancel:" + eventId + ":payment");
  }
}
