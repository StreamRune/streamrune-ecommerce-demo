package org.streamrune.ecommerce.commands.saga;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;

/**
 * OrderFulfillment saga: OrderPlaced → InitiatePayment → PaymentCaptured → ReserveStock →
 * StockReserved → ConfirmOrder → COMPLETED.
 *
 * <p>On PaymentFailed → CANCELLING → CancelOrder → OrderCancelled → FAILED. The CANCELLING step is
 * a non-terminal intermediate (not a direct evolve to FAILED): {@code SagaRunner} skips {@link
 * #handle} once {@code evolve} reaches a terminal {@code SagaStatus}, so a final command can only
 * be dispatched from a state that is still running. Compensation is step-appropriate: a pre-payment
 * failure → CancelOrder only; a post-payment, pre-reservation failure → RefundPayment +
 * CancelOrder; a post-reservation failure → ReleaseStock + RefundPayment + CancelOrder (see {@link
 * #compensate}).
 *
 * <p>Correlation: an event belongs to the saga of the order it is about, and {@link #correlate}
 * reads that order from the event's own data: the {@code orderId} of an order, payment or inventory
 * event, or the payment id of a payment event that carries no order id ({@code "pay-<orderId>"},
 * the id {@link #evolve} derives from {@code OrderPlaced}). The event's correlation id decides
 * nothing. {@code SagaRunner} binds the saga id as the correlation id of every command the saga
 * dispatches, which is useful for tracing one fulfillment through the log, but a correlation id is
 * request metadata: an HTTP request's is whatever the client put in {@code X-Correlation-Id}, and a
 * client that could address a saga with it would have its own order's {@code OrderConfirmed}
 * evolved by another order's saga. {@link #evolve} and {@link #handle} check the same thing from
 * the other side, against the order id and the payment id the saga stores in its own state: an
 * event that is not about this saga's order changes nothing and dispatches nothing, whoever hands
 * it over and whatever routed it.
 *
 * <p>A {@code SagaId} holds 255 characters at most, so {@code OrderCommand.PlaceOrder} refuses an
 * order id longer than 255 minus the prefix ({@code SAGA_ID_PREFIX}, which this class uses to build
 * the id) when the order is placed; an order id that got past it would make {@link #extractSagaId}
 * throw on {@code OrderPlaced}, and the order would stay {@code CREATED} with no saga. For every
 * other event of such an order {@link #correlate} answers "no saga", which is the truth.
 *
 * <p>The capture leg is driven by a payment process manager (in the runtime app), which reacts to
 * {@code PaymentInitiated} by calling the payment gateway and dispatching {@code CapturePayment}
 * (success) or {@code FailPayment} (failure).
 *
 * <p>Command targets are built with the {@code AggregateId} constructor, never {@code
 * AggregateId.of}. Every target comes from the saga's stored state — the order, payment and product
 * ids recorded from {@code OrderPlaced} — so it is data already in the log, and the constructor is
 * the decode door for it. {@code AggregateId.of} is the ingress factory: it refuses a control
 * character or an id longer than 255 characters, so inside {@link #handle} or {@link #compensate}
 * it would make the framework quarantine the event as poison (payment captured, never refunded).
 * The order endpoints never record such a product id: {@code OrderCommand.OrderLine} refuses one
 * that carries a control character, or is longer than 255 characters, when the order is placed, so
 * the client gets a {@code 400} and nothing is written. Built with the constructor, the command
 * still reaches the bus, whose id extractor applies {@code AggregateId.of}: a product id that
 * reached the log without crossing that check (an {@code OrderPlaced} event an application appended
 * itself, say) is refused there with an {@code IllegalArgumentException}, and the framework
 * compensates the step (refund + cancel).
 *
 * <p>A saga command's target is only an id: the bus resolves the aggregate type from the command's
 * registration. {@code ReserveStock} and {@code ReleaseStock} are registered under {@code
 * inventory}, so the stock streams are {@code inventory:<productId>} — the product id is the
 * inventory id, with no hand-made prefix.
 */
public class OrderFulfillmentSaga implements SagaOrchestrator<OrderFulfillmentState> {

  /** A fulfillment's payment id is this prefix and the order id. */
  private static final String PAYMENT_ID_PREFIX = "pay-";

  @Override
  public Class<OrderFulfillmentState> stateType() {
    return OrderFulfillmentState.class;
  }

  @Override
  public OrderFulfillmentState initialState(SagaId sagaId) {
    return new OrderFulfillmentState(
        sagaId, null, null, null, null, List.of(), OrderFulfillmentStatus.AWAITING_PAYMENT);
  }

  @Override
  public boolean isStartEvent(EventEnvelope event) {
    return event.event() instanceof OrderEvent.OrderPlaced;
  }

  @Override
  public SagaId extractSagaId(EventEnvelope event) {
    if (event.event() instanceof OrderEvent.OrderPlaced placed) {
      return new SagaId(OrderCommand.PlaceOrder.SAGA_ID_PREFIX + placed.orderId());
    }
    throw new IllegalArgumentException("Not a start event: " + event.eventType().name());
  }

  /**
   * The saga an event belongs to: the saga of the order the event is about. Only the events of the
   * fulfillment process are routed, each by the field that names its order; every other event, and
   * an event whose order no saga id can hold, belongs to no saga.
   *
   * <p>The order, payment and inventory events below are written by commands this saga dispatched
   * (and, for the capture leg, by the payment process manager), with ids taken from the saga's own
   * state. {@code OrderConfirmed} and {@code OrderCancelled} are also written by a request of the
   * order's customer or an {@code ADMIN}: such an event reaches the saga of that order and no
   * other, and {@link #evolve} decides what it means there.
   */
  @Override
  public Optional<SagaId> correlate(EventEnvelope event) {
    return switch (event.event()) {
      case OrderEvent.OrderConfirmed e -> sagaOfOrder(e.orderId());
      case OrderEvent.OrderCancelled e -> sagaOfOrder(e.orderId());
      case PaymentEvent.PaymentInitiated e -> sagaOfOrder(e.orderId());
      case PaymentEvent.PaymentCaptured e -> sagaOfOrder(e.orderId());
      case PaymentEvent.PaymentFailed e -> sagaOfPayment(e.paymentId());
      case PaymentEvent.PaymentRefunded e -> sagaOfPayment(e.paymentId());
      case InventoryEvent.StockReserved e -> sagaOfOrder(e.orderId());
      case InventoryEvent.StockReleased e -> sagaOfOrder(e.orderId());
      default -> Optional.empty();
    };
  }

  /** The saga that fulfils the given order, or none for an id no saga id can hold. */
  private static Optional<SagaId> sagaOfOrder(String orderId) {
    if (orderId == null
        || orderId.isEmpty()
        || orderId.length() > OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH) {
      return Optional.empty();
    }
    return Optional.of(new SagaId(OrderCommand.PlaceOrder.SAGA_ID_PREFIX + orderId));
  }

  /**
   * The saga behind a payment id. A fulfillment's payment id is {@code "pay-" + orderId}, set by
   * {@link #evolve} on {@code OrderPlaced}; a payment under any other id is not a fulfillment's.
   */
  private static Optional<SagaId> sagaOfPayment(String paymentId) {
    if (paymentId == null || !paymentId.startsWith(PAYMENT_ID_PREFIX)) {
      return Optional.empty();
    }
    return sagaOfOrder(paymentId.substring(PAYMENT_ID_PREFIX.length()));
  }

  /**
   * Whether the event is about this saga's order, answered from what the saga itself stores and
   * without asking {@link #correlate}: the {@code OrderPlaced} that starts this saga names the
   * order the saga id is built from, and every later event names the order id or the payment id the
   * saga recorded from that {@code OrderPlaced}. The answer holds whoever routed the event here.
   */
  private static boolean isAboutThisSaga(OrderFulfillmentState state, EventEnvelope event) {
    return switch (event.event()) {
      case OrderEvent.OrderPlaced e ->
          sagaOfOrder(e.orderId()).filter(state.sagaId()::equals).isPresent();
      case OrderEvent.OrderConfirmed e -> isTheSagasOwn(state.orderId(), e.orderId());
      case OrderEvent.OrderCancelled e -> isTheSagasOwn(state.orderId(), e.orderId());
      case PaymentEvent.PaymentInitiated e -> isTheSagasOwn(state.orderId(), e.orderId());
      case PaymentEvent.PaymentCaptured e -> isTheSagasOwn(state.orderId(), e.orderId());
      case PaymentEvent.PaymentFailed e -> isTheSagasOwn(state.paymentId(), e.paymentId());
      case PaymentEvent.PaymentRefunded e -> isTheSagasOwn(state.paymentId(), e.paymentId());
      case InventoryEvent.StockReserved e -> isTheSagasOwn(state.orderId(), e.orderId());
      case InventoryEvent.StockReleased e -> isTheSagasOwn(state.orderId(), e.orderId());
      default -> false;
    };
  }

  /**
   * Whether an id an event carries is the id this saga recorded; a saga that recorded none has
   * none.
   */
  private static boolean isTheSagasOwn(String recorded, String inTheEvent) {
    return recorded != null && recorded.equals(inTheEvent);
  }

  @Override
  public OrderFulfillmentState evolve(OrderFulfillmentState state, EventEnvelope event) {
    if (!isAboutThisSaga(state, event)) {
      return state;
    }
    Object evt = event.event();
    return switch (evt) {
      case OrderEvent.OrderPlaced e ->
          new OrderFulfillmentState(
              state.sagaId(),
              e.orderId(),
              e.customerId(),
              PAYMENT_ID_PREFIX + e.orderId(),
              e.total(),
              e.lines(),
              OrderFulfillmentStatus.AWAITING_PAYMENT);

      case PaymentEvent.PaymentCaptured e ->
          new OrderFulfillmentState(
              state.sagaId(),
              state.orderId(),
              state.customerId(),
              state.paymentId(),
              state.orderTotal(),
              state.lines(),
              OrderFulfillmentStatus.AWAITING_INVENTORY);

      // PaymentFailed lands on the non-terminal CANCELLING (not directly on FAILED): the
      // framework's SagaRunner skips handle() once evolve() reaches a terminal SagaStatus, which
      // would make the CancelOrder dispatch below unreachable. CANCELLING keeps the saga RUNNING
      // long enough for handle() to emit CancelOrder; OrderCancelled then closes it out to FAILED.
      case PaymentEvent.PaymentFailed e ->
          new OrderFulfillmentState(
              state.sagaId(),
              state.orderId(),
              state.customerId(),
              state.paymentId(),
              state.orderTotal(),
              state.lines(),
              OrderFulfillmentStatus.CANCELLING);

      case OrderEvent.OrderCancelled e ->
          state.fulfillmentStatus() == OrderFulfillmentStatus.CANCELLING
              ? new OrderFulfillmentState(
                  state.sagaId(),
                  state.orderId(),
                  state.customerId(),
                  state.paymentId(),
                  state.orderTotal(),
                  state.lines(),
                  OrderFulfillmentStatus.FAILED)
              : state;

      case InventoryEvent.StockReserved e ->
          state.fulfillmentStatus() == OrderFulfillmentStatus.AWAITING_INVENTORY
              ? new OrderFulfillmentState(
                  state.sagaId(),
                  state.orderId(),
                  state.customerId(),
                  state.paymentId(),
                  state.orderTotal(),
                  state.lines(),
                  OrderFulfillmentStatus.AWAITING_CONFIRMATION)
              : state;

      // The confirmation this saga asked for, after the payment and the reservation. An order its
      // customer or an ADMIN confirms by hand earlier does not complete the saga: it goes on, and
      // its own ConfirmOrder is then refused and compensated.
      case OrderEvent.OrderConfirmed e ->
          state.fulfillmentStatus() == OrderFulfillmentStatus.AWAITING_CONFIRMATION
              ? new OrderFulfillmentState(
                  state.sagaId(),
                  state.orderId(),
                  state.customerId(),
                  state.paymentId(),
                  state.orderTotal(),
                  state.lines(),
                  OrderFulfillmentStatus.COMPLETED)
              : state;

      default -> state;
    };
  }

  @Override
  public List<SagaCommand> handle(OrderFulfillmentState state, EventEnvelope event) {
    if (!isAboutThisSaga(state, event)) {
      return List.of();
    }
    return switch (state.fulfillmentStatus()) {
      case AWAITING_PAYMENT -> {
        if (event.event() instanceof OrderEvent.OrderPlaced) {
          yield List.of(
              new SagaCommand(
                  new PaymentCommand.InitiatePayment(
                      state.paymentId(), state.orderId(), state.orderTotal()),
                  new AggregateId(state.paymentId())));
        }
        yield List.of();
      }

      // Reserve stock once the payment is captured. The demo's orders carry a single line; we
      // reserve that line, which drives one StockReserved → AWAITING_CONFIRMATION → ConfirmOrder.
      // (A multi-line order would need per-line reservation tracking before confirming; out of
      // scope for the demo.)
      case AWAITING_INVENTORY -> {
        if (!(event.event() instanceof PaymentEvent.PaymentCaptured) || state.lines().isEmpty()) {
          yield List.of();
        }
        var line = state.lines().get(0);
        yield List.of(
            new SagaCommand(
                new InventoryCommand.ReserveStock(
                    line.productId(), state.orderId(), line.quantity()),
                new AggregateId(line.productId())));
      }

      case AWAITING_CONFIRMATION -> {
        if (event.event() instanceof InventoryEvent.StockReserved) {
          yield List.of(
              new SagaCommand(
                  new OrderCommand.ConfirmOrder(state.orderId()),
                  new AggregateId(state.orderId())));
        }
        yield List.of();
      }

      case CANCELLING -> {
        if (event.event() instanceof PaymentEvent.PaymentFailed) {
          yield List.of(
              new SagaCommand(
                  new OrderCommand.CancelOrder(state.orderId(), "Payment failed"),
                  new AggregateId(state.orderId())));
        }
        yield List.of();
      }

      default -> List.of();
    };
  }

  @Override
  public List<SagaCommand> compensate(
      OrderFulfillmentState state, Throwable failure, SagaCommand failedCommand) {
    return switch (state.fulfillmentStatus()) {
      // Payment not yet captured — nothing to refund, just cancel.
      case AWAITING_PAYMENT ->
          List.of(
              new SagaCommand(
                  new OrderCommand.CancelOrder(state.orderId(), failure.getMessage()),
                  new AggregateId(state.orderId())));

      // Payment captured, stock not reserved — refund the payment and cancel.
      case AWAITING_INVENTORY ->
          List.of(
              new SagaCommand(
                  new PaymentCommand.RefundPayment(
                      state.paymentId(), "Inventory reservation failed"),
                  new AggregateId(state.paymentId())),
              new SagaCommand(
                  new OrderCommand.CancelOrder(state.orderId(), "Inventory reservation failed"),
                  new AggregateId(state.orderId())));

      // Payment captured AND stock reserved — release the reservation before refunding and
      // cancelling, so the held stock isn't stranded.
      case AWAITING_CONFIRMATION -> {
        var commands = new ArrayList<SagaCommand>();
        if (!state.lines().isEmpty()) {
          var line = state.lines().get(0);
          commands.add(
              new SagaCommand(
                  new InventoryCommand.ReleaseStock(
                      line.productId(), state.orderId(), line.quantity()),
                  new AggregateId(line.productId())));
        }
        commands.add(
            new SagaCommand(
                new PaymentCommand.RefundPayment(state.paymentId(), "Order confirmation failed"),
                new AggregateId(state.paymentId())));
        commands.add(
            new SagaCommand(
                new OrderCommand.CancelOrder(state.orderId(), "Order confirmation failed"),
                new AggregateId(state.orderId())));
        yield List.copyOf(commands);
      }

      // Terminal/other states (COMPLETED, FAILED, COMPENSATING, CANCELLING, ...) — nothing to
      // compensate; an empty list is classified by the framework as FAILED, which is correct for
      // an unexpected compensation request in a terminal (or already-cancelling) state.
      default -> List.of();
    };
  }
}
