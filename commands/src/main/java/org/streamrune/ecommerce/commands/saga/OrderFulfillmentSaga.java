package org.streamrune.ecommerce.commands.saga;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentIds;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;

/**
 * OrderFulfillment saga: OrderPlaced → InitiatePayment → PaymentCaptured → one ReserveStock per
 * line → one StockReserved per line → ConfirmOrder → COMPLETED. The saga records each product whose
 * stock is reserved; the {@code StockReserved} that completes the set of the order's lines moves it
 * on to the confirmation, and every other one waits for the rest.
 *
 * <p>On PaymentFailed → CANCELLING → CancelOrder → OrderCancelled → FAILED. The CANCELLING step is
 * a non-terminal intermediate (not a direct evolve to FAILED): {@code SagaRunner} skips {@link
 * #handle} once {@code evolve} reaches a terminal {@code SagaStatus}, so a final command can only
 * be dispatched from a state that is still running.
 *
 * <p><b>A cancellation the saga did not ask for.</b> The order's customer or an {@code ADMIN} can
 * cancel the order while the saga runs. Its {@code OrderCancelled} moves the saga to {@code
 * COMPENSATING}: the step's row write opens the compensation episode, and the forward fence refuses
 * every forward command of the saga from then on. The compensation sweeper, the timeout runner or
 * the saga's next event resumes the episode.
 *
 * <p><b>Compensation.</b> In every state the compensation is one command, {@code CancelOrder} with
 * {@link #COMPENSATION_REASON} (see {@link #compensate}). The order's {@code OrderCancelled} is
 * what undoes the money and the stock: {@code OrderCancellationProcessManager} releases the order's
 * stock line by line and voids or refunds its payment for every {@code OrderCancelled}, whoever
 * cancelled the order. The release and the refund are not in the compensation, because the
 * framework dispatches every compensation command even after one of them is refused: a cancellation
 * of a shipped order is refused, and a release and a refund dispatched next to it would undo an
 * order that is shipped. Through the cancellation's event, a refused cancellation undoes nothing.
 *
 * <p>Correlation: an event reaches this saga in one of two ways, and neither reads the event's
 * correlation id.
 *
 * <ul>
 *   <li><b>An event this saga caused.</b> The command bus stores, with every event of a command a
 *       saga dispatched, the saga's origin: the saga type, the saga id and a hop count ({@code
 *       EventMetadata.sagaOrigin()}). {@code SagaRunner} hands such an event to the saga its origin
 *       names and asks this class nothing. {@code PaymentInitiated}, {@code StockReserved} and the
 *       saga's own {@code OrderConfirmed} and {@code OrderCancelled} arrive this way; so do {@code
 *       PaymentCaptured} and {@code PaymentFailed}, which the payment process manager causes by
 *       continuing {@code PaymentInitiated}, and the {@code StockReleased}, {@code PaymentVoided}
 *       and {@code PaymentRefunded} the cancellation process manager causes by continuing the
 *       saga's own {@code OrderCancelled} (each one hop further from the saga, the same origin).
 *   <li><b>An event somebody else caused.</b> A request of the order's customer or an {@code ADMIN}
 *       cancels an order too. Its event carries no origin, so the runner asks {@link
 *       #correlateByEventData}, which answers from the order id in the event.
 * </ul>
 *
 * <p>A correlation id is tracing data. An HTTP request's is whatever the client put in {@code
 * X-Correlation-Id}; if it addressed a saga, a caller cancelling their own order could have that
 * {@code OrderCancelled} evolved by another order's saga. An origin has one writer, the command
 * bus, and no request header reaches it; {@link #correlateByEventData} receives the stream id and
 * the payload and nothing of the metadata. Saga dispatch binds a correlation id of its own, {@code
 * streamrune:saga:<sagaId>}, so one fulfillment reads as one flow in the log and in the audit
 * trail; a request that sends an id with the {@code streamrune:} prefix is answered {@code 400}.
 *
 * <p>{@link #evolve} and {@link #handle} check from the other side, against the order id and the
 * payment id the saga stores in its own state: an event that is not about this saga's order changes
 * nothing and dispatches nothing, whichever way it arrived.
 *
 * <p>A {@code SagaId} holds 255 characters at most, so {@code OrderCommand.PlaceOrder} refuses an
 * order id longer than 255 minus the prefix ({@code SAGA_ID_PREFIX}, which this class uses to build
 * the id) when the order is placed; an order id that got past it would make {@link #extractSagaId}
 * throw on {@code OrderPlaced}, and the order would stay {@code CREATED} with no saga. For a
 * cancellation of such an order {@link #correlateByEventData} answers "no saga", which is the
 * truth.
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
 * compensates the step (the order is cancelled, and its cancellation refunds the payment).
 *
 * <p>A saga command's target is only an id: the bus resolves the aggregate type from the command's
 * registration. {@code ReserveStock} and {@code ReleaseStock} are registered under {@code
 * inventory}, so the stock streams are {@code inventory:<productId>} — the product id is the
 * inventory id, with no hand-made prefix.
 *
 * <p><b>Deadline.</b> The saga is a {@link SagaDecider} as well as a {@link SagaOrchestrator}: the
 * two interfaces share {@code stateType}, {@code initialState}, {@code evolve}, {@code handle} and
 * {@code compensate}, and {@link #timeout()} exists only on the decider. A fulfillment that has not
 * reached {@code COMPLETED}, {@code COMPENSATED} or {@code FAILED} within its timeout, measured
 * from the saga's start, is compensated by the application's {@code SagaTimeoutRunner} with the
 * same {@link #compensate} the event path uses: the order is cancelled. The runner is built from
 * the saga's {@code SagaRunner}, whose forward fence is on: a forward command of a step still in
 * flight when the deadline claims the saga is stored before the claim, or is refused at its commit.
 * A forward command stored before the claim is covered: the compensation's cancellation starts the
 * undo of whatever the streams hold. The timeout is {@link #DEFAULT_TIMEOUT} unless the application
 * passes one ({@code app.saga.fulfillment-timeout} in the three apps).
 */
public class OrderFulfillmentSaga
    implements SagaOrchestrator<OrderFulfillmentState>, SagaDecider<OrderFulfillmentState> {

  /** The reason of the cancellation every compensation of this saga dispatches. */
  public static final String COMPENSATION_REASON = "Order fulfillment did not complete";

  /** The deadline of a fulfillment when the application names none: fifteen minutes. */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(15);

  private final Duration timeout;

  /** A saga whose deadline is {@link #DEFAULT_TIMEOUT}. */
  public OrderFulfillmentSaga() {
    this(DEFAULT_TIMEOUT);
  }

  /**
   * A saga whose deadline is the given duration.
   *
   * @param timeout how long a fulfillment may stay non-terminal, measured from its start; positive
   * @throws NullPointerException when {@code timeout} is {@code null}
   * @throws IllegalArgumentException when {@code timeout} is zero or negative
   */
  public OrderFulfillmentSaga(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout is required");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive, got " + timeout);
    }
    this.timeout = timeout;
  }

  /** The fulfillment's deadline, measured from the saga's start. */
  @Override
  public Optional<Duration> timeout() {
    return Optional.of(timeout);
  }

  @Override
  public Class<OrderFulfillmentState> stateType() {
    return OrderFulfillmentState.class;
  }

  @Override
  public OrderFulfillmentState initialState(SagaId sagaId) {
    return new OrderFulfillmentState(
        sagaId,
        null,
        null,
        null,
        null,
        List.of(),
        List.of(),
        OrderFulfillmentStatus.AWAITING_PAYMENT);
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
   * The saga of an event no fulfillment caused. A request writes one of the events this saga reacts
   * to: {@code OrderCancelled}, by the order's customer or an {@code ADMIN}. It belongs to the saga
   * of the order it names. {@code OrderConfirmed} is written by this saga's own command only, and
   * arrives by its origin. Every other event, and one whose order no saga id can hold, belongs to
   * no saga.
   *
   * <p>The payment and inventory events are not listed: only this saga's commands and the process
   * managers that continue its events write them for a fulfillment, and those events carry the
   * saga's origin, by which {@code SagaRunner} routes them without calling this method.
   */
  @Override
  public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
    return event instanceof OrderEvent.OrderCancelled e
        ? sagaOfOrder(e.orderId())
        : Optional.empty();
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
   * Whether the event is about this saga's order, answered from what the saga itself stores and not
   * from how the event was routed: the {@code OrderPlaced} that starts this saga names the order
   * the saga id is built from, and every later event names the order id or the payment id the saga
   * recorded from that {@code OrderPlaced}. The answer holds whichever way the event arrived.
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
    OrderFulfillmentStatus status = state.fulfillmentStatus();
    return switch (event.event()) {
      case OrderEvent.OrderPlaced e ->
          new OrderFulfillmentState(
              state.sagaId(),
              e.orderId(),
              e.customerId(),
              PaymentIds.forOrder(e.orderId()),
              e.total(),
              e.lines(),
              List.of(),
              OrderFulfillmentStatus.AWAITING_PAYMENT);

      // The payment decides once, while the saga waits for it. A capture recorded after the
      // payment failed (the gateway took the money after all) is refunded by the payment itself
      // and does not move a cancelling saga back to the stock.
      case PaymentEvent.PaymentCaptured e ->
          status == OrderFulfillmentStatus.AWAITING_PAYMENT
              ? state.withStatus(OrderFulfillmentStatus.AWAITING_INVENTORY)
              : state;

      // CANCELLING, not FAILED: the runner skips handle() once evolve() reaches a terminal status,
      // and handle() has to dispatch the CancelOrder.
      case PaymentEvent.PaymentFailed e ->
          status == OrderFulfillmentStatus.AWAITING_PAYMENT
              ? state.withStatus(OrderFulfillmentStatus.CANCELLING)
              : state;

      // A cancellation the saga did not ask for — the customer's or an ADMIN's — ends the
      // fulfillment: the step's row write opens the compensation episode, and the forward fence
      // refuses every forward command of the saga from then on. The saga's own cancellation after a
      // failed payment closes the saga FAILED.
      case OrderEvent.OrderCancelled e ->
          switch (status) {
            case AWAITING_PAYMENT, AWAITING_INVENTORY, AWAITING_CONFIRMATION ->
                state.withStatus(OrderFulfillmentStatus.COMPENSATING);
            case CANCELLING -> state.withStatus(OrderFulfillmentStatus.FAILED);
            default -> state;
          };

      // One StockReserved per line; the one that completes the set moves on to the confirmation. A
      // product the order does not hold, or one already counted, changes nothing.
      case InventoryEvent.StockReserved e -> {
        if (status != OrderFulfillmentStatus.AWAITING_INVENTORY
            || !state.hasLineFor(e.productId())
            || state.reservedProductIds().contains(e.productId())) {
          yield state;
        }
        OrderFulfillmentState reserved = state.withReservedProduct(e.productId());
        yield reserved.everyLineReserved()
            ? reserved.withStatus(OrderFulfillmentStatus.AWAITING_CONFIRMATION)
            : reserved;
      }

      case OrderEvent.OrderConfirmed e ->
          status == OrderFulfillmentStatus.AWAITING_CONFIRMATION
              ? state.withStatus(OrderFulfillmentStatus.COMPLETED)
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
      case AWAITING_PAYMENT ->
          event.event() instanceof OrderEvent.OrderPlaced
              ? List.of(
                  new SagaCommand(
                      new PaymentCommand.InitiatePayment(
                          state.paymentId(), state.orderId(), state.orderTotal()),
                      new AggregateId(state.paymentId())))
              : List.of();

      // Every line is reserved once the payment is captured, in the order of the lines.
      case AWAITING_INVENTORY ->
          event.event() instanceof PaymentEvent.PaymentCaptured
              ? state.lines().stream()
                  .map(
                      line ->
                          new SagaCommand(
                              new InventoryCommand.ReserveStock(
                                  line.productId(), state.orderId(), line.quantity()),
                              new AggregateId(line.productId())))
                  .toList()
              : List.of();

      // The StockReserved that completed the set asks for the confirmation: one of a product the
      // order holds, which evolve() counted last. A StockReserved of a product the order does not
      // hold changes nothing and asks for nothing.
      case AWAITING_CONFIRMATION ->
          event.event() instanceof InventoryEvent.StockReserved e && state.hasLineFor(e.productId())
              ? List.of(
                  new SagaCommand(
                      new OrderCommand.ConfirmOrder(state.orderId()),
                      new AggregateId(state.orderId())))
              : List.of();

      case CANCELLING ->
          event.event() instanceof PaymentEvent.PaymentFailed
              ? List.of(
                  new SagaCommand(
                      new OrderCommand.CancelOrder(state.orderId(), "Payment failed"),
                      new AggregateId(state.orderId())))
              : List.of();

      default -> List.of();
    };
  }

  /**
   * The compensation in every state: cancel the order. A pure function of the state, the same list
   * on every resume, with a constant reason. The cancellation is the order's {@code
   * OrderCancelled}, and {@code OrderCancellationProcessManager} releases the order's stock and
   * voids or refunds its payment for every {@code OrderCancelled}, this one included — so the undo
   * covers every forward command stored before the claim, whatever the row recorded of it. When the
   * order has shipped meanwhile, the cancellation is refused, the episode ends {@code FAILED}, and
   * nothing is undone: the order is shipped and paid for.
   */
  @Override
  public List<SagaCommand> compensate(
      OrderFulfillmentState state, Throwable failure, SagaCommand failedCommand) {
    return List.of(
        new SagaCommand(
            new OrderCommand.CancelOrder(state.orderId(), COMPENSATION_REASON),
            new AggregateId(state.orderId())));
  }
}
