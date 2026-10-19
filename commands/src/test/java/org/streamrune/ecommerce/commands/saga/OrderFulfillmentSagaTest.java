package org.streamrune.ecommerce.commands.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;
import org.streamrune.test.SagaEnvelopes;

class OrderFulfillmentSagaTest {

  private final OrderFulfillmentSaga saga = new OrderFulfillmentSaga();

  /**
   * An event a request caused: its metadata carries no saga origin, and its correlation id is
   * whatever the request carried.
   */
  private EventEnvelope envelope(
      DomainEvent event, AggregateType type, String id, String correlationId) {
    var metadata =
        new EventMetadata(
            new EventId(UUID.randomUUID().toString()),
            new CommandId(UUID.randomUUID().toString()),
            null,
            null,
            new CorrelationId(correlationId),
            null,
            null,
            Instant.now());
    return new EventEnvelope(
        GlobalOffset.initial(),
        StreamId.of(type, new AggregateId(id)),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        metadata);
  }

  /**
   * An event a command of the given order's saga caused: the command bus stores it with that saga's
   * origin, and {@code SagaRunner} hands it to that saga without asking the orchestrator.
   */
  private EventEnvelope causedBySagaOf(
      String orderId, DomainEvent event, AggregateType type, String id) {
    return SagaEnvelopes.causedBy(
        OrderFulfillmentState.class,
        new SagaId("fulfillment-" + orderId),
        StreamId.of(type, new AggregateId(id)),
        event);
  }

  // ---- Deadline: the saga is a SagaDecider too, and its timeout is the deadline a timeout runner
  // compensates it at. ----

  @Test
  void timeout_isTheConfiguredDeadline() {
    assertThat(saga.timeout()).contains(OrderFulfillmentSaga.DEFAULT_TIMEOUT);
    assertThat(OrderFulfillmentSaga.DEFAULT_TIMEOUT).isEqualTo(Duration.ofMinutes(15));
    assertThat(new OrderFulfillmentSaga(Duration.ofSeconds(2)).timeout())
        .contains(Duration.ofSeconds(2));
  }

  @Test
  void aNonPositiveTimeoutIsRefused() {
    assertThatThrownBy(() -> new OrderFulfillmentSaga(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("timeout is required");
    assertThatThrownBy(() -> new OrderFulfillmentSaga(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
    assertThatThrownBy(() -> new OrderFulfillmentSaga(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
  }

  @Test
  void isStartEvent_orderPlaced() {
    var evt =
        envelope(
            new OrderEvent.OrderPlaced("o-1", "c-1", List.of(), new Money(BigDecimal.TEN, "USD")),
            OrderState.TYPE,
            "o-1",
            "corr-1");
    assertThat(saga.isStartEvent(evt)).isTrue();
  }

  @Test
  void isStartEvent_otherEvent_false() {
    var evt = envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "corr-1");
    assertThat(saga.isStartEvent(evt)).isFalse();
  }

  @Test
  void extractSagaId() {
    var evt =
        envelope(
            new OrderEvent.OrderPlaced("o-1", "c-1", List.of(), new Money(BigDecimal.TEN, "USD")),
            OrderState.TYPE,
            "o-1",
            "corr-1");
    assertThat(saga.extractSagaId(evt)).isEqualTo(new SagaId("fulfillment-o-1"));
  }

  // ---- PlaceOrder refuses an order id longer than MAX_ORDER_ID_LENGTH, so that the saga id the
  // order starts always fits the 255 characters a SagaId holds. The bound is the saga's own: ----

  @Test
  void extractSagaId_orderIdAtThePlaceOrderBound_buildsTheSagaId() {
    var orderId = "o".repeat(OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH);
    var evt =
        envelope(
            new OrderEvent.OrderPlaced(orderId, "c-1", List.of(), new Money(BigDecimal.TEN, "USD")),
            OrderState.TYPE,
            orderId,
            "corr-1");

    var sagaId = saga.extractSagaId(evt);

    assertThat(sagaId.value()).isEqualTo("fulfillment-" + orderId).hasSize(255);
  }

  @Test
  void extractSagaId_orderIdOneOverThePlaceOrderBound_isRefusedByTheSagaId() {
    var orderId = "o".repeat(OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH + 1);
    var evt =
        envelope(
            new OrderEvent.OrderPlaced(orderId, "c-1", List.of(), new Money(BigDecimal.TEN, "USD")),
            OrderState.TYPE,
            orderId,
            "corr-1");

    assertThatThrownBy(() -> saga.extractSagaId(evt))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at most 255 characters");
  }

  @Test
  void happyPath_orderPlaced_initiatesPayment() {
    var placed =
        envelope(
            new OrderEvent.OrderPlaced("o-1", "c-1", List.of(), new Money(BigDecimal.TEN, "USD")),
            OrderState.TYPE,
            "o-1",
            "corr-1");
    var state = saga.initialState(new SagaId("fulfillment-o-1"));
    state = saga.evolve(state, placed);
    var commands = saga.handle(state, placed);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_PAYMENT);
    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(PaymentCommand.InitiatePayment.class);
  }

  @Test
  void happyPath_paymentCaptured_reservesStock() {
    var line = new OrderEvent.OrderLine("prod-1", 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            List.of(),
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    var captured =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1");

    state = saga.evolve(state, captured);
    var commands = saga.handle(state, captured);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_INVENTORY);
    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReserveStock.class);
    var reserve = (InventoryCommand.ReserveStock) commands.get(0).command();
    assertThat(reserve.productId()).isEqualTo("prod-1");
    assertThat(reserve.quantity()).isEqualTo(2);
  }

  // ---- SagaRunner routes the reply to a saga's own command by the origin the command bus stores
  // with the event, and asks the saga only about an event no fulfillment caused. A request writes
  // one of the events this saga reacts to, OrderCancelled; it belongs to the saga of the order it
  // names. The saga's own ConfirmOrder writes OrderConfirmed, which arrives by its origin. The
  // method receives the stream id and the payload, so nothing a request header carries can take
  // part in the answer. ----

  @Test
  void correlateByEventData_routesACancellationOnly() {
    var orderStream = StreamId.of(OrderState.TYPE, new AggregateId("o-1"));

    assertThat(saga.correlateByEventData(orderStream, new OrderEvent.OrderConfirmed("o-1")))
        .isEmpty();
    assertThat(
            saga.correlateByEventData(
                orderStream, new OrderEvent.OrderCancelled("o-1", "changed my mind", List.of())))
        .contains(new SagaId("fulfillment-o-1"));
  }

  @Test
  void correlateByEventData_anOrderIdNoSagaIdCanHold_belongsToNoSaga() {
    // PlaceOrder refuses such an id, so no saga exists for it; the answer is "no saga", not a
    // throw, which the framework would quarantine as a poison event.
    var orderId = "o".repeat(OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH + 1);
    var orderStream = StreamId.of(OrderState.TYPE, new AggregateId(orderId));

    assertThat(
            saga.correlateByEventData(
                orderStream, new OrderEvent.OrderCancelled(orderId, "why", List.of())))
        .isEmpty();
    assertThat(saga.correlateByEventData(orderStream, new OrderEvent.OrderConfirmed(orderId)))
        .isEmpty();
    assertThat(saga.correlateByEventData(orderStream, new OrderEvent.OrderConfirmed(""))).isEmpty();
    assertThat(saga.correlateByEventData(orderStream, new OrderEvent.OrderConfirmed(null)))
        .isEmpty();
  }

  @Test
  void correlateByEventData_everyOtherEvent_belongsToNoSaga() {
    // The payment and inventory events of a fulfillment are written by the saga's commands and by
    // the payment process manager's, and reach the saga through their origin. One that carries no
    // origin was caused by no fulfillment. The rest are events the saga does not react to.
    var money = new Money(BigDecimal.TEN, "USD");
    var order = StreamId.of(OrderState.TYPE, new AggregateId("o-1"));
    var payment = StreamId.of(PaymentState.TYPE, new AggregateId("pay-o-1"));
    var stock = StreamId.of(InventoryState.TYPE, new AggregateId("prod-1"));
    Map<DomainEvent, StreamId> others = new LinkedHashMap<>();
    others.put(new OrderEvent.OrderPlaced("o-1", "c-1", List.of(), money), order);
    others.put(new OrderEvent.OrderShipped("o-1", List.of()), order);
    others.put(new OrderEvent.OrderDelivered("o-1"), order);
    others.put(new PaymentEvent.PaymentInitiated("pay-o-1", "o-1", money), payment);
    others.put(new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"), payment);
    others.put(new PaymentEvent.PaymentFailed("pay-o-1", "o-1", "declined"), payment);
    others.put(new PaymentEvent.PaymentVoided("pay-o-1", "o-1", "changed my mind"), payment);
    others.put(
        new PaymentEvent.PaymentRefunded("pay-o-1", "o-1", new Money(BigDecimal.TEN, "USD"), "why"),
        payment);
    others.put(new InventoryEvent.StockReserved("prod-1", "o-1", 2, 8), stock);
    others.put(new InventoryEvent.StockReleased("prod-1", "o-1", 2, 10), stock);
    others.put(new InventoryEvent.ReservationConfirmed("prod-1", "o-1", 2), stock);
    others.put(new InventoryEvent.ShipmentReceived("prod-1", 5, 5), stock);

    others.forEach(
        (event, stream) ->
            assertThat(saga.correlateByEventData(stream, event))
                .as(event.getClass().getSimpleName())
                .isEmpty());
  }

  // ---- A saga that is handed an event of another order neither evolves nor dispatches. Routing
  // says which saga's command caused an event (its origin) or which saga an outside event names;
  // evolve and handle compare the event with the order id and the payment id in the saga's own
  // state, so the answer holds whichever of the two ways handed the event over. ----

  @Test
  void evolve_anotherOrdersConfirmation_leavesTheSagaWhereItIs() {
    // AWAITING_CONFIRMATION is the one state in which a confirmation completes the saga.
    var state = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_CONFIRMATION);
    var foreign =
        envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-2");

    assertThat(saga.evolve(state, foreign)).isEqualTo(state);
    assertThat(saga.handle(state, foreign)).isEmpty();
  }

  @Test
  void evolve_anotherOrdersEvent_isIgnoredWhateverRoutedItHere() {
    // A saga that names the saga of order o-2 for every outside event: evolve and handle compare
    // the event with the ids the saga stores, so they do not depend on the routing being right.
    var misrouting =
        new OrderFulfillmentSaga() {
          @Override
          public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
            return Optional.of(new SagaId("fulfillment-o-2"));
          }
        };
    var awaitingConfirmation = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_CONFIRMATION);
    var foreignConfirmation =
        envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-2");
    assertThat(misrouting.evolve(awaitingConfirmation, foreignConfirmation))
        .isEqualTo(awaitingConfirmation);

    // An event that carries the origin of order o-2's saga is handed to that saga by the runner,
    // whatever order it names.
    var awaitingPayment = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_PAYMENT);
    var foreignFailure =
        causedBySagaOf(
            "o-2",
            new PaymentEvent.PaymentFailed("pay-o-1", "o-1", "declined"),
            PaymentState.TYPE,
            "pay-o-1");
    assertThat(misrouting.evolve(awaitingPayment, foreignFailure)).isEqualTo(awaitingPayment);
    assertThat(misrouting.handle(awaitingPayment, foreignFailure)).isEmpty();
  }

  @Test
  void evolve_anotherOrdersPaymentAndStock_leaveTheSagaWhereItIs() {
    // Each event below carries the origin of order o-2's saga and names order o-1.
    var awaitingPayment = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_PAYMENT);
    var foreignCapture =
        causedBySagaOf(
            "o-2",
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1");
    var foreignFailure =
        causedBySagaOf(
            "o-2",
            new PaymentEvent.PaymentFailed("pay-o-1", "o-1", "declined"),
            PaymentState.TYPE,
            "pay-o-1");
    assertThat(saga.evolve(awaitingPayment, foreignCapture)).isEqualTo(awaitingPayment);
    assertThat(saga.evolve(awaitingPayment, foreignFailure)).isEqualTo(awaitingPayment);

    // A capture of another order must not reserve this order's stock either.
    var awaitingInventory = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_INVENTORY);
    assertThat(saga.handle(awaitingInventory, foreignCapture)).isEmpty();

    var foreignReservation =
        causedBySagaOf(
            "o-2",
            new InventoryEvent.StockReserved("prod-1", "o-1", 2, 8),
            InventoryState.TYPE,
            "prod-1");
    assertThat(saga.evolve(awaitingInventory, foreignReservation)).isEqualTo(awaitingInventory);

    var cancelling = stateOfOrderTwo(OrderFulfillmentStatus.CANCELLING);
    var foreignCancel =
        envelope(
            new OrderEvent.OrderCancelled("o-1", "changed my mind", List.of()),
            OrderState.TYPE,
            "o-1",
            "fulfillment-o-2");
    assertThat(saga.evolve(cancelling, foreignCancel)).isEqualTo(cancelling);
  }

  @Test
  void evolve_anotherOrdersStartEvent_leavesTheSagaWhereItIs() {
    var state = saga.initialState(new SagaId("fulfillment-o-2"));
    var foreign =
        envelope(
            new OrderEvent.OrderPlaced("o-1", "c-1", List.of(), new Money(BigDecimal.TEN, "USD")),
            OrderState.TYPE,
            "o-1",
            "corr-1");

    assertThat(saga.evolve(state, foreign)).isEqualTo(state);
    assertThat(saga.handle(state, foreign)).isEmpty();
  }

  // ---- The order's own confirmation completes the saga only where the saga asked for it. The
  // saga is the only sender of ConfirmOrder (the order decider's guard refuses it from anybody
  // else); an OrderConfirmed reaching a saga that waits for its payment changes nothing. ----

  @Test
  void evolve_theOrdersOwnConfirmation_completesTheSagaOnlyWhileItAwaitsConfirmation() {
    var confirmed =
        envelope(new OrderEvent.OrderConfirmed("o-2"), OrderState.TYPE, "o-2", "corr-by-hand");

    var awaitingPayment = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_PAYMENT);
    assertThat(saga.evolve(awaitingPayment, confirmed)).isEqualTo(awaitingPayment);

    var awaitingConfirmation = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_CONFIRMATION);
    assertThat(saga.evolve(awaitingConfirmation, confirmed).fulfillmentStatus())
        .isEqualTo(OrderFulfillmentStatus.COMPLETED);
  }

  private static OrderFulfillmentState stateOfOrderTwo(OrderFulfillmentStatus status) {
    var line = new OrderEvent.OrderLine("prod-2", 1, new Money(BigDecimal.TEN, "USD"));
    return new OrderFulfillmentState(
        new SagaId("fulfillment-o-2"),
        "o-2",
        "c-2",
        "pay-o-2",
        new Money(BigDecimal.TEN, "USD"),
        List.of(line),
        List.of(),
        status);
  }

  // ---- AWAITING_INVENTORY must gate on PaymentCaptured, like its sibling branches ----

  @Test
  void awaitingInventory_unrelatedEvent_noCommands() {
    var line = new OrderEvent.OrderLine("prod-1", 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            List.of(),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    // A redelivered/unrelated event arrives while the saga sits in AWAITING_INVENTORY — must not
    // re-issue ReserveStock for anything other than the PaymentCaptured that drives this step.
    var unrelated =
        envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "corr-by-hand");

    var commands = saga.handle(state, unrelated);

    assertThat(commands).isEmpty();
  }

  @Test
  void awaitingInventory_paymentCaptured_reservesEveryLineInTheOrderOfTheLines() {
    var state =
        stateOfOrderOne(
            OrderFulfillmentStatus.AWAITING_PAYMENT,
            List.of(),
            line("prod-1", 2),
            line("prod-2", 1),
            line("prod-3", 5));
    var captured =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1");

    state = saga.evolve(state, captured);
    var commands = saga.handle(state, captured);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_INVENTORY);
    assertThat(commands)
        .containsExactly(
            new SagaCommand(
                new InventoryCommand.ReserveStock("prod-1", "o-1", 2), new AggregateId("prod-1")),
            new SagaCommand(
                new InventoryCommand.ReserveStock("prod-2", "o-1", 1), new AggregateId("prod-2")),
            new SagaCommand(
                new InventoryCommand.ReserveStock("prod-3", "o-1", 5), new AggregateId("prod-3")));
  }

  // ---- One StockReserved per line. The saga counts each product once and moves on to the
  // confirmation with the StockReserved that completes the set. ----

  @Test
  void stockReserved_ofTheLastLine_movesToAwaitingConfirmationAndAsksForTheConfirmation() {
    var state =
        stateOfOrderOne(
            OrderFulfillmentStatus.AWAITING_INVENTORY,
            List.of("prod-2"),
            line("prod-1", 2),
            line("prod-2", 1));
    var reserved = reservedBySagaOf("prod-1", 2);

    state = saga.evolve(state, reserved);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_CONFIRMATION);
    assertThat(state.reservedProductIds()).containsExactly("prod-1", "prod-2");
    assertThat(saga.handle(state, reserved))
        .containsExactly(
            new SagaCommand(new OrderCommand.ConfirmOrder("o-1"), new AggregateId("o-1")));
  }

  @Test
  void stockReserved_ofSomeLines_staysAwaitingInventoryAndDispatchesNothing() {
    var state =
        stateOfOrderOne(
            OrderFulfillmentStatus.AWAITING_INVENTORY,
            List.of(),
            line("prod-1", 2),
            line("prod-2", 1));
    var reserved = reservedBySagaOf("prod-1", 2);

    state = saga.evolve(state, reserved);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_INVENTORY);
    assertThat(state.reservedProductIds()).containsExactly("prod-1");
    assertThat(saga.handle(state, reserved)).isEmpty();
  }

  @Test
  void stockReserved_twiceForOneProduct_countsOnce() {
    var state =
        stateOfOrderOne(
            OrderFulfillmentStatus.AWAITING_INVENTORY,
            List.of(),
            line("prod-1", 2),
            line("prod-2", 1));
    var reserved = reservedBySagaOf("prod-1", 2);

    state = saga.evolve(saga.evolve(state, reserved), reserved);

    assertThat(state.reservedProductIds()).hasSize(1);
    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_INVENTORY);
  }

  @Test
  void awaitingConfirmation_aStockReservedOfAProductTheOrderDoesNotHold_asksForNoConfirmation() {
    var state =
        stateOfOrderOne(
            OrderFulfillmentStatus.AWAITING_CONFIRMATION, List.of("prod-1"), line("prod-1", 2));
    var foreign = reservedBySagaOf("prod-9", 2);

    var evolved = saga.evolve(state, foreign);

    assertThat(evolved).isEqualTo(state);
    assertThat(saga.handle(evolved, foreign)).isEmpty();
  }

  @Test
  void stockReserved_ofAProductTheOrderDoesNotHold_changesNothing() {
    var state =
        stateOfOrderOne(OrderFulfillmentStatus.AWAITING_INVENTORY, List.of(), line("prod-1", 2));

    assertThat(saga.evolve(state, reservedBySagaOf("prod-9", 2))).isEqualTo(state);
  }

  private EventEnvelope reservedBySagaOf(String productId, int quantity) {
    return causedBySagaOf(
        "o-1",
        new InventoryEvent.StockReserved(productId, "o-1", quantity, 10),
        InventoryState.TYPE,
        productId);
  }

  private static OrderEvent.OrderLine line(String productId, int quantity) {
    return new OrderEvent.OrderLine(productId, quantity, new Money(BigDecimal.TEN, "USD"));
  }

  private static OrderFulfillmentState stateOfOrderOne(
      OrderFulfillmentStatus status, List<String> reserved, OrderEvent.OrderLine... lines) {
    return new OrderFulfillmentState(
        new SagaId("fulfillment-o-1"),
        "o-1",
        "c-1",
        "pay-o-1",
        new Money(BigDecimal.TEN, "USD"),
        List.of(lines),
        reserved,
        status);
  }

  // ---- PaymentFailed must actually cancel the order. An evolve straight to FAILED would make
  // the CancelOrder dispatch unreachable, because SagaRunner skips handle() once evolve() lands
  // on a terminal SagaStatus. Hence the non-terminal CANCELLING intermediate:
  // evolve(PaymentFailed) -> CANCELLING (RUNNING), handle() dispatches CancelOrder, then
  // evolve(OrderCancelled) while CANCELLING -> FAILED (terminal, saga ends). ----

  @Test
  void paymentFailed_evolvesToCancelling_andHandleEmitsCancelOrder() {
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(),
            List.of(),
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    var failed =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentFailed("pay-o-1", "o-1", "insufficient funds"),
            PaymentState.TYPE,
            "pay-o-1");

    state = saga.evolve(state, failed);
    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.CANCELLING);

    var commands = saga.handle(state, failed);

    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(OrderCommand.CancelOrder.class);
    var cancel = (OrderCommand.CancelOrder) commands.get(0).command();
    assertThat(cancel.orderId()).isEqualTo("o-1");
  }

  @Test
  void orderCancelled_whileCancelling_terminatesFailed() {
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(),
            List.of(),
            OrderFulfillmentStatus.CANCELLING);
    var cancelled =
        causedBySagaOf(
            "o-1",
            new OrderEvent.OrderCancelled("o-1", "Payment failed", List.of()),
            OrderState.TYPE,
            "o-1");

    state = saga.evolve(state, cancelled);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.FAILED);
  }

  // ---- A cancellation the saga did not ask for ends the fulfillment: the saga moves to
  // COMPENSATING, the forward fence refuses its later forward commands, and the compensation
  // cancels the order once more. ----

  @ParameterizedTest
  @EnumSource(
      value = OrderFulfillmentStatus.class,
      names = {"AWAITING_PAYMENT", "AWAITING_INVENTORY", "AWAITING_CONFIRMATION"})
  void orderCancelled_whileAwaiting_movesToCompensatingAndDispatchesNothing(
      OrderFulfillmentStatus status) {
    var state = stateOfOrderOne(status, List.of(), line("prod-1", 2));
    var cancelled =
        envelope(
            new OrderEvent.OrderCancelled("o-1", "changed my mind", List.of(line("prod-1", 2))),
            OrderState.TYPE,
            "o-1",
            "corr-of-a-request");

    state = saga.evolve(state, cancelled);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.COMPENSATING);
    assertThat(state.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(saga.handle(state, cancelled)).isEmpty();
  }

  // ---- The payment decides once, while the saga waits for it. ----

  @Test
  void evolve_aCaptureRecordedAfterTheFailure_leavesTheSagaCancelling() {
    // The gateway took the money of a payment it reported as failed: the payment records the
    // capture and refunds it in one append. The saga is cancelling the order and stays so.
    var state = stateOfOrderOne(OrderFulfillmentStatus.CANCELLING, List.of(), line("prod-1", 2));
    var captured =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1");

    state = saga.evolve(state, captured);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.CANCELLING);
    assertThat(saga.handle(state, captured)).isEmpty();
  }

  @Test
  void evolve_aFailureOutsideTheWaitForThePayment_changesNothing() {
    var state =
        stateOfOrderOne(OrderFulfillmentStatus.AWAITING_INVENTORY, List.of(), line("prod-1", 2));
    var failed =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentFailed("pay-o-1", "o-1", "declined"),
            PaymentState.TYPE,
            "pay-o-1");

    assertThat(saga.evolve(state, failed)).isEqualTo(state);
    assertThat(saga.handle(state, failed)).isEmpty();
  }

  // ---- The compensation is the order's cancellation in every status, a pure function of the
  // state: the cancellation's OrderCancelled starts the undo of the order's stock and payment. ----

  @ParameterizedTest
  @EnumSource(OrderFulfillmentStatus.class)
  void compensate_isACancellationWithTheConstantReason_inEveryStatus(
      OrderFulfillmentStatus status) {
    var state = stateOfOrderOne(status, List.of("prod-1"), line("prod-1", 2));
    var reserve =
        new SagaCommand(
            new InventoryCommand.ReserveStock("prod-1", "o-1", 2), new AggregateId("prod-1"));
    var cancel =
        new SagaCommand(
            new OrderCommand.CancelOrder("o-1", OrderFulfillmentSaga.COMPENSATION_REASON),
            new AggregateId("o-1"));

    for (Throwable failure : Arrays.asList(null, new RuntimeException("reservation failed"))) {
      for (SagaCommand failed : Arrays.asList(null, reserve)) {
        assertThat(saga.compensate(state, failure, failed)).containsExactly(cancel);
      }
    }
  }

  @Test
  void compensate_isTheSameListOnEveryCall() {
    var state =
        stateOfOrderOne(OrderFulfillmentStatus.AWAITING_INVENTORY, List.of(), line("prod-1", 2));
    var failure = new RuntimeException("reservation failed");

    assertThat(saga.compensate(state, failure, null))
        .isEqualTo(saga.compensate(state, new RuntimeException("another failure"), null));
  }

  // ---- AggregateId.of is the INGRESS factory and refuses control characters
  // and ids longer than 255 characters. Every target this saga builds comes from its stored state
  // (the OrderPlaced lines it recorded) — data already in the log — so it uses the AggregateId
  // constructor, the decode door. Through of(), a product id carrying a control character, or one
  // longer than 255 characters, would throw inside handle(), and the framework
  // would quarantine the PaymentCaptured event as poison with the payment captured and never
  // refunded. With the constructor the ReserveStock reaches the bus, whose id extractor
  // (AggregateId.of) refuses it with an IllegalArgumentException, and the framework compensates the
  // forward step: the order is cancelled, and its cancellation releases the stock and refunds the
  // payment. ----

  @Test
  void awaitingInventory_storedProductIdWithAControlCharacter_stillYieldsReserveStock() {
    var raw = "prod\r\n-1";
    var line = new OrderEvent.OrderLine(raw, 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            List.of(),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    var captured =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1");

    var commands = saga.handle(state, captured);

    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReserveStock.class);
    assertThat(commands.get(0).aggregateId().value()).isEqualTo(raw);
  }

  @Test
  void awaitingInventory_storedProductIdTooLongForTheIngressBound_stillYieldsReserveStock() {
    var raw = "p".repeat(256); // one over the 255-character bound
    var line = new OrderEvent.OrderLine(raw, 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            List.of(),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    var captured =
        causedBySagaOf(
            "o-1",
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1");

    var commands = saga.handle(state, captured);

    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReserveStock.class);
    assertThat(commands.get(0).aggregateId().value()).isEqualTo(raw);
  }
}
