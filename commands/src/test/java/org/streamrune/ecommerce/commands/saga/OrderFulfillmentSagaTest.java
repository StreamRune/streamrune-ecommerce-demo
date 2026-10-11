package org.streamrune.ecommerce.commands.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
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

class OrderFulfillmentSagaTest {

  private final OrderFulfillmentSaga saga = new OrderFulfillmentSaga();

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
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    var captured =
        envelope(
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-1");

    state = saga.evolve(state, captured);
    var commands = saga.handle(state, captured);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.AWAITING_INVENTORY);
    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReserveStock.class);
    var reserve = (InventoryCommand.ReserveStock) commands.get(0).command();
    assertThat(reserve.productId()).isEqualTo("prod-1");
    assertThat(reserve.quantity()).isEqualTo(2);
  }

  // ---- An event is routed to the saga of the order it is about, read from the event's own data.
  // The correlation id of an event is whatever the request that caused it carried: a client sets
  // X-Correlation-Id, so it says nothing about whose saga an event belongs to. ----

  @Test
  void correlate_routesEveryEventOfTheProcessToTheSagaOfTheOrderItNames() {
    var money = new Money(BigDecimal.TEN, "USD");
    List<EventEnvelope> eventsOfOrderOne =
        List.of(
            envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "corr-1"),
            envelope(new OrderEvent.OrderCancelled("o-1", "why"), OrderState.TYPE, "o-1", "corr-1"),
            envelope(
                new PaymentEvent.PaymentInitiated("pay-o-1", "o-1", money),
                PaymentState.TYPE,
                "pay-o-1",
                "corr-1"),
            envelope(
                new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
                PaymentState.TYPE,
                "pay-o-1",
                "corr-1"),
            envelope(
                new PaymentEvent.PaymentFailed("pay-o-1", "declined"),
                PaymentState.TYPE,
                "pay-o-1",
                "corr-1"),
            envelope(
                new PaymentEvent.PaymentRefunded("pay-o-1", "why"),
                PaymentState.TYPE,
                "pay-o-1",
                "corr-1"),
            envelope(
                new InventoryEvent.StockReserved("prod-1", "o-1", 2, 8),
                InventoryState.TYPE,
                "prod-1",
                "corr-1"),
            envelope(
                new InventoryEvent.StockReleased("prod-1", "o-1", 2, 10),
                InventoryState.TYPE,
                "prod-1",
                "corr-1"));

    for (EventEnvelope event : eventsOfOrderOne) {
      assertThat(saga.correlate(event))
          .as("the saga of %s", event.eventType().name())
          .contains(new SagaId("fulfillment-o-1"));
    }
  }

  @Test
  void correlate_ignoresACorrelationIdThatNamesAnotherOrdersSaga() {
    // The caller confirmed their own order o-1 and sent X-Correlation-Id: fulfillment-o-2.
    var confirmed =
        envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-2");
    var cancelled =
        envelope(
            new OrderEvent.OrderCancelled("o-1", "changed my mind"),
            OrderState.TYPE,
            "o-1",
            "fulfillment-o-2");

    assertThat(saga.correlate(confirmed)).contains(new SagaId("fulfillment-o-1"));
    assertThat(saga.correlate(cancelled)).contains(new SagaId("fulfillment-o-1"));
  }

  @Test
  void correlate_anEventOutsideTheProcessBelongsToNoSaga_whateverItsCorrelationId() {
    List<EventEnvelope> outside =
        List.of(
            envelope(new OrderEvent.OrderShipped("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-1"),
            envelope(
                new OrderEvent.OrderDelivered("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-1"),
            envelope(
                new InventoryEvent.ShipmentReceived("prod-1", 5, 5),
                InventoryState.TYPE,
                "prod-1",
                "fulfillment-o-1"),
            envelope(
                new InventoryEvent.ReservationConfirmed("prod-1", "o-1", 2),
                InventoryState.TYPE,
                "prod-1",
                "fulfillment-o-1"));

    for (EventEnvelope event : outside) {
      assertThat(saga.correlate(event)).as(event.eventType().name()).isEmpty();
    }
  }

  @Test
  void correlate_aPaymentEventWithoutAnOrderId_isRoutedByThePaymentIdTheSagaDerived() {
    var failed =
        envelope(
            new PaymentEvent.PaymentFailed("pay-o-1", "declined"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-2");
    var notAFulfillmentPayment =
        envelope(
            new PaymentEvent.PaymentFailed("invoice-7", "declined"),
            PaymentState.TYPE,
            "invoice-7",
            "fulfillment-o-2");

    assertThat(saga.correlate(failed)).contains(new SagaId("fulfillment-o-1"));
    assertThat(saga.correlate(notAFulfillmentPayment)).isEmpty();
  }

  @Test
  void correlate_anOrderIdNoSagaIdCanHold_belongsToNoSaga() {
    // PlaceOrder refuses such an id, so no saga exists for it; correlate answers "not mine"
    // rather than throwing, which the framework would quarantine as a poison event.
    var orderId = "o".repeat(OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH + 1);
    var cancelled =
        envelope(new OrderEvent.OrderCancelled(orderId, "why"), OrderState.TYPE, orderId, "c-1");

    assertThat(saga.correlate(cancelled)).isEmpty();
  }

  // ---- A saga that is handed an event of another order neither evolves nor dispatches. ----

  @Test
  void evolve_anotherOrdersConfirmation_leavesTheSagaWhereItIs() {
    var state = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_PAYMENT);
    var foreign =
        envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-2");

    assertThat(saga.evolve(state, foreign)).isEqualTo(state);
    assertThat(saga.handle(state, foreign)).isEmpty();
  }

  @Test
  void evolve_anotherOrdersPaymentAndStock_leaveTheSagaWhereItIs() {
    var awaitingPayment = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_PAYMENT);
    var foreignCapture =
        envelope(
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-2");
    var foreignFailure =
        envelope(
            new PaymentEvent.PaymentFailed("pay-o-1", "declined"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-2");
    assertThat(saga.evolve(awaitingPayment, foreignCapture)).isEqualTo(awaitingPayment);
    assertThat(saga.evolve(awaitingPayment, foreignFailure)).isEqualTo(awaitingPayment);

    // A capture of another order must not reserve this order's stock either.
    var awaitingInventory = stateOfOrderTwo(OrderFulfillmentStatus.AWAITING_INVENTORY);
    assertThat(saga.handle(awaitingInventory, foreignCapture)).isEmpty();

    var foreignReservation =
        envelope(
            new InventoryEvent.StockReserved("prod-1", "o-1", 2, 8),
            InventoryState.TYPE,
            "prod-1",
            "fulfillment-o-2");
    assertThat(saga.evolve(awaitingInventory, foreignReservation)).isEqualTo(awaitingInventory);

    var cancelling = stateOfOrderTwo(OrderFulfillmentStatus.CANCELLING);
    var foreignCancel =
        envelope(
            new OrderEvent.OrderCancelled("o-1", "changed my mind"),
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

  // ---- The order's own confirmation completes the saga only where the saga asked for it. A
  // caller who confirms their own order by hand while the saga still waits for the payment does
  // not complete the saga: it goes on, and its ConfirmOrder is refused later. ----

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
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    // A redelivered/unrelated event arrives while the saga sits in AWAITING_INVENTORY — must not
    // re-issue ReserveStock for anything other than the PaymentCaptured that drives this step.
    var unrelated =
        envelope(new OrderEvent.OrderConfirmed("o-1"), OrderState.TYPE, "o-1", "fulfillment-o-1");

    var commands = saga.handle(state, unrelated);

    assertThat(commands).isEmpty();
  }

  @Test
  void awaitingInventory_paymentCaptured_reservesStock() {
    var line = new OrderEvent.OrderLine("prod-1", 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    var captured =
        envelope(
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-1");

    var commands = saga.handle(state, captured);

    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReserveStock.class);
    var reserve = (InventoryCommand.ReserveStock) commands.get(0).command();
    assertThat(reserve.productId()).isEqualTo("prod-1");
    assertThat(reserve.quantity()).isEqualTo(2);
  }

  // ---- PaymentFailed must actually cancel the order. A direct evolve-to-FAILED made the
  // handle() FAILED branch unreachable, because SagaRunner.persistStatusFor returns before
  // calling handle() once evolve() lands on a terminal SagaStatus. Fixed via a non-terminal
  // CANCELLING intermediate: evolve(PaymentFailed) -> CANCELLING (RUNNING), handle() dispatches
  // CancelOrder, then evolve(OrderCancelled) while CANCELLING -> FAILED (terminal, saga ends). ----

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
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    var failed =
        envelope(
            new PaymentEvent.PaymentFailed("pay-o-1", "insufficient funds"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-1");

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
            OrderFulfillmentStatus.CANCELLING);
    var cancelled =
        envelope(
            new OrderEvent.OrderCancelled("o-1", "Payment failed"),
            OrderState.TYPE,
            "o-1",
            "fulfillment-o-1");

    state = saga.evolve(state, cancelled);

    assertThat(state.fulfillmentStatus()).isEqualTo(OrderFulfillmentStatus.FAILED);
  }

  // ---- Compensation must be step-appropriate, not a blanket cancel ----

  @Test
  void compensate_awaitingPayment_cancelOnly_noRefund() {
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(),
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    var failure = new RuntimeException("payment initiation failed");
    var failedCommand =
        new SagaCommand(
            new PaymentCommand.InitiatePayment("pay-o-1", "o-1", state.orderTotal()),
            org.streamrune.core.types.AggregateId.of("pay-o-1"));

    var commands = saga.compensate(state, failure, failedCommand);

    // Payment was never captured — nothing to refund.
    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(OrderCommand.CancelOrder.class);
    var cancel = (OrderCommand.CancelOrder) commands.get(0).command();
    assertThat(cancel.orderId()).isEqualTo("o-1");
  }

  @Test
  void compensate_awaitingInventory_refundAndCancel() {
    var line = new OrderEvent.OrderLine("prod-1", 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    var failure = new RuntimeException("reservation failed");
    var failedCommand =
        new SagaCommand(
            new InventoryCommand.ReserveStock("prod-1", "o-1", 2), AggregateId.of("prod-1"));

    var commands = saga.compensate(state, failure, failedCommand);

    // Payment WAS captured, stock was not reserved — refund + cancel (existing behavior, kept).
    assertThat(commands).hasSize(2);
    assertThat(commands.get(0).command()).isInstanceOf(PaymentCommand.RefundPayment.class);
    assertThat(commands.get(1).command()).isInstanceOf(OrderCommand.CancelOrder.class);
    var refund = (PaymentCommand.RefundPayment) commands.get(0).command();
    var cancel = (OrderCommand.CancelOrder) commands.get(1).command();
    assertThat(refund.paymentId()).isEqualTo("pay-o-1");
    assertThat(cancel.orderId()).isEqualTo("o-1");
  }

  @Test
  void compensate_awaitingConfirmation_releasesStockRefundsAndCancels() {
    var line = new OrderEvent.OrderLine("prod-1", 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            OrderFulfillmentStatus.AWAITING_CONFIRMATION);
    var failure = new RuntimeException("order confirmation failed");
    var failedCommand =
        new SagaCommand(
            new OrderCommand.ConfirmOrder("o-1"), org.streamrune.core.types.AggregateId.of("o-1"));

    var commands = saga.compensate(state, failure, failedCommand);

    // Payment WAS captured AND stock WAS reserved — release the reservation, refund, cancel.
    assertThat(commands).hasSize(3);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReleaseStock.class);
    assertThat(commands.get(1).command()).isInstanceOf(PaymentCommand.RefundPayment.class);
    assertThat(commands.get(2).command()).isInstanceOf(OrderCommand.CancelOrder.class);
    var release = (InventoryCommand.ReleaseStock) commands.get(0).command();
    var refund = (PaymentCommand.RefundPayment) commands.get(1).command();
    var cancel = (OrderCommand.CancelOrder) commands.get(2).command();
    assertThat(release.productId()).isEqualTo("prod-1");
    assertThat(release.orderId()).isEqualTo("o-1");
    assertThat(release.quantity()).isEqualTo(2);
    assertThat(refund.paymentId()).isEqualTo("pay-o-1");
    assertThat(cancel.orderId()).isEqualTo("o-1");
  }

  @Test
  void compensate_terminalState_returnsEmpty() {
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(),
            OrderFulfillmentStatus.COMPLETED);
    var failure = new RuntimeException("unexpected compensation request");
    var failedCommand =
        new SagaCommand(
            new OrderCommand.ConfirmOrder("o-1"), org.streamrune.core.types.AggregateId.of("o-1"));

    var commands = saga.compensate(state, failure, failedCommand);

    // Nothing to compensate in a terminal state — the framework classifies empty as FAILED.
    assertThat(commands).isEmpty();
  }

  // ---- AggregateId.of is the INGRESS factory and refuses control characters
  // and ids longer than 255 characters. Every target this saga builds comes from its stored state
  // (the OrderPlaced lines it recorded) — data already in the log — so it uses the AggregateId
  // constructor, the decode door. Through of(), a product id carrying a control character, or one
  // longer than 255 characters, would throw inside handle(), and the framework
  // would quarantine the PaymentCaptured event as poison with the payment captured and never
  // refunded. With the constructor the ReserveStock reaches the bus, whose id extractor
  // (AggregateId.of) refuses it with an IllegalArgumentException, and the framework compensates the
  // forward step: refund + cancel. ----

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
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    var captured =
        envelope(
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-1");

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
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    var captured =
        envelope(
            new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
            PaymentState.TYPE,
            "pay-o-1",
            "fulfillment-o-1");

    var commands = saga.handle(state, captured);

    assertThat(commands).hasSize(1);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReserveStock.class);
    assertThat(commands.get(0).aggregateId().value()).isEqualTo(raw);
  }

  @Test
  void compensate_storedProductIdWithAControlCharacter_stillBuildsTheUndo() {
    var raw = "prod\u0085-1";
    var line = new OrderEvent.OrderLine(raw, 2, new Money(BigDecimal.TEN, "USD"));
    var state =
        new OrderFulfillmentState(
            new SagaId("fulfillment-o-1"),
            "o-1",
            "c-1",
            "pay-o-1",
            new Money(BigDecimal.TEN, "USD"),
            List.of(line),
            OrderFulfillmentStatus.AWAITING_CONFIRMATION);
    var failedCommand =
        new SagaCommand(new OrderCommand.ConfirmOrder("o-1"), new AggregateId("o-1"));

    var commands =
        saga.compensate(state, new RuntimeException("order confirmation failed"), failedCommand);

    assertThat(commands).hasSize(3);
    assertThat(commands.get(0).command()).isInstanceOf(InventoryCommand.ReleaseStock.class);
    assertThat(commands.get(0).aggregateId().value()).isEqualTo(raw);
  }
}
