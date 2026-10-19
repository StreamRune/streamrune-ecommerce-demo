package org.streamrune.ecommerce.commands.order;

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
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.test.SagaEnvelopes;

/**
 * Every {@code OrderCancelled} is undone line by line and then at the payment, each command a
 * continuation of the cancellation under a key of the cancellation event.
 */
class OrderCancellationProcessManagerTest {

  private static final Money TEN = new Money(new BigDecimal("10.00"), "USD");
  private static final StreamId ORDER = StreamId.of(OrderState.TYPE, new AggregateId("o-1"));
  private static final SagaId SAGA = new SagaId("fulfillment-o-1");

  private final RecordingCommandBus bus = new RecordingCommandBus();
  private final OrderCancellationProcessManager manager = new OrderCancellationProcessManager(bus);

  @Test
  void aCancellationReleasesEveryLineThenCancelsThePayment() {
    EventEnvelope cancelled = byARequest(cancellation("why", line("p1", 2), line("p2", 1)));
    String eventId = cancelled.metadata().eventId().value();

    manager.onEvents(List.of(cancelled));

    assertThat(bus.commands())
        .containsExactly(
            new InventoryCommand.ReleaseStock("p1", "o-1"),
            new InventoryCommand.ReleaseStock("p2", "o-1"),
            new PaymentCommand.CancelPayment("pay-o-1", "why"));
    assertThat(bus.keys())
        .containsExactly(
            new IdempotencyKey("order-cancel:" + eventId + ":line:0"),
            new IdempotencyKey("order-cancel:" + eventId + ":line:1"),
            new IdempotencyKey("order-cancel:" + eventId + ":payment"));
  }

  @Test
  void theCommandsContinueTheCancellation() {
    // The saga's own cancellation carries its origin; a request's carries none.
    EventEnvelope bySaga =
        SagaEnvelopes.causedBy(
            OrderFulfillmentState.class, SAGA, ORDER, cancellation("why", line("p1", 2)));
    EventEnvelope byRequest = byARequest(cancellation("why", line("p1", 2)));
    SagaType sagaType = SagaType.fromClass(OrderFulfillmentState.class);

    manager.onEvents(List.of(bySaga));
    assertContinuation(bySaga, new SagaOrigin(sagaType, SAGA, 1));
    bus.clear();

    manager.onEvents(List.of(byRequest));
    assertContinuation(byRequest, null);
  }

  private void assertContinuation(EventEnvelope cause, SagaOrigin origin) {
    assertThat(bus.commands()).hasSize(2);
    for (StreamRuneContext.DispatchCause dispatched : bus.causes()) {
      assertThat(dispatched.causationId())
          .isEqualTo(CausationId.of(cause.metadata().eventId().value()));
      assertThat(dispatched.sagaOrigin()).isEqualTo(origin);
    }
    assertThat(bus.sagaDispatches()).containsOnly(false);
    assertThat(bus.contexts())
        .allSatisfy(
            context -> {
              assertThat(context.userId()).as("no user issued this command").isNull();
              assertThat(context.authority()).isNull();
            });
  }

  @Test
  void aRedeliveryUsesTheSameKeys() {
    EventEnvelope cancelled = byARequest(cancellation("why", line("p1", 2), line("p2", 1)));

    manager.onEvents(List.of(cancelled));
    List<IdempotencyKey> first = List.copyOf(bus.keys());
    bus.clear();
    manager.onEvents(List.of(cancelled));

    assertThat(bus.keys()).isEqualTo(first);
  }

  @Test
  void anotherCancellationOfTheOrderUsesKeysOfItsOwn() {
    manager.onEvents(List.of(byARequest(cancellation("why", line("p1", 2)))));
    List<IdempotencyKey> first = List.copyOf(bus.keys());
    bus.clear();

    manager.onEvents(List.of(byARequest(cancellation("again", line("p1", 2)))));

    assertThat(bus.keys()).hasSize(2).doesNotContainAnyElementsOf(first);
  }

  @Test
  void aCancellationThatNamesNoLines_undoesNothing() {
    // An ADMIN cancellation of an id nobody placed: no payment, no reservation, and an id whose
    // payment id would not fit the 255 characters an id holds.
    String unplaced = "x".repeat(255);
    EventEnvelope cancelled =
        envelope(
            StreamId.of(OrderState.TYPE, new AggregateId(unplaced)),
            new OrderEvent.OrderCancelled(unplaced, "why", List.of()));

    manager.onEvents(List.of(cancelled));

    assertThat(bus.commands()).isEmpty();
  }

  @Test
  void otherEventsAreIgnored() {
    StreamId payment = StreamId.of(PaymentState.TYPE, new AggregateId("pay-o-1"));
    manager.onEvents(
        List.of(
            byARequest(new OrderEvent.OrderPlaced("o-1", "c-1", List.of(line("p1", 2)), TEN)),
            byARequest(new OrderEvent.OrderShipped("o-1", List.of(line("p1", 2)))),
            envelope(payment, new PaymentEvent.PaymentRefunded("pay-o-1", "o-1", TEN, "why"))));

    assertThat(bus.commands()).isEmpty();
  }

  @Test
  void aFailedCommandLeavesTheListener() {
    bus.failAt(1, new IllegalStateException("store unreachable"));
    EventEnvelope cancelled = byARequest(cancellation("why", line("p1", 2), line("p2", 1)));

    assertThatThrownBy(() -> manager.onEvents(List.of(cancelled)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(bus.commands())
        .containsExactly(new InventoryCommand.ReleaseStock("p1", "o-1"))
        .noneMatch(PaymentCommand.CancelPayment.class::isInstance);
  }

  private static OrderEvent.OrderCancelled cancellation(
      String reason, OrderEvent.OrderLine... lines) {
    return new OrderEvent.OrderCancelled("o-1", reason, List.of(lines));
  }

  private static OrderEvent.OrderLine line(String productId, int quantity) {
    return new OrderEvent.OrderLine(productId, quantity, TEN);
  }

  /** An event of order {@code o-1} a request caused: no saga origin. */
  private static EventEnvelope byARequest(DomainEvent event) {
    return envelope(ORDER, event);
  }

  private static EventEnvelope envelope(StreamId stream, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(7),
        stream,
        new Version(2),
        EventType.fromClass(event.getClass()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("corr-of-a-request"),
            null,
            UserId.of("u-1"),
            Instant.now()));
  }
}
