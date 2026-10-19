package org.streamrune.ecommerce.commands.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;

/** Every line of a shipped order has its reservation confirmed, once per shipment event. */
class OrderShipmentProcessManagerTest {

  private static final Money TEN = new Money(new BigDecimal("10.00"), "USD");
  private static final StreamId ORDER = StreamId.of(OrderState.TYPE, new AggregateId("o-1"));

  private final RecordingCommandBus bus = new RecordingCommandBus();
  private final OrderShipmentProcessManager manager = new OrderShipmentProcessManager(bus);

  @Test
  void aShipmentConfirmsTheReservationOfEveryLine() {
    EventEnvelope shipped = envelope(shipment(line("p1", 2), line("p2", 1)));
    String eventId = shipped.metadata().eventId().value();

    manager.onEvents(List.of(shipped));

    assertThat(bus.commands())
        .containsExactly(
            new InventoryCommand.ConfirmReservation("p1", "o-1"),
            new InventoryCommand.ConfirmReservation("p2", "o-1"));
    assertThat(bus.keys())
        .containsExactly(
            new IdempotencyKey("order-ship:" + eventId + ":line:0"),
            new IdempotencyKey("order-ship:" + eventId + ":line:1"));
  }

  @Test
  void theCommandsContinueTheShipment() {
    EventEnvelope shipped = envelope(shipment(line("p1", 2), line("p2", 1)));

    manager.onEvents(List.of(shipped));

    assertThat(bus.causes())
        .hasSize(2)
        .allSatisfy(
            (StreamRuneContext.DispatchCause cause) -> {
              assertThat(cause.causationId())
                  .isEqualTo(CausationId.of(shipped.metadata().eventId().value()));
              assertThat(cause.sagaOrigin()).as("an ADMIN's shipment: no saga").isNull();
            });
    assertThat(bus.sagaDispatches()).containsOnly(false);
    assertThat(bus.contexts())
        .allSatisfy(context -> assertThat(context.userId()).as("no user").isNull());
  }

  @Test
  void aRedeliveryUsesTheSameKeys() {
    EventEnvelope shipped = envelope(shipment(line("p1", 2), line("p2", 1)));

    manager.onEvents(List.of(shipped));
    List<IdempotencyKey> first = List.copyOf(bus.keys());
    bus.clear();
    manager.onEvents(List.of(shipped));

    assertThat(bus.keys()).isEqualTo(first);
  }

  @Test
  void otherEventsAreIgnored() {
    manager.onEvents(
        List.of(
            envelope(new OrderEvent.OrderPlaced("o-1", "c-1", List.of(line("p1", 2)), TEN)),
            envelope(new OrderEvent.OrderCancelled("o-1", "why", List.of(line("p1", 2)))),
            envelope(new OrderEvent.OrderDelivered("o-1"))));

    assertThat(bus.commands()).isEmpty();
  }

  private static OrderEvent.OrderShipped shipment(OrderEvent.OrderLine... lines) {
    return new OrderEvent.OrderShipped("o-1", List.of(lines));
  }

  private static OrderEvent.OrderLine line(String productId, int quantity) {
    return new OrderEvent.OrderLine(productId, quantity, TEN);
  }

  /** An event of order {@code o-1} an {@code ADMIN}'s request caused: no saga origin. */
  private static EventEnvelope envelope(DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(7),
        ORDER,
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
            UserId.of("admin-1"),
            Instant.now()));
  }
}
