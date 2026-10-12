package org.streamrune.ecommerce.queries.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;

/**
 * What a frame of the open live feed holds: the four fields that say an event of a kind happened,
 * and no id of the stream it happened on, of the customer or of the order.
 */
class LiveFeedFrameTest {

  private static final Instant AT = Instant.parse("2026-10-04T10:15:30Z");

  @Test
  void aFrameHoldsTheOffsetTheTwoTypesAndTheTimestampInThatOrder() {
    Map<String, Object> frame = LiveFeedFrame.of(customerRegistered());

    assertEquals(
        List.of("globalOffset", "aggregateType", "eventType", "timestamp"),
        List.copyOf(frame.keySet()));
    assertEquals(7L, frame.get("globalOffset"));
    assertEquals("customer", frame.get("aggregateType"));
    assertEquals("CustomerRegistered", frame.get("eventType"));
    assertEquals("2026-10-04T10:15:30Z", frame.get("timestamp"));
  }

  @Test
  void aCustomerEventsFrameNamesNoCustomer() {
    String frame = LiveFeedFrame.of(customerRegistered()).toString();

    assertFalse(frame.contains("c-secret"), frame);
    assertFalse(frame.contains("Alice"), frame);
  }

  @Test
  void anOrderEventsFrameNamesNeitherTheOrderNorItsCustomer() {
    Map<String, Object> frame = LiveFeedFrame.of(orderPlaced());

    assertEquals("order", frame.get("aggregateType"));
    assertEquals("OrderPlaced", frame.get("eventType"));
    assertFalse(frame.toString().contains("o-secret"), frame.toString());
    assertFalse(frame.toString().contains("c-secret"), frame.toString());
  }

  private static EventEnvelope customerRegistered() {
    return envelope(
        7,
        StreamId.of(CustomerState.TYPE, AggregateId.of("c-secret")),
        "CustomerRegistered",
        new CustomerEvent.CustomerRegistered(
            "c-secret", "Alice", "alice@example.com", "1 Main St", "555-0100"));
  }

  private static EventEnvelope orderPlaced() {
    return envelope(
        8,
        StreamId.of(OrderState.TYPE, AggregateId.of("o-secret")),
        "OrderPlaced",
        new OrderEvent.OrderPlaced(
            "o-secret", "c-secret", List.of(), Money.usd(new BigDecimal("10.00"))));
  }

  private static EventEnvelope envelope(
      long offset, StreamId stream, String eventType, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        stream,
        new Version(1),
        new EventType(eventType),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-" + offset),
            null,
            UserId.of("c-secret"),
            AT));
  }
}
