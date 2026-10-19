package org.streamrune.ecommerce.domain.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.domain.common.Money;

/**
 * An {@code OrderCancelled} or {@code OrderShipped} read from the log without {@code lines} holds
 * an empty list, so the code that reacts to the event iterates the lines without a null check.
 */
class OrderEventTest {

  private static final OrderEvent.OrderLine LINE =
      new OrderEvent.OrderLine("p-1", 1, new Money(BigDecimal.TEN, "USD"));

  @Test
  void aCancellationWithoutLinesHoldsNone() {
    assertEquals(List.of(), new OrderEvent.OrderCancelled("o-1", "why", null).lines());
  }

  @Test
  void aShipmentWithoutLinesHoldsNone() {
    assertEquals(List.of(), new OrderEvent.OrderShipped("o-1", null).lines());
  }

  @Test
  void theLinesAreACopy() {
    List<OrderEvent.OrderLine> lines = new ArrayList<>(List.of(LINE));
    OrderEvent.OrderCancelled event = new OrderEvent.OrderCancelled("o-1", "why", lines);

    lines.clear();

    assertEquals(List.of(LINE), event.lines());
    assertThrows(UnsupportedOperationException.class, () -> event.lines().add(LINE));
  }
}
