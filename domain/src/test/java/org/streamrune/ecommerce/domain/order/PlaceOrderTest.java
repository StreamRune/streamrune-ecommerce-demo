package org.streamrune.ecommerce.domain.order;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.AggregateId;
import org.streamrune.ecommerce.domain.common.Money;

/**
 * The order id is checked when the {@code PlaceOrder} command is built, which is where all three
 * apps' order endpoints build it. The order stream takes any id {@code AggregateId.of} accepts, up
 * to 255 characters, but the fulfillment saga that {@code OrderPlaced} starts is identified by the
 * longer id {@code "fulfillment-" + orderId}, and a {@code SagaId} holds 255 characters at most.
 * Without the check here, an order id of 244 to 255 characters would be placed, and the saga could
 * never start.
 */
class PlaceOrderTest {

  private static final List<OrderCommand.OrderLine> LINES =
      List.of(new OrderCommand.OrderLine("prod-1", 1, new Money(BigDecimal.TEN, "USD")));

  private static OrderCommand.PlaceOrder placeOrder(String orderId) {
    return new OrderCommand.PlaceOrder(orderId, "cust-1", LINES);
  }

  @Test
  void anOrdinaryOrderIdIsAccepted() {
    assertEquals("o-1", placeOrder("o-1").orderId());
  }

  @Test
  void anOrderIdAtTheLengthBoundIsAccepted() {
    String orderId = "x".repeat(243);

    assertDoesNotThrow(() -> placeOrder(orderId));
  }

  @Test
  void anOrderIdOneCharacterOverTheBoundIsRefused_withoutEchoingIt() {
    String orderId = "SECRET" + "x".repeat(244 - 6);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> placeOrder(orderId));

    assertEquals("orderId must be at most 243 characters, got 244", e.getMessage());
    assertFalse(e.getMessage().contains("SECRET"));
  }

  @Test
  void anOrderIdThatTheOrderStreamAcceptsButTheSagaIdDoesNotIsRefused() {
    // 255 characters is the longest id AggregateId.of takes, and the order stream accepts it; the
    // saga id "fulfillment-" + it is 267.
    String orderId = "x".repeat(255);

    assertDoesNotThrow(() -> AggregateId.of(orderId));
    assertThrows(IllegalArgumentException.class, () -> placeOrder(orderId));
  }

  @Test
  void theBoundIsExactlyWhatTheSagaIdAcceptsAndWhatEveryDerivedIdFits() {
    String atBound = "x".repeat(243);

    assertDoesNotThrow(() -> new SagaId("fulfillment-" + atBound));
    assertThrows(IllegalArgumentException.class, () -> new SagaId("fulfillment-" + atBound + "x"));
    // The payment id "pay-" + orderId is shorter by eight characters and fits too.
    assertDoesNotThrow(() -> AggregateId.of("pay-" + atBound));
  }

  @Test
  void aBlankOrderIdIsLeftToTheOrderIdExtractor() {
    // A null or blank id is refused where the command bus builds the stream id, with nothing
    // written; the constructor adds only the length rule the saga id needs.
    assertDoesNotThrow(() -> placeOrder(null));
    assertDoesNotThrow(() -> placeOrder(" "));
  }
}
