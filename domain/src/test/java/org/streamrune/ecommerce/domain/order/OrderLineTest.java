package org.streamrune.ecommerce.domain.order;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.types.AggregateId;
import org.streamrune.ecommerce.domain.common.Money;

/**
 * The order line's product id is checked when the {@code PlaceOrder} command is built, which is
 * where all three apps' order endpoints build it. The saga later reserves stock on the inventory
 * aggregate whose id is the product id, and that id goes through {@code AggregateId.of} in the
 * inventory extractor. A product id that the extractor would refuse must therefore be refused here,
 * before the order is placed, not after the payment was captured.
 */
class OrderLineTest {

  private static final Money TEN_USD = new Money(BigDecimal.TEN, "USD");

  private static IllegalArgumentException refused(String productId) {
    return assertThrows(
        IllegalArgumentException.class, () -> new OrderCommand.OrderLine(productId, 1, TEN_USD));
  }

  @Test
  void anOrdinaryProductIdIsAccepted() {
    OrderCommand.OrderLine line = new OrderCommand.OrderLine("prod-001", 2, TEN_USD);

    assertEquals("prod-001", line.productId());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "p\nINFO forged-SECRET", // LF
        "p\rSECRET", // CR
        "p\tSECRET", // TAB
        "p\u0000SECRET", // NUL
        "p\u001FSECRET", // last C0
        "p\u007FSECRET", // DEL
        "p\u0085SECRET", // C1 (NEL)
        "p\u009FSECRET" // last C1
      })
  void aProductIdWithAControlCharacterIsRefused_withoutEchoingIt(String productId) {
    IllegalArgumentException e = refused(productId);

    assertEquals(
        "productId must not contain control characters (U+0000-U+001F, U+007F, U+0080-U+009F)",
        e.getMessage());
    assertFalse(e.getMessage().contains("SECRET"));
  }

  @Test
  void aProductIdAtTheLengthBoundIsAccepted() {
    String productId = "x".repeat(255);

    assertDoesNotThrow(() -> new OrderCommand.OrderLine(productId, 1, TEN_USD));
  }

  @Test
  void aProductIdOneCharacterOverTheBoundIsRefused_withoutEchoingIt() {
    String productId = "SECRET" + "x".repeat(256 - 6);

    IllegalArgumentException e = refused(productId);

    assertEquals("productId must be at most 255 characters, got 256", e.getMessage());
    assertFalse(e.getMessage().contains("SECRET"));
  }

  @Test
  void theBoundIsExactlyWhatTheInventoryExtractorAccepts() {
    String atBound = "x".repeat(255);

    assertDoesNotThrow(() -> AggregateId.of(atBound));
    assertThrows(IllegalArgumentException.class, () -> AggregateId.of(atBound + "x"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t"})
  void aBlankProductIdIsRefused(String productId) {
    assertEquals("productId is required", refused(productId).getMessage());
  }

  @Test
  void aNullProductIdIsRefused() {
    assertEquals("productId is required", refused(null).getMessage());
  }

  @Test
  void theRecordedLineOfAnOrderPlacedEventKeepsNoRule() {
    // OrderEvent.OrderLine is rebuilt from the event log and from saga state; a rule there would
    // make a stored order unreadable instead of stopping a write.
    OrderEvent.OrderLine stored = new OrderEvent.OrderLine("p\nstored", 1, TEN_USD);

    assertTrue(stored.productId().contains("\n"));
  }
}
