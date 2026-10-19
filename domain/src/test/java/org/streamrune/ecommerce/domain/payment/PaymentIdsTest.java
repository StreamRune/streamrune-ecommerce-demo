package org.streamrune.ecommerce.domain.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PaymentIdsTest {

  @Test
  void theOrdersPaymentIsThePrefixAndTheOrderId() {
    assertEquals("pay-o-1", PaymentIds.forOrder("o-1"));
    assertEquals("pay-", PaymentIds.PREFIX);
  }
}
