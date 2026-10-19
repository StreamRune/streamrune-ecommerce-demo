package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** The simulated gateway refunds a payment it captured, once, whatever failure injection says. */
class PaymentGatewaySimulatorTest {

  private final PaymentGatewaySimulator gateway = new PaymentGatewaySimulator();

  @Test
  void aRefundOfACapturedPaymentIsRecordedOnce() {
    gateway.processPayment("pay-1");

    gateway.refund("pay-1");
    gateway.refund("pay-1");

    assertThat(gateway.hasRefunded("pay-1")).isTrue();
    assertThat(gateway.hasCaptured("pay-1")).isTrue();
  }

  @Test
  void aRefundOfAPaymentItNeverCapturedRecordsNothing() {
    gateway.refund("pay-1");

    assertThat(gateway.hasRefunded("pay-1")).isFalse();
  }

  @Test
  void failureInjectionLeavesARefundAlone() {
    gateway.processPayment("pay-1");
    gateway.setFailureInjected(true);

    gateway.refund("pay-1");

    assertThat(gateway.hasRefunded("pay-1")).isTrue();
  }
}
