package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The simulated gateway declines while failure injection is on and answers nothing during an
 * outage, charging nothing either way, and refunds a payment it captured, once, whatever the two
 * switches say.
 */
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

  @Test
  void failureInjectionDeclinesAPaymentItHasNotCaptured_andChargesNothing() {
    gateway.setFailureInjected(true);

    assertThatThrownBy(() -> gateway.processPayment("pay-1"))
        .isInstanceOf(PaymentGatewaySimulator.PaymentDeclinedException.class);
    assertThat(gateway.hasCaptured("pay-1")).isFalse();
  }

  @Test
  void anOutageAnswersNoCall_andChargesNothing() {
    gateway.processPayment("pay-1");
    gateway.setOutage(true);

    assertThatThrownBy(() -> gateway.processPayment("pay-2"))
        .isInstanceOf(IllegalStateException.class)
        .isNotInstanceOf(PaymentGatewaySimulator.PaymentDeclinedException.class);
    assertThatThrownBy(() -> gateway.processPayment("pay-1"))
        .as("no answer for a captured payment either")
        .isInstanceOf(IllegalStateException.class);
    assertThat(gateway.hasCaptured("pay-2")).isFalse();

    gateway.setOutage(false);
    gateway.processPayment("pay-2");
    assertThat(gateway.hasCaptured("pay-2")).isTrue();
  }
}
