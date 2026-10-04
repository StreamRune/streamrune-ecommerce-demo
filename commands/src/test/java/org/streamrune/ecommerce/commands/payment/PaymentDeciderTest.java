package org.streamrune.ecommerce.commands.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.payment.*;
import org.streamrune.test.DeciderFixture;

class PaymentDeciderTest {

  private final DeciderFixture<PaymentCommand, PaymentState, PaymentEvent> fixture =
      DeciderFixture.of(new PaymentDecider());

  private static final Money HUNDRED = new Money(BigDecimal.valueOf(100), "USD");

  private PaymentEvent.PaymentInitiated initiated() {
    return new PaymentEvent.PaymentInitiated("pay-1", "o-1", HUNDRED);
  }

  @Test
  void initiatePayment() {
    fixture
        .given()
        .when(new PaymentCommand.InitiatePayment("pay-1", "o-1", HUNDRED))
        .expectEvents(initiated())
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.PENDING));
  }

  @Test
  void capturePayment() {
    fixture
        .given(initiated())
        .when(new PaymentCommand.CapturePayment("pay-1"))
        .expectEvents(new PaymentEvent.PaymentCaptured("pay-1", "o-1"))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.CAPTURED));
  }

  @Test
  void capturePayment_notPending_throws() {
    fixture
        .given(initiated(), new PaymentEvent.PaymentCaptured("pay-1", "o-1"))
        .when(new PaymentCommand.CapturePayment("pay-1"))
        .expectException(DomainException.class);
  }

  @Test
  void refundPayment_fromCaptured() {
    fixture
        .given(initiated(), new PaymentEvent.PaymentCaptured("pay-1", "o-1"))
        .when(new PaymentCommand.RefundPayment("pay-1", "Saga compensation"))
        .expectEvents(new PaymentEvent.PaymentRefunded("pay-1", "Saga compensation"))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.REFUNDED));
  }

  @Test
  void failPayment() {
    fixture
        .given(initiated())
        .when(new PaymentCommand.FailPayment("pay-1", "Gateway timeout"))
        .expectEvents(new PaymentEvent.PaymentFailed("pay-1", "Gateway timeout"))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.FAILED));
  }
}
