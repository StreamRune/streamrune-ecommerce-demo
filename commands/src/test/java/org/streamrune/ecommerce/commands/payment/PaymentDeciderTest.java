package org.streamrune.ecommerce.commands.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.payment.*;
import org.streamrune.test.DeciderFixture;

class PaymentDeciderTest {

  private final DeciderFixture<PaymentCommand, PaymentState, PaymentEvent> fixture =
      DeciderFixture.of(new PaymentDecider());

  private static final Money HUNDRED = new Money(BigDecimal.valueOf(100), "USD");

  private static PaymentEvent.PaymentInitiated initiated() {
    return new PaymentEvent.PaymentInitiated("pay-1", "o-1", HUNDRED);
  }

  private static PaymentEvent.PaymentCaptured captured() {
    return new PaymentEvent.PaymentCaptured("pay-1", "o-1");
  }

  private static PaymentEvent.PaymentRefunded refunded() {
    return new PaymentEvent.PaymentRefunded("pay-1", "o-1", HUNDRED, "earlier");
  }

  /** The events of a payment that stands in {@code status}. */
  private static PaymentEvent[] historyOf(PaymentStatus status) {
    return switch (status) {
      case PENDING -> new PaymentEvent[] {initiated()};
      case CAPTURED -> new PaymentEvent[] {initiated(), captured()};
      case REFUNDED -> new PaymentEvent[] {initiated(), captured(), refunded()};
      case VOIDED ->
          new PaymentEvent[] {
            initiated(), new PaymentEvent.PaymentVoided("pay-1", "o-1", "earlier")
          };
      case FAILED ->
          new PaymentEvent[] {
            initiated(), new PaymentEvent.PaymentFailed("pay-1", "o-1", "earlier")
          };
    };
  }

  // InitiatePayment

  @Test
  void initiatePayment_onANewStream_initiatesIt() {
    fixture
        .given()
        .when(new PaymentCommand.InitiatePayment("pay-1", "o-1", HUNDRED))
        .expectEvents(initiated())
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.PENDING));
  }

  @ParameterizedTest
  @EnumSource(PaymentStatus.class)
  void initiatePayment_onAnExistingPayment_isRefused(PaymentStatus status) {
    fixture
        .given(historyOf(status))
        .when(new PaymentCommand.InitiatePayment("pay-1", "o-1", HUNDRED))
        .expectException(DomainException.class);
  }

  // CapturePayment

  @Test
  void capturePayment_withoutAStream_isRefused() {
    fixture
        .given()
        .when(new PaymentCommand.CapturePayment("pay-1"))
        .expectException(DomainException.class);
  }

  @Test
  void capturePayment_pending_capturesIt() {
    fixture
        .given(historyOf(PaymentStatus.PENDING))
        .when(new PaymentCommand.CapturePayment("pay-1"))
        .expectEvents(captured())
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.CAPTURED));
  }

  @ParameterizedTest
  @EnumSource(
      value = PaymentStatus.class,
      names = {"CAPTURED", "REFUNDED"})
  void capturePayment_capturedOrRefunded_storesNothing(PaymentStatus status) {
    fixture
        .given(historyOf(status))
        .when(new PaymentCommand.CapturePayment("pay-1"))
        .expectNoEvents();
  }

  @ParameterizedTest
  @EnumSource(
      value = PaymentStatus.class,
      names = {"VOIDED", "FAILED"})
  void capturePayment_voidedOrFailed_recordsTheCaptureAndRefundsItInOneAppend(
      PaymentStatus status) {
    fixture
        .given(historyOf(status))
        .when(new PaymentCommand.CapturePayment("pay-1"))
        .expectEvents(
            captured(),
            new PaymentEvent.PaymentRefunded(
                "pay-1", "o-1", HUNDRED, PaymentDecider.LATE_CAPTURE_REASON))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.REFUNDED));
  }

  // FailPayment

  @Test
  void failPayment_withoutAStream_isRefused() {
    fixture
        .given()
        .when(new PaymentCommand.FailPayment("pay-1", "Gateway timeout"))
        .expectException(DomainException.class);
  }

  @Test
  void failPayment_pending_failsIt() {
    fixture
        .given(historyOf(PaymentStatus.PENDING))
        .when(new PaymentCommand.FailPayment("pay-1", "Gateway timeout"))
        .expectEvents(new PaymentEvent.PaymentFailed("pay-1", "o-1", "Gateway timeout"))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.FAILED));
  }

  @ParameterizedTest
  @EnumSource(
      value = PaymentStatus.class,
      mode = Mode.EXCLUDE,
      names = {"PENDING"})
  void failPayment_notPending_storesNothing(PaymentStatus status) {
    fixture
        .given(historyOf(status))
        .when(new PaymentCommand.FailPayment("pay-1", "Gateway timeout"))
        .expectNoEvents();
  }

  // CancelPayment

  @Test
  void cancelPayment_withoutAStream_storesNothing() {
    fixture.given().when(new PaymentCommand.CancelPayment("pay-1", "why")).expectNoEvents();
  }

  @Test
  void cancelPayment_pending_voidsIt() {
    fixture
        .given(historyOf(PaymentStatus.PENDING))
        .when(new PaymentCommand.CancelPayment("pay-1", "why"))
        .expectEvents(new PaymentEvent.PaymentVoided("pay-1", "o-1", "why"))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.VOIDED));
  }

  @Test
  void cancelPayment_captured_refundsTheFullAmount() {
    fixture
        .given(historyOf(PaymentStatus.CAPTURED))
        .when(new PaymentCommand.CancelPayment("pay-1", "why"))
        .expectEvents(new PaymentEvent.PaymentRefunded("pay-1", "o-1", HUNDRED, "why"))
        .expectState(s -> assertThat(s.status()).isEqualTo(PaymentStatus.REFUNDED));
  }

  @ParameterizedTest
  @EnumSource(
      value = PaymentStatus.class,
      names = {"REFUNDED", "VOIDED", "FAILED"})
  void cancelPayment_settledOtherwise_storesNothing(PaymentStatus status) {
    fixture
        .given(historyOf(status))
        .when(new PaymentCommand.CancelPayment("pay-1", "why"))
        .expectNoEvents();
  }
}
