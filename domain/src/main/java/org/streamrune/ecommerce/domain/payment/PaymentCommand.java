package org.streamrune.ecommerce.domain.payment;

import org.streamrune.core.Command;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface PaymentCommand extends Command {
  record InitiatePayment(String paymentId, String orderId, Money amount)
      implements PaymentCommand {}

  record CapturePayment(String paymentId) implements PaymentCommand {}

  record FailPayment(String paymentId, String reason) implements PaymentCommand {}

  /** Voids a pending payment, refunds a captured one in full, and does nothing otherwise. */
  record CancelPayment(String paymentId, String reason) implements PaymentCommand {}
}
