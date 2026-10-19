package org.streamrune.ecommerce.domain.payment;

import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface PaymentEvent extends DomainEvent {
  record PaymentInitiated(String paymentId, String orderId, Money amount) implements PaymentEvent {}

  record PaymentCaptured(String paymentId, String orderId) implements PaymentEvent {}

  record PaymentRefunded(String paymentId, String orderId, Money amount, String reason)
      implements PaymentEvent {}

  record PaymentFailed(String paymentId, String orderId, String reason) implements PaymentEvent {}

  record PaymentVoided(String paymentId, String orderId, String reason) implements PaymentEvent {}
}
