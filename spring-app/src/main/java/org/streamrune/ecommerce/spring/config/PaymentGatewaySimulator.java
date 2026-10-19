package org.streamrune.ecommerce.spring.config;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.types.LogSanitizer;

/**
 * Stands in for an external payment gateway. {@link #processPayment} captures a payment, or throws
 * the way a failing gateway call would while failure injection is on.
 *
 * <p>The payment id is the call's idempotency key, as it would be in the idempotency-key header of
 * a real gateway's API: a second call for a payment the gateway already captured returns without
 * charging again. The {@link PaymentProcessManager} relies on this when the same {@code
 * PaymentInitiated} reaches it twice — after a capture the app could not record, the gateway has
 * already taken the money. A real gateway keeps its keys for a limited time; this simulator keeps
 * them in memory until the app stops.
 *
 * <p>Refunds are idempotent by payment id too; each process keeps its own memory, so with two
 * backends a refund reaches the simulator of the backend that leads the payment process manager.
 */
public class PaymentGatewaySimulator {

  private static final Logger LOG = LoggerFactory.getLogger(PaymentGatewaySimulator.class);

  private final AtomicBoolean failureInjected = new AtomicBoolean(false);
  private final Set<String> capturedPayments = ConcurrentHashMap.newKeySet();
  private final Set<String> refundedPayments = ConcurrentHashMap.newKeySet();

  public boolean isFailureInjected() {
    return failureInjected.get();
  }

  public void setFailureInjected(boolean inject) {
    failureInjected.set(inject);
  }

  /**
   * Captures the payment, or throws while failure injection is on. A call for a payment this
   * gateway already captured returns without charging again, whether or not failure injection is
   * on.
   */
  public void processPayment(String paymentId) {
    if (capturedPayments.contains(paymentId)) {
      return;
    }
    if (failureInjected.get()) {
      throw new RuntimeException("Simulated payment gateway failure for payment " + paymentId);
    }
    capturedPayments.add(paymentId);
  }

  /** Whether this gateway has captured the payment. */
  public boolean hasCaptured(String paymentId) {
    return capturedPayments.contains(paymentId);
  }

  /**
   * Refunds a payment this gateway captured; a second refund of it refunds nothing. A refund of a
   * payment this gateway never captured records nothing and logs a warning. Failure injection
   * affects {@link #processPayment} only.
   */
  public void refund(String paymentId) {
    if (!capturedPayments.contains(paymentId)) {
      LOG.warn(
          "Refund of payment {}, which this gateway never captured: nothing refunded",
          LogSanitizer.sanitizeForLog(paymentId));
      return;
    }
    refundedPayments.add(paymentId);
  }

  /** Whether this gateway has refunded the payment. */
  public boolean hasRefunded(String paymentId) {
    return refundedPayments.contains(paymentId);
  }
}
