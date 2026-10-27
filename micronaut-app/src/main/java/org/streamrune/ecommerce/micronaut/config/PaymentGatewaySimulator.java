package org.streamrune.ecommerce.micronaut.config;

import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.types.LogSanitizer;

/**
 * Stands in for an external payment gateway. {@link #processPayment} captures a payment, declines
 * it while failure injection is on, and answers nothing while an outage is on.
 *
 * <p><b>A decline is an answer; a failed call is not.</b> A decline ({@link
 * PaymentDeclinedException}) is the gateway's answer that it took no money: the payment fails. Any
 * other failure of the call — a timeout, a reset connection, a {@code 5xx} — says nothing about the
 * money, because a real gateway may have charged the payment before its answer was lost. This
 * simulator never charges a call that fails, which is a property of the simulator and not of a
 * gateway: the {@link PaymentProcessManager} asks {@link #hasCaptured} after a call without an
 * answer, as it would ask a real gateway for the payment's status by its idempotency key.
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
@Singleton
public class PaymentGatewaySimulator {

  private static final Logger LOG = LoggerFactory.getLogger(PaymentGatewaySimulator.class);

  private final AtomicBoolean failureInjected = new AtomicBoolean(false);
  private final AtomicBoolean outage = new AtomicBoolean(false);
  private final Set<String> capturedPayments = ConcurrentHashMap.newKeySet();
  private final Set<String> refundedPayments = ConcurrentHashMap.newKeySet();

  /** Whether the gateway declines every payment it has not captured. */
  public boolean isFailureInjected() {
    return failureInjected.get();
  }

  public void setFailureInjected(boolean inject) {
    failureInjected.set(inject);
  }

  /** Whether the gateway answers no call: every call fails without an answer. */
  public boolean isOutage() {
    return outage.get();
  }

  public void setOutage(boolean down) {
    outage.set(down);
  }

  /**
   * Captures the payment. During an outage the call fails without an answer, and while failure
   * injection is on the gateway declines a payment it has not captured; a call for a payment this
   * gateway already captured returns without charging again.
   *
   * @throws PaymentDeclinedException when the gateway declines the payment: nothing was charged
   * @throws IllegalStateException when the gateway gives no answer; this simulator charged nothing
   */
  public void processPayment(String paymentId) {
    if (outage.get()) {
      throw new IllegalStateException(
          "Simulated payment gateway outage: no answer for payment " + paymentId);
    }
    if (capturedPayments.contains(paymentId)) {
      return;
    }
    if (failureInjected.get()) {
      throw new PaymentDeclinedException(paymentId);
    }
    capturedPayments.add(paymentId);
  }

  /** Whether this gateway has captured the payment: its status lookup by idempotency key. */
  public boolean hasCaptured(String paymentId) {
    return capturedPayments.contains(paymentId);
  }

  /**
   * Refunds a payment this gateway captured; a second refund of it refunds nothing. A refund of a
   * payment this gateway never captured records nothing and logs a warning. Failure injection and
   * the outage affect {@link #processPayment} only.
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

  /** The gateway declined the payment: its answer, and no money was taken. */
  public static final class PaymentDeclinedException extends RuntimeException {

    public PaymentDeclinedException(String paymentId) {
      super("Simulated payment gateway declined payment " + paymentId);
    }
  }
}
