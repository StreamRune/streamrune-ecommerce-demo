package org.streamrune.ecommerce.micronaut.config;

import jakarta.inject.Singleton;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;

/**
 * Payment process manager: reacts to {@code PaymentInitiated} by calling the payment gateway and
 * turning the gateway's answer into a command, so the order-fulfillment saga can advance or
 * compensate.
 *
 * <ul>
 *   <li>gateway succeeds → {@code CapturePayment} → {@code PaymentCaptured} → saga reserves stock;
 *   <li>gateway fails → {@code FailPayment} → {@code PaymentFailed} → saga cancels the order.
 * </ul>
 *
 * <p>It also reacts to {@code PaymentRefunded}, by refunding the payment at the gateway.
 *
 * <p>The gateway call is guarded by a {@link PaymentGatewayCircuitBreaker}: after a few consecutive
 * gateway failures the breaker opens and the manager fails the payment (dispatches {@code
 * FailPayment} without calling the gateway) until the cooldown lets a probe through.
 *
 * <p><b>The command continues the event.</b> Each command is dispatched inside {@link
 * StreamRuneContext#continuing(EventEnvelope) StreamRuneContext.continuing(initiated)}. The command
 * bus then stores {@code PaymentCaptured}/{@code PaymentFailed} with what the helper takes from
 * {@code PaymentInitiated}: its correlation id and trace id, its id as the causation id, and its
 * saga origin one hop further. The fulfillment saga caused {@code PaymentInitiated}, so the reply
 * names that saga as its origin and {@code SagaRunner} hands it to that saga; neither this class
 * nor the saga routes it. The command runs with no user and no authority: the customer who placed
 * the order did not issue it.
 *
 * <p><b>Only the gateway decides the payment.</b> A gateway failure is counted by the breaker and
 * becomes {@code FailPayment}. A failure to dispatch either command — the event store unreachable,
 * the command bus's own circuit breaker open, the bus shutting down — says nothing about the
 * payment, so it propagates out of {@link #onEvents}: the subscription keeps its checkpoint before
 * the event and delivers it again after a backoff. Treating it as a gateway failure would cancel an
 * order whose money the gateway had already taken; swallowing it would leave the payment {@code
 * PENDING} with the event already consumed. That redelivery is the command's only second attempt:
 * the bus publishes a failed command that was dispatched inside {@code continuing} to no dead
 * letter queue, whether or not a saga caused the event. An entry has no place for the causing event
 * or for a saga origin, so its replay would store the reply without them. A capture that keeps
 * failing therefore shows on the subscription and nowhere else: its poll loop logs every failed
 * attempt at {@code ERROR}.
 *
 * <p><b>Safe to deliver twice.</b> The subscription delivers at least once, and after a dispatch
 * failure it delivers the whole batch again:
 *
 * <ul>
 *   <li>A payment whose stream already holds a later event (an earlier delivery captured or failed
 *       it, or the order's cancellation voided it) is not sent to the gateway again, so a failed or
 *       voided payment is never charged later.
 *   <li>The gateway call carries the payment id as its idempotency key: calling it again for a
 *       payment it already captured — the capture could not be recorded — charges nothing.
 *   <li>Each command is dispatched under a key derived from the payment id, the same on every
 *       delivery: a delivery after the command committed is answered from the command inbox and
 *       stores no second event.
 *   <li>The deciders store nothing for a redelivered capture or failure, so no rejection is
 *       expected; one that occurs is a fault and propagates.
 * </ul>
 *
 * <p><b>A payment cancelled while the gateway charged it.</b> A delivery of {@code
 * PaymentInitiated} reads what the payment's stream holds after it. A void or a failure without a
 * capture, while the gateway holds a capture of the payment — the money was taken and the capture
 * was not recorded before the payment was cancelled — is answered with {@code CapturePayment}; the
 * payment records the capture and refunds it in one append.
 *
 * <p><b>Refunds reach the gateway.</b> On {@code PaymentRefunded} it refunds the payment at the
 * gateway; the payment id is the refund's idempotency key, so a redelivery refunds nothing twice.
 */
@Singleton
public class PaymentProcessManager implements EventListener {

  private static final Logger LOG = LoggerFactory.getLogger(PaymentProcessManager.class);

  /**
   * A payment's stream holds four events at most: initiated, voided or failed, captured, refunded.
   */
  static final int PAYMENT_STREAM_READ_LIMIT = 16;

  private final PaymentGatewaySimulator gateway;
  private final PaymentGatewayCircuitBreaker breaker;
  private final CommandBus commandBus;
  private final EventStore eventStore;

  public PaymentProcessManager(
      PaymentGatewaySimulator gateway,
      PaymentGatewayCircuitBreaker breaker,
      CommandBus commandBus,
      EventStore eventStore) {
    this.gateway = gateway;
    this.breaker = breaker;
    this.commandBus = commandBus;
    this.eventStore = eventStore;
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    for (EventEnvelope envelope : events) {
      switch (envelope.event()) {
        case PaymentEvent.PaymentInitiated initiated -> settle(initiated.paymentId(), envelope);
        case PaymentEvent.PaymentRefunded refunded -> gateway.refund(refunded.paymentId());
        default -> {}
      }
    }
  }

  /**
   * Decides what this delivery of {@code PaymentInitiated} does from what the payment's stream
   * holds after it, read from the event store: nothing yet — the gateway is asked; a capture — an
   * earlier delivery recorded it; a void or a failure and no capture — the capture is recorded only
   * when the gateway took the money, and the payment refunds it.
   */
  private void settle(String paymentId, EventEnvelope initiated) {
    List<EventEnvelope> later =
        eventStore.readStream(initiated.streamId(), initiated.version(), PAYMENT_STREAM_READ_LIMIT);
    if (later.isEmpty()) {
      capture(paymentId, initiated);
      return;
    }
    if (later.stream().anyMatch(e -> e.event() instanceof PaymentEvent.PaymentCaptured)) {
      return;
    }
    if (gateway.hasCaptured(paymentId)) {
      LOG.info(
          "Payment {} was cancelled or failed after the gateway took the money; recording the"
              + " capture, which refunds it",
          LogSanitizer.sanitizeForLog(paymentId));
      dispatch(new PaymentCommand.CapturePayment(paymentId), captureKey(paymentId), initiated);
    }
  }

  private void capture(String paymentId, EventEnvelope initiated) {
    if (!breaker.allowRequest()) {
      LOG.warn(
          "Payment gateway circuit OPEN — failing payment {}",
          LogSanitizer.sanitizeForLog(paymentId));
      dispatch(
          new PaymentCommand.FailPayment(paymentId, "payment gateway circuit open"),
          failKey(paymentId),
          initiated);
      return;
    }
    try {
      gateway.processPayment(paymentId);
    } catch (RuntimeException gatewayFailure) {
      breaker.recordFailure();
      LOG.warn(
          "Payment gateway failed for {}: {}",
          LogSanitizer.sanitizeForLog(paymentId),
          LogSanitizer.sanitizeForLog(gatewayFailure.getMessage()));
      dispatch(
          new PaymentCommand.FailPayment(paymentId, gatewayFailure.getMessage()),
          failKey(paymentId),
          initiated);
      return;
    }
    breaker.recordSuccess();
    dispatch(new PaymentCommand.CapturePayment(paymentId), captureKey(paymentId), initiated);
  }

  /** The capture's idempotency key: one per payment, the same on every delivery. */
  private static IdempotencyKey captureKey(String paymentId) {
    return new IdempotencyKey("payment-capture:" + paymentId);
  }

  /** The failure's idempotency key: one per payment, the same on every delivery. */
  private static IdempotencyKey failKey(String paymentId) {
    return new IdempotencyKey("payment-fail:" + paymentId);
  }

  /**
   * Dispatches the command as a continuation of {@code PaymentInitiated}: the events it produces
   * carry that event's correlation id, its id as their causation id, and its saga origin.
   */
  private void dispatch(PaymentCommand command, IdempotencyKey key, EventEnvelope initiated) {
    StreamRuneContext.continuing(initiated).run(() -> commandBus.execute(command, key));
  }
}
