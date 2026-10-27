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
 *   <li>gateway captures → {@code CapturePayment} → {@code PaymentCaptured} → saga reserves stock;
 *   <li>gateway declines → {@code FailPayment} → {@code PaymentFailed} → saga cancels the order;
 *   <li>gateway gives no answer → nothing is decided: the event is delivered again, unless the
 *       gateway reports that it took the money, which records the capture.
 * </ul>
 *
 * <p>It also reacts to {@code PaymentRefunded}, by refunding the payment at the gateway.
 *
 * <p><b>A decline is an answer; a call without one is not.</b> Only the gateway's decline ({@link
 * PaymentGatewaySimulator.PaymentDeclinedException}) fails the payment. Any other failure of the
 * call — a timeout, a reset connection, a {@code 5xx} — leaves the outcome unknown: a real gateway
 * may have taken the money before its answer was lost. The manager then asks the gateway whether it
 * holds a capture of the payment, by the payment id that is the call's idempotency key. If it does,
 * the capture is recorded. If it does not, {@link #onEvents} throws {@link
 * PaymentOutcomeUnknownException}: the payment stays {@code PENDING}, the subscription delivers
 * {@code PaymentInitiated} again after a backoff, and the gateway is asked again. The order's
 * deadline bounds the wait: its cancellation voids the pending payment, and a capture the gateway
 * reports after that is recorded and refunded (below). Failing the payment on an unknown outcome
 * would cancel an order whose money the gateway may hold, and nothing would ask the gateway again.
 *
 * <p>The gateway call is guarded by a {@link PaymentGatewayCircuitBreaker}, which counts the calls
 * that got no answer: after a few in a row it opens, and the manager does not call the gateway
 * until the cooldown lets a probe through. A payment that meets the open breaker has no answer
 * either, and is held the same way. A decline comes from a gateway that works, so the breaker
 * counts it as a success.
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
 * <p><b>Only the gateway decides the payment.</b> A failure to dispatch either command — the event
 * store unreachable, the command bus's own circuit breaker open, the bus shutting down — says
 * nothing about the payment, so it propagates out of {@link #onEvents}: the subscription keeps its
 * checkpoint before the event and delivers it again after a backoff. Treating it as a decline would
 * cancel an order whose money the gateway had already taken; swallowing it would leave the payment
 * {@code PENDING} with the event already consumed. That redelivery is the command's only second
 * attempt: the bus publishes a failed command that was dispatched inside {@code continuing} to no
 * dead letter queue, whether or not a saga caused the event. An entry has no place for the causing
 * event or for a saga origin, so its replay would store the reply without them. A capture that
 * keeps failing therefore shows on the subscription and nowhere else: its poll loop logs every
 * failed attempt at {@code ERROR}, and so does a payment the gateway gives no answer for.
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
      withoutAnAnswer(paymentId, initiated, null);
      return;
    }
    try {
      gateway.processPayment(paymentId);
    } catch (PaymentGatewaySimulator.PaymentDeclinedException declined) {
      // The gateway answered: it works, and it took no money.
      breaker.recordSuccess();
      LOG.info("Payment gateway declined {}", LogSanitizer.sanitizeForLog(paymentId));
      dispatch(
          new PaymentCommand.FailPayment(paymentId, declined.getMessage()),
          failKey(paymentId),
          initiated);
      return;
    } catch (RuntimeException noAnswer) {
      breaker.recordFailure();
      withoutAnAnswer(paymentId, initiated, noAnswer);
      return;
    }
    breaker.recordSuccess();
    dispatch(new PaymentCommand.CapturePayment(paymentId), captureKey(paymentId), initiated);
  }

  /**
   * The gateway gave no answer for the payment, or its open breaker kept the call from being made:
   * the outcome is unknown. A capture the gateway reports is recorded; otherwise the payment stays
   * {@code PENDING} and the event is delivered again.
   *
   * @param noAnswer the call's failure, or {@code null} when the breaker was open
   * @throws PaymentOutcomeUnknownException when the gateway reports no capture of the payment
   */
  private void withoutAnAnswer(
      String paymentId, EventEnvelope initiated, RuntimeException noAnswer) {
    if (gateway.hasCaptured(paymentId)) {
      LOG.info(
          "The payment gateway gave no answer for payment {} but holds its capture; recording it",
          LogSanitizer.sanitizeForLog(paymentId));
      dispatch(new PaymentCommand.CapturePayment(paymentId), captureKey(paymentId), initiated);
      return;
    }
    throw new PaymentOutcomeUnknownException(paymentId, noAnswer);
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

  /**
   * The gateway gave no answer for a payment and holds no capture of it: the payment stays {@code
   * PENDING}, and the subscription delivers {@code PaymentInitiated} again after a backoff.
   */
  public static final class PaymentOutcomeUnknownException extends RuntimeException {

    PaymentOutcomeUnknownException(String paymentId, RuntimeException noAnswer) {
      super(
          (noAnswer == null
                  ? "The payment gateway's circuit is open: no call for payment "
                  : "The payment gateway gave no answer for payment ")
              + LogSanitizer.sanitizeForLog(paymentId)
              + ", and it holds no capture of it; the payment stays PENDING until a delivery"
              + " gets an answer",
          noAnswer);
    }
  }
}
