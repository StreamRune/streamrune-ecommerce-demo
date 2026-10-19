package org.streamrune.ecommerce.commands.payment;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.payment.*;

public class PaymentDecider implements Decider<PaymentCommand, PaymentState, PaymentEvent> {

  /** The reason of the refund recorded with a capture of a payment that was voided or failed. */
  public static final String LATE_CAPTURE_REASON = "captured after the payment was cancelled";

  @Override
  public PaymentState initialState() {
    return new PaymentState();
  }

  /**
   * {@code CapturePayment} is sent only after the gateway took the money, so it is never refused
   * for a status the money cannot undo: on a voided or failed payment the capture is recorded and
   * refunded in one append. {@code FailPayment} and {@code CancelPayment} store nothing for a
   * payment they do not apply to, so a redelivered or late command settles nothing twice.
   */
  @Override
  public List<PaymentEvent> decide(PaymentCommand cmd, PaymentState state) {
    return switch (cmd) {
      case PaymentCommand.InitiatePayment c -> {
        if (exists(state)) throw new DomainException("Payment already exists: " + c.paymentId());
        yield List.of(new PaymentEvent.PaymentInitiated(c.paymentId(), c.orderId(), c.amount()));
      }

      case PaymentCommand.CapturePayment c -> {
        requireExists(state, c.paymentId());
        yield switch (state.status()) {
          case PENDING -> List.of(new PaymentEvent.PaymentCaptured(c.paymentId(), state.orderId()));
          case CAPTURED, REFUNDED -> List.of();
          case VOIDED, FAILED ->
              List.of(
                  new PaymentEvent.PaymentCaptured(c.paymentId(), state.orderId()),
                  new PaymentEvent.PaymentRefunded(
                      c.paymentId(), state.orderId(), state.amount(), LATE_CAPTURE_REASON));
        };
      }

      case PaymentCommand.FailPayment c -> {
        requireExists(state, c.paymentId());
        yield state.status() == PaymentStatus.PENDING
            ? List.of(new PaymentEvent.PaymentFailed(c.paymentId(), state.orderId(), c.reason()))
            : List.of();
      }

      case PaymentCommand.CancelPayment c -> {
        if (!exists(state)) yield List.of();
        yield switch (state.status()) {
          case PENDING ->
              List.of(new PaymentEvent.PaymentVoided(c.paymentId(), state.orderId(), c.reason()));
          case CAPTURED ->
              List.of(
                  new PaymentEvent.PaymentRefunded(
                      c.paymentId(), state.orderId(), state.amount(), c.reason()));
          case REFUNDED, VOIDED, FAILED -> List.of();
        };
      }
    };
  }

  /** A payment that was never initiated has no id: its empty state reads {@code PENDING}. */
  private static boolean exists(PaymentState state) {
    return state.paymentId() != null;
  }

  private static void requireExists(PaymentState state, String paymentId) {
    if (!exists(state)) throw new DomainException("Payment not found: " + paymentId);
  }

  @Override
  public PaymentState evolve(PaymentState state, PaymentEvent evt) {
    return switch (evt) {
      case PaymentEvent.PaymentInitiated e ->
          new PaymentState(e.paymentId(), e.orderId(), e.amount(), PaymentStatus.PENDING);
      case PaymentEvent.PaymentCaptured e -> state.withStatus(PaymentStatus.CAPTURED);
      case PaymentEvent.PaymentRefunded e -> state.withStatus(PaymentStatus.REFUNDED);
      case PaymentEvent.PaymentFailed e -> state.withStatus(PaymentStatus.FAILED);
      case PaymentEvent.PaymentVoided e -> state.withStatus(PaymentStatus.VOIDED);
    };
  }
}
