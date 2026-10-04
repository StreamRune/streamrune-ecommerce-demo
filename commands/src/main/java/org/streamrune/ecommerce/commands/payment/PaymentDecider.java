package org.streamrune.ecommerce.commands.payment;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.payment.*;

public class PaymentDecider implements Decider<PaymentCommand, PaymentState, PaymentEvent> {

  @Override
  public PaymentState initialState() {
    return new PaymentState();
  }

  @Override
  public List<PaymentEvent> decide(PaymentCommand cmd, PaymentState state) {
    return switch (cmd) {
      case PaymentCommand.InitiatePayment c ->
          List.of(new PaymentEvent.PaymentInitiated(c.paymentId(), c.orderId(), c.amount()));

      case PaymentCommand.CapturePayment c -> {
        if (state.status() != PaymentStatus.PENDING)
          throw new DomainException("Can only capture PENDING payments");
        yield List.of(new PaymentEvent.PaymentCaptured(c.paymentId(), state.orderId()));
      }

      case PaymentCommand.RefundPayment c -> {
        if (state.status() != PaymentStatus.CAPTURED)
          throw new DomainException("Can only refund CAPTURED payments");
        yield List.of(new PaymentEvent.PaymentRefunded(c.paymentId(), c.reason()));
      }

      case PaymentCommand.FailPayment c -> {
        if (state.status() != PaymentStatus.PENDING)
          throw new DomainException("Can only fail PENDING payments");
        yield List.of(new PaymentEvent.PaymentFailed(c.paymentId(), c.reason()));
      }
    };
  }

  @Override
  public PaymentState evolve(PaymentState state, PaymentEvent evt) {
    return switch (evt) {
      case PaymentEvent.PaymentInitiated e ->
          new PaymentState(e.paymentId(), e.orderId(), e.amount(), PaymentStatus.PENDING);
      case PaymentEvent.PaymentCaptured e ->
          new PaymentState(
              state.paymentId(), state.orderId(), state.amount(), PaymentStatus.CAPTURED);
      case PaymentEvent.PaymentRefunded e ->
          new PaymentState(
              state.paymentId(), state.orderId(), state.amount(), PaymentStatus.REFUNDED);
      case PaymentEvent.PaymentFailed e ->
          new PaymentState(
              state.paymentId(), state.orderId(), state.amount(), PaymentStatus.FAILED);
    };
  }
}
