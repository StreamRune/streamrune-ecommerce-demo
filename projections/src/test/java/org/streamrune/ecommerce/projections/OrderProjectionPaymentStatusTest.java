package org.streamrune.ecommerce.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The payment status an order's row shows: {@link OrderProjection} follows the payment events of
 * the order, by the order id they name, and an order event leaves the payment status as it is.
 */
class OrderProjectionPaymentStatusTest {

  private static final Money TEN = new Money(BigDecimal.TEN, "USD");
  private static final Money TWENTY = new Money(new BigDecimal("20.00"), "USD");

  private final OrderProjection projection =
      new OrderProjection(new InMemoryProjectionRepository());
  private final AtomicLong offset = new AtomicLong();

  @Test
  void thePaymentStatusFollowsTheOrdersPaymentEvents() {
    process(placed("o-1"));
    assertThat(projection.get("o-1").paymentStatus()).as("no payment yet").isNull();

    process(new PaymentEvent.PaymentInitiated("pay-o-1", "o-1", TWENTY));
    assertThat(projection.get("o-1").paymentStatus()).isEqualTo(PaymentStatus.PENDING);

    process(new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"));
    assertThat(projection.get("o-1").paymentStatus()).isEqualTo(PaymentStatus.CAPTURED);

    process(new PaymentEvent.PaymentRefunded("pay-o-1", "o-1", TWENTY, "Order cancelled"));
    assertThat(projection.get("o-1").paymentStatus()).isEqualTo(PaymentStatus.REFUNDED);

    process(
        placed("o-2"),
        new PaymentEvent.PaymentInitiated("pay-o-2", "o-2", TWENTY),
        new PaymentEvent.PaymentVoided("pay-o-2", "o-2", "Order cancelled"));
    assertThat(projection.get("o-2").paymentStatus()).isEqualTo(PaymentStatus.VOIDED);

    process(
        placed("o-3"),
        new PaymentEvent.PaymentInitiated("pay-o-3", "o-3", TWENTY),
        new PaymentEvent.PaymentFailed("pay-o-3", "o-3", "Card declined"));
    assertThat(projection.get("o-3").paymentStatus()).isEqualTo(PaymentStatus.FAILED);

    assertThat(projection.get("o-1").paymentStatus())
        .as("another order's payment leaves this one's row as it is")
        .isEqualTo(PaymentStatus.REFUNDED);
  }

  @Test
  void aLateCaptureEndsRefunded() {
    process(placed("o-1"));
    process(
        new PaymentEvent.PaymentInitiated("pay-o-1", "o-1", TWENTY),
        new PaymentEvent.PaymentVoided("pay-o-1", "o-1", "Order cancelled"),
        new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"),
        new PaymentEvent.PaymentRefunded(
            "pay-o-1", "o-1", TWENTY, "captured after the payment was cancelled"));

    assertThat(projection.get("o-1").paymentStatus()).isEqualTo(PaymentStatus.REFUNDED);
  }

  @Test
  void anOrderEventKeepsThePaymentStatus() {
    process(
        placed("o-1"),
        new PaymentEvent.PaymentInitiated("pay-o-1", "o-1", TWENTY),
        new PaymentEvent.PaymentCaptured("pay-o-1", "o-1"));

    process(new OrderEvent.OrderCancelled("o-1", "changed my mind", List.of()));

    OrderView view = projection.get("o-1");
    assertThat(view.status()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(view.paymentStatus()).isEqualTo(PaymentStatus.CAPTURED);
  }

  @Test
  void aPaymentOfAnOrderTheReadModelDoesNotHoldChangesNothing() {
    process(
        new PaymentEvent.PaymentInitiated("pay-o-9", "o-9", TWENTY),
        new PaymentEvent.PaymentCaptured("pay-o-9", "o-9"));

    assertThat(projection.get("o-9")).isNull();
    assertThat(projection.listAll()).isEmpty();
  }

  private static OrderEvent.OrderPlaced placed(String orderId) {
    return new OrderEvent.OrderPlaced(
        orderId, "c-1", List.of(new OrderEvent.OrderLine("p-1", 2, TEN)), TWENTY);
  }

  private void process(DomainEvent... events) {
    projection.process(Arrays.stream(events).map(this::envelope).toList());
  }

  private EventEnvelope envelope(DomainEvent event) {
    long at = offset.incrementAndGet();
    String type = event.getClass().getSimpleName();
    AggregateType aggregate =
        event instanceof PaymentEvent ? AggregateType.of("payment") : AggregateType.of("order");
    return new EventEnvelope(
        GlobalOffset.of(at),
        StreamId.of(aggregate, AggregateId.of("s-" + at)),
        new Version(1),
        new EventType(type),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("c-" + at),
            null,
            UserId.of("u-1"),
            Instant.now()));
  }
}
