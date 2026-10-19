package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.order.*;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;

@org.streamrune.core.ProjectionConfig(
    name = "orders",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class OrderProjection extends BaseProjection {

  public OrderProjection(ProjectionRepository repository) {
    super(repository, "orders");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      Instant at = envelope.metadata().timestamp();
      switch (envelope.event()) {
        case OrderEvent evt -> applyOrderEvent(evt, at);
        case PaymentEvent.PaymentInitiated e ->
            updatePayment(e.orderId(), PaymentStatus.PENDING, at);
        case PaymentEvent.PaymentCaptured e ->
            updatePayment(e.orderId(), PaymentStatus.CAPTURED, at);
        case PaymentEvent.PaymentFailed e -> updatePayment(e.orderId(), PaymentStatus.FAILED, at);
        case PaymentEvent.PaymentVoided e -> updatePayment(e.orderId(), PaymentStatus.VOIDED, at);
        case PaymentEvent.PaymentRefunded e ->
            updatePayment(e.orderId(), PaymentStatus.REFUNDED, at);
        default -> {}
      }
    }
  }

  private void applyOrderEvent(OrderEvent evt, Instant at) {
    switch (evt) {
      case OrderEvent.OrderPlaced e ->
          save(
              e.orderId(),
              new OrderView(
                  e.orderId(),
                  e.customerId(),
                  e.lines().stream()
                      .map(
                          l ->
                              new OrderView.OrderLineView(
                                  l.productId(), l.quantity(), l.unitPrice()))
                      .toList(),
                  e.total(),
                  OrderStatus.CREATED,
                  null,
                  at,
                  at));
      case OrderEvent.OrderConfirmed e -> updateStatus(e.orderId(), OrderStatus.CONFIRMED, at);
      case OrderEvent.OrderShipped e -> updateStatus(e.orderId(), OrderStatus.SHIPPED, at);
      case OrderEvent.OrderDelivered e -> updateStatus(e.orderId(), OrderStatus.DELIVERED, at);
      case OrderEvent.OrderCancelled e -> updateStatus(e.orderId(), OrderStatus.CANCELLED, at);
    }
  }

  /** The status of the order's row; its payment status stays as it is. */
  private void updateStatus(String id, OrderStatus status, Instant timestamp) {
    findById(id, OrderView.class)
        .ifPresent(
            existing ->
                save(
                    id,
                    new OrderView(
                        existing.orderId(),
                        existing.customerId(),
                        existing.lines(),
                        existing.total(),
                        status,
                        existing.paymentStatus(),
                        existing.createdAt(),
                        timestamp)));
  }

  /**
   * The payment status of the order's row; a payment of an order the read model does not hold
   * changes nothing.
   */
  private void updatePayment(String orderId, PaymentStatus paymentStatus, Instant timestamp) {
    findById(orderId, OrderView.class)
        .ifPresent(
            existing ->
                save(
                    orderId,
                    new OrderView(
                        existing.orderId(),
                        existing.customerId(),
                        existing.lines(),
                        existing.total(),
                        existing.status(),
                        paymentStatus,
                        existing.createdAt(),
                        timestamp)));
  }

  public OrderView get(String orderId) {
    return findById(orderId, OrderView.class).orElse(null);
  }

  public List<OrderView> listAll() {
    return repository.findAll(projectionName(), OrderView.class);
  }

  public List<OrderView> listByCustomer(String customerId) {
    return repository.findAll(projectionName(), OrderView.class).stream()
        .filter(o -> customerId.equals(o.customerId()))
        .toList();
  }
}
