package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.order.*;
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
      if (envelope.event() instanceof OrderEvent evt) {
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
                      envelope.metadata().timestamp(),
                      envelope.metadata().timestamp()));
          case OrderEvent.OrderConfirmed e ->
              updateStatus(e.orderId(), OrderStatus.CONFIRMED, envelope.metadata().timestamp());
          case OrderEvent.OrderShipped e ->
              updateStatus(e.orderId(), OrderStatus.SHIPPED, envelope.metadata().timestamp());
          case OrderEvent.OrderDelivered e ->
              updateStatus(e.orderId(), OrderStatus.DELIVERED, envelope.metadata().timestamp());
          case OrderEvent.OrderCancelled e ->
              updateStatus(e.orderId(), OrderStatus.CANCELLED, envelope.metadata().timestamp());
        }
      }
    }
  }

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
