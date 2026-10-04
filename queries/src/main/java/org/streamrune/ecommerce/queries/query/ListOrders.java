package org.streamrune.ecommerce.queries.query;

import org.streamrune.core.Cacheable;
import org.streamrune.ecommerce.domain.order.OrderEvent;

@Cacheable(
    ttlSeconds = 30,
    invalidateOn = {
      OrderEvent.OrderPlaced.class, OrderEvent.OrderConfirmed.class,
      OrderEvent.OrderShipped.class, OrderEvent.OrderDelivered.class,
      OrderEvent.OrderCancelled.class
    })
public record ListOrders(String customerId)
    implements org.streamrune.core.Query<
        java.util.List<org.streamrune.ecommerce.queries.dto.OrderView>> {}
