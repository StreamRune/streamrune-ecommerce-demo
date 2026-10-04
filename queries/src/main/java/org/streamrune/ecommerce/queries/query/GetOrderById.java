package org.streamrune.ecommerce.queries.query;

public record GetOrderById(String orderId)
    implements org.streamrune.core.Query<org.streamrune.ecommerce.queries.dto.OrderView> {}
