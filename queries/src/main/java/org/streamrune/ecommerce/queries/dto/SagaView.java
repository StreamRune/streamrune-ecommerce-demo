package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;

public record SagaView(
    String sagaId,
    String orderId,
    String customerId,
    String paymentId,
    OrderFulfillmentStatus status,
    Instant updatedAt) {}
