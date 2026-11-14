package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import java.util.List;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.domain.payment.PaymentStatus;

/**
 * An order as the read model holds it. {@code paymentStatus} is the status of the order's payment,
 * {@code null} until the payment is initiated. {@code reason} is why the order was cancelled: the
 * customer's or an operator's reason, or the fulfillment's, such as the stock a line asked for and
 * its product did not hold; {@code null} for an order that is not cancelled.
 */
public record OrderView(
    String orderId,
    String customerId,
    List<OrderLineView> lines,
    Money total,
    OrderStatus status,
    PaymentStatus paymentStatus,
    String reason,
    Instant createdAt,
    Instant updatedAt) {
  public record OrderLineView(String productId, int quantity, Money unitPrice) {}
}
