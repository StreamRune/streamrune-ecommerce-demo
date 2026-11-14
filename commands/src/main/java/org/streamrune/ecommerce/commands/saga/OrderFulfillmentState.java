package org.streamrune.ecommerce.commands.saga;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;

/**
 * What the fulfillment saga keeps of its order: the ids, the total, the lines, the products whose
 * stock is reserved so far, once each, in the order of the lines, and the reason it cancels the
 * order with when a product's stock is refused ({@code null} otherwise).
 */
public record OrderFulfillmentState(
    SagaId sagaId,
    String orderId,
    String customerId,
    String paymentId,
    Money orderTotal,
    List<OrderEvent.OrderLine> lines,
    List<String> reservedProductIds,
    OrderFulfillmentStatus fulfillmentStatus,
    String cancellationReason)
    implements SagaState {

  @JsonCreator
  public OrderFulfillmentState(
      @JsonProperty("sagaId") SagaId sagaId,
      @JsonProperty("orderId") String orderId,
      @JsonProperty("customerId") String customerId,
      @JsonProperty("paymentId") String paymentId,
      @JsonProperty("orderTotal") Money orderTotal,
      @JsonProperty("lines") List<OrderEvent.OrderLine> lines,
      @JsonProperty("reservedProductIds") List<String> reservedProductIds,
      @JsonProperty("fulfillmentStatus") OrderFulfillmentStatus fulfillmentStatus,
      @JsonProperty("cancellationReason") String cancellationReason) {
    this.sagaId = sagaId;
    this.orderId = orderId;
    this.customerId = customerId;
    this.paymentId = paymentId;
    this.orderTotal = orderTotal;
    this.lines = lines == null ? List.of() : List.copyOf(lines);
    this.reservedProductIds =
        reservedProductIds == null ? List.of() : List.copyOf(reservedProductIds);
    this.fulfillmentStatus = fulfillmentStatus;
    this.cancellationReason = cancellationReason;
  }

  @Override
  public SagaStatus status() {
    return switch (fulfillmentStatus) {
      case COMPLETED -> SagaStatus.COMPLETED;
      case FAILED -> SagaStatus.FAILED;
      case COMPENSATING -> SagaStatus.COMPENSATING;
      case AWAITING_PAYMENT -> SagaStatus.STARTED;
      // CANCELLING is intentionally non-terminal (SagaStatus.RUNNING): the framework's
      // SagaRunner skips handle() once evolve() reaches a terminal status, so PaymentFailed
      // must land here first to let handle() dispatch CancelOrder before the saga ends.
      default -> SagaStatus.RUNNING;
    };
  }

  OrderFulfillmentState withStatus(OrderFulfillmentStatus status) {
    return new OrderFulfillmentState(
        sagaId,
        orderId,
        customerId,
        paymentId,
        orderTotal,
        lines,
        reservedProductIds,
        status,
        cancellationReason);
  }

  /** The saga cancelling its order for {@code reason}. */
  OrderFulfillmentState cancellingFor(String reason) {
    return new OrderFulfillmentState(
        sagaId,
        orderId,
        customerId,
        paymentId,
        orderTotal,
        lines,
        reservedProductIds,
        OrderFulfillmentStatus.CANCELLING,
        reason);
  }

  /** The products reserved so far plus {@code productId}, once each, in the order of the lines. */
  OrderFulfillmentState withReservedProduct(String productId) {
    List<String> reserved =
        lines.stream()
            .map(OrderEvent.OrderLine::productId)
            .filter(p -> p.equals(productId) || reservedProductIds.contains(p))
            .distinct()
            .toList();
    return new OrderFulfillmentState(
        sagaId,
        orderId,
        customerId,
        paymentId,
        orderTotal,
        lines,
        reserved,
        fulfillmentStatus,
        cancellationReason);
  }

  boolean hasLineFor(String productId) {
    return lines.stream().anyMatch(l -> l.productId().equals(productId));
  }

  boolean everyLineReserved() {
    return lines.stream()
        .map(OrderEvent.OrderLine::productId)
        .allMatch(reservedProductIds::contains);
  }
}
