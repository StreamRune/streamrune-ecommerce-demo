package org.streamrune.ecommerce.commands.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;

/**
 * Curated, PII-safe outbox mapper for the e-commerce demo.
 *
 * <p>ONLY the allowlisted integration events below are published to the broker. All {@code
 * CustomerEvent.*} and {@code InventoryEvent.*} types — and {@code OrderPlaced} (carries {@code
 * customerId}) and {@code PaymentInitiated} — are explicitly excluded. Each published entry
 * contains a small, hand-crafted integration DTO with ONLY non-PII business fields.
 *
 * <p>Every entry carries a {@code streamId} — the stream the event was appended to ({@code
 * order:<id>}, {@code payment:<id>}, {@code product:<id>}): the demo's outbox channel is {@code
 * STRICT_PER_AGGREGATE}, where a {@code null} stream fails the append with {@code
 * OutboxOrderingViolationException}.
 */
public class EcommerceIntegrationEventMapper implements OutboxEventMapper {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public List<OutboxEntry> toOutbox(EventEnvelope envelope) {
    String eventId = envelope.metadata().eventId().value();
    StreamId stream = envelope.streamId();
    return switch (envelope.event()) {
      // --- Order lifecycle (OrderPlaced excluded — carries customerId) ---
      case OrderEvent.OrderConfirmed e ->
          List.of(entry(eventId, new OrderConfirmedDto(e.orderId()), "OrderConfirmed", stream));
      case OrderEvent.OrderShipped e ->
          List.of(entry(eventId, new OrderShippedDto(e.orderId()), "OrderShipped", stream));
      case OrderEvent.OrderDelivered e ->
          List.of(entry(eventId, new OrderDeliveredDto(e.orderId()), "OrderDelivered", stream));
      case OrderEvent.OrderCancelled e ->
          List.of(
              entry(
                  eventId,
                  new OrderCancelledDto(e.orderId(), e.reason()),
                  "OrderCancelled",
                  stream));

      // --- Payment outcomes (PaymentInitiated excluded) ---
      case PaymentEvent.PaymentCaptured e ->
          List.of(
              entry(
                  eventId,
                  new PaymentCapturedDto(e.paymentId(), e.orderId()),
                  "PaymentCaptured",
                  stream));
      case PaymentEvent.PaymentRefunded e ->
          List.of(
              entry(
                  eventId,
                  new PaymentRefundedDto(e.paymentId(), e.reason()),
                  "PaymentRefunded",
                  stream));
      case PaymentEvent.PaymentFailed e ->
          List.of(
              entry(
                  eventId,
                  new PaymentFailedDto(e.paymentId(), e.reason()),
                  "PaymentFailed",
                  stream));

      // --- Product catalog ---
      case ProductEvent.ProductCreated e ->
          List.of(
              entry(
                  eventId,
                  new ProductCreatedDto(
                      e.productId(),
                      e.name(),
                      e.category(),
                      e.price().amount(),
                      e.price().currency(),
                      e.stock()),
                  "ProductCreated",
                  stream));
      case ProductEvent.PriceUpdated e ->
          List.of(
              entry(
                  eventId,
                  new PriceUpdatedDto(
                      e.productId(),
                      e.previousPrice().amount(),
                      e.previousPrice().currency(),
                      e.newPrice().amount(),
                      e.newPrice().currency()),
                  "PriceUpdated",
                  stream));
      case ProductEvent.StockAdjusted e ->
          List.of(
              entry(
                  eventId,
                  new StockAdjustedDto(e.productId(), e.previousStock(), e.newStock(), e.reason()),
                  "StockAdjusted",
                  stream));
      case ProductEvent.ProductDiscontinued e ->
          List.of(
              entry(
                  eventId,
                  new ProductDiscontinuedDto(e.productId()),
                  "ProductDiscontinued",
                  stream));

      // --- Everything else: CustomerEvent.*, InventoryEvent.*, OrderPlaced,
      //                       PaymentInitiated — excluded to prevent PII leakage ---
      default -> List.of();
    };
  }

  private OutboxEntry entry(String eventId, Object dto, String payloadType, StreamId stream) {
    try {
      return OutboxEntry.pending(
          OutboxEntryId.of(eventId), OBJECT_MAPPER.writeValueAsString(dto), payloadType, stream);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize integration event DTO: " + payloadType, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Integration DTOs — contain only non-PII business fields
  // ---------------------------------------------------------------------------

  record OrderConfirmedDto(String orderId) {}

  record OrderShippedDto(String orderId) {}

  record OrderDeliveredDto(String orderId) {}

  record OrderCancelledDto(String orderId, String reason) {}

  record PaymentCapturedDto(String paymentId, String orderId) {}

  record PaymentRefundedDto(String paymentId, String reason) {}

  record PaymentFailedDto(String paymentId, String reason) {}

  record ProductCreatedDto(
      String productId,
      String name,
      String category,
      BigDecimal price,
      String currency,
      int stock) {}

  record PriceUpdatedDto(
      String productId,
      BigDecimal previousPrice,
      String previousCurrency,
      BigDecimal newPrice,
      String newCurrency) {}

  record StockAdjustedDto(String productId, int previousStock, int newStock, String reason) {}

  record ProductDiscontinuedDto(String productId) {}
}
