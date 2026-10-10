package org.streamrune.ecommerce.micronaut;

import io.micronaut.runtime.Micronaut;
import io.micronaut.serde.annotation.SerdeImport;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;

/**
 * Micronaut entry point. The read-model DTOs and the domain events live in the framework-agnostic
 * modules and cannot carry Micronaut annotations, so micronaut-serde learns them via {@link
 * SerdeImport}.
 *
 * <p>The DTOs are the bodies of the query endpoints. The domain events are the {@code data} of the
 * frames {@code GET /api/sse/{aggregateType}/{aggregateId}} writes: the framework's SSE controller
 * hands each stored event to Micronaut Serialization, which refuses a type it has no serializer for
 * and ends the stream. Every event the event type registry knows is therefore imported here, with
 * the records nested in one ({@code OrderLine}); {@code DomainEventSerializationTest} fails for an
 * event that is registered but not imported.
 */
@SerdeImport(org.streamrune.ecommerce.domain.common.Money.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.OrderView.OrderLineView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.ProductView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.OrderView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.CustomerView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.InventoryView.class)
@SerdeImport(ProductEvent.ProductCreated.class)
@SerdeImport(ProductEvent.PriceUpdated.class)
@SerdeImport(ProductEvent.StockAdjusted.class)
@SerdeImport(ProductEvent.ProductDiscontinued.class)
@SerdeImport(OrderEvent.OrderPlaced.class)
@SerdeImport(OrderEvent.OrderLine.class)
@SerdeImport(OrderEvent.OrderConfirmed.class)
@SerdeImport(OrderEvent.OrderShipped.class)
@SerdeImport(OrderEvent.OrderDelivered.class)
@SerdeImport(OrderEvent.OrderCancelled.class)
@SerdeImport(CustomerEvent.CustomerRegistered.class)
@SerdeImport(CustomerEvent.ProfileUpdated.class)
@SerdeImport(CustomerEvent.DataExportRequested.class)
@SerdeImport(CustomerEvent.CustomerForgotten.class)
@SerdeImport(PaymentEvent.PaymentInitiated.class)
@SerdeImport(PaymentEvent.PaymentCaptured.class)
@SerdeImport(PaymentEvent.PaymentRefunded.class)
@SerdeImport(PaymentEvent.PaymentFailed.class)
@SerdeImport(InventoryEvent.StockReserved.class)
@SerdeImport(InventoryEvent.StockReleased.class)
@SerdeImport(InventoryEvent.ReservationConfirmed.class)
@SerdeImport(InventoryEvent.ShipmentReceived.class)
public class MicronautEcommerceApplication {
  public static void main(String[] args) {
    Micronaut.run(MicronautEcommerceApplication.class, args);
  }
}
