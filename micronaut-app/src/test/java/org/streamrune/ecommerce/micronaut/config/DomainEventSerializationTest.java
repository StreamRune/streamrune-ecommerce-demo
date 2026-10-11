package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.micronaut.serde.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;

/**
 * Every domain event the app stores can be written by Micronaut Serialization, which is what writes
 * the {@code data} of a frame of {@code GET /api/sse/{aggregateType}/{aggregateId}}. The events
 * live in the framework-agnostic {@code domain} module, so each one needs a {@code @SerdeImport} on
 * {@code MicronautEcommerceApplication}; without it the SSE stream ends with an encoding error on
 * the first event of that type.
 */
class DomainEventSerializationTest {

  private static final Money PRICE = Money.usd(new BigDecimal("12.50"));

  /** One instance of every registered event, with every component set. */
  private static final List<DomainEvent> SAMPLES =
      List.of(
          new ProductEvent.ProductCreated("p-1", "Widget", "A widget", "tools", PRICE, 7),
          new ProductEvent.PriceUpdated("p-1", PRICE, Money.usd(new BigDecimal("13.00"))),
          new ProductEvent.StockAdjusted("p-1", 7, 9, "recount"),
          new ProductEvent.ProductDiscontinued("p-1"),
          new OrderEvent.OrderPlaced(
              "o-1", "c-1", List.of(new OrderEvent.OrderLine("p-1", 2, PRICE)), PRICE.multiply(2)),
          new OrderEvent.OrderConfirmed("o-1"),
          new OrderEvent.OrderShipped("o-1"),
          new OrderEvent.OrderDelivered("o-1"),
          new OrderEvent.OrderCancelled("o-1", "out of stock"),
          new CustomerEvent.CustomerRegistered(
              "c-1", "Ada", "ada@example.com", "1 Main St", "555-0100"),
          new CustomerEvent.ProfileUpdated(
              "c-1", "Ada L.", "ada@example.com", "2 Main St", "555-0101"),
          new CustomerEvent.DataExportRequested("c-1"),
          new CustomerEvent.CustomerForgotten("c-1"),
          new PaymentEvent.PaymentInitiated("pay-1", "o-1", PRICE),
          new PaymentEvent.PaymentCaptured("pay-1", "o-1"),
          new PaymentEvent.PaymentRefunded("pay-1", "order cancelled"),
          new PaymentEvent.PaymentFailed("pay-1", "card declined"),
          new InventoryEvent.StockReserved("p-1", "o-1", 2, 5),
          new InventoryEvent.StockReleased("p-1", "o-1", 2, 7),
          new InventoryEvent.ReservationConfirmed("p-1", "o-1", 2),
          new InventoryEvent.ShipmentReceived("p-1", 10, 17));

  @Test
  void thereIsASampleOfEveryRegisteredEvent() {
    Set<Class<?>> registeredEvents =
        new StreamRuneFactory()
            .eventTypeRegistry().registeredTypes().stream()
                .filter(DomainEvent.class::isAssignableFrom)
                .collect(Collectors.toSet());
    Set<Class<?>> sampled = SAMPLES.stream().map(Object::getClass).collect(Collectors.toSet());

    assertThat(registeredEvents).as("events in the event type registry").isNotEmpty();
    assertThat(sampled)
        .as("a registered event without a sample here is not checked for serialization")
        .isEqualTo(registeredEvents);
  }

  @Test
  void micronautSerializationWritesEveryEventAsJacksonDoes() throws IOException {
    ObjectMapper micronaut = ObjectMapper.getDefault();
    var jackson = new com.fasterxml.jackson.databind.ObjectMapper();

    for (DomainEvent event : SAMPLES) {
      JsonNode written = jackson.readTree(micronaut.writeValueAsString(event));

      // The same JSON the Spring and Quarkus apps write for the event, nested records included.
      assertThat(written)
          .as("%s written by Micronaut Serialization", event.getClass().getSimpleName())
          .isEqualTo(jackson.readTree(jackson.writeValueAsString(event)));
    }
  }
}
