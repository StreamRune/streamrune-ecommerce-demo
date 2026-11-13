package org.streamrune.ecommerce.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The operators' inventory view: a row per product of the catalogue, opened empty when the product
 * is created, whose counts follow the inventory events.
 */
class InventoryProjectionTest {

  private final InventoryProjection projection =
      new InventoryProjection(new InMemoryProjectionRepository());
  private final ProjectionEvents events = new ProjectionEvents();

  @Test
  void aCreatedProductIsListedWithNothingHeld() {
    process(created("p-1", "Widget"));

    assertThat(projection.get("p-1")).isEqualTo(new InventoryView("p-1", "Widget", 0, 0, 0));
    assertThat(projection.listAll()).hasSize(1);
  }

  @Test
  void theCountsFollowTheInventoryEvents() {
    process(created("p-1", "Widget"), new InventoryEvent.ShipmentReceived("p-1", 20, 20));
    assertThat(projection.get("p-1")).isEqualTo(new InventoryView("p-1", "Widget", 20, 0, 0));

    process(new InventoryEvent.StockReserved("p-1", "o-1", 3, 17));
    assertThat(projection.get("p-1")).isEqualTo(new InventoryView("p-1", "Widget", 17, 3, 0));

    process(new InventoryEvent.ReservationConfirmed("p-1", "o-1", 3));
    assertThat(projection.get("p-1")).isEqualTo(new InventoryView("p-1", "Widget", 17, 0, 3));

    process(
        new InventoryEvent.StockReserved("p-1", "o-2", 2, 15),
        new InventoryEvent.StockReleased("p-1", "o-2", 2, 17));
    assertThat(projection.get("p-1")).isEqualTo(new InventoryView("p-1", "Widget", 17, 0, 3));

    process(new InventoryEvent.InventoryAdjusted("p-1", -7, "Stocktake", 10));
    assertThat(projection.get("p-1")).isEqualTo(new InventoryView("p-1", "Widget", 10, 0, 3));
  }

  private static ProductEvent.ProductCreated created(String productId, String name) {
    return new ProductEvent.ProductCreated(
        productId, name, null, "Gadgets", new Money(BigDecimal.TEN, "USD"));
  }

  private void process(DomainEvent... list) {
    projection.process(events.of(list));
  }
}
