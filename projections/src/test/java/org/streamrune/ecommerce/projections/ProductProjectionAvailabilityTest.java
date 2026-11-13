package org.streamrune.ecommerce.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductStatus;
import org.streamrune.ecommerce.queries.dto.Availability;
import org.streamrune.ecommerce.queries.dto.ProductView;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The storefront's product view: {@link ProductProjection} takes the catalogue's fields from the
 * product events and the available units from the inventory events of the same product id, and
 * derives the availability from both.
 */
class ProductProjectionAvailabilityTest {

  private static final Money PRICE = new Money(new BigDecimal("9.99"), "USD");
  private static final int LOW = ProductProjection.LOW_STOCK_THRESHOLD;

  private final ProductProjection projection =
      new ProductProjection(new InMemoryProjectionRepository());
  private final ProjectionEvents events = new ProjectionEvents();

  @Test
  void aNewProductHoldsNothingAndIsOutOfStock() {
    process(created("p-1"));

    ProductView view = projection.get("p-1");
    assertThat(view.status()).isEqualTo(ProductStatus.AVAILABLE);
    assertThat(view.available()).isZero();
    assertThat(view.availability()).isEqualTo(Availability.OUT_OF_STOCK);
  }

  @Test
  void theAvailableUnitsFollowTheInventory() {
    process(created("p-1"), new InventoryEvent.ShipmentReceived("p-1", LOW + 5, LOW + 5));
    assertThat(projection.get("p-1").available()).isEqualTo(LOW + 5);
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.IN_STOCK);

    process(new InventoryEvent.StockReserved("p-1", "o-1", 6, LOW - 1));
    assertThat(projection.get("p-1").available()).isEqualTo(LOW - 1);
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.LOW_STOCK);

    process(new InventoryEvent.ReservationConfirmed("p-1", "o-1", 6));
    assertThat(projection.get("p-1").available())
        .as("a confirmation moves reserved units to committed, the available units stay")
        .isEqualTo(LOW - 1);

    process(new InventoryEvent.InventoryAdjusted("p-1", -(LOW - 1), "Stocktake", 0));
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.OUT_OF_STOCK);

    process(
        new InventoryEvent.ShipmentReceived("p-1", 3, 3),
        new InventoryEvent.StockReserved("p-1", "o-2", 3, 0),
        new InventoryEvent.StockReleased("p-1", "o-2", 3, 3));
    assertThat(projection.get("p-1").available()).isEqualTo(3);
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.LOW_STOCK);
  }

  @Test
  void theLowStockThresholdIsTheFirstUnitCountThatIsInStock() {
    process(created("p-1"), new InventoryEvent.ShipmentReceived("p-1", LOW, LOW));
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.IN_STOCK);

    process(new InventoryEvent.InventoryAdjusted("p-1", -1, "Damaged", LOW - 1));
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.LOW_STOCK);
  }

  @Test
  void aDiscontinuedProductIsDiscontinuedWhateverItHolds() {
    process(
        created("p-1"),
        new InventoryEvent.ShipmentReceived("p-1", 50, 50),
        new ProductEvent.ProductDiscontinued("p-1"));
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.DISCONTINUED);
    assertThat(projection.get("p-1").status()).isEqualTo(ProductStatus.DISCONTINUED);

    process(new InventoryEvent.InventoryAdjusted("p-1", -50, "Written off", 0));
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.DISCONTINUED);
    assertThat(projection.get("p-1").available()).isZero();
  }

  @Test
  void aPriceChangeKeepsTheAvailableUnits() {
    Money newPrice = new Money(new BigDecimal("12.00"), "USD");
    process(
        created("p-1"),
        new InventoryEvent.ShipmentReceived("p-1", 20, 20),
        new ProductEvent.PriceUpdated("p-1", PRICE, newPrice));

    assertThat(projection.get("p-1").price()).isEqualTo(newPrice);
    assertThat(projection.get("p-1").available()).isEqualTo(20);
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.IN_STOCK);
  }

  @Test
  void anotherProductsInventoryLeavesThisOneAsItIs() {
    process(
        created("p-1"),
        created("p-2"),
        new InventoryEvent.ShipmentReceived("p-1", 20, 20),
        new InventoryEvent.ShipmentReceived("p-2", 3, 3));

    assertThat(projection.get("p-1").available()).isEqualTo(20);
    assertThat(projection.get("p-2").available()).isEqualTo(3);
  }

  @Test
  void aRefusedReservationChangesNothing() {
    process(
        created("p-1"),
        new InventoryEvent.ShipmentReceived("p-1", 2, 2),
        new InventoryEvent.StockReservationRefused("p-1", "o-1", 3, 2));

    assertThat(projection.get("p-1").available()).isEqualTo(2);
    assertThat(projection.get("p-1").availability()).isEqualTo(Availability.LOW_STOCK);
  }

  @Test
  void anInventoryEventOfAProductNeverCreatedChangesNothing() {
    process(new InventoryEvent.ShipmentReceived("p-none", 5, 5));

    assertThat(projection.get("p-none")).isNull();
  }

  private static ProductEvent.ProductCreated created(String productId) {
    return new ProductEvent.ProductCreated(productId, "Widget", "A widget", "Gadgets", PRICE);
  }

  private void process(DomainEvent... list) {
    projection.process(events.of(list));
  }
}
