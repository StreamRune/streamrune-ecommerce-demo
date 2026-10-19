package org.streamrune.ecommerce.domain.inventory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.OptionalInt;
import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record InventoryState(
    String productId, int available, int reserved, int committed, Map<String, Integer> reservations)
    implements AggregateState {
  /**
   * The aggregate type the inventory decider is registered under: streams are
   * inventory:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("inventory");

  /**
   * {@code reservations} holds the open reservation of each order at this product, by order id: an
   * entry is added by {@code StockReserved} and removed by {@code StockReleased} or {@code
   * ReservationConfirmed}. The state is snapshotted with the map.
   */
  @JsonCreator
  public InventoryState(
      @JsonProperty("productId") String productId,
      @JsonProperty("available") int available,
      @JsonProperty("reserved") int reserved,
      @JsonProperty("committed") int committed,
      @JsonProperty("reservations") Map<String, Integer> reservations) {
    this.productId = productId;
    this.available = available;
    this.reserved = reserved;
    this.committed = committed;
    this.reservations = reservations == null ? Map.of() : Map.copyOf(reservations);
  }

  public InventoryState() {
    this(null, 0, 0, 0, Map.of());
  }

  /** The quantity the order holds reserved at this product, or none. */
  public OptionalInt reservationOf(String orderId) {
    Integer quantity = reservations.get(orderId);
    return quantity == null ? OptionalInt.empty() : OptionalInt.of(quantity);
  }
}
