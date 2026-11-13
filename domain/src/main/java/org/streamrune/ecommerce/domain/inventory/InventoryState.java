package org.streamrune.ecommerce.domain.inventory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record InventoryState(
    String productId,
    int available,
    int reserved,
    int committed,
    Map<String, Integer> reservations,
    Set<String> refusedOrders)
    implements AggregateState {
  /**
   * The aggregate type the inventory decider is registered under: streams are
   * inventory:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("inventory");

  /**
   * {@code reservations} holds the open reservation of each order at this product, by order id: an
   * entry is added by {@code StockReserved} and removed by {@code StockReleased} or {@code
   * ReservationConfirmed}. {@code refusedOrders} holds the orders whose reservation was refused
   * ({@code StockReservationRefused}), so a second request of such an order stores nothing. The
   * state is snapshotted with both.
   */
  @JsonCreator
  public InventoryState(
      @JsonProperty("productId") String productId,
      @JsonProperty("available") int available,
      @JsonProperty("reserved") int reserved,
      @JsonProperty("committed") int committed,
      @JsonProperty("reservations") Map<String, Integer> reservations,
      @JsonProperty("refusedOrders") Set<String> refusedOrders) {
    this.productId = productId;
    this.available = available;
    this.reserved = reserved;
    this.committed = committed;
    this.reservations = reservations == null ? Map.of() : Map.copyOf(reservations);
    this.refusedOrders = refusedOrders == null ? Set.of() : Set.copyOf(refusedOrders);
  }

  public InventoryState() {
    this(null, 0, 0, 0, Map.of(), Set.of());
  }

  /** Whether a reservation of the order at this product was refused. */
  public boolean refused(String orderId) {
    return refusedOrders.contains(orderId);
  }

  /** The quantity the order holds reserved at this product, or none. */
  public OptionalInt reservationOf(String orderId) {
    Integer quantity = reservations.get(orderId);
    return quantity == null ? OptionalInt.empty() : OptionalInt.of(quantity);
  }
}
