package org.streamrune.ecommerce.commands.inventory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;

public class InventoryDecider implements Decider<InventoryCommand, InventoryState, InventoryEvent> {

  @Override
  public InventoryState initialState() {
    return new InventoryState();
  }

  /**
   * Stock is held per order: a second reservation of one order at one product stores nothing, and a
   * release or a confirmation acts on this order's reservation alone — nothing when it holds none.
   * An order that asks for more than the product holds available is refused with an event, {@code
   * StockReservationRefused}, not an exception: the refusal is a fact of the order's fulfillment,
   * and the saga cancels the order with its reason. A second request of a refused order stores
   * nothing.
   */
  @Override
  public List<InventoryEvent> decide(InventoryCommand cmd, InventoryState state) {
    return switch (cmd) {
      case InventoryCommand.ReserveStock c -> {
        if (c.quantity() <= 0) {
          throw new DomainException("Reservation quantity must be positive, got " + c.quantity());
        }
        if (state.reservationOf(c.orderId()).isPresent() || state.refused(c.orderId())) {
          yield List.of();
        }
        if (state.available() < c.quantity()) {
          yield List.of(
              new InventoryEvent.StockReservationRefused(
                  c.productId(), c.orderId(), c.quantity(), state.available()));
        }
        yield List.of(
            new InventoryEvent.StockReserved(
                c.productId(), c.orderId(), c.quantity(), state.available() - c.quantity()));
      }

      case InventoryCommand.ReleaseStock c -> {
        OptionalInt held = state.reservationOf(c.orderId());
        if (held.isEmpty()) {
          yield List.of();
        }
        yield List.of(
            new InventoryEvent.StockReleased(
                c.productId(), c.orderId(), held.getAsInt(), state.available() + held.getAsInt()));
      }

      case InventoryCommand.ConfirmReservation c -> {
        OptionalInt held = state.reservationOf(c.orderId());
        if (held.isEmpty()) {
          yield List.of();
        }
        yield List.of(
            new InventoryEvent.ReservationConfirmed(c.productId(), c.orderId(), held.getAsInt()));
      }

      case InventoryCommand.ReceiveShipment c ->
          List.of(
              new InventoryEvent.ShipmentReceived(
                  c.productId(), c.quantity(), availableAfter(state, c.productId(), c.quantity())));

      // A correction of the units on hand, never of what orders hold: reserved and committed stay.
      case InventoryCommand.AdjustInventory c -> {
        if (c.quantityChange() == 0) {
          throw new DomainException("An inventory adjustment must change the quantity");
        }
        if (c.reason() == null || c.reason().isBlank()) {
          throw new DomainException("An inventory adjustment needs a reason");
        }
        int availableAfter = availableAfter(state, c.productId(), c.quantityChange());
        if (availableAfter < 0) {
          throw new DomainException(
              "Insufficient stock for "
                  + c.productId()
                  + ": available="
                  + state.available()
                  + ", change="
                  + c.quantityChange());
        }
        yield List.of(
            new InventoryEvent.InventoryAdjusted(
                c.productId(), c.quantityChange(), c.reason(), availableAfter));
      }
    };
  }

  /** The available units after a change, refused when they would not fit in an {@code int}. */
  private static int availableAfter(InventoryState state, String productId, int change) {
    try {
      return Math.addExact(state.available(), change);
    } catch (ArithmeticException e) {
      throw new DomainException(
          "Stock for " + productId + " cannot exceed " + Integer.MAX_VALUE + " units");
    }
  }

  @Override
  public InventoryState evolve(InventoryState state, InventoryEvent evt) {
    return switch (evt) {
      case InventoryEvent.StockReserved e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() + e.quantity(),
              state.committed(),
              with(state.reservations(), e.orderId(), e.quantity()),
              state.refusedOrders());
      case InventoryEvent.StockReservationRefused e ->
          new InventoryState(
              e.productId(),
              state.available(),
              state.reserved(),
              state.committed(),
              state.reservations(),
              plus(state.refusedOrders(), e.orderId()));
      case InventoryEvent.StockReleased e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() - e.quantity(),
              state.committed(),
              without(state.reservations(), e.orderId()),
              state.refusedOrders());
      case InventoryEvent.ReservationConfirmed e ->
          new InventoryState(
              state.productId(),
              state.available(),
              state.reserved() - e.quantity(),
              state.committed() + e.quantity(),
              without(state.reservations(), e.orderId()),
              state.refusedOrders());
      case InventoryEvent.ShipmentReceived e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved(),
              state.committed(),
              state.reservations(),
              state.refusedOrders());
      case InventoryEvent.InventoryAdjusted e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved(),
              state.committed(),
              state.reservations(),
              state.refusedOrders());
    };
  }

  private static Map<String, Integer> with(
      Map<String, Integer> reservations, String orderId, int quantity) {
    var next = new HashMap<>(reservations);
    next.put(orderId, quantity);
    return next;
  }

  private static Map<String, Integer> without(Map<String, Integer> reservations, String orderId) {
    var next = new HashMap<>(reservations);
    next.remove(orderId);
    return next;
  }

  private static Set<String> plus(Set<String> orders, String orderId) {
    var next = new HashSet<>(orders);
    next.add(orderId);
    return next;
  }
}
