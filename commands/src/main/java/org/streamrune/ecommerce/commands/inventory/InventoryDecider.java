package org.streamrune.ecommerce.commands.inventory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
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
   */
  @Override
  public List<InventoryEvent> decide(InventoryCommand cmd, InventoryState state) {
    return switch (cmd) {
      case InventoryCommand.ReserveStock c -> {
        if (c.quantity() <= 0)
          throw new DomainException("Reservation quantity must be positive, got " + c.quantity());
        if (state.reservationOf(c.orderId()).isPresent()) yield List.of();
        if (state.available() < c.quantity())
          throw new DomainException(
              "Insufficient stock for "
                  + c.productId()
                  + ": available="
                  + state.available()
                  + ", requested="
                  + c.quantity());
        yield List.of(
            new InventoryEvent.StockReserved(
                c.productId(), c.orderId(), c.quantity(), state.available() - c.quantity()));
      }

      case InventoryCommand.ReleaseStock c -> {
        OptionalInt held = state.reservationOf(c.orderId());
        if (held.isEmpty()) yield List.of();
        yield List.of(
            new InventoryEvent.StockReleased(
                c.productId(), c.orderId(), held.getAsInt(), state.available() + held.getAsInt()));
      }

      case InventoryCommand.ConfirmReservation c -> {
        OptionalInt held = state.reservationOf(c.orderId());
        if (held.isEmpty()) yield List.of();
        yield List.of(
            new InventoryEvent.ReservationConfirmed(c.productId(), c.orderId(), held.getAsInt()));
      }

      case InventoryCommand.ReceiveShipment c ->
          List.of(
              new InventoryEvent.ShipmentReceived(
                  c.productId(), c.quantity(), state.available() + c.quantity()));
    };
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
              with(state.reservations(), e.orderId(), e.quantity()));
      case InventoryEvent.StockReleased e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() - e.quantity(),
              state.committed(),
              without(state.reservations(), e.orderId()));
      case InventoryEvent.ReservationConfirmed e ->
          new InventoryState(
              state.productId(),
              state.available(),
              state.reserved() - e.quantity(),
              state.committed() + e.quantity(),
              without(state.reservations(), e.orderId()));
      case InventoryEvent.ShipmentReceived e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved(),
              state.committed(),
              state.reservations());
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
}
