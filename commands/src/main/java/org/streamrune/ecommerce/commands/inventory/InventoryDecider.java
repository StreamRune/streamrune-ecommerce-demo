package org.streamrune.ecommerce.commands.inventory;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;

public class InventoryDecider implements Decider<InventoryCommand, InventoryState, InventoryEvent> {

  @Override
  public InventoryState initialState() {
    return new InventoryState();
  }

  @Override
  public List<InventoryEvent> decide(InventoryCommand cmd, InventoryState state) {
    return switch (cmd) {
      case InventoryCommand.ReserveStock c -> {
        if (state.available() < c.quantity())
          throw new DomainException(
              "Insufficient stock for "
                  + c.productId()
                  + ": available="
                  + state.available()
                  + ", requested="
                  + c.quantity());
        int newAvailable = state.available() - c.quantity();
        yield List.of(
            new InventoryEvent.StockReserved(
                c.productId(), c.orderId(), c.quantity(), newAvailable));
      }

      case InventoryCommand.ReleaseStock c -> {
        int newAvailable = state.available() + c.quantity();
        yield List.of(
            new InventoryEvent.StockReleased(
                c.productId(), c.orderId(), c.quantity(), newAvailable));
      }

      case InventoryCommand.ConfirmReservation c ->
          List.of(
              new InventoryEvent.ReservationConfirmed(
                  c.productId(), c.orderId(), state.reserved()));

      case InventoryCommand.ReceiveShipment c -> {
        int newAvailable = state.available() + c.quantity();
        yield List.of(
            new InventoryEvent.ShipmentReceived(c.productId(), c.quantity(), newAvailable));
      }
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
              state.committed());
      case InventoryEvent.StockReleased e ->
          new InventoryState(
              e.productId(),
              e.availableAfter(),
              state.reserved() - e.quantity(),
              state.committed());
      case InventoryEvent.ReservationConfirmed e ->
          new InventoryState(
              state.productId(),
              state.available(),
              state.reserved() - e.quantity(),
              state.committed() + e.quantity());
      case InventoryEvent.ShipmentReceived e ->
          new InventoryState(
              e.productId(), e.availableAfter(), state.reserved(), state.committed());
    };
  }
}
