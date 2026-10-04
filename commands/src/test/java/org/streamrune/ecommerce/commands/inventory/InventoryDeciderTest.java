package org.streamrune.ecommerce.commands.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;
import org.streamrune.test.DeciderFixture;

class InventoryDeciderTest {

  private final DeciderFixture<InventoryCommand, InventoryState, InventoryEvent> fixture =
      DeciderFixture.of(new InventoryDecider());

  private InventoryEvent.ShipmentReceived received() {
    return new InventoryEvent.ShipmentReceived("p-1", 100, 100);
  }

  @Test
  void receiveShipment() {
    fixture
        .given()
        .when(new InventoryCommand.ReceiveShipment("p-1", 100))
        .expectEvents(received())
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(100);
              assertThat(s.reserved()).isEqualTo(0);
            });
  }

  @Test
  void reserveStock() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 10))
        .expectEvents(new InventoryEvent.StockReserved("p-1", "o-1", 10, 90))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(90);
              assertThat(s.reserved()).isEqualTo(10);
            });
  }

  @Test
  void reserveStock_insufficient_throws() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 200))
        .expectException(DomainException.class);
  }

  @Test
  void releaseStock() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 10, 90))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1", 10))
        .expectEvents(new InventoryEvent.StockReleased("p-1", "o-1", 10, 100))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(100);
              assertThat(s.reserved()).isEqualTo(0);
            });
  }

  @Test
  void confirmReservation() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 10, 90))
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectEvents(new InventoryEvent.ReservationConfirmed("p-1", "o-1", 10))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(0);
              assertThat(s.committed()).isEqualTo(10);
            });
  }
}
