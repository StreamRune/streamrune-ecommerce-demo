package org.streamrune.ecommerce.commands.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.inventory.*;
import org.streamrune.test.DeciderFixture;

class InventoryDeciderTest {

  private final DeciderFixture<InventoryCommand, InventoryState, InventoryEvent> fixture =
      DeciderFixture.of(new InventoryDecider());

  private InventoryEvent.ShipmentReceived received() {
    return new InventoryEvent.ShipmentReceived("p-1", 10, 10);
  }

  @Test
  void receiveShipment() {
    fixture
        .given()
        .when(new InventoryCommand.ReceiveShipment("p-1", 100))
        .expectEvents(new InventoryEvent.ShipmentReceived("p-1", 100, 100))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(100);
              assertThat(s.reserved()).isEqualTo(0);
            });
  }

  @Test
  void reserveStock_recordsTheOrdersReservation() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 3))
        .expectEvents(new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(7);
              assertThat(s.reserved()).isEqualTo(3);
              assertThat(s.reservationOf("o-1")).isEqualTo(OptionalInt.of(3));
            });
  }

  @Test
  void reserveStock_twiceForOneOrder_storesNothingTheSecondTime() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 3))
        .expectNoEvents();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void reserveStock_aNonPositiveQuantity_isRefused(int quantity) {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", quantity))
        .expectException(DomainException.class);
  }

  @Test
  void reserveStock_insufficient_throws() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReserveStock("p-1", "o-1", 200))
        .expectException(DomainException.class);
  }

  @Test
  void releaseStock_releasesWhatTheOrderHolds() {
    fixture
        .given(received(), new InventoryEvent.StockReserved("p-1", "o-1", 3, 7))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectEvents(new InventoryEvent.StockReleased("p-1", "o-1", 3, 10))
        .expectState(
            s -> {
              assertThat(s.available()).isEqualTo(10);
              assertThat(s.reserved()).isEqualTo(0);
              assertThat(s.reservations()).isEmpty();
            });
  }

  @Test
  void releaseStock_withoutAReservation_storesNothing() {
    fixture
        .given(received())
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void releaseStock_afterTheReservationWasConfirmed_storesNothing() {
    fixture
        .given(
            received(),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 7),
            new InventoryEvent.ReservationConfirmed("p-1", "o-1", 3))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void releaseStock_twice_releasesOnce() {
    fixture
        .given(
            received(),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 7),
            new InventoryEvent.StockReleased("p-1", "o-1", 3, 10))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void confirmReservation_commitsTheOrdersQuantity() {
    fixture
        .given(
            new InventoryEvent.ShipmentReceived("p-1", 20, 20),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 17),
            new InventoryEvent.StockReserved("p-1", "o-2", 5, 12))
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectEvents(new InventoryEvent.ReservationConfirmed("p-1", "o-1", 3))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(5);
              assertThat(s.committed()).isEqualTo(3);
            });
  }

  @Test
  void confirmReservation_withoutAReservation_storesNothing() {
    fixture
        .given(received())
        .when(new InventoryCommand.ConfirmReservation("p-1", "o-1"))
        .expectNoEvents();
  }

  @Test
  void twoOrdersAtOneProduct_areReleasedAndConfirmedApart() {
    fixture
        .given(
            new InventoryEvent.ShipmentReceived("p-1", 20, 20),
            new InventoryEvent.StockReserved("p-1", "o-1", 3, 17),
            new InventoryEvent.StockReserved("p-1", "o-2", 5, 12))
        .when(new InventoryCommand.ReleaseStock("p-1", "o-2"))
        .expectEvents(new InventoryEvent.StockReleased("p-1", "o-2", 5, 17))
        .expectState(
            s -> {
              assertThat(s.reserved()).isEqualTo(3);
              assertThat(s.reservationOf("o-1")).isEqualTo(OptionalInt.of(3));
              assertThat(s.reservationOf("o-2")).isEqualTo(OptionalInt.empty());
            });
  }
}
