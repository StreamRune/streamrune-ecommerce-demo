package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The command bus snapshots an aggregate every few events. An inventory snapshot that lost the open
 * reservations would make every release and confirmation after it store nothing and strand the
 * stock without an error. The product here holds seven events, so the state a command loads is the
 * snapshot and the events after it.
 *
 * <p>The test relies on the command bus's snapshot policy, {@code SnapshotPolicy.everyNEvents(5)}
 * in {@code StreamRuneConfig}: a shipment and four reservations are five events, so the snapshot
 * holds the reservations of orders 1 to 4. A policy with another interval fails the assertion that
 * the snapshot exists, or the one on its contents, and the test needs the counts below adjusted.
 */
class InventoryReservationsSnapshotIT extends AbstractIntegrationTest {

  /** The interval of the bus's snapshot policy, {@code everyNEvents(5)}. */
  private static final int SNAPSHOT_EVERY_N_EVENTS = 5;

  @Autowired VirtualThreadCommandBus commandBus;
  @Autowired EventStore eventStore;

  @Test
  void aReleaseAfterTheSnapshotReleasesTheOrdersOwnQuantity() {
    String productId = "snap-prod-" + System.nanoTime();
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 100));
    // The shipment is the first event; the fifth is the last reservation the snapshot holds.
    for (int i = 1; i <= SNAPSHOT_EVERY_N_EVENTS + 1; i++) {
      commandBus.execute(new InventoryCommand.ReserveStock(productId, "o-" + i, i));
    }

    // The snapshot exists and holds the reservations open at the fifth event.
    var snapshot = eventStore.load(StreamId.of(InventoryState.TYPE, AggregateId.of(productId)), 1);
    assertThat(snapshot.snapshotState())
        .as(
            "a snapshot after %d events, the interval of the bus's snapshot policy",
            SNAPSHOT_EVERY_N_EVENTS)
        .isInstanceOfSatisfying(
            InventoryState.class,
            state -> {
              assertThat(state.reservationOf("o-1")).hasValue(1);
              assertThat(state.reservationOf("o-4")).hasValue(4);
            });

    var released = commandBus.execute(new InventoryCommand.ReleaseStock(productId, "o-1"));
    assertThat(released.events())
        .singleElement()
        .isInstanceOfSatisfying(
            InventoryEvent.StockReleased.class, e -> assertThat(e.quantity()).isEqualTo(1));

    var confirmed = commandBus.execute(new InventoryCommand.ConfirmReservation(productId, "o-6"));
    assertThat(confirmed.events())
        .singleElement()
        .isInstanceOfSatisfying(
            InventoryEvent.ReservationConfirmed.class, e -> assertThat(e.quantity()).isEqualTo(6));

    var again = commandBus.execute(new InventoryCommand.ReleaseStock(productId, "o-1"));
    assertThat(again.events()).isEmpty();
  }
}
