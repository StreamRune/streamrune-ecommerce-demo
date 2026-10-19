package org.streamrune.ecommerce.quarkus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.inventory.InventoryDecider;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.payment.PaymentIds;
import org.streamrune.ecommerce.domain.payment.PaymentState;

/**
 * Reads what an order's undo left in the event store: the payment's events, the stock events of the
 * order at a product, the folded inventory of a product, the fulfillment saga's row and a
 * subscription's checkpoint. Read from the store and the saga table, never from a read model.
 */
final class OrderUndoProbe {

  private static final SagaType FULFILLMENT = SagaType.fromClass(OrderFulfillmentState.class);

  private final EventStore eventStore;
  private final SagaStore sagaStore;
  private final DataSource dataSource;

  OrderUndoProbe(EventStore eventStore, SagaStore sagaStore, DataSource dataSource) {
    this.eventStore = eventStore;
    this.sagaStore = sagaStore;
    this.dataSource = dataSource;
  }

  List<EventEnvelope> events(AggregateType type, String id) {
    return eventStore.readStream(
        StreamId.of(type, new AggregateId(id)), Version.initial(), Integer.MAX_VALUE);
  }

  List<String> eventTypes(AggregateType type, String id) {
    return events(type, id).stream().map(e -> e.eventType().name()).toList();
  }

  /** The event types of the order's payment, oldest first. */
  List<String> paymentEvents(String orderId) {
    return eventTypes(PaymentState.TYPE, PaymentIds.forOrder(orderId));
  }

  /** The stock events of the order at the product, oldest first, as type and quantity. */
  List<String> stockEventsOf(String productId, String orderId) {
    return events(InventoryState.TYPE, productId).stream()
        .map(EventEnvelope::event)
        .<String>mapMulti(
            (event, out) -> {
              switch (event) {
                case InventoryEvent.StockReserved e when e.orderId().equals(orderId) ->
                    out.accept("StockReserved:" + e.quantity());
                case InventoryEvent.StockReleased e when e.orderId().equals(orderId) ->
                    out.accept("StockReleased:" + e.quantity());
                case InventoryEvent.ReservationConfirmed e when e.orderId().equals(orderId) ->
                    out.accept("ReservationConfirmed:" + e.quantity());
                default -> {}
              }
            })
        .toList();
  }

  /** The inventory of the product, folded from its stream. */
  InventoryState inventory(String productId) {
    InventoryDecider decider = new InventoryDecider();
    InventoryState state = decider.initialState();
    for (EventEnvelope envelope : events(InventoryState.TYPE, productId)) {
      state = decider.evolve(state, (InventoryEvent) envelope.event());
    }
    return state;
  }

  /** The fulfillment saga of the order, if it has a row. */
  Optional<LoadedSaga<OrderFulfillmentState>> saga(String orderId) {
    return sagaStore.load(
        new SagaId("fulfillment-" + orderId), FULFILLMENT, OrderFulfillmentState.class);
  }

  /** The status of the order's fulfillment saga, or {@code null} while it has no row. */
  SagaStatus sagaStatus(String orderId) {
    return saga(orderId).map(LoadedSaga::status).orElse(null);
  }

  /** The last global offset the subscription has checkpointed, or -1 before its first. */
  long checkpoint(String subscriptionName) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT last_offset FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, subscriptionName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : -1;
      }
    }
  }
}
