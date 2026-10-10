package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * The seed is applied once per database, however often the application starts on it. The runner
 * already ran when this test's application context started; running it again must add nothing.
 */
class SeedDataRunnerIT extends AbstractIntegrationTest {

  @Autowired SeedDataRunner seedDataRunner;
  @Autowired EventStore eventStore;

  /** How many events of one type a stream holds. */
  private long count(AggregateType type, String id, String eventType) {
    return eventStore.load(StreamId.of(type, AggregateId.of(id))).events().stream()
        .filter(e -> e.eventType().name().equals(eventType))
        .count();
  }

  /** The event each seed command appends, counted per seeded aggregate. */
  private Map<String, Long> seedEvents() {
    Map<String, Long> counts = new LinkedHashMap<>();
    for (String product : new String[] {"prod-widget", "prod-gadget", "prod-doohickey"}) {
      counts.put("product:" + product, count(ProductState.TYPE, product, "ProductCreated"));
      counts.put("inventory:" + product, count(InventoryState.TYPE, product, "ShipmentReceived"));
    }
    for (String customer : new String[] {"cust-alice", "cust-bob"}) {
      counts.put("customer:" + customer, count(CustomerState.TYPE, customer, "CustomerRegistered"));
    }
    counts.put("order:order-seed-1", count(OrderState.TYPE, "order-seed-1", "OrderPlaced"));
    return counts;
  }

  @Test
  void startingAgainOnASeededDatabaseAddsNothing() {
    seedDataRunner.run(null);
    seedDataRunner.run(null);

    assertThat(seedEvents()).hasSize(9).allSatisfy((stream, n) -> assertThat(n).isEqualTo(1L));
  }
}
