package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.quarkus.PostgresTestResource;

/**
 * The seed is applied once per database, however often the application starts on it. Seeding at
 * startup is switched off in the test profile, so the test runs the seed itself: the first run
 * applies it, a second run finds every aggregate there and adds nothing.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class SeedDataRunnerIT {

  @Inject SeedDataRunner seedDataRunner;
  @Inject EventStore eventStore;

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
  void seedingAgainOnASeededDatabaseAddsNothing() {
    seedDataRunner.seed();

    assertThat(seedDataRunner.seed()).isZero();
    assertThat(seedEvents()).hasSize(9).allSatisfy((stream, n) -> assertThat(n).isEqualTo(1L));
  }
}
