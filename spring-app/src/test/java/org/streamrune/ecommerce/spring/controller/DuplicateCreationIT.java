package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * A creation command for an id that already has an aggregate is refused with 400 and appends
 * nothing: the stream still holds the one creation event, with the first request's data.
 */
class DuplicateCreationIT extends AbstractIntegrationTest {

  @Autowired EventStore eventStore;

  private List<EventEnvelope> events(AggregateType type, String id) {
    return eventStore.load(StreamId.of(type, AggregateId.of(id))).events();
  }

  private WebTestClient.ResponseSpec post(String uri, String role, String userId, String body) {
    return client
        .post()
        .uri(uri)
        .header("X-User-Role", role)
        .header("X-User-Id", userId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange();
  }

  @Test
  void aSecondCreateProductIsRefusedAndTheProductIsUnchanged() {
    String pid = "dup-p-" + System.nanoTime();
    String product =
        """
        {"productId": "%s", "name": "%s", "description": "d", "category": "Test",
         "price": %s, "initialStock": %d}""";

    post("/api/products", "ADMIN", "test-admin", product.formatted(pid, "Original", "19.99", 100))
        .expectStatus()
        .is2xxSuccessful();
    post("/api/products", "ADMIN", "test-admin", product.formatted(pid, "Overwrite", "1.00", 5))
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Product already exists: " + pid);

    List<EventEnvelope> stream = events(ProductState.TYPE, pid);
    assertThat(stream).hasSize(1);
    var created = (ProductEvent.ProductCreated) stream.getFirst().event();
    assertThat(created.name()).isEqualTo("Original");
    assertThat(created.price().amount()).isEqualByComparingTo(new BigDecimal("19.99"));
    assertThat(created.stock()).isEqualTo(100);
  }

  @Test
  void aSecondRegisterCustomerIsRefusedAndTheProfileIsUnchanged() {
    String cid = "dup-c-" + System.nanoTime();
    String customer =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "1 Test St",
         "phone": "555-0100"}""";

    post("/api/customers", "CUSTOMER", cid, customer.formatted(cid, "Original", "o@example.com"))
        .expectStatus()
        .is2xxSuccessful();
    post("/api/customers", "CUSTOMER", cid, customer.formatted(cid, "Overwrite", "x@example.com"))
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Customer already registered: " + cid);

    List<EventEnvelope> stream = events(CustomerState.TYPE, cid);
    assertThat(stream).hasSize(1);
    var registered = (CustomerEvent.CustomerRegistered) stream.getFirst().event();
    assertThat(registered.name()).isEqualTo("Original");
    assertThat(registered.email()).isEqualTo("o@example.com");
  }

  @Test
  void aSecondPlaceOrderIsRefusedAndTheOrderIsUnchanged() {
    String oid = "dup-o-" + System.nanoTime();
    String cid = "dup-o-cust-" + System.nanoTime();
    String order =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": %d, "unitPrice": 9.99}]}""";

    post("/api/orders", "CUSTOMER", cid, order.formatted(oid, cid, "dup-o-first", 1))
        .expectStatus()
        .is2xxSuccessful();
    post("/api/orders", "CUSTOMER", cid, order.formatted(oid, cid, "dup-o-second", 7))
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Order already exists: " + oid);

    // The fulfillment saga may add its own events to the order; there is still one OrderPlaced.
    List<OrderEvent.OrderPlaced> placed =
        events(OrderState.TYPE, oid).stream()
            .map(EventEnvelope::event)
            .filter(OrderEvent.OrderPlaced.class::isInstance)
            .map(OrderEvent.OrderPlaced.class::cast)
            .toList();
    assertThat(placed).hasSize(1);
    assertThat(placed.getFirst().lines()).hasSize(1);
    assertThat(placed.getFirst().lines().getFirst().productId()).isEqualTo("dup-o-first");
    assertThat(placed.getFirst().lines().getFirst().quantity()).isEqualTo(1);
  }
}
