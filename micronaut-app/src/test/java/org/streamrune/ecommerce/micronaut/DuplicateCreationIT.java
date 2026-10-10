package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
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

/**
 * A creation command for an id that already has an aggregate is refused with 400 and appends
 * nothing: the stream still holds the one creation event, with the first request's data.
 */
@MicronautTest(transactional = false)
class DuplicateCreationIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject EventStore eventStore;

  private List<EventEnvelope> events(AggregateType type, String id) {
    return eventStore.load(StreamId.of(type, AggregateId.of(id))).events();
  }

  private HttpRequest<String> post(String uri, String role, String userId, String body) {
    return HttpRequest.POST(uri, body)
        .contentType(MediaType.APPLICATION_JSON)
        .header("X-User-Role", role)
        .header("X-User-Id", userId);
  }

  private void accepted(HttpRequest<String> request) {
    assertThat(client.toBlocking().exchange(request).getStatus().getCode()).isBetween(200, 299);
  }

  private void refused(HttpRequest<String> request, String message) {
    HttpClientResponseException refusal =
        assertThrows(
            HttpClientResponseException.class,
            () -> client.toBlocking().exchange(request, String.class));
    assertThat(refusal.getStatus().getCode()).isEqualTo(400);
    assertThat(refusal.getResponse().getBody(String.class)).contains(message);
  }

  @Test
  void aSecondCreateProductIsRefusedAndTheProductIsUnchanged() {
    String pid = "m-dup-p-" + System.nanoTime();
    String product =
        """
        {"productId": "%s", "name": "%s", "description": "d", "category": "Test",
         "price": %s, "initialStock": %d}""";

    accepted(
        post(
            "/api/products",
            "ADMIN",
            "test-admin",
            product.formatted(pid, "Original", "19.99", 100)));
    refused(
        post(
            "/api/products", "ADMIN", "test-admin", product.formatted(pid, "Overwrite", "1.00", 5)),
        "Product already exists: " + pid);

    List<EventEnvelope> stream = events(ProductState.TYPE, pid);
    assertThat(stream).hasSize(1);
    var created = (ProductEvent.ProductCreated) stream.getFirst().event();
    assertThat(created.name()).isEqualTo("Original");
    assertThat(created.price().amount()).isEqualByComparingTo(new BigDecimal("19.99"));
    assertThat(created.stock()).isEqualTo(100);
  }

  @Test
  void aSecondRegisterCustomerIsRefusedAndTheProfileIsUnchanged() {
    String cid = "m-dup-c-" + System.nanoTime();
    String customer =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "1 Test St",
         "phone": "555-0100"}""";

    accepted(
        post(
            "/api/customers",
            "CUSTOMER",
            cid,
            customer.formatted(cid, "Original", "o@example.com")));
    refused(
        post(
            "/api/customers",
            "CUSTOMER",
            cid,
            customer.formatted(cid, "Overwrite", "x@example.com")),
        "Customer already registered: " + cid);

    List<EventEnvelope> stream = events(CustomerState.TYPE, cid);
    assertThat(stream).hasSize(1);
    var registered = (CustomerEvent.CustomerRegistered) stream.getFirst().event();
    assertThat(registered.name()).isEqualTo("Original");
    assertThat(registered.email()).isEqualTo("o@example.com");
  }

  @Test
  void aSecondPlaceOrderIsRefusedAndTheOrderIsUnchanged() {
    String oid = "m-dup-o-" + System.nanoTime();
    String cid = "m-dup-o-cust-" + System.nanoTime();
    String order =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": %d, "unitPrice": 9.99}]}""";

    accepted(post("/api/orders", "CUSTOMER", cid, order.formatted(oid, cid, "m-dup-o-first", 1)));
    refused(
        post("/api/orders", "CUSTOMER", cid, order.formatted(oid, cid, "m-dup-o-second", 7)),
        "Order already exists: " + oid);

    // The fulfillment saga may add its own events to the order; there is still one OrderPlaced.
    List<OrderEvent.OrderPlaced> placed =
        events(OrderState.TYPE, oid).stream()
            .map(EventEnvelope::event)
            .filter(OrderEvent.OrderPlaced.class::isInstance)
            .map(OrderEvent.OrderPlaced.class::cast)
            .toList();
    assertThat(placed).hasSize(1);
    assertThat(placed.getFirst().lines()).hasSize(1);
    assertThat(placed.getFirst().lines().getFirst().productId()).isEqualTo("m-dup-o-first");
    assertThat(placed.getFirst().lines().getFirst().quantity()).isEqualTo(1);
  }
}
