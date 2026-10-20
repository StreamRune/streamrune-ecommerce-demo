package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * The {@code Idempotency-Key} header of the two relative writes, {@code PUT
 * /api/products/{id}/stock} and {@code POST /api/inventory/{productId}/receive}. A change by a
 * quantity applied twice is a different stock, so a client that sends the request again (a timeout,
 * a retry button) sends the same key, and the command bus runs the command once. A request without
 * the header runs as it comes. The key lives in the caller's namespace: two admins who happen to
 * send the same value do not share a result.
 */
class IdempotencyKeyIT extends AbstractIntegrationTest {

  @Autowired private EventStore eventStore;

  @Test
  void theSameKeyTwiceAdjustsTheStockOnce() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    adjustStock(pid, 5, "admin-1", key).expectStatus().isOk();
    adjustStock(pid, 5, "admin-1", key).expectStatus().isOk();

    assertThat(stockAdjustments(pid)).as("the second request is the first one's retry").isOne();
  }

  @Test
  void aNewKeyIsANewAdjustment() {
    String pid = createProduct();

    adjustStock(pid, 5, "admin-1", UUID.randomUUID().toString()).expectStatus().isOk();
    adjustStock(pid, 5, "admin-1", UUID.randomUUID().toString()).expectStatus().isOk();

    assertThat(stockAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void withoutAKeyEveryRequestAdjusts() {
    String pid = createProduct();

    adjustStock(pid, 5, "admin-1", null).expectStatus().isOk();
    adjustStock(pid, 5, "admin-1", null).expectStatus().isOk();

    assertThat(stockAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void theKeyIsScopedToTheCaller() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    adjustStock(pid, 5, "admin-1", key).expectStatus().isOk();
    adjustStock(pid, 5, "admin-2", key).expectStatus().isOk();

    assertThat(stockAdjustments(pid))
        .as("another caller's value is another key, not a bearer token for admin-1's result")
        .isEqualTo(2);
  }

  @Test
  void aKeyPresentedForAnotherProductIsRefused() {
    String first = createProduct();
    String second = createProduct();
    String key = UUID.randomUUID().toString();

    adjustStock(first, 5, "admin-1", key).expectStatus().isOk();
    adjustStock(second, 5, "admin-1", key).expectStatus().isBadRequest();

    assertThat(stockAdjustments(second)).isZero();
  }

  @Test
  void aKeyLongerThanTheFrameworkAcceptsIsRefused() {
    String pid = createProduct();

    adjustStock(pid, 5, "admin-1", "k".repeat(600)).expectStatus().isBadRequest();

    assertThat(stockAdjustments(pid)).isZero();
  }

  @Test
  void theSameKeyTwiceReceivesAShipmentOnce() {
    String pid = "idem-recv-" + System.nanoTime();
    String key = UUID.randomUUID().toString();

    receiveShipment(pid, 7, key).expectStatus().isOk();
    receiveShipment(pid, 7, key).expectStatus().isOk();

    assertThat(shipmentsReceived(pid)).isOne();
  }

  private String createProduct() {
    String pid = "idem-" + System.nanoTime();
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "%s", "name": "Keyed", "description": "idempotency key",
             "category": "Test", "price": 5.00, "initialStock": 3}"""
                .formatted(pid))
        .exchange()
        .expectStatus()
        .isOk();
    return pid;
  }

  private WebTestClient.ResponseSpec adjustStock(
      String pid, int quantity, String userId, String key) {
    var request =
        client
            .put()
            .uri("/api/products/" + pid + "/stock")
            .header("X-User-Role", "ADMIN")
            .header("X-User-Id", userId);
    if (key != null) {
      request = request.header("Idempotency-Key", key);
    }
    return request
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantity\": %d, \"reason\": \"restock\"}".formatted(quantity))
        .exchange();
  }

  private WebTestClient.ResponseSpec receiveShipment(String pid, int quantity, String key) {
    return client
        .post()
        .uri("/api/inventory/" + pid + "/receive")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .header("Idempotency-Key", key)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantity\": %d}".formatted(quantity))
        .exchange();
  }

  private long stockAdjustments(String pid) {
    return eventStore
        .readStream(
            StreamId.of(ProductState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> e.event() instanceof ProductEvent.StockAdjusted)
        .count();
  }

  private long shipmentsReceived(String pid) {
    return eventStore
        .readStream(
            StreamId.of(InventoryState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> e.event() instanceof InventoryEvent.ShipmentReceived)
        .count();
  }
}
