package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
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
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.Availability;
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.dto.ProductView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * The catalogue and the inventory over HTTP: the catalogue says what is sold, the inventory holds
 * the units, and the storefront's product view combines both. A product created through the API
 * holds nothing until a shipment is received; an order reserves from the inventory, and the
 * storefront follows.
 */
class CatalogInventoryE2EIT extends AbstractIntegrationTest {

  @Autowired private EventStore eventStore;

  @Test
  void anOrderReservesFromTheInventoryAndTheStorefrontFollows() {
    String pid = createProduct("cat-inv-");
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK);

    receive(pid, 12).expectStatus().isOk();
    awaitProduct(pid, 12, Availability.IN_STOCK);

    String cid = registerCustomer();
    String oid = "cat-inv-order-" + System.nanoTime();
    placeOrder(oid, cid, pid, 3);

    awaitOrder(oid, OrderStatus.CONFIRMED);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 9, 3, 0));
    awaitProduct(pid, 9, Availability.LOW_STOCK);
  }

  @Test
  void anOrderOfMoreThanTheInventoryHoldsIsCancelledAndReservesNothing() {
    String pid = createProduct("cat-short-");
    receive(pid, 2).expectStatus().isOk();
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 2, 0, 0));

    String oid = "cat-short-order-" + System.nanoTime();
    placeOrder(oid, registerCustomer(), pid, 3);

    awaitCancelled(oid, "Insufficient stock for " + pid + ": requested 3, available 2");
    assertThat(inventoryEvents(pid, InventoryEvent.StockReserved.class))
        .as("nothing is reserved")
        .isZero();
    assertThat(inventoryEvents(pid, InventoryEvent.StockReservationRefused.class)).isOne();
    awaitProduct(pid, 2, Availability.LOW_STOCK);
  }

  @Test
  void aProductCreatedWithoutStockIsCancelledWhenOrdered() {
    String pid = createProduct("cat-empty-");
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK);

    String oid = "cat-empty-order-" + System.nanoTime();
    placeOrder(oid, registerCustomer(), pid, 1);

    awaitCancelled(oid, "Insufficient stock for " + pid + ": requested 1, available 0");
  }

  /**
   * An order of two products, one of them short: the line that fits is reserved, the other is
   * refused, and the cancellation with the refusal's reason releases the reservation already made.
   */
  @Test
  void aShortLineCancelsTheOrderAndReleasesTheOtherLinesReservation() {
    String stocked = createProduct("cat-two-ok-");
    String scarce = createProduct("cat-two-scarce-");
    receive(stocked, 10).expectStatus().isOk();
    receive(scarce, 1).expectStatus().isOk();
    awaitInventory(scarce, new InventoryView(scarce, "Catalogued", 1, 0, 0));
    String cid = registerCustomer();
    String oid = "cat-two-order-" + System.nanoTime();

    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "%s", "customerId": "%s", "lines": [
              {"productId": "%s", "quantity": 2}, {"productId": "%s", "quantity": 4}]}"""
                .formatted(oid, cid, stocked, scarce))
        .exchange()
        .expectStatus()
        .is2xxSuccessful();

    awaitCancelled(oid, "Insufficient stock for " + scarce + ": requested 4, available 1");
    // The release is the cancellation process manager's, a subscription of its own: wait for it.
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> assertThat(inventoryEvents(stocked, InventoryEvent.StockReleased.class)).isOne());
    assertThat(inventoryEvents(stocked, InventoryEvent.StockReserved.class))
        .as("the stocked line was reserved before it was released")
        .isOne();
    awaitInventory(stocked, new InventoryView(stocked, "Catalogued", 10, 0, 0));
  }

  @Test
  void receivingOrAdjustingStockOfAProductNobodyCreatedIsRefused() {
    String unknown = "cat-unknown-" + System.nanoTime();

    receive(unknown, 5)
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Product not found: " + unknown);
    adjust(unknown, 5, "Found some")
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Product not found: " + unknown);

    assertThat(
            eventStore.readStream(
                StreamId.of(InventoryState.TYPE, AggregateId.of(unknown)), Version.initial(), 10))
        .as("no inventory is opened for a product nobody created")
        .isEmpty();
    asAdmin(client.get().uri("/api/inventory/" + unknown)).exchange().expectStatus().isNotFound();
  }

  @Test
  void anAdjustmentMovesTheAvailableUnitsAndNeverBelowZero() {
    String pid = createProduct("cat-adjust-");
    receive(pid, 9).expectStatus().isOk();

    adjust(pid, -10, "Stocktake")
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Insufficient stock for " + pid + ": available=9, change=-10");

    adjust(pid, -2, "Stocktake").expectStatus().isOk();
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 7, 0, 0));
    awaitProduct(pid, 7, Availability.LOW_STOCK);

    adjust(pid, -7, "Water damage").expectStatus().isOk();
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK);
  }

  private String createProduct(String prefix) {
    String pid = prefix + System.nanoTime();
    asAdmin(client.post().uri("/api/products"))
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "%s", "name": "Catalogued", "category": "Test", "price": 6.00}"""
                .formatted(pid))
        .exchange()
        .expectStatus()
        .isOk();
    return pid;
  }

  private String registerCustomer() {
    String cid = "cat-cust-" + System.nanoTime();
    client
        .post()
        .uri("/api/customers")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"customerId": "%s", "name": "Catalogue Customer", "email": "%s@example.com",
             "address": "1 Shop St", "phone": "555-0300"}"""
                .formatted(cid, cid))
        .exchange()
        .expectStatus()
        .isOk();
    return cid;
  }

  private void placeOrder(String oid, String cid, String pid, int quantity) {
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "%s", "customerId": "%s",
             "lines": [{"productId": "%s", "quantity": %d}]}"""
                .formatted(oid, cid, pid, quantity))
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  private WebTestClient.ResponseSpec receive(String pid, int quantity) {
    return asAdmin(client.post().uri("/api/inventory/" + pid + "/receive"))
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantity\": %d}".formatted(quantity))
        .exchange();
  }

  private WebTestClient.ResponseSpec adjust(String pid, int change, String reason) {
    return asAdmin(client.post().uri("/api/inventory/" + pid + "/adjust"))
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantityChange\": %d, \"reason\": \"%s\"}".formatted(change, reason))
        .exchange();
  }

  private void awaitProduct(String pid, int available, Availability availability) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              ProductView product =
                  client
                      .get()
                      .uri("/api/products/" + pid)
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(ProductView.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(product).isNotNull();
              assertThat(product.available()).isEqualTo(available);
              assertThat(product.availability()).isEqualTo(availability);
            });
  }

  private void awaitInventory(String pid, InventoryView expected) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(
                        asAdmin(client.get().uri("/api/inventory/" + pid))
                            .exchange()
                            .expectStatus()
                            .isOk()
                            .expectBody(InventoryView.class)
                            .returnResult()
                            .getResponseBody())
                    .isEqualTo(expected));
  }

  private void awaitCancelled(String oid, String reason) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              OrderView order =
                  asAdmin(client.get().uri("/api/orders/" + oid))
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(OrderView.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(order).isNotNull();
              assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
              assertThat(order.reason()).isEqualTo(reason);
            });
  }

  private long inventoryEvents(String pid, Class<?> type) {
    return eventStore
        .readStream(
            StreamId.of(InventoryState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> type.isInstance(e.event()))
        .count();
  }

  private void awaitOrder(String oid, OrderStatus status) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(
                        asAdmin(client.get().uri("/api/orders/" + oid))
                            .exchange()
                            .expectStatus()
                            .isOk()
                            .expectBody(OrderView.class)
                            .returnResult()
                            .getResponseBody())
                    .extracting(OrderView::status)
                    .isEqualTo(status));
  }

  private static <S extends WebTestClient.RequestHeadersSpec<?>> S asAdmin(S request) {
    @SuppressWarnings("unchecked")
    S withHeaders = (S) request.header("X-User-Role", "ADMIN").header("X-User-Id", "test-admin");
    return withHeaders;
  }
}
