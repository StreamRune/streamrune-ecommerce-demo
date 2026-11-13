package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
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

/**
 * The catalogue and the inventory over HTTP, as in the Spring app's {@code CatalogInventoryE2EIT}
 * and {@code InventoryControllerIT} and the Quarkus {@code CatalogInventoryE2EIT}: the catalogue
 * says what is sold, the inventory holds the units, and the storefront's product view combines
 * both. A product created through the API holds nothing until a shipment is received; an order
 * reserves from the inventory, and the storefront follows. The inventory endpoints are an {@code
 * ADMIN}'s and refuse a product the catalogue does not hold.
 */
@MicronautTest(transactional = false)
class CatalogInventoryE2EIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject EventStore eventStore;

  /** An answer's status and body. */
  private record Answer(int status, String body) {}

  @Test
  void anOrderReservesFromTheInventoryAndTheStorefrontFollows() {
    String pid = createProduct("m-cat-inv-");
    // 30s: under a full parallel suite this can be the first projection await after startup.
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK, Duration.ofSeconds(30));

    assertThat(receive(pid, 12).status()).isEqualTo(200);
    awaitProduct(pid, 12, Availability.IN_STOCK, Duration.ofSeconds(10));

    String oid = "m-cat-inv-order-" + System.nanoTime();
    placeOrder(oid, "m-cat-cust-" + System.nanoTime(), pid, 3);

    awaitOrder(oid, OrderStatus.CONFIRMED);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 9, 3, 0));
    awaitProduct(pid, 9, Availability.LOW_STOCK, Duration.ofSeconds(10));
  }

  @Test
  void anOrderOfMoreThanTheInventoryHoldsIsCancelledAndReservesNothing() {
    String pid = createProduct("m-cat-short-");
    assertThat(receive(pid, 2).status()).isEqualTo(200);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 2, 0, 0));

    String oid = "m-cat-short-order-" + System.nanoTime();
    placeOrder(oid, "m-cat-cust-" + System.nanoTime(), pid, 3);

    awaitCancelled(oid, "Insufficient stock for " + pid + ": requested 3, available 2");
    assertThat(inventoryEvents(pid, InventoryEvent.StockReserved.class))
        .as("nothing is reserved")
        .isZero();
    assertThat(inventoryEvents(pid, InventoryEvent.StockReservationRefused.class)).isOne();
    awaitProduct(pid, 2, Availability.LOW_STOCK, Duration.ofSeconds(10));
  }

  @Test
  void aProductCreatedWithoutStockIsCancelledWhenOrdered() {
    String pid = createProduct("m-cat-empty-");
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK, Duration.ofSeconds(30));

    String oid = "m-cat-empty-order-" + System.nanoTime();
    placeOrder(oid, "m-cat-cust-" + System.nanoTime(), pid, 1);

    awaitCancelled(oid, "Insufficient stock for " + pid + ": requested 1, available 0");
  }

  @Test
  void receivingOrAdjustingStockOfAProductNobodyCreatedIsRefused() {
    String unknown = "m-cat-unknown-" + System.nanoTime();

    assertThat(receive(unknown, 5)).isEqualTo(new Answer(400, "Product not found: " + unknown));
    assertThat(adjust(unknown, 5, "Found some"))
        .isEqualTo(new Answer(400, "Product not found: " + unknown));

    assertThat(
            eventStore.readStream(
                StreamId.of(InventoryState.TYPE, AggregateId.of(unknown)), Version.initial(), 10))
        .as("no inventory is opened for a product nobody created")
        .isEmpty();
    assertThat(send(asAdmin(HttpRequest.GET("/api/inventory/" + unknown))).status()).isEqualTo(404);
  }

  @Test
  void anAdjustmentMovesTheAvailableUnitsAndNeverBelowZero() {
    String pid = createProduct("m-cat-adjust-");
    assertThat(receive(pid, 9).status()).isEqualTo(200);

    assertThat(adjust(pid, -10, "Stocktake"))
        .isEqualTo(new Answer(400, "Insufficient stock for " + pid + ": available=9, change=-10"));

    assertThat(adjust(pid, -2, "Stocktake").status()).isEqualTo(200);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 7, 0, 0));
    awaitProduct(pid, 7, Availability.LOW_STOCK, Duration.ofSeconds(10));

    assertThat(adjust(pid, -7, "Water damage").status()).isEqualTo(200);
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK, Duration.ofSeconds(10));
  }

  @Test
  void theInventoryReadModelListsEveryProductWithItsCounts() {
    String pid = createProduct("m-cat-read-");
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 0, 0, 0));

    assertThat(receive(pid, 20).status()).isEqualTo(200);
    assertThat(adjust(pid, -4, "Stocktake").status()).isEqualTo(200);

    awaitInventory(pid, new InventoryView(pid, "Catalogued", 16, 0, 0));
    List<InventoryView> all =
        client
            .toBlocking()
            .retrieve(
                asAdmin(HttpRequest.GET("/api/inventory")), Argument.listOf(InventoryView.class));
    assertThat(all).contains(new InventoryView(pid, "Catalogued", 16, 0, 0));
  }

  /** Without an identity, or as a customer, both reads and both writes answer {@code 403}. */
  @Test
  void theInventoryEndpointsAreForAnAdmin() {
    String pid = createProduct("m-cat-auth-");
    for (String path : new String[] {"/api/inventory", "/api/inventory/" + pid}) {
      assertThat(send(HttpRequest.GET(path)).status()).isEqualTo(403);
      assertThat(send(asCustomer(HttpRequest.GET(path))).status()).isEqualTo(403);
    }
    assertThat(
            send(asCustomer(
                    HttpRequest.POST(
                            "/api/inventory/" + pid + "/adjust",
                            "{\"quantityChange\": 5, \"reason\": \"mine now\"}")
                        .contentType(MediaType.APPLICATION_JSON)))
                .status())
        .isEqualTo(403);
    assertThat(
            send(asCustomer(
                    HttpRequest.POST("/api/inventory/" + pid + "/receive", "{\"quantity\": 5}")
                        .contentType(MediaType.APPLICATION_JSON)))
                .status())
        .isEqualTo(403);
  }

  /** A blank reason is refused by the inventory decider with its own message; nothing is stored. */
  @Test
  void anAdjustmentWithoutAReasonIsRefused() {
    String pid = createProduct("m-cat-reason-");
    assertThat(receive(pid, 5).status()).isEqualTo(200);

    assertThat(adjust(pid, -1, " "))
        .isEqualTo(new Answer(400, "An inventory adjustment needs a reason"));
    assertThat(inventoryEvents(pid, InventoryEvent.InventoryAdjusted.class)).isZero();
  }

  /** A body without {@code quantityChange} is refused with the same answer in the three apps. */
  @Test
  void anAdjustmentWithoutAQuantityChangeIsRefused() {
    String pid = createProduct("m-cat-missing-");

    assertThat(
            send(
                asAdmin(
                    HttpRequest.POST(
                            "/api/inventory/" + pid + "/adjust", "{\"reason\": \"Stocktake\"}")
                        .contentType(MediaType.APPLICATION_JSON))))
        .isEqualTo(new Answer(400, "quantityChange is required"));
  }

  /**
   * An order of two products, one of them short: the line that fits is reserved, the other is
   * refused, and the cancellation with the refusal's reason releases the reservation already made.
   */
  @Test
  void aShortLineCancelsTheOrderAndReleasesTheOtherLinesReservation() {
    String stocked = createProduct("m-cat-two-ok-");
    String scarce = createProduct("m-cat-two-scarce-");
    assertThat(receive(stocked, 10).status()).isEqualTo(200);
    assertThat(receive(scarce, 1).status()).isEqualTo(200);
    awaitInventory(scarce, new InventoryView(scarce, "Catalogued", 1, 0, 0));
    String cid = "m-cat-cust-" + System.nanoTime();
    String oid = "m-cat-two-order-" + System.nanoTime();

    Answer placed =
        send(
            HttpRequest.POST(
                    "/api/orders",
                    """
                    {"orderId": "%s", "customerId": "%s", "lines": [
                      {"productId": "%s", "quantity": 2}, {"productId": "%s", "quantity": 4}]}"""
                        .formatted(oid, cid, stocked, scarce))
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-User-Role", "CUSTOMER")
                .header("X-User-Id", cid));
    assertThat(placed.status()).as(placed.body()).isBetween(200, 299);

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
  void anAdjustmentThatChangesNothingIsRefused() {
    String pid = createProduct("m-cat-zero-");

    assertThat(adjust(pid, 0, "Stocktake"))
        .isEqualTo(new Answer(400, "An inventory adjustment must change the quantity"));
  }

  private String createProduct(String prefix) {
    String pid = prefix + System.nanoTime();
    Answer created =
        send(
            asAdmin(
                HttpRequest.POST(
                        "/api/products",
                        """
                        {"productId": "%s", "name": "Catalogued", "category": "Test",
                         "price": 6.00}"""
                            .formatted(pid))
                    .contentType(MediaType.APPLICATION_JSON)));
    assertThat(created.status()).isEqualTo(200);
    return pid;
  }

  private void placeOrder(String oid, String cid, String pid, int quantity) {
    Answer placed =
        send(
            HttpRequest.POST(
                    "/api/orders",
                    """
                    {"orderId": "%s", "customerId": "%s",
                     "lines": [{"productId": "%s", "quantity": %d}]}"""
                        .formatted(oid, cid, pid, quantity))
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-User-Role", "CUSTOMER")
                .header("X-User-Id", cid));
    assertThat(placed.status()).as(placed.body()).isBetween(200, 299);
  }

  private Answer receive(String pid, int quantity) {
    return send(
        asAdmin(
            HttpRequest.POST(
                    "/api/inventory/" + pid + "/receive", "{\"quantity\": %d}".formatted(quantity))
                .contentType(MediaType.APPLICATION_JSON)));
  }

  private Answer adjust(String pid, int change, String reason) {
    return send(
        asAdmin(
            HttpRequest.POST(
                    "/api/inventory/" + pid + "/adjust",
                    "{\"quantityChange\": %d, \"reason\": \"%s\"}".formatted(change, reason))
                .contentType(MediaType.APPLICATION_JSON)));
  }

  private void awaitProduct(String pid, int available, Availability availability, Duration atMost) {
    await()
        .atMost(atMost)
        .pollInterval(Duration.ofMillis(200))
        // A 404 until the projection has the row: retried like a failed assertion.
        .ignoreExceptionsInstanceOf(HttpClientResponseException.class)
        .untilAsserted(
            () -> {
              ProductView product =
                  client
                      .toBlocking()
                      .retrieve(HttpRequest.GET("/api/products/" + pid), ProductView.class);
              assertThat(product.available()).isEqualTo(available);
              assertThat(product.availability()).isEqualTo(availability);
            });
  }

  private void awaitInventory(String pid, InventoryView expected) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        // A 404 until the projection has the row: retried like a failed assertion.
        .ignoreExceptionsInstanceOf(HttpClientResponseException.class)
        .untilAsserted(
            () ->
                assertThat(
                        client
                            .toBlocking()
                            .retrieve(
                                asAdmin(HttpRequest.GET("/api/inventory/" + pid)),
                                InventoryView.class))
                    .isEqualTo(expected));
  }

  private void awaitOrder(String oid, OrderStatus status) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        // A 404 until the projection has the row: retried like a failed assertion.
        .ignoreExceptionsInstanceOf(HttpClientResponseException.class)
        .untilAsserted(
            () ->
                assertThat(
                        client
                            .toBlocking()
                            .retrieve(
                                asAdmin(HttpRequest.GET("/api/orders/" + oid)), OrderView.class)
                            .status())
                    .isEqualTo(status));
  }

  private void awaitCancelled(String oid, String reason) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        // A 404 until the projection has the row: retried like a failed assertion.
        .ignoreExceptionsInstanceOf(HttpClientResponseException.class)
        .untilAsserted(
            () -> {
              OrderView order =
                  client
                      .toBlocking()
                      .retrieve(asAdmin(HttpRequest.GET("/api/orders/" + oid)), OrderView.class);
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

  private static <T> MutableHttpRequest<T> asAdmin(MutableHttpRequest<T> request) {
    return request.header("X-User-Role", "ADMIN").header("X-User-Id", "test-admin");
  }

  private static <T> MutableHttpRequest<T> asCustomer(MutableHttpRequest<T> request) {
    return request.header("X-User-Role", "CUSTOMER").header("X-User-Id", "m-cat-alice");
  }

  private Answer send(MutableHttpRequest<?> request) {
    try {
      var response = client.toBlocking().exchange(request, String.class);
      return new Answer(response.getStatus().getCode(), response.getBody().orElse(""));
    } catch (HttpClientResponseException e) {
      return new Answer(e.getStatus().getCode(), e.getResponse().getBody(String.class).orElse(""));
    }
  }
}
