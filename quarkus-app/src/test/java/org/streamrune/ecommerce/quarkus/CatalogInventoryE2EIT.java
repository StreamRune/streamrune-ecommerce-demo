package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.Availability;
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.dto.ProductView;

/**
 * The catalogue and the inventory over HTTP, as in the Spring app's {@code CatalogInventoryE2EIT}
 * and {@code InventoryControllerIT} and the Micronaut {@code CatalogInventoryE2EIT}: the catalogue
 * says what is sold, the inventory holds the units, and the storefront's product view combines
 * both. A product created through the API holds nothing until a shipment is received; an order
 * reserves from the inventory, and the storefront follows. The inventory endpoints are an {@code
 * ADMIN}'s and refuse a product the catalogue does not hold.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class CatalogInventoryE2EIT {

  @Inject EventStore eventStore;

  @Test
  void anOrderReservesFromTheInventoryAndTheStorefrontFollows() {
    String pid = createProduct("q-cat-inv-");
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK);

    receive(pid, 12).statusCode(200);
    awaitProduct(pid, 12, Availability.IN_STOCK);

    String oid = "q-cat-inv-order-" + System.nanoTime();
    placeOrder(oid, "q-cat-cust-" + System.nanoTime(), pid, 3);

    awaitOrder(oid, OrderStatus.CONFIRMED);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 9, 3, 0));
    awaitProduct(pid, 9, Availability.LOW_STOCK);
  }

  @Test
  void anOrderOfMoreThanTheInventoryHoldsIsCancelledAndReservesNothing() {
    String pid = createProduct("q-cat-short-");
    receive(pid, 2).statusCode(200);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 2, 0, 0));

    String oid = "q-cat-short-order-" + System.nanoTime();
    placeOrder(oid, "q-cat-cust-" + System.nanoTime(), pid, 3);

    awaitOrder(oid, OrderStatus.CANCELLED);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 2, 0, 0));
    awaitProduct(pid, 2, Availability.LOW_STOCK);
  }

  @Test
  void aProductCreatedWithoutStockIsCancelledWhenOrdered() {
    String pid = createProduct("q-cat-empty-");
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK);

    String oid = "q-cat-empty-order-" + System.nanoTime();
    placeOrder(oid, "q-cat-cust-" + System.nanoTime(), pid, 1);

    awaitOrder(oid, OrderStatus.CANCELLED);
  }

  @Test
  void receivingOrAdjustingStockOfAProductNobodyCreatedIsRefused() {
    String unknown = "q-cat-unknown-" + System.nanoTime();

    receive(unknown, 5).statusCode(400).body(equalTo("Product not found: " + unknown));
    adjust(unknown, 5, "Found some").statusCode(400).body(equalTo("Product not found: " + unknown));

    assertThat(
            eventStore.readStream(
                StreamId.of(InventoryState.TYPE, AggregateId.of(unknown)), Version.initial(), 10))
        .as("no inventory is opened for a product nobody created")
        .isEmpty();
    asAdmin().when().get("/api/inventory/" + unknown).then().statusCode(404);
  }

  @Test
  void anAdjustmentMovesTheAvailableUnitsAndNeverBelowZero() {
    String pid = createProduct("q-cat-adjust-");
    receive(pid, 9).statusCode(200);

    adjust(pid, -10, "Stocktake")
        .statusCode(400)
        .body(equalTo("Insufficient stock for " + pid + ": available=9, change=-10"));

    adjust(pid, -2, "Stocktake").statusCode(200);
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 7, 0, 0));
    awaitProduct(pid, 7, Availability.LOW_STOCK);

    adjust(pid, -7, "Water damage").statusCode(200);
    awaitProduct(pid, 0, Availability.OUT_OF_STOCK);
  }

  @Test
  void theInventoryReadModelListsEveryProductWithItsCounts() {
    String pid = createProduct("q-cat-read-");
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 0, 0, 0));

    receive(pid, 20).statusCode(200);
    adjust(pid, -4, "Stocktake").statusCode(200);

    awaitInventory(pid, new InventoryView(pid, "Catalogued", 16, 0, 0));
    List<InventoryView> all =
        List.of(
            asAdmin()
                .when()
                .get("/api/inventory")
                .then()
                .statusCode(200)
                .extract()
                .as(InventoryView[].class));
    assertThat(all).contains(new InventoryView(pid, "Catalogued", 16, 0, 0));
  }

  /** Without an identity, or as a customer, both reads and both writes answer {@code 403}. */
  @Test
  void theInventoryEndpointsAreForAnAdmin() {
    String pid = createProduct("q-cat-auth-");
    for (String path : new String[] {"/api/inventory", "/api/inventory/" + pid}) {
      given().when().get(path).then().statusCode(403);
      asCustomer().when().get(path).then().statusCode(403);
    }
    asCustomer()
        .contentType(ContentType.JSON)
        .body("{\"quantityChange\": 5, \"reason\": \"mine now\"}")
        .when()
        .post("/api/inventory/" + pid + "/adjust")
        .then()
        .statusCode(403);
    asCustomer()
        .contentType(ContentType.JSON)
        .body("{\"quantity\": 5}")
        .when()
        .post("/api/inventory/" + pid + "/receive")
        .then()
        .statusCode(403);
  }

  /**
   * A blank reason passes the handler's checks, which leave {@code reason} open, and the command
   * bus's Bean Validation refuses the command ({@code @NotBlank} on {@code AdjustInventory}) with
   * {@code 400} and the interceptor's message, as in the Spring app. The validator is Hibernate
   * Validator, which {@code quarkus-hibernate-validator} brings. Nothing is stored.
   */
  @Test
  void anAdjustmentWithoutAReasonIsRefusedByTheCommandsConstraint() {
    String pid = createProduct("q-cat-reason-");
    receive(pid, 5).statusCode(200);

    adjust(pid, -1, " ")
        .statusCode(400)
        .body(equalTo("Command validation failed for AdjustInventory"));
    awaitInventory(pid, new InventoryView(pid, "Catalogued", 5, 0, 0));
  }

  @Test
  void anAdjustmentThatChangesNothingIsRefused() {
    String pid = createProduct("q-cat-zero-");

    adjust(pid, 0, "Stocktake")
        .statusCode(400)
        .body(equalTo("An inventory adjustment must change the quantity"));
  }

  private static String createProduct(String prefix) {
    String pid = prefix + System.nanoTime();
    asAdmin()
        .contentType(ContentType.JSON)
        .body(
            """
            {"productId": "%s", "name": "Catalogued", "category": "Test", "price": 6.00}"""
                .formatted(pid))
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);
    return pid;
  }

  private static void placeOrder(String oid, String cid, String pid, int quantity) {
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(ContentType.JSON)
        .body(
            """
            {"orderId": "%s", "customerId": "%s",
             "lines": [{"productId": "%s", "quantity": %d}]}"""
                .formatted(oid, cid, pid, quantity))
        .when()
        .post("/api/orders")
        .then()
        .statusCode(200);
  }

  private static ValidatableResponse receive(String pid, int quantity) {
    return asAdmin()
        .contentType(ContentType.JSON)
        .body("{\"quantity\": %d}".formatted(quantity))
        .when()
        .post("/api/inventory/" + pid + "/receive")
        .then();
  }

  private static ValidatableResponse adjust(String pid, int change, String reason) {
    return asAdmin()
        .contentType(ContentType.JSON)
        .body("{\"quantityChange\": %d, \"reason\": \"%s\"}".formatted(change, reason))
        .when()
        .post("/api/inventory/" + pid + "/adjust")
        .then();
  }

  private static void awaitProduct(String pid, int available, Availability availability) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              ProductView product =
                  given()
                      .when()
                      .get("/api/products/" + pid)
                      .then()
                      .statusCode(200)
                      .extract()
                      .as(ProductView.class);
              assertThat(product.available()).isEqualTo(available);
              assertThat(product.availability()).isEqualTo(availability);
            });
  }

  private static void awaitInventory(String pid, InventoryView expected) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(
                        asAdmin()
                            .when()
                            .get("/api/inventory/" + pid)
                            .then()
                            .statusCode(200)
                            .extract()
                            .as(InventoryView.class))
                    .isEqualTo(expected));
  }

  private static void awaitOrder(String oid, OrderStatus status) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(
                        asAdmin()
                            .when()
                            .get("/api/orders/" + oid)
                            .then()
                            .statusCode(200)
                            .extract()
                            .as(OrderView.class)
                            .status())
                    .isEqualTo(status));
  }

  private static RequestSpecification asAdmin() {
    return given().header("X-User-Role", "ADMIN").header("X-User-Id", "test-admin");
  }

  private static RequestSpecification asCustomer() {
    return given().header("X-User-Role", "CUSTOMER").header("X-User-Id", "q-cat-alice");
  }
}
