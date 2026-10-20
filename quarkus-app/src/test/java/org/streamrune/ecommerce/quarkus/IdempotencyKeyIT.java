package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;

/**
 * The {@code Idempotency-Key} header of the two relative writes, {@code PUT
 * /api/products/{id}/stock} and {@code POST /api/inventory/{productId}/receive}, as in the Spring
 * and Micronaut apps: the command runs once per key, every time without one, and the key lives in
 * the caller's namespace.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class IdempotencyKeyIT {

  @Inject EventStore eventStore;

  @Test
  void theSameKeyTwiceAdjustsTheStockOnce() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    adjustStock(pid, "admin-1", key).statusCode(200);
    adjustStock(pid, "admin-1", key).statusCode(200);

    assertThat(stockAdjustments(pid)).as("the second request is the first one's retry").isOne();
  }

  @Test
  void aNewKeyIsANewAdjustment() {
    String pid = createProduct();

    adjustStock(pid, "admin-1", UUID.randomUUID().toString()).statusCode(200);
    adjustStock(pid, "admin-1", UUID.randomUUID().toString()).statusCode(200);

    assertThat(stockAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void withoutAKeyEveryRequestAdjusts() {
    String pid = createProduct();

    adjustStock(pid, "admin-1", null).statusCode(200);
    adjustStock(pid, "admin-1", null).statusCode(200);

    assertThat(stockAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void theKeyIsScopedToTheCaller() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    adjustStock(pid, "admin-1", key).statusCode(200);
    adjustStock(pid, "admin-2", key).statusCode(200);

    assertThat(stockAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void aKeyPresentedForAnotherProductIsRefused() {
    String first = createProduct();
    String second = createProduct();
    String key = UUID.randomUUID().toString();

    adjustStock(first, "admin-1", key).statusCode(200);
    adjustStock(second, "admin-1", key).statusCode(400);

    assertThat(stockAdjustments(second)).isZero();
  }

  @Test
  void aKeyLongerThanTheFrameworkAcceptsIsRefused() {
    String pid = createProduct();

    adjustStock(pid, "admin-1", "k".repeat(600)).statusCode(400);

    assertThat(stockAdjustments(pid)).isZero();
  }

  @Test
  void theSameKeyTwiceReceivesAShipmentOnce() {
    String pid = "q-idem-recv-" + System.nanoTime();
    String key = UUID.randomUUID().toString();

    receiveShipment(pid, key).statusCode(200);
    receiveShipment(pid, key).statusCode(200);

    assertThat(shipmentsReceived(pid)).isOne();
  }

  private static String createProduct() {
    String pid = "q-idem-" + System.nanoTime();
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .contentType(ContentType.JSON)
        .body(
            """
            {"productId": "%s", "name": "Keyed", "description": "idempotency key",
             "category": "Test", "price": 5.00, "initialStock": 3}"""
                .formatted(pid))
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);
    return pid;
  }

  private static ValidatableResponse adjustStock(String pid, String userId, String key) {
    RequestSpecification request =
        given().header("X-User-Role", "ADMIN").header("X-User-Id", userId);
    if (key != null) {
      request = request.header("Idempotency-Key", key);
    }
    return request
        .contentType(ContentType.JSON)
        .body("{\"quantity\": 5, \"reason\": \"restock\"}")
        .when()
        .put("/api/products/" + pid + "/stock")
        .then();
  }

  private static ValidatableResponse receiveShipment(String pid, String key) {
    return given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .header("Idempotency-Key", key)
        .contentType(ContentType.JSON)
        .body("{\"quantity\": 7}")
        .when()
        .post("/api/inventory/" + pid + "/receive")
        .then();
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
