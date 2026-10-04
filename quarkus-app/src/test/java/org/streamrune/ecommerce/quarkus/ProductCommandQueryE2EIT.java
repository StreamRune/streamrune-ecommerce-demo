package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.queries.dto.ProductView;

/**
 * HTTP end-to-end parity suite for the Quarkus product flow, mirroring the Spring app's {@code
 * ProductControllerIT}. Exercises the full CQRS round-trip (POST/PUT command → event → projection →
 * GET query) over real HTTP against a running Quarkus app backed by a real PostgreSQL
 * (Testcontainers).
 *
 * <p>The Quarkus {@code ProductCommandController} exposes create / adjust-stock / update-price (no
 * discontinue endpoint, unlike Spring), so the discontinue case is intentionally absent — the suite
 * covers exactly what the Quarkus surface exposes.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class ProductCommandQueryE2EIT {

  private void createProduct(String id, String name, double price, int stock) {
    String body =
        """
        {
          "productId": "%s",
          "name": "%s",
          "description": "test product",
          "category": "Test",
          "price": %s,
          "initialStock": %d
        }"""
            .formatted(id, name, price, stock);
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body(body)
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);
  }

  private ProductView fetchProduct(String id) {
    return given()
        .when()
        .get("/api/products/" + id)
        .then()
        .statusCode(200)
        .extract()
        .as(ProductView.class);
  }

  @Test
  void createProductReturns2xxAndProjectionEventuallyExposesIt() {
    String pid = "q-p-create-" + System.nanoTime();
    createProduct(pid, "Widget", 19.99, 100);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              ProductView v = fetchProduct(pid);
              assertThat(v).isNotNull();
              assertThat(v.productId()).isEqualTo(pid);
              assertThat(v.name()).isEqualTo("Widget");
              assertThat(v.status().name()).isEqualTo("AVAILABLE");
            });
  }

  @Test
  void listProductsReturnsAllCreated() {
    String pid1 = "q-p-list-1-" + System.nanoTime();
    String pid2 = "q-p-list-2-" + System.nanoTime();
    createProduct(pid1, "ListA", 9.99, 50);
    createProduct(pid2, "ListB", 14.99, 30);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              List<String> ids =
                  given()
                      .when()
                      .get("/api/products")
                      .then()
                      .statusCode(200)
                      .extract()
                      .jsonPath()
                      .getList("productId", String.class);
              assertThat(ids).contains(pid1, pid2);
            });
  }

  @Test
  void getMissingProductReturns404() {
    given().when().get("/api/products/does-not-exist-" + System.nanoTime()).then().statusCode(404);
  }

  @Test
  void updatePriceReflectsInProjection() {
    String pid = "q-p-price-" + System.nanoTime();
    createProduct(pid, "PriceWidget", 10.00, 10);
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchProduct(pid)).isNotNull());

    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body(
            """
            {"price": 25.50}""")
        .when()
        .put("/api/products/" + pid + "/price")
        .then()
        .statusCode(200);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              ProductView v = fetchProduct(pid);
              assertThat(v.price().amount().doubleValue()).isEqualTo(25.50);
            });
  }

  @Test
  void adjustStockBelowThresholdMarksLowStock() {
    String pid = "q-p-stock-" + System.nanoTime();
    createProduct(pid, "StockWidget", 5.00, 100);
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchProduct(pid)).isNotNull());

    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body(
            """
            {"quantity": -95, "reason": "test adjustment"}""")
        .when()
        .put("/api/products/" + pid + "/stock")
        .then()
        .statusCode(200);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              ProductView v = fetchProduct(pid);
              assertThat(v.stock()).isEqualTo(5);
              assertThat(v.status().name()).isEqualTo("LOW_STOCK");
            });
  }

  @Test
  void invalidPayloadReturns400() {
    // Blank productId: the command bus rejects the blank aggregate id with an
    // IllegalArgumentException, mapped to 400 by IllegalArgumentExceptionMapper.
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body(
            """
            {"productId": "", "name": "", "price": -1}""")
        .when()
        .post("/api/products")
        .then()
        .statusCode(400);
  }
}
