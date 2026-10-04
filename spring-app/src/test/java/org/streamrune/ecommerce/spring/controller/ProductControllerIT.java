package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.queries.dto.ProductView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

class ProductControllerIT extends AbstractIntegrationTest {

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
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  private ProductView fetchProduct(String id) {
    return client
        .get()
        .uri("/api/products/" + id)
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(ProductView.class)
        .returnResult()
        .getResponseBody();
  }

  @Test
  void createProductReturns2xxAndProjectionEventuallyExposesIt() {
    String pid = "p-create-" + System.nanoTime();
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
  void listProductsViaQueryBusReturnsAllCreated() {
    String pid1 = "p-list-1-" + System.nanoTime();
    String pid2 = "p-list-2-" + System.nanoTime();
    createProduct(pid1, "ListA", 9.99, 50);
    createProduct(pid2, "ListB", 14.99, 30);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> products =
                  client
                      .get()
                      .uri("/api/products")
                      .header("X-User-Role", "ADMIN")
                      .header("X-User-Id", "test-admin")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(products).isNotNull();
              List<String> ids = products.stream().map(p -> (String) p.get("productId")).toList();
              assertThat(ids).contains(pid1, pid2);
            });
  }

  @Test
  void getMissingProductReturns404() {
    client
        .get()
        .uri("/api/products/does-not-exist-" + System.nanoTime())
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  @Test
  void updatePriceReflectsInProjection() {
    String pid = "p-price-" + System.nanoTime();
    createProduct(pid, "PriceWidget", 10.00, 10);

    client
        .put()
        .uri("/api/products/" + pid + "/price")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"price": 25.50}""")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();

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
    String pid = "p-stock-" + System.nanoTime();
    createProduct(pid, "StockWidget", 5.00, 100);

    client
        .put()
        .uri("/api/products/" + pid + "/stock")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"quantity": -95, "reason": "test adjustment"}""")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();

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
  void discontinueMarksDiscontinued() {
    String pid = "p-discontinue-" + System.nanoTime();
    createProduct(pid, "Doomed", 1.00, 1);

    // Wait for projection to receive ProductCreated before discontinuing — discontinue dispatches
    // a command that the domain validates against current aggregate state.
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchProduct(pid)).isNotNull());

    client
        .post()
        .uri("/api/products/" + pid + "/discontinue")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectBody(String.class)
        .consumeWith(
            r -> {
              if (!r.getStatus().is2xxSuccessful()) {
                throw new AssertionError(
                    "discontinue failed: " + r.getStatus() + " body=" + r.getResponseBody());
              }
            });

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              ProductView v = fetchProduct(pid);
              assertThat(v.status().name()).isEqualTo("DISCONTINUED");
            });
  }

  @Test
  void invalidPayloadReturns400() {
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "", "name": "", "price": -1}""")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }
}
