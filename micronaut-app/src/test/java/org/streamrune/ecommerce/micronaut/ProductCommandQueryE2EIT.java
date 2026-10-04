package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.queries.dto.ProductView;

/**
 * HTTP end-to-end parity suite for the Micronaut product flow, mirroring the Spring app's {@code
 * ProductControllerIT} and the Quarkus {@code ProductCommandQueryE2EIT}. Exercises the full CQRS
 * round-trip (POST/PUT command → event → projection → GET query) over real HTTP against the running
 * Micronaut app backed by a real PostgreSQL (Testcontainers).
 *
 * <p>The Micronaut {@code ProductCommandController} exposes create / adjust-stock / update-price
 * (no discontinue endpoint, matching Quarkus), so the discontinue case is intentionally absent.
 */
@MicronautTest(transactional = false)
class ProductCommandQueryE2EIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  private record GetResult(int status, ProductView body) {}

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
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/products", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
  }

  private GetResult getProduct(String id) {
    try {
      ProductView body =
          client.toBlocking().retrieve(HttpRequest.GET("/api/products/" + id), ProductView.class);
      return new GetResult(200, body);
    } catch (HttpClientResponseException e) {
      return new GetResult(e.getStatus().getCode(), null);
    }
  }

  @Test
  void createProductReturns2xxAndProjectionEventuallyExposesIt() {
    String pid = "m-p-create-" + System.nanoTime();
    createProduct(pid, "Widget", 19.99, 100);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              GetResult r = getProduct(pid);
              assertThat(r.status()).isEqualTo(200);
              assertThat(r.body()).isNotNull();
              assertThat(r.body().productId()).isEqualTo(pid);
              assertThat(r.body().name()).isEqualTo("Widget");
              assertThat(r.body().status().name()).isEqualTo("AVAILABLE");
            });
  }

  @Test
  void listProductsReturnsAllCreated() {
    String pid1 = "m-p-list-1-" + System.nanoTime();
    String pid2 = "m-p-list-2-" + System.nanoTime();
    createProduct(pid1, "ListA", 9.99, 50);
    createProduct(pid2, "ListB", 14.99, 30);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              List<ProductView> products =
                  client
                      .toBlocking()
                      .retrieve(
                          HttpRequest.GET("/api/products"), Argument.listOf(ProductView.class));
              List<String> ids = products.stream().map(ProductView::productId).toList();
              assertThat(ids).contains(pid1, pid2);
            });
  }

  @Test
  void getMissingProductReturns404() {
    assertThat(getProduct("does-not-exist-" + System.nanoTime()).status()).isEqualTo(404);
  }

  @Test
  void updatePriceReflectsInProjection() {
    String pid = "m-p-price-" + System.nanoTime();
    createProduct(pid, "PriceWidget", 10.00, 10);
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(getProduct(pid).status()).isEqualTo(200));

    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.PUT(
                        "/api/products/" + pid + "/price",
                        """
                        {"price": 25.50}""")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              GetResult r = getProduct(pid);
              assertThat(r.status()).isEqualTo(200);
              assertThat(r.body().price().amount().doubleValue()).isEqualTo(25.50);
            });
  }

  @Test
  void adjustStockBelowThresholdMarksLowStock() {
    String pid = "m-p-stock-" + System.nanoTime();
    createProduct(pid, "StockWidget", 5.00, 100);
    // 30s, not the usual 10s: this is the FIRST projection await after container startup, so under
    // a full parallel suite it also absorbs the subscription's cold-start catch-up. Failed once at
    // 10s in a full-suite run (404 for the whole window) while passing 3/3 in isolation; the 30s
    // bound matches the existing cold-start awaits elsewhere in these ITs.
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(getProduct(pid).status()).isEqualTo(200));

    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.PUT(
                        "/api/products/" + pid + "/stock",
                        """
                        {"quantity": -95, "reason": "test adjustment"}""")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              GetResult r = getProduct(pid);
              assertThat(r.status()).isEqualTo(200);
              assertThat(r.body().stock()).isEqualTo(5);
              assertThat(r.body().status().name()).isEqualTo("LOW_STOCK");
            });
  }

  @Test
  void invalidPayloadReturns400() {
    // Blank productId: the command bus rejects the blank aggregate id with an
    // IllegalArgumentException, mapped to 400 by IllegalArgumentExceptionHandler.
    HttpClientResponseException ex =
        assertThrows(
            HttpClientResponseException.class,
            () ->
                client
                    .toBlocking()
                    .exchange(
                        HttpRequest.POST(
                                "/api/products",
                                """
                                {"productId": "", "name": "", "price": -1}""")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-User-Role", "ADMIN")
                            .header("X-User-Id", "test-admin")));
    assertThat(ex.getStatus().getCode()).isEqualTo(400);
  }
}
