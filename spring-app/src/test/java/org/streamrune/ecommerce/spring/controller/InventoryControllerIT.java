package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * The inventory endpoints: receiving and adjusting stock for a product of the catalogue, and the
 * inventory read model, all for an {@code ADMIN}.
 */
class InventoryControllerIT extends AbstractIntegrationTest {

  @Test
  void receiveShipmentReturns2xxAndEventIsRecordedInInventoryStream() {
    String pid = createProduct("recv-");

    receive(pid, 50).expectStatus().is2xxSuccessful();

    // The InventoryCommand id extractor returns the productId, so the stream is "inventory:{pid}".
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> events =
                  client
                      .get()
                      .uri("/api/events/inventory/" + pid)
                      .header("X-User-Id", "admin-1")
                      .header("X-User-Role", "ADMIN")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();

              assertThat(events).isNotNull();
              assertThat(events).anyMatch(e -> "ShipmentReceived".equals(e.get("eventType")));
            });
  }

  @Test
  void receiveShipmentWithZeroQuantityReturns400() {
    receive(createProduct("bad-"), 0).expectStatus().isBadRequest();
  }

  @Test
  void receiveShipmentWithNegativeQuantityReturns400() {
    receive(createProduct("neg-"), -10).expectStatus().isBadRequest();
  }

  @Test
  void theInventoryReadModelListsEveryProductWithItsCounts() {
    String pid = createProduct("inv-read-");
    awaitInventory(pid, new InventoryView(pid, "Stocked", 0, 0, 0));

    receive(pid, 20).expectStatus().isOk();
    adjust(pid, -4, "Stocktake").expectStatus().isOk();

    awaitInventory(pid, new InventoryView(pid, "Stocked", 16, 0, 0));
    List<InventoryView> all =
        asAdmin(client.get().uri("/api/inventory"))
            .exchange()
            .expectStatus()
            .isOk()
            .expectBodyList(InventoryView.class)
            .returnResult()
            .getResponseBody();
    assertThat(all).contains(new InventoryView(pid, "Stocked", 16, 0, 0));
  }

  @Test
  void theInventoryOfAnUnknownProductIs404() {
    asAdmin(client.get().uri("/api/inventory/inv-none-" + System.nanoTime()))
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  /**
   * The inventory names what each product holds and what orders hold of it: an operator's view.
   * Without an identity, or as a customer, both reads and both writes answer {@code 403}.
   */
  @Test
  void theInventoryEndpointsAreForAnAdmin() {
    String pid = createProduct("inv-auth-");
    for (String path : new String[] {"/api/inventory", "/api/inventory/" + pid}) {
      client.get().uri(path).exchange().expectStatus().isForbidden();
      client
          .get()
          .uri(path)
          .header("X-User-Role", "CUSTOMER")
          .header("X-User-Id", "cust-alice")
          .exchange()
          .expectStatus()
          .isForbidden();
    }
    client
        .post()
        .uri("/api/inventory/" + pid + "/adjust")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "cust-alice")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantityChange\": 5, \"reason\": \"mine now\"}")
        .exchange()
        .expectStatus()
        .isForbidden();
    client
        .post()
        .uri("/api/inventory/" + pid + "/receive")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "cust-alice")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantity\": 5}")
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  /**
   * A blank reason passes the request record, which leaves {@code reason} open, and the command
   * bus's Bean Validation refuses the command ({@code @NotBlank} on {@code AdjustInventory}) with
   * {@code 400} and the interceptor's message. Nothing is stored. The Quarkus and Micronaut apps
   * answer the same request alike.
   */
  @Test
  void anAdjustmentWithoutAReasonIsRefusedByTheCommandsConstraint() {
    String pid = createProduct("inv-reason-");
    receive(pid, 5).expectStatus().isOk();

    adjust(pid, -1, " ")
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("Command validation failed for AdjustInventory");
    awaitInventory(pid, new InventoryView(pid, "Stocked", 5, 0, 0));
  }

  @Test
  void anAdjustmentThatChangesNothingIsRefused() {
    String pid = createProduct("inv-zero-");

    adjust(pid, 0, "Stocktake")
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("An inventory adjustment must change the quantity");
  }

  private String createProduct(String prefix) {
    String pid = prefix + System.nanoTime();
    asAdmin(client.post().uri("/api/products"))
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "%s", "name": "Stocked", "category": "Test", "price": 4.00}"""
                .formatted(pid))
        .exchange()
        .expectStatus()
        .isOk();
    return pid;
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

  private void awaitInventory(String pid, InventoryView expected) {
    await()
        .atMost(Duration.ofSeconds(10))
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

  private static <S extends WebTestClient.RequestHeadersSpec<?>> S asAdmin(S request) {
    @SuppressWarnings("unchecked")
    S withHeaders = (S) request.header("X-User-Role", "ADMIN").header("X-User-Id", "test-admin");
    return withHeaders;
  }
}
