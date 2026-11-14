package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * The inventory endpoints: receiving and adjusting stock for a product of the catalogue, and the
 * inventory read model, all for an {@code ADMIN}.
 */
class InventoryControllerIT extends AbstractIntegrationTest {

  @Autowired private EventStore eventStore;

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
   * A blank reason is refused by the inventory decider with its own message, {@code 400}, and
   * nothing is stored. The Quarkus and Micronaut apps answer the same request alike.
   */
  @Test
  void anAdjustmentWithoutAReasonIsRefused() {
    String pid = createProduct("inv-reason-");
    receive(pid, 5).expectStatus().isOk();

    adjust(pid, -1, " ")
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("An inventory adjustment needs a reason");
    assertThat(inventoryEvents(pid, InventoryEvent.InventoryAdjusted.class)).isZero();
  }

  /** A body without {@code quantityChange} is refused with the same answer in the three apps. */
  @Test
  void anAdjustmentWithoutAQuantityChangeIsRefused() {
    String pid = createProduct("inv-missing-");

    asAdmin(client.post().uri("/api/inventory/" + pid + "/adjust"))
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"reason\": \"Stocktake\"}")
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .isEqualTo("quantityChange is required");
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
