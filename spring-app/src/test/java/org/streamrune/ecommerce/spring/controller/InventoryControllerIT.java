package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

class InventoryControllerIT extends AbstractIntegrationTest {

  @Test
  void receiveShipmentReturns2xxAndEventIsRecordedInInventoryStream() {
    String pid = "recv-" + System.nanoTime();
    // The InventoryCommand id extractor returns the productId, so the aggregate stream is
    // "inventory:{pid}".
    // There is no separate HTTP query endpoint for InventoryProjection, so we verify the shipment
    // was recorded by reading the inventory event stream directly.
    client
        .post()
        .uri("/api/inventory/" + pid + "/receive")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"quantity": 50}""")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();

    // Wait for the ShipmentReceived event to appear in the inventory stream "inventory:{pid}"
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
                      .header("X-User-Role", "ADMIN")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();

              assertThat(events).isNotNull();
              assertThat(events).isNotEmpty();
              boolean hasShipmentReceived =
                  events.stream().anyMatch(e -> "ShipmentReceived".equals(e.get("eventType")));
              assertThat(hasShipmentReceived).isTrue();
            });
  }

  @Test
  void receiveShipmentWithZeroQuantityReturns400() {
    String pid = "bad-" + System.nanoTime();
    client
        .post()
        .uri("/api/inventory/" + pid + "/receive")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"quantity": 0}""")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  @Test
  void receiveShipmentWithNegativeQuantityReturns400() {
    String pid = "neg-" + System.nanoTime();
    client
        .post()
        .uri("/api/inventory/" + pid + "/receive")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"quantity": -10}""")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }
}
