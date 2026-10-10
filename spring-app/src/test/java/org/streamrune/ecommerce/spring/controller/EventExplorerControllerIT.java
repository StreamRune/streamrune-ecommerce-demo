package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

class EventExplorerControllerIT extends AbstractIntegrationTest {

  private String createProduct(String id) {
    String body =
        """
        {
          "productId": "%s",
          "name": "Event Explorer Widget",
          "description": "event test",
          "category": "Test",
          "price": 5.00,
          "initialStock": 10
        }"""
            .formatted(id);
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
    return id;
  }

  @Test
  void listEventsReturnsNonNullList() {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> events =
        client
            .get()
            .uri("/api/events")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(events).isNotNull();
  }

  @Test
  void listEventsWithOffsetAndLimitReturnsOk() {
    client
        .get()
        .uri("/api/events?offset=0&limit=5")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isOk();
  }

  @Test
  void listEventsContainsExpectedFieldsAfterProductCreation() {
    String pid = "evt-prod-" + System.nanoTime();
    createProduct(pid);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> events =
                  client
                      .get()
                      .uri("/api/events?offset=0&limit=200")
                      .header("X-User-Role", "ADMIN")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();

              assertThat(events).isNotNull();
              // each event should have the expected keys
              if (!events.isEmpty()) {
                Map<String, Object> first = events.get(0);
                assertThat(first)
                    .containsKeys("globalOffset", "streamId", "eventType", "timestamp");
              }

              // at least one event for the product stream should be present
              boolean hasProductEvent =
                  events.stream()
                      .anyMatch(
                          e -> {
                            String streamId = (String) e.get("streamId");
                            return streamId != null && streamId.contains(pid);
                          });
              assertThat(hasProductEvent).isTrue();
            });
  }

  @Test
  void streamEventsByStreamIdReturnsEventsForProduct() {
    String pid = "evt-stream-" + System.nanoTime();
    createProduct(pid);

    // The command bus id extractor for ProductCommand returns the productId directly, so the
    // stream is the product type plus the productId value.
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> events =
                  client
                      .get()
                      .uri("/api/events/product/" + pid)
                      .header("X-User-Role", "ADMIN")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();

              assertThat(events).isNotNull();
              assertThat(events).isNotEmpty();
              // all events should belong to this stream
              events.forEach(
                  e -> assertThat((String) e.get("streamId")).isEqualTo("product:" + pid));
            });
  }

  @Test
  void streamEventsByNonExistentStreamIdReturnsEmptyList() {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> events =
        client
            .get()
            .uri("/api/events/product/nonexistent-stream-" + System.nanoTime())
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(events).isNotNull();
    assertThat(events).isEmpty();
  }

  @Test
  void aNonConformingType_isA400() {
    client
        .get()
        .uri("/api/events/Order/x")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isBadRequest();
    client
        .get()
        .uri("/api/events/order-line/x")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void aSingleSegmentOtherThanSse_matchesNoRoute() {
    client
        .get()
        .uri("/api/events/o-1")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  @Test
  void sseEndpointReturnsTextEventStreamContentType() {
    // Only check that the SSE endpoint is reachable and returns the correct content type.
    // We do not consume the full stream to avoid blocking the test indefinitely.
    client
        .get()
        .uri("/api/events/sse")
        .accept(MediaType.TEXT_EVENT_STREAM)
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM);
  }
}
