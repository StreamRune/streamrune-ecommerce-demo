package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/** A product and its inventory share an id value but never a stream. */
class SharedIdStreamsIT extends AbstractIntegrationTest {

  @Autowired EventStore eventStore;

  @Test
  void productAndInventoryWithTheSameId_areTwoStreams_eachStartingAtVersionOne() {
    String id = "p-shared-" + System.nanoTime();
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "%s", "name": "Shared", "description": "shared id", "category": "Test",
             "price": 1.00, "initialStock": 1}"""
                .formatted(id))
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
    client
        .post()
        .uri("/api/inventory/" + id + "/receive")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("{\"quantity\": 5}")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              var product = events("/api/events/product/" + id);
              var inventory = events("/api/events/inventory/" + id);
              assertThat(product)
                  .extracting(e -> e.get("eventType"))
                  .containsExactly("ProductCreated");
              assertThat(inventory)
                  .extracting(e -> e.get("eventType"))
                  .contains("ShipmentReceived")
                  .doesNotContain("ProductCreated");
              assertThat(product)
                  .allSatisfy(e -> assertThat(e.get("streamId")).isEqualTo("product:" + id));
              assertThat(inventory)
                  .allSatisfy(
                      e -> {
                        assertThat(e.get("aggregateType")).isEqualTo("inventory");
                        assertThat(e.get("aggregateId")).isEqualTo(id);
                      });
            });
    assertThat(firstVersion(StreamId.of(ProductState.TYPE, AggregateId.of(id)))).isEqualTo(1);
    assertThat(firstVersion(StreamId.of(InventoryState.TYPE, AggregateId.of(id)))).isEqualTo(1);
  }

  private long firstVersion(StreamId stream) {
    return eventStore.load(stream).events().getFirst().version().value();
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> events(String uri) {
    return client
        .get()
        .uri(uri)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(List.class)
        .returnResult()
        .getResponseBody();
  }
}
