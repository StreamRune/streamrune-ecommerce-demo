package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** A product and its inventory share an id value but never a stream. */
@MicronautTest(transactional = false)
class SharedIdStreamsIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  private List<Map> events(String path) {
    return client
        .toBlocking()
        .retrieve(HttpRequest.GET(path).header("X-User-Role", "ADMIN"), Argument.listOf(Map.class));
  }

  private List<String> eventTypes(List<Map> events) {
    return events.stream().map(e -> (String) e.get("eventType")).toList();
  }

  @Test
  void productAndInventoryWithTheSameId_areTwoStreams() {
    String id = "p-shared-" + System.nanoTime();
    String product =
        """
        {"productId": "%s", "name": "Shared", "description": "shared id", "category": "Test",
         "price": 1.00, "initialStock": 1}"""
            .formatted(id);
    var created =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/products", product)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"));
    assertThat(created.getStatus().getCode()).isEqualTo(HttpStatus.OK.getCode());
    var received =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/inventory/" + id + "/receive", Map.of("quantity", 5))
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"));
    assertThat(received.getStatus().getCode()).isEqualTo(HttpStatus.OK.getCode());

    var productEvents = events("/api/events/product/" + id);
    var inventoryEvents = events("/api/events/inventory/" + id);

    assertThat(eventTypes(productEvents)).containsExactly("ProductCreated");
    assertThat(eventTypes(inventoryEvents))
        .contains("ShipmentReceived")
        .doesNotContain("ProductCreated");
    assertThat(productEvents.get(0)).containsEntry("streamId", "product:" + id);
    assertThat(inventoryEvents.get(0))
        .containsEntry("aggregateType", "inventory")
        .containsEntry("aggregateId", id);
  }

  @Test
  void explorerRefusesANonConformingType_andMatchesNoSingleSegment() {
    var badType =
        assertThrows(
            HttpClientResponseException.class,
            () ->
                client
                    .toBlocking()
                    .exchange(
                        HttpRequest.GET("/api/events/Order/x").header("X-User-Role", "ADMIN")));
    assertThat(badType.getStatus().getCode()).isEqualTo(HttpStatus.BAD_REQUEST.getCode());

    var singleSegment =
        assertThrows(
            HttpClientResponseException.class,
            () ->
                client
                    .toBlocking()
                    .exchange(HttpRequest.GET("/api/events/o-1").header("X-User-Role", "ADMIN")));
    assertThat(singleSegment.getStatus().getCode()).isEqualTo(HttpStatus.NOT_FOUND.getCode());

    assertThat(events("/api/events/order/no-such-order")).isEmpty();
  }
}
