package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the live Server-Sent Events pipeline against the running Micronaut app
 * (Testcontainers PostgreSQL, random HTTP port), mirroring the Spring app's {@code SseLiveE2EIT}.
 *
 * <p>The application adds no feeder code: with {@code streamrune.sse.enabled=true} the framework
 * serves {@code GET /api/sse/{aggregateType}/{aggregateId}} and runs an {@code SseEventFeed} that
 * reads the global stream from its head and publishes every stored event to the clients of that
 * event's stream. A client subscribes over real HTTP, a command is sent over HTTP, and the frame of
 * the event it stored arrives.
 *
 * <p>The frame's {@code data} is the domain event written by Micronaut Serialization, so this test
 * also proves that the shared domain events, which carry no Micronaut annotation, are imported for
 * it ({@code @SerdeImport} on {@code MicronautEcommerceApplication}).
 *
 * <p>The feed delivers only to clients that are subscribed when it reads an event. The tests
 * therefore wait for the stream's opening comment before they send the command: the endpoint writes
 * {@code : keepalive} as soon as the client is subscribed, and with it the response headers. The
 * application runs with the default {@code streamrune.sse.keep-alive-interval} of 30 seconds, so a
 * stream that opened without that comment would fail these tests.
 */
@MicronautTest(transactional = false)
class SseLiveE2EIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject EmbeddedServer server;

  /**
   * Opens the stream of one order as an operator. The tests subscribe before the order exists, and
   * only an {@code ADMIN} may do that: the order's customer is let in once the order read model
   * names them as its owner ({@code SseStreamAccessIT} covers that side).
   */
  private SseTestStream orderStream(String orderId) {
    return new SseTestStream(server.getURI(), "order", orderId, "sse-operator", "ADMIN");
  }

  private void placeOrder(String orderId, String customerId, String productId) {
    String body =
        """
        {
          "orderId": "%s",
          "customerId": "%s",
          "lines": [
            {"productId": "%s", "quantity": 1, "unitPrice": 12.50}
          ]
        }"""
            .formatted(orderId, customerId, productId);
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/orders", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "CUSTOMER")
                    .header("X-User-Id", customerId));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
  }

  /** The frames whose event is the {@code OrderPlaced} of the given order. */
  private static List<JsonNode> orderPlacedFrames(List<SseTestStream.Frame> frames, String orderId)
      throws IOException {
    ObjectMapper json = new ObjectMapper();
    List<JsonNode> placed = new ArrayList<>();
    for (SseTestStream.Frame frame : frames) {
      JsonNode event = json.readTree(frame.data());
      if (orderId.equals(event.path("orderId").asText()) && event.has("lines")) {
        assertThat(frame.id()).as("the frame's id is the event's global offset").matches("\\d+");
        placed.add(event);
      }
    }
    return placed;
  }

  @Test
  void subscriberReceivesLiveFrameForItsStream() throws Exception {
    String oid = "m-sse-order-" + System.nanoTime();
    String cid = "m-sse-cust-" + System.nanoTime();
    String pid = "m-sse-prod-" + System.nanoTime();

    try (SseTestStream stream = orderStream(oid)) {
      stream.awaitSubscribed();
      // Placed AFTER the stream is subscribed: the feed delivers only to the clients connected
      // when it reads an event and never replays history.
      placeOrder(oid, cid, pid);

      await()
          .atMost(Duration.ofSeconds(15))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () ->
                  assertThat(orderPlacedFrames(stream.frames(), oid))
                      .as("SSE subscriber must receive the OrderPlaced frame of order %s", oid)
                      .hasSize(1));

      // The whole event, nested records included, as Micronaut Serialization wrote it.
      JsonNode placed = orderPlacedFrames(stream.frames(), oid).getFirst();
      assertThat(placed.path("customerId").asText()).isEqualTo(cid);
      assertThat(placed.path("lines")).hasSize(1);
      assertThat(placed.path("lines").path(0).path("productId").asText()).isEqualTo(pid);
      assertThat(placed.path("lines").path(0).path("quantity").asInt()).isEqualTo(1);
      assertThat(placed.path("lines").path(0).path("unitPrice").path("amount").decimalValue())
          .isEqualByComparingTo("12.50");
      assertThat(placed.path("total").path("amount").decimalValue()).isEqualByComparingTo("12.50");
      assertThat(placed.path("total").path("currency").asText()).isEqualTo("USD");
    }
  }

  @Test
  void subscriberIsIsolatedToItsOwnStream() throws Exception {
    String orderA = "m-sse-isoA-" + System.nanoTime();
    String orderB = "m-sse-isoB-" + System.nanoTime();
    String cid = "m-sse-iso-cust-" + System.nanoTime();
    String pid = "m-sse-iso-prod-" + System.nanoTime();

    try (SseTestStream streamA = orderStream(orderA);
        SseTestStream streamB = orderStream(orderB)) {
      // Both streams are subscribed before the event exists, so the control (B receives) is
      // deterministic and the isolation (A does not) is not vacuous.
      streamA.awaitSubscribed();
      streamB.awaitSubscribed();

      placeOrder(orderB, cid, pid);

      await()
          .atMost(Duration.ofSeconds(15))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () ->
                  assertThat(orderPlacedFrames(streamB.frames(), orderB))
                      .as("control: the client of order B receives B's frame")
                      .hasSize(1));

      assertThat(streamA.frames())
          .as("the client of order A receives no frame of order B")
          .noneMatch(frame -> frame.data().contains(orderB));
    }
  }
}
