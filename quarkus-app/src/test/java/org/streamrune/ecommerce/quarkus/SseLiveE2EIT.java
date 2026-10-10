package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the live Server-Sent Events pipeline against the running Quarkus app
 * (Testcontainers PostgreSQL, the test HTTP port), mirroring the Spring app's {@code SseLiveE2EIT}.
 *
 * <p>The application adds no feeder code: with {@code streamrune.sse.enabled=true} the framework
 * serves {@code GET /api/sse/{aggregateType}/{aggregateId}} and runs an {@code SseEventFeed} that
 * reads the global stream from its head and publishes every stored event to the clients of that
 * event's stream. A client subscribes over real HTTP, a command is sent over HTTP, and the frame of
 * the event it stored arrives.
 *
 * <p>The feed delivers only to clients that are subscribed when it reads an event. The tests
 * therefore wait for the stream's opening comment before they send the command: the endpoint writes
 * {@code : keepalive} as soon as the client is subscribed. On Quarkus the response headers arrive
 * an instant earlier, so the comment, not the {@code 200}, is the sign. The application runs with
 * the default {@code streamrune.sse.keep-alive-interval} of 30 seconds, so a stream that opened
 * without that comment would fail these tests.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class SseLiveE2EIT {

  @TestHTTPResource("/")
  URI server;

  /**
   * Opens the stream of one order as an operator. The tests subscribe before the order exists, and
   * only an {@code ADMIN} may do that: the order's customer is let in once the order read model
   * names them as its owner ({@code SseStreamAccessIT} covers that side).
   */
  private SseTestStream orderStream(String orderId) {
    return new SseTestStream(server, "order", orderId, "sse-operator", "ADMIN");
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
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(ContentType.JSON)
        .body(body)
        .when()
        .post("/api/orders")
        .then()
        .statusCode(200);
  }

  /**
   * The frames of the {@code OrderPlaced} event of the given order: of the order's events, the only
   * one that names the product.
   */
  private static List<SseTestStream.Frame> orderPlacedFrames(
      List<SseTestStream.Frame> frames, String orderId, String productId) {
    return frames.stream()
        .filter(frame -> frame.data().contains(orderId) && frame.data().contains(productId))
        .toList();
  }

  @Test
  void subscriberReceivesLiveFrameForItsStream() throws Exception {
    String oid = "q-sse-order-" + System.nanoTime();
    String cid = "q-sse-cust-" + System.nanoTime();
    String pid = "q-sse-prod-" + System.nanoTime();

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
                  assertThat(orderPlacedFrames(stream.frames(), oid, pid))
                      .as("SSE subscriber must receive the OrderPlaced frame of order %s", oid)
                      .hasSize(1));

      SseTestStream.Frame placed = orderPlacedFrames(stream.frames(), oid, pid).getFirst();
      assertThat(placed.id()).as("the frame's id is the event's global offset").matches("\\d+");
      // The data field is the whole event as JSON, nested records included, as the application's
      // Jackson writer wrote it.
      JsonNode event = new ObjectMapper().readTree(placed.data());
      assertThat(event.path("orderId").asText()).isEqualTo(oid);
      assertThat(event.path("customerId").asText()).isEqualTo(cid);
      assertThat(event.path("lines")).hasSize(1);
      assertThat(event.path("lines").path(0).path("productId").asText()).isEqualTo(pid);
    }
  }

  @Test
  void subscriberIsIsolatedToItsOwnStream() throws Exception {
    String orderA = "q-sse-isoA-" + System.nanoTime();
    String orderB = "q-sse-isoB-" + System.nanoTime();
    String cid = "q-sse-iso-cust-" + System.nanoTime();
    String pid = "q-sse-iso-prod-" + System.nanoTime();

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
                  assertThat(orderPlacedFrames(streamB.frames(), orderB, pid))
                      .as("control: the client of order B receives B's frame")
                      .hasSize(1));

      // A frame of B misrouted to A would travel on another connection and could arrive after
      // B's own. An event of A is stored after B's, so its frame is written to A's client after
      // anything of B could have been: once it is there, a look at what A received is final.
      placeOrder(orderA, cid, pid);
      await()
          .atMost(Duration.ofSeconds(15))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () ->
                  assertThat(orderPlacedFrames(streamA.frames(), orderA, pid))
                      .as("the client of order A receives A's frame")
                      .hasSize(1));

      assertThat(streamA.frames())
          .as("the client of order A receives no frame of order B")
          .noneMatch(frame -> frame.data().contains(orderB));
    }
  }
}
