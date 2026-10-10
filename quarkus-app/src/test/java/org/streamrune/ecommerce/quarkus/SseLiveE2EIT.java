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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
 * therefore wait for the first keepalive comment ({@code streamrune.sse.keep-alive-interval},
 * shortened for the test profile in {@code application.properties}) before they send the command: a
 * stream that has written a keepalive is subscribed.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class SseLiveE2EIT {

  @TestHTTPResource("/")
  URI server;

  /** One frame of an SSE stream: its {@code id} and {@code data} fields. */
  private record Frame(String id, String data) {}

  /** An open SSE stream of one order, read on a virtual thread. */
  private static final class OrderStream implements AutoCloseable {

    private final HttpClient http = HttpClient.newHttpClient();
    private final List<Frame> frames = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Void> subscribed = new CompletableFuture<>();

    OrderStream(URI server, String orderId) {
      HttpRequest request =
          HttpRequest.newBuilder(server.resolve("/api/sse/order/" + orderId))
              .header("Accept", "text/event-stream")
              .GET()
              .build();
      Thread.ofVirtual().start(() -> read(request));
    }

    private void read(HttpRequest request) {
      try {
        HttpResponse<Stream<String>> response =
            http.send(request, HttpResponse.BodyHandlers.ofLines());
        if (response.statusCode() != 200) {
          subscribed.completeExceptionally(
              new AssertionError("the stream answered " + response.statusCode()));
          return;
        }
        String id = null;
        String data = null;
        try (Stream<String> lines = response.body()) {
          for (String line : (Iterable<String>) lines::iterator) {
            // Any line, the keepalive comment included, is written by a subscribed stream.
            subscribed.complete(null);
            if (line.startsWith("id:")) {
              id = line.substring(3).trim();
            } else if (line.startsWith("data:")) {
              data = line.substring(5).trim();
            } else if (line.isEmpty()) {
              if (data != null) {
                frames.add(new Frame(id, data));
              }
              id = null;
              data = null;
            }
          }
        }
      } catch (IOException | UncheckedIOException e) {
        // The test closed the stream, or it never opened: awaitSubscribed reports the latter.
        subscribed.completeExceptionally(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        subscribed.completeExceptionally(e);
      }
    }

    /** Returns once the server has written to the stream, so its subscription exists. */
    void awaitSubscribed() throws Exception {
      subscribed.get(15, TimeUnit.SECONDS);
    }

    List<Frame> frames() {
      return frames;
    }

    @Override
    public void close() {
      http.shutdownNow();
    }
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
  private static List<Frame> orderPlacedFrames(
      List<Frame> frames, String orderId, String productId) {
    return frames.stream()
        .filter(frame -> frame.data().contains(orderId) && frame.data().contains(productId))
        .toList();
  }

  @Test
  void subscriberReceivesLiveFrameForItsStream() throws Exception {
    String oid = "q-sse-order-" + System.nanoTime();
    String cid = "q-sse-cust-" + System.nanoTime();
    String pid = "q-sse-prod-" + System.nanoTime();

    try (OrderStream stream = new OrderStream(server, oid)) {
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

      Frame placed = orderPlacedFrames(stream.frames(), oid, pid).getFirst();
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

    try (OrderStream streamA = new OrderStream(server, orderA);
        OrderStream streamB = new OrderStream(server, orderB)) {
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

      assertThat(streamA.frames())
          .as("the client of order A receives no frame of order B")
          .noneMatch(frame -> frame.data().contains(orderB));
    }
  }
}
