package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * End-to-end proof of the live Server-Sent Events pipeline against a real Spring Boot server
 * (Testcontainers PostgreSQL, random HTTP port).
 *
 * <p>This exercises the full wiring added to {@code StreamRuneConfig}: the framework {@code
 * SseController} ({@code GET /api/sse/{aggregateType}/{aggregateId}}), the {@code
 * SseEventPublisher}, and the {@code sse-fanout} polling subscription that pumps every persisted
 * event into the publisher keyed by its stream id. A client subscribes over real HTTP, a command is
 * issued that produces an event on that stream, and the test asserts the subscriber receives the
 * live frame.
 *
 * <p>SSE delivery is asynchronous: the publisher enqueues onto a bounded per-subscriber queue and a
 * dedicated virtual-thread worker drains it, and the {@code sse-fanout} subscription only polls the
 * event store periodically. Every assertion therefore awaits rather than checking synchronously.
 */
class SseLiveE2EIT extends AbstractIntegrationTest {

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
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  /**
   * Opens a real HTTP SSE subscription to {@code /api/sse/order/{orderId}} and collects received
   * frames into a thread-safe list on a background Reactor subscription. The returned {@link
   * Disposable} cancels the subscription (closing the connection) when the test is done.
   */
  private Disposable subscribeSse(String orderId, List<ServerSentEvent<String>> sink) {
    Flux<ServerSentEvent<String>> events =
        client
            .get()
            .uri("/api/sse/order/{orderId}", orderId)
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus()
            .isOk()
            .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
            .getResponseBody();
    return events.subscribe(sink::add);
  }

  @Test
  void subscriberReceivesLiveFrameForItsStream() {
    String oid = "sse-order-" + System.nanoTime();
    String cid = "sse-cust-" + System.nanoTime();
    String pid = "sse-prod-" + System.nanoTime();

    List<ServerSentEvent<String>> received = new CopyOnWriteArrayList<>();
    Disposable sub = subscribeSse(oid, received);
    try {
      // Place the order AFTER the subscription is open so the OrderPlaced event is delivered live.
      // (The fanout subscription replays from the start too, but placing after open makes the
      // "live delivery" claim non-vacuous: the frame arrives without the client reconnecting.)
      placeOrder(oid, cid, pid);

      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () -> {
                // The frame's data is the OrderPlaced event serialized as JSON; its id is the
                // global offset. We assert a frame carrying the order id arrived on this stream.
                assertThat(received)
                    .as("SSE subscriber must receive a live frame for stream %s", oid)
                    .anySatisfy(
                        frame -> {
                          assertThat(frame.data()).isNotNull();
                          assertThat(frame.data()).contains(oid);
                          assertThat(frame.id()).isNotNull();
                        });
              });
    } finally {
      sub.dispose();
    }
  }

  @Test
  void subscriberIsIsolatedToItsOwnStream() {
    String streamA = "sse-isoA-" + System.nanoTime();
    String streamB = "sse-isoB-" + System.nanoTime();
    String cid = "sse-iso-cust-" + System.nanoTime();
    String pid = "sse-iso-prod-" + System.nanoTime();

    // Open BOTH subscriptions before emitting, so both are live when the fanout polls the event.
    // The fanout has a single advancing offset shared by the whole server; only subscribers that
    // are already registered when an event is polled receive it. Subscribing first makes the
    // control (B receives) deterministic and the isolation (A does not) non-vacuous.
    List<ServerSentEvent<String>> receivedA = new CopyOnWriteArrayList<>();
    List<ServerSentEvent<String>> receivedB = new CopyOnWriteArrayList<>();
    Disposable subA = subscribeSse(streamA, receivedA);
    Disposable subB = subscribeSse(streamB, receivedB);
    try {
      // Emit an event ONLY on stream B (orderId == streamB). The publisher fans out per stream id,
      // so the subscriber on stream A must never see it.
      placeOrder(streamB, cid, pid);

      // Control: stream B's subscriber must receive B's frame (proves the fanout actually ran —
      // otherwise the negative assertion below would be vacuously true).
      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () ->
                  assertThat(receivedB)
                      .as("control: stream B subscriber must receive B's frame")
                      .anySatisfy(f -> assertThat(f.data()).contains(streamB)));

      // Isolation: the stream A subscriber never received B's frame.
      assertThat(receivedA)
          .as("stream A subscriber must not receive frames from stream B")
          .noneSatisfy(f -> assertThat(f.data()).contains(streamB));
    } finally {
      subA.dispose();
      subB.dispose();
    }
  }
}
