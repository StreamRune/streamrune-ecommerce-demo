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
 * <p>The application adds no feeder code: with {@code streamrune.sse.enabled=true} the framework
 * registers the {@code SseEventPublisher} behind {@code GET /api/sse/{aggregateType}/{aggregateId}}
 * and an {@code SseEventFeed} that reads the global stream from its head and publishes every stored
 * event to the subscribers of that event's stream. A client subscribes over real HTTP, a command is
 * issued that produces an event on that stream, and the test asserts the subscriber receives the
 * live frame.
 *
 * <p>SSE delivery is asynchronous: the feed polls the event store every {@code
 * streamrune.sse.polling-interval}, the publisher enqueues onto a bounded per-subscriber queue and
 * a dedicated virtual-thread worker drains it. Every assertion therefore awaits rather than
 * checking synchronously.
 *
 * <p>The feed delivers only to clients that are subscribed when it reads an event. The tests
 * therefore wait for the stream's opening comment before they send the command: the endpoint writes
 * {@code : keepalive} as soon as the client is subscribed, and on Spring that write is also what
 * commits the response. The wait is {@link #OPENS_WITHIN}, and the tests run with the periodic
 * keepalive out of reach ({@code AbstractIntegrationTest} sets {@code
 * streamrune.sse.keep-alive-interval} to an hour), so a stream that opened without that comment
 * fails these tests.
 */
class SseLiveE2EIT extends AbstractIntegrationTest {

  /** How long a stream may take to answer and deliver its opening comment. */
  private static final Duration OPENS_WITHIN = Duration.ofSeconds(5);

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
   * Opens a real HTTP SSE subscription to {@code /api/sse/order/{orderId}}, waits for its opening
   * comment, and collects received frames into a thread-safe list on a background Reactor
   * subscription. The returned {@link Disposable} cancels the subscription (closing the connection)
   * when the test is done.
   *
   * <p>The subscriber is an operator. The tests subscribe before the order exists, and only an
   * {@code ADMIN} may do that: the order's customer is let in once the order read model names them
   * as its owner ({@code SseStreamAccessIT} covers that side).
   */
  private Disposable subscribeSse(String orderId, List<ServerSentEvent<String>> sink) {
    Flux<ServerSentEvent<String>> events =
        client
            .mutate()
            .responseTimeout(OPENS_WITHIN)
            .build()
            .get()
            .uri("/api/sse/order/{orderId}", orderId)
            .header("X-User-Id", "sse-operator")
            .header("X-User-Role", "ADMIN")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus()
            .isOk()
            .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
            .getResponseBody();
    // exchange() waited at most OPENS_WITHIN for the response head. Nothing but the opening
    // comment commits the response within a test run (see the class documentation), so without it
    // the call above has already failed. The comment itself is the first frame the client reads.
    Disposable subscription = events.subscribe(sink::add);
    try {
      await()
          .atMost(OPENS_WITHIN)
          .untilAsserted(
              () ->
                  assertThat(sink)
                      .as("the stream of %s opens with its ': keepalive' comment", orderId)
                      .first()
                      .satisfies(frame -> assertThat(frame.comment()).contains("keepalive")));
    } catch (RuntimeException | Error notOpened) {
      subscription.dispose();
      throw notOpened;
    }
    return subscription;
  }

  @Test
  void subscriberReceivesLiveFrameForItsStream() {
    String oid = "sse-order-" + System.nanoTime();
    String cid = "sse-cust-" + System.nanoTime();
    String pid = "sse-prod-" + System.nanoTime();

    List<ServerSentEvent<String>> received = new CopyOnWriteArrayList<>();
    Disposable sub = subscribeSse(oid, received);
    try {
      // Place the order AFTER the subscription is open: the feed delivers only to the clients
      // connected when it reads an event and never replays history, so an order placed earlier
      // would produce no frame.
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

    // Open BOTH subscriptions before emitting, so both are live when the feed reads the event.
    // The feed has a single advancing offset shared by the whole server; only subscribers that
    // are already registered when an event is read receive it. Subscribing first makes the
    // control (B receives) deterministic and the isolation (A does not) non-vacuous.
    List<ServerSentEvent<String>> receivedA = new CopyOnWriteArrayList<>();
    List<ServerSentEvent<String>> receivedB = new CopyOnWriteArrayList<>();
    Disposable subA = subscribeSse(streamA, receivedA);
    Disposable subB = subscribeSse(streamB, receivedB);
    try {
      // Emit an event on stream B first (orderId == streamB). The publisher fans out per stream id,
      // so the subscriber on stream A must never see it.
      placeOrder(streamB, cid, pid);

      // Control: stream B's subscriber must receive B's frame (proves the feed actually ran —
      // otherwise the negative assertion below would be vacuously true).
      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () ->
                  assertThat(receivedB)
                      .as("control: stream B subscriber must receive B's frame")
                      .anySatisfy(f -> assertThat(f.data()).contains(streamB)));

      // A frame of B misrouted to A would travel on another connection and could arrive after
      // B's own. An event of A is stored after B's, so its frame is written to A's client after
      // anything of B could have been: once it is there, a look at what A received is final.
      placeOrder(streamA, cid, pid);
      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () ->
                  assertThat(receivedA)
                      .as("stream A subscriber must receive A's frame")
                      .anySatisfy(f -> assertThat(f.data()).contains(streamA)));

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
