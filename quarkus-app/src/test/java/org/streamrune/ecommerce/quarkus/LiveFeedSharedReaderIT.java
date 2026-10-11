package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.quarkus.controller.EventExplorerController;
import org.streamrune.ecommerce.queries.access.LiveFeed;

/**
 * What the clients of the open live feed, {@code GET /api/events/sse}, share in the running Quarkus
 * app, over real HTTP: one reader, whose cost does not grow with the number of connections, and a
 * bounded number of places.
 *
 * <p>The app runs one {@link LiveFeed} reader. It asks the event store once per polling interval
 * and hands every frame to every connected client, so the clients see the same frames in the same
 * order. The feed has {@link LiveFeed#MAX_CONNECTIONS} places; the connection that would be one
 * more is answered {@code 503} with {@code Retry-After}. A client that does not read costs the
 * others nothing, and the reader runs only while a client is connected.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class LiveFeedSharedReaderIT {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "q-shared-operator";

  private final HttpClient http = HttpClient.newHttpClient();

  @TestHTTPResource("/")
  URI server;

  @Inject EventExplorerController controller;

  /** Every test starts with all the places free: the clients of an earlier test have left. */
  @BeforeEach
  void everyPlaceIsFree() throws Exception {
    awaitEveryPlaceFree();
  }

  @Test
  void everyClientIsSentTheSameFramesInTheSameOrder() throws Exception {
    List<LiveFeedTestClient> feeds = connected(5);
    try {
      long first = firstOffsetOf("customer", registeredCustomer());
      long second = firstOffsetOf("customer", registeredCustomer());
      long third = firstOffsetOf("customer", registeredCustomer());
      for (LiveFeedTestClient feed : feeds) {
        awaitFrameAt(feed, third);
      }

      List<Long> expected = between(feeds.get(0).offsets(), first, third);
      assertThat(expected).as("the three events, in the order they were stored").contains(first);
      assertThat(expected).containsSubsequence(first, second, third);
      for (LiveFeedTestClient feed : feeds) {
        assertThat(between(feed.offsets(), first, third))
            .as("the frames of every client")
            .isEqualTo(expected);
      }
    } finally {
      feeds.forEach(LiveFeedTestClient::close);
    }
  }

  @Test
  void theStoreIsAskedOncePerPollingIntervalHoweverManyClientsAreConnected() throws Exception {
    Duration window = LiveFeed.POLLING_INTERVAL.multipliedBy(3);
    List<LiveFeedTestClient> feeds = new ArrayList<>(connected(1));
    try {
      long readsForOneClient = storeReadsDuring(window);

      feeds.addAll(connected(7));
      long readsForEightClients = storeReadsDuring(window);

      // Three polling intervals hold three reads, and one more when the window opens just
      // before a read. A reader per connection would ask eight times as often.
      assertThat(readsForOneClient).as("store reads with one client").isBetween(1L, 4L);
      assertThat(readsForEightClients).as("store reads with eight clients").isBetween(1L, 4L);
    } finally {
      feeds.forEach(LiveFeedTestClient::close);
    }
  }

  @Test
  void theConnectionAfterTheLastPlaceIsRefusedAndTheOthersKeepReceiving() throws Exception {
    List<LiveFeedTestClient> feeds = connected(LiveFeed.MAX_CONNECTIONS);
    try {
      assertThat(controller.openFeedConnections()).isEqualTo(LiveFeed.MAX_CONNECTIONS);

      // The answer is read as a stream: a connection that is let in never ends its body.
      HttpResponse<InputStream> refused =
          http.send(
              HttpRequest.newBuilder(server().resolve("/api/events/sse"))
                  .header("Accept", "text/event-stream")
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofInputStream());
      try (InputStream body = refused.body()) {
        assertThat(refused.statusCode()).isEqualTo(503);
        assertThat(refused.headers().firstValue("Retry-After"))
            .hasValue(String.valueOf(LiveFeed.RETRY_AFTER_SECONDS));
        assertThat(new String(body.readAllBytes(), StandardCharsets.UTF_8)).contains("live feed");
      }
      assertThat(controller.openFeedConnections())
          .as("the refused connection holds no place")
          .isEqualTo(LiveFeed.MAX_CONNECTIONS);

      long stored = firstOffsetOf("customer", registeredCustomer());
      for (LiveFeedTestClient feed : feeds) {
        awaitFrameAt(feed, stored);
      }
    } finally {
      feeds.forEach(LiveFeedTestClient::close);
    }

    // A place is free again once its client has left.
    awaitEveryPlaceFree();
    try (LiveFeedTestClient feed = new LiveFeedTestClient(server(), null, null)) {
      feed.awaitConnected();
    }
  }

  @Test
  void aClientThatDoesNotReadDoesNotDelayTheOthers() throws Exception {
    try (Socket stalled = connectedAndNeverRead()) {
      awaitOpenConnections(1);
      List<LiveFeedTestClient> feeds = connected(2);
      try {
        List<Long> stored = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
          long offset = firstOffsetOf("customer", registeredCustomer());
          stored.add(offset);
          for (LiveFeedTestClient feed : feeds) {
            // A frame is on its way within one polling interval of its event.
            await()
                .atMost(LiveFeed.POLLING_INTERVAL.plusSeconds(4))
                .until(() -> feed.offsets().contains(offset));
          }
        }
        assertThat(between(feeds.get(1).offsets(), stored.get(0), stored.get(2)))
            .as("the two clients that read are sent the same frames in the same order")
            .isEqualTo(between(feeds.get(0).offsets(), stored.get(0), stored.get(2)))
            .containsSubsequence(stored);
        assertThat(stalled.isClosed()).isFalse();
      } finally {
        feeds.forEach(LiveFeedTestClient::close);
      }
    }
  }

  @Test
  void aClientThatConnectsAfterAnEventIsNotSentItAndIsSentTheNextOne() throws Exception {
    try (LiveFeedTestClient early = new LiveFeedTestClient(server(), null, null)) {
      early.awaitConnected();
      // Stored while the reader waits for its next round: the late client connects before the
      // reader has read this event.
      long before = firstOffsetOf("customer", registeredCustomer());

      try (LiveFeedTestClient late = new LiveFeedTestClient(server(), null, null)) {
        late.awaitConnected();
        long after = firstOffsetOf("customer", registeredCustomer());
        awaitFrameAt(late, after);
        awaitFrameAt(early, after);

        assertThat(late.offsets())
            .as("the client that connected after the first event")
            .allMatch(offset -> offset > before)
            .contains(after);
        assertThat(early.offsets())
            .as("the client that was connected all along")
            .containsSubsequence(before, after);
      }
    }
  }

  @Test
  void noReaderIsLeftWhenTheLastClientHasGoneAndWhenTheFeedHasStopped() throws Exception {
    awaitNoReader();

    LiveFeedTestClient leaving = new LiveFeedTestClient(server(), null, null);
    leaving.awaitConnected();
    assertThat(controller.feedReaderRuns()).isTrue();
    assertThat(readerThreads()).isEqualTo(1);
    leaving.close();
    awaitEveryPlaceFree();
    awaitNoReader();

    try (LiveFeedTestClient staying = new LiveFeedTestClient(server(), null, null)) {
      staying.awaitConnected();
      try {
        // What the application's shutdown does.
        controller.closeFeed();

        staying.awaitEnded(Duration.ofSeconds(10));
        assertThat(controller.openFeedConnections()).isZero();
        assertThat(controller.feedReaderRuns()).isFalse();
        assertThat(readerThreads()).isZero();
      } finally {
        controller.openFeed();
      }
    }

    // The feed that was built again serves a client again.
    try (LiveFeedTestClient feed = new LiveFeedTestClient(server(), null, null)) {
      feed.awaitConnected();
      awaitFrameAt(feed, firstOffsetOf("customer", registeredCustomer()));
    }
  }

  // --- the feed ---

  /** Opens the given number of connections and returns once each has read the opening comment. */
  private List<LiveFeedTestClient> connected(int count) throws Exception {
    List<LiveFeedTestClient> feeds = new ArrayList<>();
    try {
      for (int i = 0; i < count; i++) {
        feeds.add(new LiveFeedTestClient(server(), null, null));
      }
      for (LiveFeedTestClient feed : feeds) {
        feed.awaitConnected();
      }
      return feeds;
    } catch (Exception | AssertionError e) {
      feeds.forEach(LiveFeedTestClient::close);
      throw e;
    }
  }

  /**
   * A client that asks for the feed and never reads the answer: a browser tab that hangs. Its
   * receive buffer is the smallest the system gives, so what the app sends it is soon not taken.
   */
  private Socket connectedAndNeverRead() throws Exception {
    Socket socket = new Socket();
    socket.setReceiveBufferSize(1024);
    socket.connect(new InetSocketAddress(server().getHost(), server().getPort()), 5_000);
    OutputStream request = socket.getOutputStream();
    request.write(
        ("GET /api/events/sse HTTP/1.1\r\nHost: "
                + server().getHost()
                + ":"
                + server().getPort()
                + "\r\nAccept: text/event-stream\r\n\r\n")
            .getBytes(StandardCharsets.US_ASCII));
    request.flush();
    return socket;
  }

  /** How many times the feed's reader asks the event store while the given time passes. */
  private long storeReadsDuring(Duration window) throws InterruptedException {
    long before = controller.feedStoreReads();
    Thread.sleep(window);
    return controller.feedStoreReads() - before;
  }

  /** The number of live threads with the name of the feed's reader. */
  private static long readerThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.getName().equals(LiveFeed.READER_THREAD_NAME))
        .count();
  }

  private void awaitNoReader() {
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> !controller.feedReaderRuns() && readerThreads() == 0);
  }

  /**
   * Waits until the given number of places is taken. The server cancels the stream of a closed
   * connection at once and the reader forgets the client in its next round; the bound is the one
   * the Spring app's test needs, two keep-alive intervals and a margin.
   */
  private void awaitOpenConnections(int expected) {
    await()
        .atMost(LiveFeed.KEEP_ALIVE_INTERVAL.multipliedBy(2).plusSeconds(15))
        .until(() -> controller.openFeedConnections() == expected);
  }

  /**
   * Waits until no place is taken. A place is freed when the app learns that its client left, and a
   * servlet container learns that from a write that fails: this stores an event per polling
   * interval, so the feed has a frame to write and the test does not wait for the keep-alive.
   */
  private void awaitEveryPlaceFree() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (controller.openFeedConnections() != 0) {
      assertThat(System.nanoTime())
          .as("every place is free within 30 seconds")
          .isLessThan(deadline);
      registeredCustomer();
      Thread.sleep(LiveFeed.POLLING_INTERVAL);
    }
  }

  private static void awaitFrameAt(LiveFeedTestClient feed, long offset) {
    await()
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> feed.offsets().stream().anyMatch(read -> read >= offset));
  }

  /** The offsets from {@code first} to {@code last}, as the client read them. */
  private static List<Long> between(List<Long> offsets, long first, long last) {
    return offsets.stream().filter(offset -> offset >= first && offset <= last).toList();
  }

  // --- helpers ---

  private URI server() {
    return server;
  }

  private static String uniqueId(String prefix) {
    return "q-shared-" + prefix + "-" + System.nanoTime();
  }

  /** Registers a customer as that customer and returns the id. */
  private String registeredCustomer() throws Exception {
    String customer = uniqueId("c");
    String body =
        """
        {"customerId": "%s", "name": "Shared Feed", "email": "shared-feed@example.com",
         "address": "1 Feed St", "phone": "555-0198"}"""
            .formatted(customer);
    assertThat(send("POST", "/api/customers", customer, "CUSTOMER", body).statusCode())
        .isBetween(200, 299);
    return customer;
  }

  /** The global offset of the first event of a stream, read from the history as an ADMIN. */
  private long firstOffsetOf(String aggregateType, String aggregateId) throws Exception {
    HttpResponse<String> history =
        send("GET", "/api/events/" + aggregateType + "/" + aggregateId, OPERATOR, "ADMIN", null);
    assertThat(history.statusCode()).isEqualTo(200);
    return JSON.readTree(history.body()).get(0).get("globalOffset").asLong();
  }

  private HttpResponse<String> send(
      String method, String path, String userId, String role, String body) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server().resolve(path))
            .header("Accept", "application/json")
            .header("X-User-Id", userId)
            .header("X-User-Role", role);
    if (body == null) {
      request.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      request
          .header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(body));
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
