package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.streamrune.ecommerce.queries.access.LiveFeed;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * What the open live feed, {@code GET /api/events/sse}, tells a caller of the running Spring Boot
 * app, over real HTTP, and what a connection costs the app.
 *
 * <p><b>A frame names nobody.</b> The feed cannot ask who is calling, so a frame holds nothing an
 * anonymous caller may not learn: the global offset, the aggregate type, the event type and the
 * timestamp. No frame names a stream, a customer or an order, whatever headers the request carries.
 *
 * <p><b>The feed is live.</b> A connection starts at the head of the global stream: it is sent
 * nothing that was stored before it connected and a frame for every event stored after it. The
 * place of a client that left is free again, and the feed's one reader ends with its last client.
 */
class LiveFeedAccessIT extends AbstractIntegrationTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "s-feed-operator";

  private final HttpClient http = HttpClient.newHttpClient();

  @Autowired EventExplorerController controller;

  @Test
  void aFrameNamesNoStreamNoCustomerAndNoOrderForACallerWithoutHeaders() throws Exception {
    try (LiveFeedTestClient feed = new LiveFeedTestClient(server(), null, null)) {
      feed.awaitConnected();
      String customer = registeredCustomer();
      String order = placedOrder(customer);
      long customerOffset = firstOffsetOf("customer", customer);
      long orderOffset = firstOffsetOf("order", order);
      awaitFrameAt(feed, Math.max(customerOffset, orderOffset));

      List<List<String>> frames = List.copyOf(feed.frames());
      assertNoFrameNamesAStream(frames, customer, order);
      assertFrame(
          frameAt(frames, customerOffset), customerOffset, "customer", "CustomerRegistered");
      assertFrame(frameAt(frames, orderOffset), orderOffset, "order", "OrderPlaced");
    }
  }

  @Test
  void theHeadersOfAnAdminAddNothingToAFrame() throws Exception {
    try (LiveFeedTestClient feed = new LiveFeedTestClient(server(), OPERATOR, "ADMIN")) {
      feed.awaitConnected();
      String customer = registeredCustomer();
      String order = placedOrder(customer);
      long customerOffset = firstOffsetOf("customer", customer);
      long orderOffset = firstOffsetOf("order", order);
      awaitFrameAt(feed, Math.max(customerOffset, orderOffset));

      List<List<String>> frames = List.copyOf(feed.frames());
      assertNoFrameNamesAStream(frames, customer, order);
      assertFrame(
          frameAt(frames, customerOffset), customerOffset, "customer", "CustomerRegistered");
      assertFrame(frameAt(frames, orderOffset), orderOffset, "order", "OrderPlaced");
    }
  }

  @Test
  void aConnectionIsSentNothingStoredBeforeItAndEveryEventStoredAfterIt() throws Exception {
    long storedBefore = firstOffsetOf("customer", registeredCustomer());

    try (LiveFeedTestClient feed = new LiveFeedTestClient(server(), null, null)) {
      feed.awaitConnected();
      // One event at a time, each after the feed has caught up with the one before.
      long first = firstOffsetOf("customer", registeredCustomer());
      awaitFrameAt(feed, first);
      long second = firstOffsetOf("customer", registeredCustomer());
      awaitFrameAt(feed, second);

      List<Long> offsets = feed.offsets();
      assertThat(offsets)
          .as("nothing of what the store held when the client connected")
          .allMatch(offset -> offset > storedBefore);
      assertThat(offsets).as("the two events stored afterwards").contains(first, second);
      for (int i = 1; i < offsets.size(); i++) {
        assertThat(offsets.get(i))
            .as("the frame after the event at offset %s", offsets.get(i - 1))
            .isEqualTo(offsets.get(i - 1) + 1);
      }
    }
  }

  @Test
  void thePlaceOfAClientThatLeftIsFreeAgain() throws Exception {
    // The place of an earlier test's client is freed when that client has left; wait for the
    // last of them.
    awaitOpenConnections(0);

    LiveFeedTestClient feed = new LiveFeedTestClient(server(), null, null);
    feed.awaitConnected();
    awaitOpenConnections(1);

    feed.close();

    // Nothing is stored in the meantime: the place is freed although the store stays quiet.
    awaitOpenConnections(0);
  }

  /**
   * Waits until the feed serves the given number of clients. The servlet container learns of a
   * closed connection when a write to it fails, and the write that follows a client's close can
   * still succeed: the bound is two keep-alive intervals and a margin.
   */
  private void awaitOpenConnections(int expected) {
    await()
        .atMost(LiveFeed.KEEP_ALIVE_INTERVAL.multipliedBy(2).plusSeconds(15))
        .until(() -> controller.openFeedConnections() == expected);
  }

  private static void awaitFrameAt(LiveFeedTestClient feed, long offset) {
    await()
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> feed.offsets().stream().anyMatch(read -> read >= offset));
  }

  // --- assertions ---

  /**
   * Every frame holds the four fields and no other, and the raw text of the feed holds neither of
   * the two ids, no field that would name a stream and nothing of the customer's personal data.
   */
  private static void assertNoFrameNamesAStream(
      List<List<String>> frames, String customer, String order) throws Exception {
    String raw = String.join("\n\n", frames.stream().map(f -> String.join("\n", f)).toList());
    assertThat(raw)
        .doesNotContain(
            customer,
            order,
            "streamId",
            "aggregateId",
            "version",
            "payload",
            "Feed Secret",
            "feed-secret@example.com",
            "1 Feed St");
    for (List<String> frame : frames) {
      // An unnamed frame, an id line and a data line: EventSource delivers it to onmessage.
      assertThat(frame)
          .as("the lines of a frame")
          .allMatch(line -> line.startsWith("id:") || line.startsWith("data:"));
      List<String> fields = new ArrayList<>();
      LiveFeedTestClient.data(frame).fieldNames().forEachRemaining(fields::add);
      assertThat(fields)
          .as("the fields of the frame %s", frame)
          .containsExactly("globalOffset", "aggregateType", "eventType", "timestamp");
    }
  }

  private static void assertFrame(
      List<String> frame, long offset, String aggregateType, String eventType) throws Exception {
    JsonNode data = LiveFeedTestClient.data(frame);
    assertThat(data.path("globalOffset").asLong()).isEqualTo(offset);
    assertThat(data.path("aggregateType").asText()).isEqualTo(aggregateType);
    assertThat(data.path("eventType").asText()).isEqualTo(eventType);
    assertThat(data.path("timestamp").asText()).isNotEmpty();
    // The frame's id is the global offset, the position a client has read up to.
    assertThat(frame)
        .filteredOn(line -> line.startsWith("id:"))
        .singleElement()
        .satisfies(line -> assertThat(line.substring(3).trim()).isEqualTo(String.valueOf(offset)));
  }

  private static List<String> frameAt(List<List<String>> frames, long offset) throws Exception {
    for (List<String> frame : frames) {
      if (LiveFeedTestClient.data(frame).path("globalOffset").asLong() == offset) {
        return frame;
      }
    }
    throw new AssertionError("the feed has no frame for the event at offset " + offset);
  }

  // --- helpers ---

  private URI server() {
    return URI.create("http://localhost:" + port);
  }

  private static String uniqueId(String prefix) {
    return "s-feed-" + prefix + "-" + System.nanoTime();
  }

  /** Registers a customer as that customer and returns the id. */
  private String registeredCustomer() throws Exception {
    String customer = uniqueId("c");
    String body =
        """
        {"customerId": "%s", "name": "Feed Secret", "email": "feed-secret@example.com",
         "address": "1 Feed St", "phone": "555-0199"}"""
            .formatted(customer);
    assertThat(send("POST", "/api/customers", customer, "CUSTOMER", body).statusCode())
        .isBetween(200, 299);
    return customer;
  }

  /** Creates a product in the catalogue, which prices an order's line, and returns its id. */
  private String createdProduct() throws Exception {
    String product = uniqueId("p");
    String body =
        """
        {"productId": "%s", "name": "Feed product", "category": "General", "price": 12.50,
         "initialStock": 0}"""
            .formatted(product);
    assertThat(send("POST", "/api/products", OPERATOR, "ADMIN", body).statusCode())
        .isBetween(200, 299);
    return product;
  }

  /** Places an order as the given customer and returns its id. */
  private String placedOrder(String customer) throws Exception {
    String order = uniqueId("o");
    String body =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": 1}]}"""
            .formatted(order, customer, createdProduct());
    assertThat(send("POST", "/api/orders", customer, "CUSTOMER", body).statusCode())
        .isBetween(200, 299);
    return order;
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
