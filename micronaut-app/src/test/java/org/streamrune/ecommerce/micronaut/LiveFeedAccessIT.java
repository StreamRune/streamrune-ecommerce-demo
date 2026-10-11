package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * What the open live feed, {@code GET /api/events/sse}, tells a caller of the running Micronaut
 * app, over real HTTP. The feed cannot ask who is calling, so a frame holds nothing an anonymous
 * caller may not learn: the global offset, the aggregate type, the event type and the timestamp. No
 * frame names a stream, a customer or an order, whatever headers the request carries.
 *
 * <p>The test registers a customer and places an order of its own, reads their two offsets from the
 * event history as an {@code ADMIN}, and then reads the feed from the first event of the store up
 * to those two events. It asserts on the raw text of every frame it read.
 */
@MicronautTest(transactional = false)
class LiveFeedAccessIT extends AbstractIntegrationTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "m-feed-operator";

  private final HttpClient http = HttpClient.newHttpClient();

  @Inject EmbeddedServer server;

  @Test
  void aFrameNamesNoStreamNoCustomerAndNoOrderForACallerWithoutHeaders() throws Exception {
    String customer = registeredCustomer();
    String order = placedOrder(customer);
    long customerOffset = firstOffsetOf("customer", customer);
    long orderOffset = firstOffsetOf("order", order);

    List<List<String>> frames = framesUpTo(Math.max(customerOffset, orderOffset), null, null);

    assertNoFrameNamesAStream(frames, customer, order);
    assertFrame(frameAt(frames, customerOffset), customerOffset, "customer", "CustomerRegistered");
    assertFrame(frameAt(frames, orderOffset), orderOffset, "order", "OrderPlaced");
  }

  @Test
  void theHeadersOfAnAdminAddNothingToAFrame() throws Exception {
    String customer = registeredCustomer();
    String order = placedOrder(customer);
    long customerOffset = firstOffsetOf("customer", customer);
    long orderOffset = firstOffsetOf("order", order);

    List<List<String>> frames =
        framesUpTo(Math.max(customerOffset, orderOffset), OPERATOR, "ADMIN");

    assertNoFrameNamesAStream(frames, customer, order);
    assertFrame(frameAt(frames, customerOffset), customerOffset, "customer", "CustomerRegistered");
    assertFrame(frameAt(frames, orderOffset), orderOffset, "order", "OrderPlaced");
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
      data(frame).fieldNames().forEachRemaining(fields::add);
      assertThat(fields)
          .as("the fields of the frame %s", frame)
          .containsExactly("globalOffset", "aggregateType", "eventType", "timestamp");
    }
  }

  private static void assertFrame(
      List<String> frame, long offset, String aggregateType, String eventType) throws Exception {
    JsonNode data = data(frame);
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
      if (data(frame).path("globalOffset").asLong() == offset) {
        return frame;
      }
    }
    throw new AssertionError("the feed has no frame for the event at offset " + offset);
  }

  /** The frame's data line as JSON. */
  private static JsonNode data(List<String> frame) throws Exception {
    String data =
        frame.stream().filter(l -> l.startsWith("data:")).findFirst().orElseThrow().substring(5);
    return JSON.readTree(data);
  }

  // --- the feed ---

  /**
   * Reads the live feed from its first frame up to the frame of the event at the given offset and
   * returns the lines of every frame read. The feed starts at the first event of the store, so that
   * frame is always reached.
   *
   * @param userId the {@code X-User-Id} header, or {@code null} to send none
   * @param role the {@code X-User-Role} header, or {@code null} to send none
   */
  private List<List<String>> framesUpTo(long offset, String userId, String role) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server().resolve("/api/events/sse"))
            .header("Accept", "text/event-stream")
            .GET();
    if (userId != null) {
      request.header("X-User-Id", userId);
    }
    if (role != null) {
      request.header("X-User-Role", role);
    }
    ExecutorService reader = Executors.newVirtualThreadPerTaskExecutor();
    try (HttpClient feed = HttpClient.newHttpClient()) {
      Future<List<List<String>>> read =
          reader.submit(
              () -> {
                HttpResponse<Stream<String>> response =
                    feed.send(request.build(), HttpResponse.BodyHandlers.ofLines());
                assertThat(response.statusCode()).isEqualTo(200);
                List<List<String>> frames = new ArrayList<>();
                List<String> current = new ArrayList<>();
                try (Stream<String> lines = response.body()) {
                  for (String line : (Iterable<String>) lines::iterator) {
                    if (!line.isEmpty()) {
                      current.add(line);
                    } else if (!current.isEmpty()) {
                      frames.add(current);
                      if (data(current).path("globalOffset").asLong() >= offset) {
                        return frames;
                      }
                      current = new ArrayList<>();
                    }
                  }
                }
                throw new AssertionError("the feed ended before the event at offset " + offset);
              });
      return read.get(60, TimeUnit.SECONDS);
    } finally {
      reader.shutdownNow();
    }
  }

  // --- helpers ---

  private URI server() {
    return server.getURI();
  }

  private static String uniqueId(String prefix) {
    return "m-feed-" + prefix + "-" + System.nanoTime();
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

  /** Places an order as the given customer and returns its id. */
  private String placedOrder(String customer) throws Exception {
    String order = uniqueId("o");
    String body =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": 1, "unitPrice": 12.50}]}"""
            .formatted(order, customer, uniqueId("p"));
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
