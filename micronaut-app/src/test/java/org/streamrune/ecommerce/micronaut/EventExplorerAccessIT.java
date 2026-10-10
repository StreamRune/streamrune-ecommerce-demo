package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Who may read events over HTTP. The two Event Explorer endpoints return event payloads as the
 * event store reads them, decrypted, so they answer only to the ADMIN role. The live feed stays
 * open to every caller and therefore carries no payload: a frame names the event and its stream and
 * nothing else.
 */
@MicronautTest(transactional = false)
class EventExplorerAccessIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject EmbeddedServer server;

  private String registerCustomer(String name, String email) {
    String cid = "m-feed-c-" + System.nanoTime();
    String body =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "1 Feed St",
         "phone": "555-0199"}"""
            .formatted(cid, name, email);
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/customers", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "CUSTOMER")
                    .header("X-User-Id", cid));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
    return cid;
  }

  private void assertForbidden(HttpRequest<?> request) {
    HttpClientResponseException refusal =
        assertThrows(
            HttpClientResponseException.class, () -> client.toBlocking().exchange(request));
    assertThat(refusal.getStatus().getCode()).isEqualTo(403);
  }

  @Test
  void theEventListIsForbiddenWithoutTheAdminRole() {
    assertForbidden(HttpRequest.GET("/api/events"));
    assertForbidden(
        HttpRequest.GET("/api/events?offset=0&limit=5")
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", "cust-1"));
  }

  @Test
  void aStreamIsForbiddenWithoutTheAdminRole() {
    String cid = registerCustomer("Stream Secret", "stream-secret@example.com");

    assertForbidden(HttpRequest.GET("/api/events/customer/" + cid));
    // Not even the customer whose stream it is: the explorer is an operator's tool.
    assertForbidden(
        HttpRequest.GET("/api/events/customer/" + cid)
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", cid));
  }

  @Test
  @SuppressWarnings("unchecked")
  void anAdminReadsTheDecryptedPayload() {
    String cid = registerCustomer("Admin Visible", "admin-visible@example.com");

    List<Map<String, Object>> events =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/events/customer/" + cid)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(events).hasSize(1);
    assertThat(events.getFirst()).containsEntry("eventType", "CustomerRegistered");
    Map<String, Object> payload = (Map<String, Object>) events.getFirst().get("payload");
    assertThat(payload)
        .containsEntry("name", "Admin Visible")
        .containsEntry("email", "admin-visible@example.com");
  }

  @Test
  void aLiveFeedFrameCarriesNoPayload() throws Exception {
    String cid = registerCustomer("Feed Secret", "feed-secret@example.com");

    List<String> frame = frameOf(server.getURI().resolve("/api/events/sse"), "customer:" + cid);

    assertPayloadFree(frame, cid, "Feed Secret", "feed-secret@example.com", "1 Feed St");
  }

  /**
   * Reads the live feed until the frame of the given stream arrives and returns that frame's lines.
   * The feed starts at the first event of the store, so the frame is always reached.
   */
  private static List<String> frameOf(URI feed, String streamId) throws Exception {
    java.net.http.HttpRequest request =
        java.net.http.HttpRequest.newBuilder(feed)
            .header("Accept", "text/event-stream")
            .GET()
            .build();
    ExecutorService reader = Executors.newVirtualThreadPerTaskExecutor();
    try (java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient()) {
      Future<List<String>> frame =
          reader.submit(
              () -> {
                HttpResponse<Stream<String>> response =
                    http.send(request, HttpResponse.BodyHandlers.ofLines());
                assertThat(response.statusCode()).isEqualTo(200);
                List<String> current = new ArrayList<>();
                try (Stream<String> lines = response.body()) {
                  for (String line : (Iterable<String>) lines::iterator) {
                    if (!line.isEmpty()) {
                      current.add(line);
                    } else if (current.stream().anyMatch(l -> l.contains("\"" + streamId + "\""))) {
                      return current;
                    } else {
                      current = new ArrayList<>();
                    }
                  }
                }
                throw new AssertionError("the feed ended without a frame of " + streamId);
              });
      return frame.get(30, TimeUnit.SECONDS);
    } finally {
      reader.shutdownNow();
    }
  }

  /** The frame's data line as JSON. */
  private static JsonNode data(List<String> frame) throws Exception {
    String data =
        frame.stream().filter(l -> l.startsWith("data:")).findFirst().orElseThrow().substring(5);
    return new ObjectMapper().readTree(data);
  }

  private static void assertPayloadFree(List<String> frame, String cid, String... pii)
      throws Exception {
    JsonNode data = data(frame);
    List<String> fields = new ArrayList<>();
    data.fieldNames().forEachRemaining(fields::add);
    assertThat(fields)
        .containsExactlyInAnyOrder(
            "globalOffset",
            "streamId",
            "aggregateType",
            "aggregateId",
            "eventType",
            "version",
            "timestamp");
    assertThat(data.path("eventType").asText()).isEqualTo("CustomerRegistered");
    assertThat(data.path("aggregateType").asText()).isEqualTo("customer");
    assertThat(data.path("aggregateId").asText()).isEqualTo(cid);
    assertThat(data.path("version").asLong()).isEqualTo(1L);
    // An unnamed frame: the browser's EventSource delivers it to onmessage.
    assertThat(frame).noneMatch(l -> l.startsWith("event:"));
    assertThat(String.join("\n", frame)).doesNotContain(pii);
  }
}
