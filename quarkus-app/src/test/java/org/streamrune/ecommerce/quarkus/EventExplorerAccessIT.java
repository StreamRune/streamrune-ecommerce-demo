package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
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
 * Who may read events over HTTP. The two Event Explorer endpoints return event payloads as the
 * event store reads them, decrypted, so they answer only to the ADMIN role. The live feed stays
 * open to every caller and therefore carries no payload: a frame names the event and its stream and
 * nothing else.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class EventExplorerAccessIT {

  @TestHTTPResource("/api/events/sse")
  URI feed;

  private String registerCustomer(String name, String email) {
    String cid = "q-feed-c-" + System.nanoTime();
    String body =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "1 Feed St",
         "phone": "555-0199"}"""
            .formatted(cid, name, email);
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(ContentType.JSON)
        .body(body)
        .when()
        .post("/api/customers")
        .then()
        .statusCode(200);
    return cid;
  }

  @Test
  void theEventListIsForbiddenWithoutTheAdminRole() {
    given().when().get("/api/events").then().statusCode(403);
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "cust-1")
        .queryParam("offset", 0)
        .queryParam("limit", 5)
        .when()
        .get("/api/events")
        .then()
        .statusCode(403);
  }

  @Test
  void aStreamIsForbiddenWithoutTheAdminRole() {
    String cid = registerCustomer("Stream Secret", "stream-secret@example.com");

    given().when().get("/api/events/customer/" + cid).then().statusCode(403);
    // Not even the customer whose stream it is: the explorer is an operator's tool.
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .when()
        .get("/api/events/customer/" + cid)
        .then()
        .statusCode(403);
  }

  @Test
  void anAdminReadsTheDecryptedPayload() {
    String cid = registerCustomer("Admin Visible", "admin-visible@example.com");

    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .when()
        .get("/api/events/customer/" + cid)
        .then()
        .statusCode(200)
        .body("[0].eventType", equalTo("CustomerRegistered"))
        .body("[0].payload.name", equalTo("Admin Visible"))
        .body("[0].payload.email", equalTo("admin-visible@example.com"));
  }

  @Test
  void aLiveFeedFrameCarriesNoPayload() throws Exception {
    String cid = registerCustomer("Feed Secret", "feed-secret@example.com");

    List<String> frame = frameOf(feed, "customer:" + cid);

    assertPayloadFree(frame, cid, "Feed Secret", "feed-secret@example.com", "1 Feed St");
  }

  /**
   * Reads the live feed until the frame of the given stream arrives and returns that frame's lines.
   * The feed starts at the first event of the store, so the frame is always reached.
   */
  private static List<String> frameOf(URI feed, String streamId) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(feed).header("Accept", "text/event-stream").GET().build();
    ExecutorService reader = Executors.newVirtualThreadPerTaskExecutor();
    try (HttpClient http = HttpClient.newHttpClient()) {
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
