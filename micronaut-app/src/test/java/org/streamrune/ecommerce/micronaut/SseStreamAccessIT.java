package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Who may open {@code GET /api/sse/{aggregateType}/{aggregateId}} on the running Micronaut app,
 * over real HTTP: an authenticated {@code ADMIN}, or the customer the stream belongs to. A frame of
 * the endpoint carries the event with its encrypted fields decrypted, so every other caller must be
 * turned away before a subscription exists.
 *
 * <p>The decisions are {@code OwnerOrAdminStreamAccess}'s (unit-tested as a table in the {@code
 * queries} module); this test proves that the app registers it, that the caller's id and role reach
 * it on the thread the framework's SSE controller calls it on, and which status a refused caller
 * gets: {@code 403}, with or without an identity.
 */
@MicronautTest(transactional = false)
class SseStreamAccessIT extends AbstractIntegrationTest {

  private static final String ADMIN = "ADMIN";
  private static final String CUSTOMER = "CUSTOMER";

  @Inject EmbeddedServer server;

  private final HttpClient http = HttpClient.newHttpClient();

  @Test
  void aCallerWithoutAnIdentityIsRefused() throws Exception {
    String customer = registeredCustomer("anon");

    assertThat(statusOf("customer", customer, null, null)).isEqualTo(403);
    // A role without an identity is still nobody: ADMIN is an authenticated caller.
    assertThat(statusOf("customer", customer, null, ADMIN)).isEqualTo(403);
    assertThat(statusOf("product", uniqueId("p"), null, ADMIN)).isEqualTo(403);
  }

  @Test
  void anotherCustomerIsRefusedOnACustomerStream() throws Exception {
    String customer = registeredCustomer("mine");
    String other = registeredCustomer("other");

    assertThat(statusOf("customer", customer, other, CUSTOMER)).isEqualTo(403);
    assertThat(statusOf("customer", customer, other, null)).isEqualTo(403);
  }

  @Test
  void aCustomerReceivesTheFramesOfTheirOwnStream() throws Exception {
    String customer = registeredCustomer("own");

    try (SseTestStream stream =
        new SseTestStream(server(), "customer", customer, customer, CUSTOMER)) {
      assertThat(stream.status()).isEqualTo(200);
      stream.awaitSubscribed();

      updateProfile(customer, "Renamed " + customer);

      awaitFrameContaining(stream, "Renamed " + customer);
    }
  }

  @Test
  void anAdminReceivesTheFramesOfAnotherCustomersStream() throws Exception {
    String customer = registeredCustomer("watched");

    try (SseTestStream stream =
        new SseTestStream(server(), "customer", customer, uniqueId("ops"), ADMIN)) {
      assertThat(stream.status()).isEqualTo(200);
      stream.awaitSubscribed();

      updateProfile(customer, "Renamed " + customer);

      awaitFrameContaining(stream, "Renamed " + customer);
    }
  }

  @Test
  void aCustomerMayOpenTheirOwnOrderAndNobodyElses() throws Exception {
    String customer = uniqueId("buyer");
    String product = uniqueId("p");
    String order = uniqueId("o");
    stockProduct(product, 5);
    placeOrder(order, customer, product);
    // The saga confirms the order; by then the order read model has its row, which is what names
    // the order's owner.
    awaitOrderStatus(order, "CONFIRMED");

    assertThat(statusOf("order", order, uniqueId("stranger"), CUSTOMER)).isEqualTo(403);

    try (SseTestStream stream = new SseTestStream(server(), "order", order, customer, CUSTOMER)) {
      assertThat(stream.status()).isEqualTo(200);
      stream.awaitSubscribed();

      shipOrderAsAdmin(order);

      awaitFrameContaining(stream, order);
    }
  }

  @Test
  void anOrderTheReadModelHasNoRowForIsRefused() throws Exception {
    // Never placed, so no row names an owner: the rule cannot tell this order from one whose row
    // the projection has yet to write, and refuses both.
    assertThat(statusOf("order", uniqueId("o-unknown"), uniqueId("c"), CUSTOMER)).isEqualTo(403);
  }

  @Test
  void productInventoryAndPaymentStreamsAreForAnAdminOnly() throws Exception {
    String customer = uniqueId("c");
    String operator = uniqueId("ops");

    for (String type : new String[] {"product", "inventory", "payment"}) {
      String id = uniqueId(type);
      assertThat(statusOf(type, id, customer, CUSTOMER))
          .as("a customer on %s", type)
          .isEqualTo(403);
      assertThat(statusOf(type, id, operator, ADMIN)).as("an ADMIN on %s", type).isEqualTo(200);
    }
  }

  @Test
  void anAggregateTypeTheRuleDoesNotNameIsRefused() throws Exception {
    String customer = uniqueId("c");

    assertThat(statusOf("shipment", uniqueId("s"), uniqueId("ops"), ADMIN)).isEqualTo(403);
    assertThat(statusOf("shipment", customer, customer, CUSTOMER)).isEqualTo(403);
  }

  @Test
  void aPathThatIsNoAggregateTypeIsABadRequest() throws Exception {
    // Not the rule's answer: the framework rejects a segment that is not a valid aggregate type
    // before it asks the authorizer.
    assertThat(statusOf("Customer", uniqueId("c"), uniqueId("ops"), ADMIN)).isEqualTo(400);
  }

  private URI server() {
    return server.getURI();
  }

  private static String uniqueId(String prefix) {
    return "m-sse-" + prefix + "-" + System.nanoTime();
  }

  /** The status the SSE endpoint answers the given caller with; the stream is closed at once. */
  private int statusOf(String aggregateType, String aggregateId, String userId, String role)
      throws Exception {
    try (SseTestStream stream =
        new SseTestStream(server(), aggregateType, aggregateId, userId, role)) {
      return stream.status();
    }
  }

  private static void awaitFrameContaining(SseTestStream stream, String text) {
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(stream.frames())
                    .anySatisfy(
                        frame -> {
                          assertThat(frame.data()).contains(text);
                          assertThat(frame.id())
                              .as("the frame's id is the event's global offset")
                              .matches("\\d+");
                        }));
  }

  /** Registers a customer and returns its id. */
  private String registeredCustomer(String prefix) throws Exception {
    String customer = uniqueId(prefix);
    String body =
        """
        {"customerId": "%s", "name": "Name %s", "email": "%s@example.com",
         "address": "1 Test Street", "phone": "+420123456789"}"""
            .formatted(customer, customer, customer);
    assertThat(send("POST", "/api/customers", customer, CUSTOMER, body).statusCode())
        .isBetween(200, 299);
    return customer;
  }

  private void updateProfile(String customer, String name) throws Exception {
    String body =
        """
        {"name": "%s", "email": "%s@example.com", "address": "2 Test Street",
         "phone": "+420123456789"}"""
            .formatted(name, customer);
    assertThat(send("PUT", "/api/customers/" + customer, customer, CUSTOMER, body).statusCode())
        .isBetween(200, 299);
  }

  private void stockProduct(String product, int quantity) throws Exception {
    String body = "{\"quantity\": " + quantity + "}";
    assertThat(
            send("POST", "/api/inventory/" + product + "/receive", "stock-clerk", ADMIN, body)
                .statusCode())
        .isBetween(200, 299);
  }

  private void placeOrder(String order, String customer, String product) throws Exception {
    String body =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": 1, "unitPrice": 12.50}]}"""
            .formatted(order, customer, product);
    assertThat(send("POST", "/api/orders", customer, CUSTOMER, body).statusCode())
        .isBetween(200, 299);
  }

  private void shipOrderAsAdmin(String order) throws Exception {
    assertThat(
            send("POST", "/api/orders/" + order + "/ship", "shipping-clerk", ADMIN, "")
                .statusCode())
        .isBetween(200, 299);
  }

  private void awaitOrderStatus(String order, String status) {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              HttpResponse<String> response =
                  send("GET", "/api/orders/" + order, "stock-clerk", ADMIN, null);
              assertThat(response.statusCode()).isEqualTo(200);
              assertThat(response.body()).contains("\"status\":\"" + status + "\"");
            });
  }

  /** One JSON request as the given caller; {@code body} is {@code null} for a request without. */
  private HttpResponse<String> send(
      String method, String path, String userId, String role, String body) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server().resolve(path))
            .header("X-User-Id", userId)
            .header("X-User-Role", role)
            .header("Accept", "application/json");
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
