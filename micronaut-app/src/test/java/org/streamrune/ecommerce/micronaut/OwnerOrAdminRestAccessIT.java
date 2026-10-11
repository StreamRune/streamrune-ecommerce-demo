package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Who gets what from the REST API of the running Micronaut app, over real HTTP. The rule is {@code
 * OwnerOrAdminAccess}'s (unit-tested as a table in the {@code queries} module, and for the rows of
 * a list in {@code OwnerOrAdminQueriesTest}): an authenticated {@code ADMIN}, or the customer the
 * data belongs to. This test proves that every customer and order endpoint of the app asks it, with
 * the caller the request named, and which status a refused caller gets: {@code 403}, with or
 * without an identity.
 *
 * <p>Each endpoint is called by four callers: one without an identity, an authenticated customer
 * who is not the owner, the owner, and an {@code ADMIN}. For a list the test asserts exactly which
 * rows come back. Every test registers customers and places orders of its own, so a list read as a
 * customer holds those rows and no others; a list read as an {@code ADMIN} holds every row of the
 * shared database, so the test asserts the rows it knows to be there and, with the {@code
 * customerId} filter, exactly one customer's.
 */
@MicronautTest(transactional = false)
class OwnerOrAdminRestAccessIT extends AbstractIntegrationTest {

  private static final String ADMIN = "ADMIN";
  private static final String CUSTOMER = "CUSTOMER";

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "m-acl-operator";

  private static final ObjectMapper JSON = new ObjectMapper();

  @Inject EmbeddedServer server;

  private final HttpClient http = HttpClient.newHttpClient();

  // --- GET /api/customers/{id} ---

  @Test
  void oneCustomerIsReadByTheCustomerAndByAnAdminOnly() throws Exception {
    String alice = registeredCustomer("alice");
    String bob = registeredCustomer("bob");

    // No identity: refused, whatever the role header says.
    assertThat(get("/api/customers/" + alice, null, null).statusCode()).isEqualTo(403);
    assertThat(get("/api/customers/" + alice, null, ADMIN).statusCode()).isEqualTo(403);
    // Another customer: refused, with the same answer for a customer that does not exist.
    assertThat(get("/api/customers/" + alice, bob, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get("/api/customers/" + uniqueId("nobody"), bob, CUSTOMER).statusCode())
        .isEqualTo(403);
    // The customer themself, with or without a role header.
    HttpResponse<String> own = get("/api/customers/" + alice, alice, CUSTOMER);
    assertThat(own.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(own.body()).get("customerId").asText()).isEqualTo(alice);
    assertThat(JSON.readTree(own.body()).get("email").asText()).isEqualTo(alice + "@example.com");
    assertThat(get("/api/customers/" + alice, alice, null).statusCode()).isEqualTo(200);
    // An ADMIN reads any customer, and learns that one does not exist.
    HttpResponse<String> asAdmin = get("/api/customers/" + alice, OPERATOR, ADMIN);
    assertThat(asAdmin.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(asAdmin.body()).get("customerId").asText()).isEqualTo(alice);
    assertThat(get("/api/customers/" + uniqueId("nobody"), OPERATOR, ADMIN).statusCode())
        .isEqualTo(404);
    // A caller with an identity and no customer record reads "not found" for their own id.
    String carol = uniqueId("carol");
    assertThat(get("/api/customers/" + carol, carol, CUSTOMER).statusCode()).isEqualTo(404);
  }

  // --- GET /api/customers ---

  @Test
  void theCustomerListHoldsTheCallersOwnRecordAndEveryRecordForAnAdmin() throws Exception {
    String alice = registeredCustomer("alice");
    String bob = registeredCustomer("bob");

    assertThat(get("/api/customers", null, null).statusCode()).isEqualTo(403);
    assertThat(get("/api/customers", null, ADMIN).statusCode()).isEqualTo(403);

    assertThat(ids(get("/api/customers", alice, CUSTOMER), "customerId")).containsExactly(alice);
    assertThat(ids(get("/api/customers", bob, CUSTOMER), "customerId")).containsExactly(bob);
    assertThat(ids(get("/api/customers", alice, null), "customerId")).containsExactly(alice);
    // An identity without a customer record: an empty list, not everybody's.
    assertThat(ids(get("/api/customers", uniqueId("carol"), CUSTOMER), "customerId")).isEmpty();
    // Query parameters select nothing: the list is the caller's own record whatever they say.
    assertThat(ids(get("/api/customers?customerId=" + bob, alice, CUSTOMER), "customerId"))
        .containsExactly(alice);
    assertThat(
            ids(get("/api/customers?limit=1000&offset=0&all=true", alice, CUSTOMER), "customerId"))
        .containsExactly(alice);

    assertThat(ids(get("/api/customers", OPERATOR, ADMIN), "customerId")).contains(alice, bob);
  }

  // --- POST /api/customers, PUT /api/customers/{id}, the GDPR routes ---

  @Test
  void aCustomerIsRegisteredAndChangedByTheCustomerAndByAnAdminOnly() throws Exception {
    String alice = registeredCustomer("alice");
    String bob = registeredCustomer("bob");

    // Registration: a customer registers themself, an ADMIN registers anyone.
    String mallory = uniqueId("mallory");
    assertThat(send("POST", "/api/customers", null, null, customerBody(mallory)).statusCode())
        .isEqualTo(403);
    assertThat(send("POST", "/api/customers", null, ADMIN, customerBody(mallory)).statusCode())
        .isEqualTo(403);
    assertThat(send("POST", "/api/customers", bob, CUSTOMER, customerBody(mallory)).statusCode())
        .isEqualTo(403);
    String byAdmin = uniqueId("by-admin");
    assertThat(send("POST", "/api/customers", OPERATOR, ADMIN, customerBody(byAdmin)).statusCode())
        .isBetween(200, 299);
    awaitCustomer(byAdmin);

    // Profile update.
    String update = profileBody("Renamed By Nobody", alice);
    assertThat(send("PUT", "/api/customers/" + alice, null, null, update).statusCode())
        .isEqualTo(403);
    assertThat(send("PUT", "/api/customers/" + alice, null, ADMIN, update).statusCode())
        .isEqualTo(403);
    assertThat(send("PUT", "/api/customers/" + alice, bob, CUSTOMER, update).statusCode())
        .isEqualTo(403);
    assertThat(
            send("PUT", "/api/customers/" + alice, alice, CUSTOMER, profileBody("Alice Own", alice))
                .statusCode())
        .isBetween(200, 299);
    awaitCustomerName(alice, "Alice Own");
    assertThat(
            send("PUT", "/api/customers/" + alice, OPERATOR, ADMIN, profileBody("Alice Ops", alice))
                .statusCode())
        .isBetween(200, 299);
    awaitCustomerName(alice, "Alice Ops");

    // The GDPR routes: the role header alone is nobody, another customer is refused.
    for (String route : new String[] {"/export-data", "/forget"}) {
      String path = "/api/customers/" + alice + route;
      assertThat(send("POST", path, null, null, null).statusCode()).isEqualTo(403);
      assertThat(send("POST", path, null, ADMIN, null).statusCode()).isEqualTo(403);
      assertThat(send("POST", path, bob, CUSTOMER, null).statusCode()).isEqualTo(403);
    }
    assertThat(
            send("POST", "/api/customers/" + alice + "/export-data", alice, CUSTOMER, null)
                .statusCode())
        .isBetween(200, 299);

    // Nothing a refused caller sent was written: mallory has no record, alice kept her name.
    assertThat(get("/api/customers/" + mallory, OPERATOR, ADMIN).statusCode()).isEqualTo(404);
    assertThat(
            JSON.readTree(get("/api/customers/" + alice, OPERATOR, ADMIN).body())
                .get("name")
                .asText())
        .isEqualTo("Alice Ops");
  }

  @Test
  void aForgottenCustomerIsNotFoundForTheCustomerAndForAnAdminAndOnNoList() throws Exception {
    String alice = registeredCustomer("alice");
    String bob = registeredCustomer("bob");

    assertThat(
            send("POST", "/api/customers/" + alice + "/forget", alice, CUSTOMER, null).statusCode())
        .isEqualTo(200);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(get("/api/customers/" + alice, OPERATOR, ADMIN).statusCode())
                    .isEqualTo(404));
    assertThat(get("/api/customers/" + alice, alice, CUSTOMER).statusCode()).isEqualTo(404);
    assertThat(ids(get("/api/customers", alice, CUSTOMER), "customerId")).isEmpty();
    assertThat(ids(get("/api/customers", OPERATOR, ADMIN), "customerId"))
        .contains(bob)
        .doesNotContain(alice);
    // To anyone else the forgotten customer answers as every other customer does.
    assertThat(get("/api/customers/" + alice, bob, CUSTOMER).statusCode()).isEqualTo(403);
  }

  // --- GET /api/orders/{id} ---

  @Test
  void oneOrderIsReadByItsCustomerAndByAnAdminOnly() throws Exception {
    String alice = uniqueId("alice");
    String bob = uniqueId("bob");
    String order = placedOrder(alice);

    assertThat(get("/api/orders/" + order, null, null).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders/" + order, null, ADMIN).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders/" + order, bob, CUSTOMER).statusCode()).isEqualTo(403);
    // An order without a row answers a customer as another customer's order does.
    assertThat(get("/api/orders/" + uniqueId("o-none"), bob, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders/" + uniqueId("o-none"), alice, CUSTOMER).statusCode())
        .isEqualTo(403);
    // An order id equal to the caller's id is not ownership.
    assertThat(get("/api/orders/" + order, order, CUSTOMER).statusCode()).isEqualTo(403);

    HttpResponse<String> own = get("/api/orders/" + order, alice, CUSTOMER);
    assertThat(own.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(own.body()).get("orderId").asText()).isEqualTo(order);
    assertThat(JSON.readTree(own.body()).get("customerId").asText()).isEqualTo(alice);

    HttpResponse<String> asAdmin = get("/api/orders/" + order, OPERATOR, ADMIN);
    assertThat(asAdmin.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(asAdmin.body()).get("orderId").asText()).isEqualTo(order);
    assertThat(get("/api/orders/" + uniqueId("o-none"), OPERATOR, ADMIN).statusCode())
        .isEqualTo(404);
  }

  // --- GET /api/orders ---

  @Test
  void theOrderListHoldsTheCallersOwnOrdersAndEveryOrderForAnAdmin() throws Exception {
    String alice = uniqueId("alice");
    String bob = uniqueId("bob");
    String a1 = placedOrder(alice);
    String a2 = placedOrder(alice);
    String b1 = placedOrder(bob);

    assertThat(get("/api/orders", null, null).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders", null, ADMIN).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders?customerId=" + alice, null, null).statusCode()).isEqualTo(403);

    assertThat(ids(get("/api/orders", alice, CUSTOMER), "orderId"))
        .containsExactlyInAnyOrder(a1, a2);
    assertThat(ids(get("/api/orders", bob, CUSTOMER), "orderId")).containsExactly(b1);
    assertThat(ids(get("/api/orders", alice, null), "orderId")).containsExactlyInAnyOrder(a1, a2);
    assertThat(ids(get("/api/orders", uniqueId("carol"), CUSTOMER), "orderId")).isEmpty();

    // The filter: a customer may repeat their own id and may not name anybody else.
    assertThat(ids(get("/api/orders?customerId=" + alice, alice, CUSTOMER), "orderId"))
        .containsExactlyInAnyOrder(a1, a2);
    assertThat(ids(get("/api/orders?customerId=", alice, CUSTOMER), "orderId"))
        .containsExactlyInAnyOrder(a1, a2);
    assertThat(get("/api/orders?customerId=" + bob, alice, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders?customerId=" + uniqueId("nobody"), alice, CUSTOMER).statusCode())
        .isEqualTo(403);
    // No other parameter widens the list, and neither does naming two customers.
    assertThat(ids(get("/api/orders?limit=1000&offset=0&all=true", alice, CUSTOMER), "orderId"))
        .containsExactlyInAnyOrder(a1, a2);
    HttpResponse<String> twoFilters =
        get("/api/orders?customerId=" + alice + "&customerId=" + bob, alice, CUSTOMER);
    assertThat(twoFilters.statusCode()).isIn(200, 403);
    if (twoFilters.statusCode() == 200) {
      assertThat(ids(twoFilters, "orderId")).containsExactlyInAnyOrder(a1, a2);
    }

    assertThat(ids(get("/api/orders", OPERATOR, ADMIN), "orderId")).contains(a1, a2, b1);
    assertThat(ids(get("/api/orders?customerId=" + alice, OPERATOR, ADMIN), "orderId"))
        .containsExactlyInAnyOrder(a1, a2);
    assertThat(ids(get("/api/orders?customerId=" + bob, OPERATOR, ADMIN), "orderId"))
        .containsExactly(b1);
    assertThat(ids(get("/api/orders?customerId=" + uniqueId("nobody"), OPERATOR, ADMIN), "orderId"))
        .isEmpty();
  }

  // --- POST /api/orders, /api/orders/{id}/confirm, /api/orders/{id}/cancel ---

  @Test
  void anOrderIsPlacedAndChangedByItsCustomerAndByAnAdminOnly() throws Exception {
    String alice = uniqueId("alice");
    String bob = uniqueId("bob");
    String product = stockedProduct();

    // Placing: the order's customer is the caller, unless the caller is an ADMIN.
    String forged = uniqueId("o-forged");
    assertThat(
            send("POST", "/api/orders", null, null, orderBody(forged, alice, product)).statusCode())
        .isEqualTo(403);
    assertThat(
            send("POST", "/api/orders", null, ADMIN, orderBody(forged, alice, product))
                .statusCode())
        .isEqualTo(403);
    assertThat(
            send("POST", "/api/orders", bob, CUSTOMER, orderBody(forged, alice, product))
                .statusCode())
        .isEqualTo(403);
    String byAdmin = uniqueId("o-by-admin");
    assertThat(
            send("POST", "/api/orders", OPERATOR, ADMIN, orderBody(byAdmin, alice, product))
                .statusCode())
        .isBetween(200, 299);
    awaitOrderStatus(byAdmin, "CONFIRMED");
    assertThat(ids(get("/api/orders", alice, CUSTOMER), "orderId")).containsExactly(byAdmin);

    // Confirming and cancelling an order that exists.
    String order = placedOrder(alice);
    String cancel = "{\"reason\": \"changed my mind\"}";
    for (String action : new String[] {"/confirm", "/cancel"}) {
      String path = "/api/orders/" + order + action;
      assertThat(send("POST", path, null, null, cancel).statusCode()).isEqualTo(403);
      assertThat(send("POST", path, null, ADMIN, cancel).statusCode()).isEqualTo(403);
      assertThat(send("POST", path, bob, CUSTOMER, cancel).statusCode()).isEqualTo(403);
    }
    // An order without a row has no known owner: a customer is refused, as for another's order.
    assertThat(
            send("POST", "/api/orders/" + uniqueId("o-none") + "/cancel", alice, CUSTOMER, cancel)
                .statusCode())
        .isEqualTo(403);
    assertThat(get("/api/orders/" + order, OPERATOR, ADMIN).body()).contains("\"CONFIRMED\"");

    assertThat(
            send("POST", "/api/orders/" + order + "/cancel", alice, CUSTOMER, cancel).statusCode())
        .isBetween(200, 299);
    awaitOrderStatus(order, "CANCELLED");
    assertThat(
            send("POST", "/api/orders/" + byAdmin + "/cancel", OPERATOR, ADMIN, cancel)
                .statusCode())
        .isBetween(200, 299);
    awaitOrderStatus(byAdmin, "CANCELLED");

    // Nothing a refused caller sent was written.
    assertThat(get("/api/orders/" + forged, OPERATOR, ADMIN).statusCode()).isEqualTo(404);
  }

  // --- the reads that are for the ADMIN role, and the open ones ---

  @Test
  void operationalReadsAreForTheAdminRole() throws Exception {
    String customer = registeredCustomer("alice");
    for (String path :
        new String[] {
          "/api/audit/commands",
          "/api/saga/fulfillments",
          "/api/events",
          "/api/events/customer/" + customer,
          "/api/admin/circuit-breaker",
          "/api/admin/dead-letters",
          "/api/admin/outbox",
          "/api/admin/outbox/failed",
          "/api/admin/saga-dead-letters",
          "/api/admin/sagas/faulted",
          "/api/admin/payment-failure"
        }) {
      assertThat(get(path, null, null).statusCode())
          .as("%s without an identity", path)
          .isEqualTo(403);
      assertThat(get(path, customer, CUSTOMER).statusCode())
          .as("%s as a customer", path)
          .isEqualTo(403);
      assertThat(get(path, customer, null).statusCode())
          .as("%s as a caller without a role", path)
          .isEqualTo(403);
      assertThat(get(path, OPERATOR, ADMIN).statusCode()).as("%s as an ADMIN", path).isEqualTo(200);
    }

    // One saga: refused before the saga is looked up, so the status says nothing about its
    // existence; an ADMIN learns that it does not exist.
    String saga = "/api/saga/fulfillments/fulfillment-" + uniqueId("o-none");
    assertThat(get(saga, null, null).statusCode()).isEqualTo(403);
    assertThat(get(saga, customer, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get(saga, OPERATOR, ADMIN).statusCode()).isEqualTo(404);
  }

  @Test
  void theProductCatalogueAndTheHealthAnswerAreOpen() throws Exception {
    assertThat(get("/api/products", null, null).statusCode()).isEqualTo(200);
    assertThat(get("/api/products/" + uniqueId("p-none"), null, null).statusCode()).isEqualTo(404);
    assertThat(get("/api/admin/health", null, null).statusCode()).isEqualTo(200);
  }

  // --- helpers ---

  private URI server() {
    return server.getURI();
  }

  private static String uniqueId(String prefix) {
    return "m-acl-" + prefix + "-" + System.nanoTime();
  }

  private HttpResponse<String> get(String path, String userId, String role) throws Exception {
    return send("GET", path, userId, role, null);
  }

  /**
   * One JSON request as the given caller.
   *
   * @param userId the {@code X-User-Id} header, or {@code null} to send none
   * @param role the {@code X-User-Role} header, or {@code null} to send none
   * @param body the JSON body, or {@code null} for a request without one
   */
  private HttpResponse<String> send(
      String method, String path, String userId, String role, String body) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server().resolve(path)).header("Accept", "application/json");
    if (userId != null) {
      request.header("X-User-Id", userId);
    }
    if (role != null) {
      request.header("X-User-Role", role);
    }
    if (body == null) {
      request.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      request
          .header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(body));
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  /** The given field of every element of a {@code 200} JSON array answer. */
  private static List<String> ids(HttpResponse<String> response, String field) throws Exception {
    assertThat(response.statusCode()).isEqualTo(200);
    List<String> ids = new ArrayList<>();
    for (JsonNode row : JSON.readTree(response.body())) {
      ids.add(row.get(field).asText());
    }
    return ids;
  }

  private static String customerBody(String customer) {
    return """
        {"customerId": "%s", "name": "Name %s", "email": "%s@example.com",
         "address": "1 Test Street", "phone": "+420123456789"}"""
        .formatted(customer, customer, customer);
  }

  private static String profileBody(String name, String customer) {
    return """
        {"name": "%s", "email": "%s@example.com", "address": "2 Test Street",
         "phone": "+420123456789"}"""
        .formatted(name, customer);
  }

  private static String orderBody(String order, String customer, String product) {
    return """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": 1, "unitPrice": 12.50}]}"""
        .formatted(order, customer, product);
  }

  /** Registers a customer as that customer, waits for the read-model row and returns the id. */
  private String registeredCustomer(String prefix) throws Exception {
    String customer = uniqueId(prefix);
    assertThat(
            send("POST", "/api/customers", customer, CUSTOMER, customerBody(customer)).statusCode())
        .isBetween(200, 299);
    awaitCustomer(customer);
    return customer;
  }

  private void awaitCustomer(String customer) {
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(get("/api/customers/" + customer, OPERATOR, ADMIN).statusCode())
                    .isEqualTo(200));
  }

  private void awaitCustomerName(String customer, String name) {
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              HttpResponse<String> response = get("/api/customers/" + customer, OPERATOR, ADMIN);
              assertThat(response.statusCode()).isEqualTo(200);
              assertThat(JSON.readTree(response.body()).get("name").asText()).isEqualTo(name);
            });
  }

  /** A product with stock, so that the fulfillment saga confirms the orders that name it. */
  private String stockedProduct() throws Exception {
    String product = uniqueId("p");
    assertThat(
            send(
                    "POST",
                    "/api/inventory/" + product + "/receive",
                    OPERATOR,
                    ADMIN,
                    "{\"quantity\": 20}")
                .statusCode())
        .isBetween(200, 299);
    return product;
  }

  /**
   * Places an order as the given customer and returns its id once the saga confirmed it: by then
   * the order read model has the row that names the order's owner, and the saga has finished with
   * the order's stream, so a later command does not race it.
   */
  private String placedOrder(String customer) throws Exception {
    String order = uniqueId("o");
    assertThat(
            send(
                    "POST",
                    "/api/orders",
                    customer,
                    CUSTOMER,
                    orderBody(order, customer, stockedProduct()))
                .statusCode())
        .isBetween(200, 299);
    awaitOrderStatus(order, "CONFIRMED");
    return order;
  }

  private void awaitOrderStatus(String order, String status) {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              HttpResponse<String> response = get("/api/orders/" + order, OPERATOR, ADMIN);
              assertThat(response.statusCode()).isEqualTo(200);
              assertThat(response.body()).contains("\"status\":\"" + status + "\"");
            });
  }
}
