package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Who gets what from the REST API of the running Quarkus app, over real HTTP. The rule is {@code
 * OwnerOrAdminAccess}'s (unit-tested as a table in the {@code queries} module, and for the rows of
 * a list in {@code OwnerOrAdminQueriesTest}): an authenticated {@code ADMIN}, or the customer the
 * data belongs to. This test proves that every customer, order and fulfillment endpoint of the app
 * asks it, with the caller the request named, and which status a refused caller gets: {@code 403},
 * with or without an identity.
 *
 * <p>Each endpoint is called by four callers: one without an identity (with and without the {@code
 * ADMIN} role header), an authenticated customer who is not the owner, the owner, and an {@code
 * ADMIN}. The endpoints that are for an {@code ADMIN} alone refuse the first three and an identity
 * without a role. For a list the test asserts exactly which rows come back. Every test registers
 * customers and places orders of its own, so a list read as a customer holds those rows and no
 * others; a list read as an {@code ADMIN} holds every row of the shared database, so the test
 * asserts the rows it knows to be there and, with the {@code customerId} filter, exactly one
 * customer's.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class OwnerOrAdminRestAccessIT {

  private static final String ADMIN = "ADMIN";
  private static final String CUSTOMER = "CUSTOMER";

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "q-acl-operator";

  private static final ObjectMapper JSON = new ObjectMapper();

  @TestHTTPResource("/")
  URI server;

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
    String order = placedOrder(alice);

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
    // The forgotten customer's stream takes no further event: an export has nothing to hand out.
    assertThat(
            send("POST", "/api/customers/" + alice + "/export-data", alice, CUSTOMER, null)
                .statusCode())
        .isEqualTo(400);
    assertThat(
            send("POST", "/api/customers/" + alice + "/export-data", OPERATOR, ADMIN, null)
                .statusCode())
        .isEqualTo(400);
    // Her order outlives the customer record, and the same rule guards it: the former owner
    // and an ADMIN read it, nobody else does.
    assertThat(get("/api/orders/" + order, alice, CUSTOMER).statusCode()).isEqualTo(200);
    assertThat(ids(get("/api/orders", alice, CUSTOMER), "orderId")).containsExactly(order);
    assertThat(get("/api/orders/" + order, OPERATOR, ADMIN).statusCode()).isEqualTo(200);
    assertThat(get("/api/orders/" + order, bob, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get("/api/orders/" + order, null, ADMIN).statusCode()).isEqualTo(403);
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

  // --- POST /api/orders, /api/orders/{id}/cancel ---

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

    // Cancelling an order that exists.
    String order = placedOrder(alice);
    String cancel = "{\"reason\": \"changed my mind\"}";
    String cancelPath = "/api/orders/" + order + "/cancel";
    assertThat(send("POST", cancelPath, null, null, cancel).statusCode()).isEqualTo(403);
    assertThat(send("POST", cancelPath, null, ADMIN, cancel).statusCode()).isEqualTo(403);
    assertThat(send("POST", cancelPath, bob, CUSTOMER, cancel).statusCode()).isEqualTo(403);
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

  // --- GET /api/saga/fulfillments/{id} ---

  @Test
  void oneFulfillmentIsReadByTheCustomerOfItsOrderAndByAnAdminOnly() throws Exception {
    String alice = uniqueId("alice");
    String bob = uniqueId("bob");
    String order = placedOrder(alice);
    String saga = "/api/saga/fulfillments/fulfillment-" + order;
    awaitFulfillmentStatus(order, "COMPLETED");

    // No identity: refused, whatever the role header says.
    assertThat(get(saga, null, null).statusCode()).isEqualTo(403);
    assertThat(get(saga, null, ADMIN).statusCode()).isEqualTo(403);
    // Another customer: refused, with the answer an order of somebody else gets.
    HttpResponse<String> refused = get(saga, bob, CUSTOMER);
    assertThat(refused.statusCode()).isEqualTo(403);
    assertThat(refused.body()).isEqualTo(get("/api/orders/" + order, bob, CUSTOMER).body());
    assertThat(refused.body()).doesNotContain(order, alice);
    // The saga of an order without a row answers a customer as another customer's saga does.
    String none = "/api/saga/fulfillments/fulfillment-" + uniqueId("o-none");
    assertThat(get(none, bob, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get(none, alice, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get(none, alice, CUSTOMER).body()).isEqualTo(refused.body());
    // An id that is not the fulfillment id of an order has no owner: the order id alone, the
    // caller's own id, the prefix with nothing behind it.
    assertThat(get("/api/saga/fulfillments/" + order, alice, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get("/api/saga/fulfillments/" + alice, alice, CUSTOMER).statusCode()).isEqualTo(403);
    assertThat(get("/api/saga/fulfillments/fulfillment-", alice, CUSTOMER).statusCode())
        .isEqualTo(403);

    // The order's customer, with or without a role header: the saga of their own order.
    HttpResponse<String> own = get(saga, alice, CUSTOMER);
    assertThat(own.statusCode()).isEqualTo(200);
    JsonNode state = JSON.readTree(own.body());
    List<String> fields = new ArrayList<>();
    state.fieldNames().forEachRemaining(fields::add);
    assertThat(fields)
        .containsExactlyInAnyOrder(
            "sagaId",
            "orderId",
            "customerId",
            "paymentId",
            "orderTotal",
            "lines",
            "reservedProductIds",
            "fulfillmentStatus");
    assertThat(state.get("orderId").asText()).isEqualTo(order);
    assertThat(state.get("customerId").asText()).isEqualTo(alice);
    assertThat(state.get("paymentId").asText()).isEqualTo("pay-" + order);
    assertThat(state.get("fulfillmentStatus").asText()).isEqualTo("COMPLETED");
    assertThat(get(saga, alice, null).statusCode()).isEqualTo(200);

    // An ADMIN reads any saga, the same answer the owner gets, and learns that one does not exist.
    HttpResponse<String> asAdmin = get(saga, OPERATOR, ADMIN);
    assertThat(asAdmin.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(asAdmin.body())).isEqualTo(state);
    assertThat(get(none, OPERATOR, ADMIN).statusCode()).isEqualTo(404);
  }

  // --- GET /api/saga/fulfillments ---

  @Test
  void theFulfillmentListHoldsTheSagasOfTheCallersOwnOrdersAndEverySagaForAnAdmin()
      throws Exception {
    String alice = uniqueId("alice");
    String bob = uniqueId("bob");
    String a1 = "fulfillment-" + placedOrder(alice);
    String a2 = "fulfillment-" + placedOrder(alice);
    String b1 = "fulfillment-" + placedOrder(bob);
    String list = "/api/saga/fulfillments";

    assertThat(get(list, null, null).statusCode()).isEqualTo(403);
    assertThat(get(list, null, ADMIN).statusCode()).isEqualTo(403);
    assertThat(get(list + "?limit=1000", null, null).statusCode()).isEqualTo(403);

    assertThat(ids(get(list, alice, CUSTOMER), "sagaId")).containsExactlyInAnyOrder(a1, a2);
    assertThat(ids(get(list, bob, CUSTOMER), "sagaId")).containsExactly(b1);
    assertThat(ids(get(list, alice, null), "sagaId")).containsExactlyInAnyOrder(a1, a2);
    // An identity without an order: an empty list, not everybody's.
    assertThat(ids(get(list, uniqueId("carol"), CUSTOMER), "sagaId")).isEmpty();
    // No parameter widens the list, and naming another customer selects nothing of theirs.
    assertThat(ids(get(list + "?limit=1000&offset=0&all=true", alice, CUSTOMER), "sagaId"))
        .containsExactlyInAnyOrder(a1, a2);
    assertThat(ids(get(list + "?customerId=" + bob, alice, CUSTOMER), "sagaId"))
        .containsExactlyInAnyOrder(a1, a2);
    // The page size narrows the caller's own rows: the highest saga id first.
    assertThat(ids(get(list + "?limit=1", alice, CUSTOMER), "sagaId"))
        .containsExactly(a1.compareTo(a2) > 0 ? a1 : a2);
    assertThat(ids(get(list + "?limit=0", alice, CUSTOMER), "sagaId")).isEmpty();

    // A row: the saga, its type, the framework's status and the state, which names the caller.
    for (JsonNode row : JSON.readTree(get(list, alice, CUSTOMER).body())) {
      List<String> fields = new ArrayList<>();
      row.fieldNames().forEachRemaining(fields::add);
      assertThat(fields).containsExactlyInAnyOrder("sagaId", "sagaType", "status", "state");
      JsonNode state = JSON.readTree(row.get("state").asText());
      assertThat(state.get("customerId").asText()).isEqualTo(alice);
      assertThat("fulfillment-" + state.get("orderId").asText())
          .isEqualTo(row.get("sagaId").asText());
    }
    assertThat(get(list, alice, CUSTOMER).body()).doesNotContain(bob, b1);

    // An ADMIN lists every saga.
    assertThat(ids(get(list + "?limit=100000", OPERATOR, ADMIN), "sagaId")).contains(a1, a2, b1);
    assertThat(ids(get(list + "?limit=1", OPERATOR, ADMIN), "sagaId")).hasSize(1);

    // A page size that cannot be read is a 400 for a caller the rule lets in, a 403 for the rest.
    assertThat(get(list + "?limit=-1", alice, CUSTOMER).statusCode()).isEqualTo(400);
    assertThat(get(list + "?limit=-1", OPERATOR, ADMIN).statusCode()).isEqualTo(400);
    assertThat(get(list + "?limit=-1", null, null).statusCode()).isEqualTo(403);
    assertThat(get(list + "?limit=-1", null, ADMIN).statusCode()).isEqualTo(403);
  }

  // --- the endpoints that are for an ADMIN, and the open ones ---

  @Test
  void operationalReadsAreForAnAdmin() throws Exception {
    String customer = registeredCustomer("alice");
    for (String path :
        new String[] {
          "/api/audit/commands",
          "/api/events",
          "/api/events/customer/" + customer,
          "/api/admin/circuit-breaker",
          "/api/admin/dead-letters",
          "/api/admin/outbox",
          "/api/admin/outbox/failed",
          "/api/admin/saga-dead-letters",
          "/api/admin/sagas/faulted",
          "/api/admin/payment-failure",
          "/api/admin/payment-outage"
        }) {
      assertRefusedForEveryoneButAnAdmin("GET", path, null, customer);
      assertThat(get(path, OPERATOR, ADMIN).statusCode()).as("%s as an ADMIN", path).isEqualTo(200);
    }

    // A page size that cannot be read is the ADMIN's mistake, a 400, and still a 403 for the rest.
    assertRefusedForEveryoneButAnAdmin("GET", "/api/events?limit=-1", null, customer);
    assertThat(get("/api/events?limit=-1", OPERATOR, ADMIN).statusCode()).isEqualTo(400);
  }

  @Test
  void operationalWritesAreForAnAdmin() throws Exception {
    String customer = uniqueId("alice");
    String none = uniqueId("none");
    String failureSwitch = get("/api/admin/payment-failure", OPERATOR, ADMIN).body();
    String outageSwitch = get("/api/admin/payment-outage", OPERATOR, ADMIN).body();

    // Every POST carries a JSON body; the routes that read none ignore it.
    String body = "{\"reason\": \"sent by a refused caller\"}";
    String[][] writes = {
      {"POST", "/api/admin/payment-failure/toggle", body},
      {"POST", "/api/admin/payment-outage/toggle", body},
      {"POST", "/api/admin/dead-letters/" + none + "/retry", body},
      {"POST", "/api/admin/outbox/failed/" + none + "/replay", body},
      {"POST", "/api/admin/outbox/failed/" + none + "/skip", body},
      {"POST", "/api/admin/saga-dead-letters/replay?sagaId=" + none + "&eventOffset=1", body},
      {"POST", "/api/admin/saga-dead-letters/replay-all?sagaId=" + none, body},
      {"POST", "/api/admin/sagas/faulted/resume?sagaId=" + none, body},
      {"DELETE", "/api/admin/saga-dead-letters?sagaId=" + none + "&eventOffset=1", null},
      {"POST", "/api/saga/fulfillments/fulfillment-" + none + "/inject-failure", body}
    };
    for (String[] write : writes) {
      assertRefusedForEveryoneButAnAdmin(write[0], write[1], write[2], customer);
    }

    // Two of the routes flip the payment-failure switch, one the outage: no refused call did.
    assertThat(get("/api/admin/payment-failure", OPERATOR, ADMIN).body()).isEqualTo(failureSwitch);
    assertThat(get("/api/admin/payment-outage", OPERATOR, ADMIN).body()).isEqualTo(outageSwitch);
  }

  // --- POST /api/products, PUT …/price, PUT …/stock, POST …/discontinue, POST /api/inventory ---

  @Test
  void productAndInventoryWritesAreForAnAdmin() throws Exception {
    String customer = uniqueId("alice");
    String product = uniqueId("p");
    String[] create = {"POST", "/api/products", productBody(product)};
    String[] price = {"PUT", "/api/products/" + product + "/price", "{\"price\": 0.01}"};
    String[] stock = {
      "PUT", "/api/products/" + product + "/stock", "{\"quantity\": 5, \"reason\": \"recount\"}"
    };
    String[] receive = {"POST", "/api/inventory/" + product + "/receive", "{\"quantity\": 1}"};
    String[] discontinue = {"POST", "/api/products/" + product + "/discontinue", "{}"};

    for (String[] write : new String[][] {create, price, stock, receive, discontinue}) {
      assertRefusedForEveryoneButAnAdmin(write[0], write[1], write[2], customer);
    }
    // Nothing a refused caller sent was written: neither stream holds an event.
    assertThat(JSON.readTree(get("/api/events/product/" + product, OPERATOR, ADMIN).body()))
        .isEmpty();
    assertThat(JSON.readTree(get("/api/events/inventory/" + product, OPERATOR, ADMIN).body()))
        .isEmpty();

    for (String[] write : new String[][] {create, price, stock, receive, discontinue}) {
      assertThat(send(write[0], write[1], OPERATOR, ADMIN, write[2]).statusCode())
          .as("%s %s as an ADMIN", write[0], write[1])
          .isBetween(200, 299);
    }
    assertThat(JSON.readTree(get("/api/events/product/" + product, OPERATOR, ADMIN).body()))
        .hasSize(4);
    assertThat(JSON.readTree(get("/api/events/inventory/" + product, OPERATOR, ADMIN).body()))
        .hasSize(1);
  }

  @Test
  void theProductCatalogueAndTheHealthAnswerAreOpen() throws Exception {
    assertThat(get("/api/products", null, null).statusCode()).isEqualTo(200);
    assertThat(get("/api/products/" + uniqueId("p-none"), null, null).statusCode()).isEqualTo(404);
    assertThat(get("/api/admin/health", null, null).statusCode()).isEqualTo(200);
  }

  // --- commands for an id nothing was created under ---

  @Test
  void aCommandForAnIdNothingWasCreatedUnderIsRefusedAndWritesNothing() throws Exception {
    // A customer who never registered, as the caller the id belongs to and as an ADMIN: personal
    // data stored under that id could never be erased, because a forget for it is refused too.
    String nobody = uniqueId("nobody");
    for (String[] caller : new String[][] {{nobody, CUSTOMER}, {OPERATOR, ADMIN}}) {
      assertThat(
              send(
                      "PUT",
                      "/api/customers/" + nobody,
                      caller[0],
                      caller[1],
                      profileBody("Nobody", nobody))
                  .statusCode())
          .as("profile update of an unregistered id as %s", caller[1])
          .isEqualTo(400);
      for (String route : new String[] {"/export-data", "/forget"}) {
        assertThat(
                send("POST", "/api/customers/" + nobody + route, caller[0], caller[1], null)
                    .statusCode())
            .as("%s of an unregistered id as %s", route, caller[1])
            .isEqualTo(400);
      }
    }
    assertThat(JSON.readTree(get("/api/events/customer/" + nobody, OPERATOR, ADMIN).body()))
        .isEmpty();

    // An order nobody placed. A customer is refused by the access rule (403, no row); an ADMIN
    // gets past it and the decider refuses to ship or deliver it. Cancelling is the saga's
    // compensation and is accepted for any order that is not shipped or delivered.
    String order = uniqueId("o-none");
    for (String action : new String[] {"/ship", "/deliver"}) {
      assertThat(
              send(
                      "POST",
                      "/api/orders/" + order + action,
                      OPERATOR,
                      ADMIN,
                      "{\"reason\": \"no such order\"}")
                  .statusCode())
          .as("%s of an order nobody placed", action)
          .isEqualTo(400);
    }
    assertThat(JSON.readTree(get("/api/events/order/" + order, OPERATOR, ADMIN).body())).isEmpty();

    // A product nobody created.
    String product = uniqueId("p-none");
    String[][] writes = {
      {"PUT", "/api/products/" + product + "/price", "{\"price\": 0.01}"},
      {"PUT", "/api/products/" + product + "/stock", "{\"quantity\": 5, \"reason\": \"recount\"}"},
      {"POST", "/api/products/" + product + "/discontinue", "{}"}
    };
    for (String[] write : writes) {
      assertThat(send(write[0], write[1], OPERATOR, ADMIN, write[2]).statusCode())
          .as("%s %s of a product nobody created", write[0], write[1])
          .isEqualTo(400);
    }
    assertThat(JSON.readTree(get("/api/events/product/" + product, OPERATOR, ADMIN).body()))
        .isEmpty();
  }

  // --- a request that cannot be read is 400 before the caller is decided ---

  @Test
  void aMalformedRequestIsAnswered400WhoeverSendsIt() throws Exception {
    String customer = uniqueId("alice");
    String noBody = "";
    String[][] malformed = {
      {"POST", "/api/orders", noBody},
      {"POST", "/api/orders", "{}"},
      {"POST", "/api/orders", "{\"orderId\": \"o-1\", \"customerId\": \"" + customer + "\"}"},
      {"POST", "/api/orders/" + uniqueId("o") + "/cancel", noBody},
      {"POST", "/api/customers", noBody},
      {"POST", "/api/customers", "{}"},
      {
        "POST",
        "/api/customers",
        "{\"customerId\": \"" + customer + "\", \"name\": \"Alice\", \"email\": \"no-at-sign\"}"
      },
      {"PUT", "/api/customers/" + customer, noBody},
      {"POST", "/api/products", noBody},
      {"POST", "/api/products", "{}"},
      {"PUT", "/api/products/" + uniqueId("p") + "/price", "{}"},
      {"PUT", "/api/products/" + uniqueId("p") + "/stock", noBody},
      {"POST", "/api/inventory/" + uniqueId("p") + "/receive", "{\"quantity\": 0}"}
    };
    String[][] callers = {{null, null}, {null, ADMIN}, {customer, CUSTOMER}, {OPERATOR, ADMIN}};
    for (String[] request : malformed) {
      for (String[] caller : callers) {
        assertThat(send(request[0], request[1], caller[0], caller[1], request[2]).statusCode())
            .as(
                "%s %s with the body '%s' as id=%s role=%s",
                request[0], request[1], request[2], caller[0], caller[1])
            .isEqualTo(400);
      }
    }
  }

  @Test
  void aQueryParameterThatCannotBeReadIsAnswered400WhoeverSendsIt() throws Exception {
    String customer = uniqueId("alice");
    String[][] unreadable = {
      // A number that is not one, on every endpoint that takes a page size or an offset.
      {"GET", "/api/saga/fulfillments?limit=abc"},
      {"GET", "/api/saga/fulfillments?limit=1.5"},
      {"GET", "/api/saga/fulfillments?limit=99999999999"},
      {"GET", "/api/events?limit=abc"},
      {"GET", "/api/events?offset=abc"},
      {"GET", "/api/audit/commands?limit=abc"},
      {"GET", "/api/admin/dead-letters?limit=abc"},
      {"GET", "/api/admin/outbox?limit=abc"},
      {"GET", "/api/admin/outbox/failed?limit=abc"},
      {"GET", "/api/admin/saga-dead-letters?limit=abc"},
      {"GET", "/api/admin/sagas/faulted?limit=abc"},
      {"POST", "/api/admin/saga-dead-letters/replay?sagaId=fulfillment-o-1&eventOffset=abc"},
      {"DELETE", "/api/admin/saga-dead-letters?sagaId=fulfillment-o-1&eventOffset=abc"},
      // A required parameter that is missing.
      {"POST", "/api/admin/saga-dead-letters/replay?sagaId=fulfillment-o-1"},
      {"DELETE", "/api/admin/saga-dead-letters?sagaId=fulfillment-o-1"},
      {"POST", "/api/admin/saga-dead-letters/replay-all"},
      {"POST", "/api/admin/sagas/faulted/resume"}
    };
    String[][] callers = {{null, null}, {null, ADMIN}, {customer, CUSTOMER}, {OPERATOR, ADMIN}};
    for (String[] request : unreadable) {
      for (String[] caller : callers) {
        assertThat(send(request[0], request[1], caller[0], caller[1], null).statusCode())
            .as("%s %s as id=%s role=%s", request[0], request[1], caller[0], caller[1])
            .isEqualTo(400);
      }
    }
  }

  // --- helpers ---

  private URI server() {
    return server;
  }

  private static String uniqueId(String prefix) {
    return "q-acl-" + prefix + "-" + System.nanoTime();
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

  /**
   * The four callers an ADMIN-only endpoint refuses, each with {@code 403}: no header at all, the
   * {@code ADMIN} role without an identity, a customer, and an identity without a role.
   *
   * @param body the JSON body, or {@code null} for a request without one
   * @param customer the identity of the two callers who have one
   */
  private void assertRefusedForEveryoneButAnAdmin(
      String method, String path, String body, String customer) throws Exception {
    assertThat(send(method, path, null, null, body).statusCode())
        .as("%s %s without an identity", method, path)
        .isEqualTo(403);
    assertThat(send(method, path, null, ADMIN, body).statusCode())
        .as("%s %s with the ADMIN role and no identity", method, path)
        .isEqualTo(403);
    assertThat(send(method, path, customer, CUSTOMER, body).statusCode())
        .as("%s %s as a customer", method, path)
        .isEqualTo(403);
    assertThat(send(method, path, customer, null, body).statusCode())
        .as("%s %s as a caller without a role", method, path)
        .isEqualTo(403);
  }

  private static String productBody(String product) {
    return """
        {"productId": "%s", "name": "Product %s", "description": "For the access test",
         "category": "Test", "price": 12.50, "initialStock": 3}"""
        .formatted(product, product);
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
         "lines": [{"productId": "%s", "quantity": 1}]}"""
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

  /**
   * A product in the catalogue, which prices the orders that name it, with stock, so that the
   * fulfillment saga confirms them.
   */
  private String stockedProduct() throws Exception {
    String product = uniqueId("p");
    assertThat(send("POST", "/api/products", OPERATOR, ADMIN, productBody(product)).statusCode())
        .isBetween(200, 299);
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

  /** Waits until the fulfillment saga of the order reached the given fulfillment status. */
  private void awaitFulfillmentStatus(String order, String status) {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              HttpResponse<String> response =
                  get("/api/saga/fulfillments/fulfillment-" + order, OPERATOR, ADMIN);
              assertThat(response.statusCode()).isEqualTo(200);
              assertThat(response.body()).contains("\"fulfillmentStatus\":\"" + status + "\"");
            });
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
