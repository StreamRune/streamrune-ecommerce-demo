package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

/**
 * Who may read the event history over HTTP. The two Event Explorer endpoints return event payloads
 * as the event store reads them, decrypted, so they answer only to the ADMIN role. What the open
 * live feed says instead is {@code LiveFeedAccessIT}'s.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class EventExplorerAccessIT {

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
}
