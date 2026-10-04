package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Focused smoke suite for the Quarkus observability / admin HTTP surface added in QB3:
 * AdminController, AuditController, SagaController, and EventExplorerController.
 *
 * <p>Exercises key endpoints for 2xx + basic shape; does NOT require RabbitMQ — outbox / DLQ
 * endpoints just read their tables (which the PostgreSQL container provides). The outbox is
 * disabled under {@code %test}, so the outbox replay and skip endpoints answer 503 once their own
 * checks pass (the role; for skip also the request identity and a non-blank reason). Every mutating
 * admin endpoint requires the {@code ADMIN} role, which this demo reads from {@code X-User-Role}
 * (its trusted-gateway stand-in).
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class ObservabilitySmokeSuiteIT {

  // ── Admin endpoints ────────────────────────────────────────────────────────

  @Test
  void adminHealthReturnsUp() {
    String status =
        given()
            .when()
            .get("/api/admin/health")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getString("status");

    assertThat(status).isEqualTo("UP");
  }

  @Test
  void circuitBreakerEndpointHasExpectedKeys() {
    io.restassured.path.json.JsonPath body =
        given()
            .when()
            .get("/api/admin/circuit-breaker")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();

    assertThat(body.getString("state")).isNotNull();
    assertThat(body.getString("paymentGateway")).isNotNull();
  }

  @Test
  void deadLettersEndpointReturnsArray() {
    given().when().get("/api/admin/dead-letters").then().statusCode(200);
  }

  @Test
  void outboxEndpointReturnsArray() {
    given().when().get("/api/admin/outbox").then().statusCode(200);
  }

  @Test
  void paymentFailureToggleFlips() {
    // Read initial state
    boolean initial =
        given()
            .when()
            .get("/api/admin/payment-failure")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getBoolean("failureInjected");

    // Toggle — should be opposite of initial
    boolean afterToggle =
        given()
            .header("X-User-Role", "ADMIN")
            .contentType(ContentType.JSON)
            .when()
            .post("/api/admin/payment-failure/toggle")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getBoolean("failureInjected");

    assertThat(afterToggle).isEqualTo(!initial);

    // Restore to original state
    given()
        .header("X-User-Role", "ADMIN")
        .contentType(ContentType.JSON)
        .when()
        .post("/api/admin/payment-failure/toggle")
        .then()
        .statusCode(200);
  }

  @Test
  void mutatingAdminEndpointsRequireAdminRole() {
    // Payment-failure toggle: 403 without the role, 200 with it (then toggled back).
    given().when().post("/api/admin/payment-failure/toggle").then().statusCode(403);
    given()
        .header("X-User-Role", "CUSTOMER")
        .when()
        .post("/api/admin/payment-failure/toggle")
        .then()
        .statusCode(403);
    given()
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/admin/payment-failure/toggle")
        .then()
        .statusCode(200);
    given()
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/admin/payment-failure/toggle")
        .then()
        .statusCode(200);

    // Dead-letter retry: 403 without the role; with it, an unknown entry is 404.
    given().when().post("/api/admin/dead-letters/no-such-command/retry").then().statusCode(403);
    given()
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/admin/dead-letters/no-such-command/retry")
        .then()
        .statusCode(404);

    // The FAILED listing is a read: no role needed.
    given().when().get("/api/admin/outbox/failed").then().statusCode(200);

    // Outbox replay: 403 without the role; with it, 503 — the outbox is disabled under %test, so
    // the replayer is absent and the endpoint says so instead of failing with a 500.
    given().when().post("/api/admin/outbox/failed/x/replay").then().statusCode(403);
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "smoke")
        .when()
        .post("/api/admin/outbox/failed/x/replay")
        .then()
        .statusCode(503);
  }

  @Test
  void outboxSkipChecksRoleIdentityAndReasonBeforeTheReplayer() {
    // Every refusal is decided before the replayer is consulted, so an unknown entry id is enough;
    // a complete request reaches the replayer, which is absent under %test (503).
    String uri = "/api/admin/outbox/failed/no-such-entry-" + System.nanoTime() + "/skip";
    String reason = "{\"reason\": \"poison payload, superseded by a later correction\"}";

    given().contentType(ContentType.JSON).body(reason).when().post(uri).then().statusCode(403);
    given()
        .header("X-User-Role", "ADMIN")
        .contentType(ContentType.JSON)
        .body(reason)
        .when()
        .post(uri)
        .then()
        .statusCode(400); // no request identity (X-User-Id)
    for (String body : new String[] {"", "null", "{}", "{\"reason\": \"   \"}"}) {
      given()
          .header("X-User-Role", "ADMIN")
          .header("X-User-Id", "smoke")
          .contentType(ContentType.JSON)
          .body(body)
          .when()
          .post(uri)
          .then()
          .statusCode(400); // no body, JSON null, absent reason, blank reason
    }
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "smoke")
        .contentType(ContentType.JSON)
        .body(reason)
        .when()
        .post(uri)
        .then()
        .statusCode(503);
  }

  // ── Saga endpoints ─────────────────────────────────────────────────────────

  @Test
  void sagaFulfillmentsEndpointReturnsArray() {
    given().when().get("/api/saga/fulfillments").then().statusCode(200);
  }

  @Test
  void sagaFulfillmentByUnknownIdReturns404() {
    given()
        .when()
        .get("/api/saga/fulfillments/does-not-exist-" + System.nanoTime())
        .then()
        .statusCode(404);
  }

  /** The flag both payment-failure routes flip, read through the open admin listing. */
  private static boolean failureInjected() {
    return given()
        .when()
        .get("/api/admin/payment-failure")
        .then()
        .statusCode(200)
        .extract()
        .jsonPath()
        .getBoolean("failureInjected");
  }

  @Test
  void sagaInjectFailureWithAdminRoleFlipsTheFlag() {
    // The route ignores the saga id and flips the shared PaymentGatewaySimulator flag: it is the
    // admin toggle's shorthand and needs the ADMIN role like it.
    boolean initial = failureInjected();

    boolean afterInject =
        given()
            .header("X-User-Role", "ADMIN")
            .when()
            .post("/api/saga/fulfillments/any-saga-id/inject-failure")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getBoolean("failureInjected");

    assertThat(afterInject).isEqualTo(!initial);
    assertThat(failureInjected()).isEqualTo(!initial);

    // Restore to original state
    given()
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/saga/fulfillments/any-saga-id/inject-failure")
        .then()
        .statusCode(200);
    assertThat(failureInjected()).isEqualTo(initial);
  }

  @Test
  void sagaInjectFailureWithoutAdminRoleIsForbiddenAndLeavesTheFlag() {
    boolean initial = failureInjected();

    given().when().post("/api/saga/fulfillments/any-saga-id/inject-failure").then().statusCode(403);
    assertThat(failureInjected()).isEqualTo(initial);

    given()
        .header("X-User-Role", "CUSTOMER")
        .when()
        .post("/api/saga/fulfillments/any-saga-id/inject-failure")
        .then()
        .statusCode(403);
    assertThat(failureInjected()).isEqualTo(initial);
  }

  // ── Audit endpoint ─────────────────────────────────────────────────────────

  @Test
  void auditCommandsEndpointReturnsArray() {
    // Emit one command so the audit_log has at least one row to shape-check
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "smoke-test")
        .contentType(ContentType.JSON)
        .body(
            """
            {
              "productId": "smoke-audit-%d",
              "name": "Smoke Widget",
              "description": "audit smoke test",
              "category": "Test",
              "price": 1.00,
              "initialStock": 1
            }"""
                .formatted(System.nanoTime()))
        .post("/api/products");

    io.restassured.path.json.JsonPath body =
        given().when().get("/api/audit/commands").then().statusCode(200).extract().jsonPath();

    List<Map<String, Object>> rows = body.getList("");
    assertThat(rows).isNotNull();
    if (!rows.isEmpty()) {
      Map<String, Object> first = rows.get(0);
      assertThat(first).containsKey("commandId");
      assertThat(first).containsKey("commandType");
      assertThat(first).containsKey("outcome");
    }
  }

  // ── Event explorer endpoints ───────────────────────────────────────────────

  @Test
  void eventsGlobalListReturnsArray() {
    given().when().get("/api/events").then().statusCode(200);
  }

  @Test
  void eventsGlobalListWithOffsetAndLimitReturnsArray() {
    given()
        .queryParam("offset", 0)
        .queryParam("limit", 10)
        .when()
        .get("/api/events")
        .then()
        .statusCode(200);
  }

  @Test
  void eventsPerStreamReturnsArrayForKnownStream() {
    // Create a product to ensure at least one stream exists
    String pid = "smoke-events-" + System.nanoTime();
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "smoke-test")
        .contentType(ContentType.JSON)
        .body(
            """
            {
              "productId": "%s",
              "name": "Events Widget",
              "description": "event explorer smoke",
              "category": "Test",
              "price": 5.00,
              "initialStock": 10
            }"""
                .formatted(pid))
        .post("/api/products")
        .then()
        .statusCode(200);

    io.restassured.path.json.JsonPath body =
        given()
            .when()
            .get("/api/events/product/" + pid)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();

    List<Map<String, Object>> events = body.getList("");
    assertThat(events).isNotEmpty();
    Map<String, Object> first = events.get(0);
    assertThat(first).containsKey("globalOffset");
    assertThat(first).containsKey("streamId");
    assertThat(first).containsEntry("aggregateType", "product");
    assertThat(first).containsEntry("aggregateId", pid);
    assertThat(first).containsKey("eventType");
    assertThat(first).containsKey("timestamp");
    assertThat(first).containsKey("payload");
  }
}
