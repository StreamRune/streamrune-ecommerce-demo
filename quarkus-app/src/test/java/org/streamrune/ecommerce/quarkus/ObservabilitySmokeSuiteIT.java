package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.runtime.SubscriptionHealthContributor;

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

  @Inject SubscriptionHealthContributor subscriptionHealth;

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

  /**
   * The saga subscription and the subscriptions of the payment, order cancellation and order
   * shipment process managers are registered with the health contributor, each under its own lease;
   * a single application takes every lease. Each subscription takes its lease on its own poll
   * thread, which starts inside the application's startup observer, so the roles are not {@code
   * LEADING} the instant the application answers: the test waits for them. The test classes
   * of this module run on one application, so there is no earlier application's lease to wait
   * out, as its Micronaut twin must (a stopping application does not give its leases back, and the
   * test lease there lives 5 s); the 30 s bound covers the default lease time to live (15 s) were
   * one still held.
   */
  @Test
  void theSagaSubscriptionIsRegisteredAndLeads() {
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(
            () -> {
              Map<String, LeaseRole> roles =
                  subscriptionHealth.health().stream()
                      .collect(
                          Collectors.toMap(SubscriptionHealth::name, SubscriptionHealth::role));
              assertThat(roles)
                  .containsEntry("order-fulfillment-saga", LeaseRole.LEADING)
                  .containsEntry("payment-process-manager", LeaseRole.LEADING)
                  .containsEntry("order-cancellation", LeaseRole.LEADING)
                  .containsEntry("order-shipment", LeaseRole.LEADING);
            });
  }

  @Test
  void circuitBreakerEndpointHasExpectedKeys() {
    io.restassured.path.json.JsonPath body =
        given()
            .header("X-User-Id", "admin-1")
            .header("X-User-Role", "ADMIN")
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
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/admin/dead-letters")
        .then()
        .statusCode(200);
  }

  @Test
  void outboxEndpointReturnsArray() {
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/admin/outbox")
        .then()
        .statusCode(200);
  }

  @Test
  void paymentFailureToggleFlips() {
    // Read initial state
    boolean initial =
        given()
            .header("X-User-Id", "admin-1")
            .header("X-User-Role", "ADMIN")
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
            .header("X-User-Id", "admin-1")
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
        .header("X-User-Id", "admin-1")
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
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/admin/payment-failure/toggle")
        .then()
        .statusCode(200);
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/admin/payment-failure/toggle")
        .then()
        .statusCode(200);

    // Dead-letter retry: 403 without the role; with it, an unknown entry is 404.
    given().when().post("/api/admin/dead-letters/no-such-command/retry").then().statusCode(403);
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .post("/api/admin/dead-letters/no-such-command/retry")
        .then()
        .statusCode(404);
    // A forced replay and a discard are decisions of an ADMIN: the role check comes first.
    given()
        .header("X-User-Id", "cust-alice")
        .header("X-User-Role", "CUSTOMER")
        .when()
        .post("/api/admin/dead-letters/no-such-command/retry?force=true")
        .then()
        .statusCode(403);
    given()
        .header("X-User-Id", "cust-alice")
        .header("X-User-Role", "CUSTOMER")
        .when()
        .delete("/api/admin/dead-letters/no-such-command")
        .then()
        .statusCode(403);
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .delete("/api/admin/dead-letters/no-such-command")
        .then()
        .statusCode(404);

    // The FAILED listing is a read: no role needed.
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/admin/outbox/failed")
        .then()
        .statusCode(200);

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
        .statusCode(403); // the ADMIN role without an identity (X-User-Id) is nobody
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
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/saga/fulfillments")
        .then()
        .statusCode(200);
  }

  @Test
  void sagaFulfillmentByUnknownIdReturns404() {
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/saga/fulfillments/does-not-exist-" + System.nanoTime())
        .then()
        .statusCode(404);
  }

  /** The flag both payment-failure routes flip, read through the admin listing. */
  private static boolean failureInjected() {
    return given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
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
            .header("X-User-Id", "admin-1")
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
        .header("X-User-Id", "admin-1")
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
        given()
            .header("X-User-Id", "admin-1")
            .header("X-User-Role", "ADMIN")
            .when()
            .get("/api/audit/commands")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();

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
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/events")
        .then()
        .statusCode(200);
  }

  @Test
  void eventsGlobalListWithOffsetAndLimitReturnsArray() {
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
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
            .header("X-User-Id", "admin-1")
            .header("X-User-Role", "ADMIN")
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

  // ── Metrics ────────────────────────────────────────────────────────────────

  /**
   * The app reports the framework's meters at its Prometheus endpoint, {@code /q/metrics}, as the
   * Spring and Micronaut apps list them at {@code /actuator/metrics} and {@code /metrics} (their
   * suites check the same meters). The two dead-letter gauges are the ones the demo's {@code
   * DeadLetterRetryRunner} samples; {@code saga.faulted_backlog} and {@code projections.processed}
   * stand for the saga and projection meters. Prometheus writes a dot as {@code _} and gives a
   * counter the suffix {@code _total}.
   */
  @Test
  void theFrameworksMetersAreListedAtTheMetricsEndpoint() {
    String scrape = given().when().get("/q/metrics").then().statusCode(200).extract().asString();

    assertThat(scrape)
        .contains("streamrune_dlq_awaiting_operator")
        .contains("streamrune_dlq_pending")
        .contains("streamrune_saga_faulted_backlog")
        .contains("streamrune_projections_processed_total")
        .contains("streamrune_commands_dispatched_total");
  }
}
