package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Focused smoke suite for the Micronaut observability / admin HTTP surface added in MB3:
 * AdminController, AuditController, SagaController, and EventExplorerController.
 *
 * <p>Exercises key endpoints for 2xx + basic shape; does NOT require RabbitMQ — outbox / DLQ
 * endpoints just read their tables (which the PostgreSQL container provides via {@link
 * AbstractIntegrationTest}). The outbox is disabled there ({@code
 * streamrune.outbox.enabled=false}), so the outbox replay and skip endpoints answer 503 once their
 * own checks pass (the role; for skip also the request identity and a non-blank reason). Every
 * mutating admin endpoint requires the {@code ADMIN} role, which this demo reads from {@code
 * X-User-Role} (its trusted-gateway stand-in).
 */
@MicronautTest(transactional = false)
class ObservabilitySmokeSuiteIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject DataSource dataSource;

  @Inject EmbeddedServer server;

  // ── Admin endpoints ────────────────────────────────────────────────────────

  @Test
  void adminHealthReturnsUp() {
    Map<String, Object> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/health"), Argument.mapOf(String.class, Object.class));

    assertThat(body.get("status")).isEqualTo("UP");
  }

  @Test
  void circuitBreakerEndpointHasExpectedKeys() {
    Map<String, Object> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/circuit-breaker"),
                Argument.mapOf(String.class, Object.class));

    assertThat(body).containsKey("state");
    assertThat(body).containsKey("paymentGateway");
  }

  @Test
  void deadLettersEndpointReturnsArray() {
    List<Map<String, Object>> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/dead-letters"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(body).isNotNull();
  }

  @Test
  void outboxEndpointReturnsArray() {
    List<Map<String, Object>> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/outbox"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(body).isNotNull();
  }

  @Test
  void paymentFailureToggleFlips() {
    // Read initial state
    Map<String, Object> initial =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/payment-failure"),
                Argument.mapOf(String.class, Object.class));
    boolean initialFlag = (Boolean) initial.get("failureInjected");

    // Toggle — should be opposite of initial
    Map<String, Object> afterToggle =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.POST("/api/admin/payment-failure/toggle", "")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN"),
                Argument.mapOf(String.class, Object.class));
    boolean toggledFlag = (Boolean) afterToggle.get("failureInjected");

    assertThat(toggledFlag).isEqualTo(!initialFlag);

    // Restore to original state
    client
        .toBlocking()
        .retrieve(
            HttpRequest.POST("/api/admin/payment-failure/toggle", "")
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-User-Role", "ADMIN"),
            Argument.mapOf(String.class, Object.class));
  }

  @Test
  void mutatingAdminEndpointsRequireAdminRole() {
    // Payment-failure toggle: 403 without the role (and with a non-ADMIN one), 200 with it — twice,
    // so the shared flag ends where it started.
    HttpClientResponseException noRole =
        assertThrows(
            HttpClientResponseException.class,
            () ->
                client
                    .toBlocking()
                    .exchange(HttpRequest.POST("/api/admin/payment-failure/toggle", "")));
    assertThat(noRole.getStatus().getCode()).isEqualTo(403);
    assertThat(
            statusOf(
                HttpRequest.POST("/api/admin/payment-failure/toggle", "")
                    .header("X-User-Role", "CUSTOMER")))
        .isEqualTo(403);
    assertThat(
            statusOf(
                HttpRequest.POST("/api/admin/payment-failure/toggle", "")
                    .header("X-User-Role", "ADMIN")))
        .isEqualTo(200);
    assertThat(
            statusOf(
                HttpRequest.POST("/api/admin/payment-failure/toggle", "")
                    .header("X-User-Role", "ADMIN")))
        .isEqualTo(200);

    // Dead-letter retry: 403 without the role; with it, an unknown entry is 404.
    assertThat(statusOf(HttpRequest.POST("/api/admin/dead-letters/no-such-command/retry", "")))
        .isEqualTo(403);
    assertThat(
            statusOf(
                HttpRequest.POST("/api/admin/dead-letters/no-such-command/retry", "")
                    .header("X-User-Role", "ADMIN")))
        .isEqualTo(404);

    // The FAILED listing is a read: no role needed. Nothing fails delivery here (no poller).
    List<Map<String, Object>> failed =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/outbox/failed"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));
    assertThat(failed).isEmpty();

    // Outbox replay: 403 without the role; with it, 503 — the outbox is disabled in these tests, so
    // the replayer bean is absent and the endpoint says so instead of failing with a 500.
    assertThat(statusOf(HttpRequest.POST("/api/admin/outbox/failed/x/replay", ""))).isEqualTo(403);
    assertThat(
            statusOf(
                HttpRequest.POST("/api/admin/outbox/failed/x/replay", "")
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "smoke")))
        .isEqualTo(503);
  }

  @Test
  void outboxListingsAreReadOnly() throws Exception {
    // The PENDING listing used to call loadPending, which CLAIMS (PENDING -> IN_PROGRESS under this
    // relay's lease) and so stalled delivery; findByStatus only reads. Seeded straight into the
    // table: the outbox is disabled here, so no poller touches these rows either.
    String pendingId = "smoke-pending-" + System.nanoTime();
    String failedId = "smoke-failed-" + System.nanoTime();
    try {
      insertOutboxRow(pendingId, "PENDING", null);
      insertOutboxRow(failedId, "FAILED", "rejected by the broker");

      List<Map<String, Object>> pending =
          client
              .toBlocking()
              .retrieve(
                  HttpRequest.GET("/api/admin/outbox?limit=1000"),
                  Argument.listOf(Argument.mapOf(String.class, Object.class)));
      assertThat(pending).anySatisfy(e -> assertThat(e.get("id")).isEqualTo(pendingId));
      assertThat(pending).allSatisfy(e -> assertThat(e.get("status")).isEqualTo("PENDING"));
      assertThat(outboxStatus(pendingId)).as("listed, not claimed").isEqualTo("PENDING");

      List<Map<String, Object>> failed =
          client
              .toBlocking()
              .retrieve(
                  HttpRequest.GET("/api/admin/outbox/failed"),
                  Argument.listOf(Argument.mapOf(String.class, Object.class)));
      assertThat(failed)
          .singleElement()
          .satisfies(
              e -> {
                assertThat(e.get("id")).isEqualTo(failedId);
                assertThat(e.get("status")).isEqualTo("FAILED");
                assertThat(e.get("lastError")).isEqualTo("rejected by the broker");
              });
      assertThat(outboxStatus(failedId)).isEqualTo("FAILED");
    } finally {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps =
              conn.prepareStatement("DELETE FROM outbox_events WHERE entry_id IN (?, ?)")) {
        ps.setString(1, pendingId);
        ps.setString(2, failedId);
        ps.executeUpdate();
      }
    }
  }

  @Test
  void outboxSkipChecksRoleIdentityAndReasonBeforeTheReplayer() {
    // Every refusal is decided before the replayer is consulted, so an unknown entry id is enough;
    // a complete request reaches the replayer, which is absent in these tests (503).
    String uri = "/api/admin/outbox/failed/no-such-entry-" + System.nanoTime() + "/skip";
    String reason = "{\"reason\": \"poison payload, superseded by a later correction\"}";

    assertThat(statusOf(HttpRequest.POST(uri, reason).contentType(MediaType.APPLICATION_JSON)))
        .isEqualTo(403);
    assertThat(
            statusOf(
                HttpRequest.POST(uri, reason)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")))
        .as("no request identity (X-User-Id)")
        .isEqualTo(400);
    for (String body : new String[] {"", "null", "{}", "{\"reason\": \"   \"}"}) {
      assertThat(
              statusOf(
                  HttpRequest.POST(uri, body)
                      .contentType(MediaType.APPLICATION_JSON)
                      .header("X-User-Role", "ADMIN")
                      .header("X-User-Id", "smoke")))
          .as("body <%s>: no body, JSON null, absent reason, blank reason", body)
          .isEqualTo(400);
    }
    assertThat(
            statusOf(
                HttpRequest.POST(uri, reason)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "smoke")))
        .isEqualTo(503);
  }

  @Test
  void outboxSkipSeesTheRequestIdentityWhenTheBodyArrivesAfterTheHeaders() throws Exception {
    // A client may send the body in a later TCP segment than the headers. The route must still
    // see the role and the identity the filter resolved from the headers, so this complete
    // request answers 503 (outbox disabled) like the one the test above ends with, never 403.
    byte[] body =
        "{\"reason\": \"poison payload, superseded by a later correction\"}"
            .getBytes(StandardCharsets.UTF_8);
    String head =
        "POST /api/admin/outbox/failed/no-such-entry-"
            + System.nanoTime()
            + "/skip HTTP/1.1\r\n"
            + "Host: localhost\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: "
            + body.length
            + "\r\n"
            + "X-User-Role: ADMIN\r\n"
            + "X-User-Id: smoke\r\n"
            + "Connection: close\r\n"
            + "\r\n";

    String response;
    try (var socket = new Socket("localhost", server.getPort())) {
      socket.setSoTimeout(10_000);
      OutputStream out = socket.getOutputStream();
      out.write(head.getBytes(StandardCharsets.US_ASCII));
      out.flush();
      Thread.sleep(500);
      out.write(body);
      out.flush();
      response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    assertThat(response).startsWith("HTTP/1.1 503");
  }

  private void insertOutboxRow(String entryId, String status, String lastError) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO outbox_events (entry_id, payload, payload_type, status, attempts,"
                    + " last_error, processed_at, aggregate_type, aggregate_id) VALUES (?,"
                    + " '{}'::jsonb, 'smoke.Probe', ?, ?, ?, CASE WHEN ? = 'FAILED' THEN NOW() END,"
                    + " 'order', ?)")) {
      ps.setString(1, entryId);
      ps.setString(2, status);
      ps.setInt(3, lastError == null ? 0 : 3);
      ps.setString(4, lastError);
      ps.setString(5, status);
      ps.setString(6, "agg-" + entryId);
      ps.executeUpdate();
    }
  }

  private String outboxStatus(String entryId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("row %s exists", entryId).isTrue();
        return rs.getString(1);
      }
    }
  }

  /** The response status code, whether the blocking client returned it or threw it (4xx/5xx). */
  private int statusOf(MutableHttpRequest<?> request) {
    try {
      return client.toBlocking().exchange(request).getStatus().getCode();
    } catch (HttpClientResponseException e) {
      return e.getStatus().getCode();
    }
  }

  // ── Saga endpoints ─────────────────────────────────────────────────────────

  @Test
  void sagaFulfillmentsEndpointReturnsArray() {
    List<Map<String, Object>> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/saga/fulfillments"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(body).isNotNull();
  }

  @Test
  void sagaFulfillmentByUnknownIdReturns404() {
    HttpClientResponseException ex =
        assertThrows(
            HttpClientResponseException.class,
            () ->
                client
                    .toBlocking()
                    .retrieve(
                        HttpRequest.GET(
                            "/api/saga/fulfillments/does-not-exist-" + System.nanoTime())));

    assertThat(ex.getStatus().getCode()).isEqualTo(404);
  }

  /** The flag both payment-failure routes flip, read through the open admin listing. */
  private boolean failureInjected() {
    Map<String, Object> state =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/admin/payment-failure"),
                Argument.mapOf(String.class, Object.class));
    return (Boolean) state.get("failureInjected");
  }

  @Test
  void sagaInjectFailureWithAdminRoleFlipsTheFlag() {
    // The route ignores the saga id and flips the shared PaymentGatewaySimulator flag: it is the
    // admin toggle's shorthand and needs the ADMIN role like it.
    boolean initial = failureInjected();

    Map<String, Object> afterInject =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.POST("/api/saga/fulfillments/any-saga-id/inject-failure", "")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN"),
                Argument.mapOf(String.class, Object.class));

    assertThat(afterInject.get("failureInjected")).isEqualTo(!initial);
    assertThat(failureInjected()).isEqualTo(!initial);

    // Restore to original state
    assertThat(
            statusOf(
                HttpRequest.POST("/api/saga/fulfillments/any-saga-id/inject-failure", "")
                    .header("X-User-Role", "ADMIN")))
        .isEqualTo(200);
    assertThat(failureInjected()).isEqualTo(initial);
  }

  @Test
  void sagaInjectFailureWithoutAdminRoleIsForbiddenAndLeavesTheFlag() {
    boolean initial = failureInjected();

    assertThat(statusOf(HttpRequest.POST("/api/saga/fulfillments/any-saga-id/inject-failure", "")))
        .isEqualTo(403);
    assertThat(failureInjected()).isEqualTo(initial);

    assertThat(
            statusOf(
                HttpRequest.POST("/api/saga/fulfillments/any-saga-id/inject-failure", "")
                    .header("X-User-Role", "CUSTOMER")))
        .isEqualTo(403);
    assertThat(failureInjected()).isEqualTo(initial);
  }

  // ── Audit endpoint ─────────────────────────────────────────────────────────

  @Test
  void auditCommandsEndpointReturnsArray() {
    // Emit one command so the audit_log has at least one row to shape-check
    client
        .toBlocking()
        .exchange(
            HttpRequest.POST(
                    "/api/products",
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
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-User-Role", "ADMIN")
                .header("X-User-Id", "smoke-test"));

    List<Map<String, Object>> rows =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/audit/commands"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

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
    List<Map<String, Object>> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/events"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(body).isNotNull();
  }

  @Test
  void eventsGlobalListWithOffsetAndLimitReturnsArray() {
    List<Map<String, Object>> body =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/events?offset=0&limit=10"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(body).isNotNull();
  }

  @Test
  void eventsPerStreamReturnsArrayForKnownStream() {
    // Create a product to ensure at least one stream exists
    String pid = "smoke-events-" + System.nanoTime();
    client
        .toBlocking()
        .exchange(
            HttpRequest.POST(
                    "/api/products",
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
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-User-Role", "ADMIN")
                .header("X-User-Id", "smoke-test"));

    List<Map<String, Object>> events =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/events/product/" + pid),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

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
