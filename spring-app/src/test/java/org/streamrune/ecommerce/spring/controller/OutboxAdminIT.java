package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * The strict outbox channel from the operator's side: a FAILED head blocks its aggregate at the
 * real relay + RabbitMQ; the admin API enumerates it, skip releases the successors (delivered in
 * order, the head never), replay delivers the head first; skippedBy is the request identity.
 */
class OutboxAdminIT extends AbstractIntegrationTest {

  @Autowired private DataSource dataSource;

  /**
   * Seeds a FAILED head n0 and a PENDING successor n1 for one aggregate straight into the table.
   */
  private void seedBlockedAggregate(String agg, String n0, String n1) throws Exception {
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type,"
                    + " aggregate_id, status, attempts, last_error, processed_at)"
                    + " VALUES (?, ?::jsonb, 'OrderConfirmed', 'order', ?, ?, ?, ?, ?)")) {
      ps.setString(1, n0);
      ps.setString(2, "{\"orderId\":\"" + agg + "\",\"seq\":0}");
      ps.setString(3, agg);
      ps.setString(4, "FAILED");
      ps.setInt(5, 10);
      ps.setString(6, "seeded poison");
      ps.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));
      ps.executeUpdate();
      ps.setString(1, n1);
      ps.setString(2, "{\"orderId\":\"" + agg + "\",\"seq\":1}");
      ps.setString(3, agg);
      ps.setString(4, "PENDING");
      ps.setInt(5, 0);
      ps.setString(6, null);
      ps.setTimestamp(7, null);
      ps.executeUpdate();
    }
  }

  private String statusOf(String entryId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  @Test
  void failedHeadBlocksSuccessor_skipReleasesIt_auditedWithTheRequestIdentity() throws Exception {
    String agg = "blocked-order-" + System.nanoTime();
    String n0 = agg + "-n0";
    String n1 = agg + "-n1";
    seedBlockedAggregate(agg, n0, n1);

    // Two poll intervals (5s default): the successor is not delivered while the head is FAILED.
    Thread.sleep(11_000);
    assertThat(statusOf(n1)).isEqualTo("PENDING");
    assertThat(testRabbitListener.messages.stream().filter(m -> m.body().contains(agg)).count())
        .isZero();

    // The read-only listing shows the head; the PENDING listing does not claim anything.
    client
        .get()
        .uri("/api/admin/outbox/failed")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$[?(@.id == '" + n0 + "')].status")
        .isEqualTo(java.util.List.of("FAILED"));
    client.get().uri("/api/admin/outbox").exchange().expectStatus().isOk();
    assertThat(statusOf(n1)).isEqualTo("PENDING");

    // Skip without an identity → 400; without the ADMIN role → 403; then the real skip.
    client
        .post()
        .uri("/api/admin/outbox/failed/" + n0 + "/skip")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("reason", "seeded poison"))
        .exchange()
        .expectStatus()
        .isBadRequest();
    client
        .post()
        .uri("/api/admin/outbox/failed/" + n0 + "/skip")
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "CUSTOMER")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("reason", "seeded poison"))
        .exchange()
        .expectStatus()
        .isForbidden();
    client
        .post()
        .uri("/api/admin/outbox/failed/" + n0 + "/skip")
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("reason", "seeded poison"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("SKIPPED");

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(statusOf(n1)).isEqualTo("DELIVERED"));
    assertThat(statusOf(n0)).isEqualTo("SKIPPED");
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT skipped_by, skip_reason, skipped_at FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, n0);
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("skipped_by")).isEqualTo("ops-1");
        assertThat(rs.getString("skip_reason")).isEqualTo("seeded poison");
        assertThat(rs.getTimestamp("skipped_at")).isNotNull();
      }
    }
    client
        .get()
        .uri("/api/admin/outbox/failed")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$[?(@.id == '" + n0 + "')]")
        .isEmpty();
    // A second skip is a no-op with a named outcome.
    client
        .post()
        .uri("/api/admin/outbox/failed/" + n0 + "/skip")
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("reason", "again"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("NOT_FAILED");
  }

  @Test
  void replayDeliversTheHeadBeforeItsSuccessor() throws Exception {
    String agg = "replayed-order-" + System.nanoTime();
    String n0 = agg + "-n0";
    String n1 = agg + "-n1";
    seedBlockedAggregate(agg, n0, n1);

    client
        .post()
        .uri("/api/admin/outbox/failed/" + n0 + "/replay")
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("REPLAYED");

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(statusOf(n1)).isEqualTo("DELIVERED"));
    assertThat(statusOf(n0)).isEqualTo("DELIVERED");
    // DELIVERED is stamped on the broker confirm; the test consumer receives asynchronously.
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(testRabbitListener.messages.stream().filter(m -> m.body().contains(agg)))
                    .hasSize(2));
    var order =
        testRabbitListener.messages.stream()
            .map(AbstractIntegrationTest.ReceivedMessage::body)
            .filter(b -> b.contains(agg))
            .toList();
    assertThat(order).hasSize(2);
    assertThat(order.get(0)).contains("\"seq\": 0");
    assertThat(order.get(1)).contains("\"seq\": 1");
    client
        .post()
        .uri("/api/admin/outbox/failed/" + n0 + "/replay")
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.outcome")
        .isEqualTo("NOT_FAILED");
  }
}
