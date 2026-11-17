package org.streamrune.ecommerce.quarkus.config;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.ecommerce.quarkus.PostgresTestResource;

/**
 * An at-least-once projection over an in-memory store ({@link AuditMemProjection}) boots and runs
 * in the real app beside the four TRANSACTIONAL_LOCAL PostgreSQL projections. The framework's
 * ProjectionProducer routes the four to the default read-model store, {@code postgres}, because
 * they write through its repository: their rows and their checkpoints ({@code
 * readmodel_checkpoint}) commit together in the app's database. The at-least-once projection writes
 * to a repository that belongs to no store, so it is routed to the event store's subscription
 * checkpoint: its checkpoint lands in {@code subscription_checkpoint}, and its rows land only in
 * its own store, never in the read-model store's database.
 *
 * <p>The subscription checkpoint's processor creates no {@code <name>_view} table, so {@code
 * audit_mem_view} is normally absent; the check counts its rows, and an absent table holds none.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class ProjectionDeliveryModeE2EIT {

  private static final ProjectionName AUDIT_MEM = ProjectionName.of("audit_mem");

  /** The default read-model store's checkpoints: the transactional four commit theirs here. */
  private static final String READ_MODEL_CHECKPOINT = "readmodel_checkpoint";

  /** The event store's subscription checkpoints: the unrouted at-least-once projection's. */
  private static final String SUBSCRIPTION_CHECKPOINT = "subscription_checkpoint";

  @Inject DataSource dataSource;

  private void createProduct(String id, String name, double price) {
    String body =
        """
        {
          "productId": "%s",
          "name": "%s",
          "description": "test product",
          "category": "Test",
          "price": %s
        }"""
            .formatted(id, name, price);
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body(body)
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);
  }

  private String regclass(String qualifiedName) throws SQLException {
    try (var c = dataSource.getConnection();
        var ps = c.prepareStatement("SELECT to_regclass(?)")) {
      ps.setString(1, qualifiedName);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getString(1);
      }
    }
  }

  /**
   * The checkpoint of {@code projection} in {@code table}: {@code readmodel_checkpoint}, where the
   * read-model store keeps the checkpoints of the projections routed to it, or {@code
   * subscription_checkpoint}, the event store's, where an at-least-once projection that writes
   * elsewhere keeps its own. -1 when the table holds no row for it.
   */
  private long lastOffset(String table, String projection) throws SQLException {
    try (var c = dataSource.getConnection();
        var ps =
            c.prepareStatement("SELECT last_offset FROM " + table + " WHERE projection_name = ?")) {
      ps.setString(1, projection);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : -1L;
      }
    }
  }

  @Test
  void anAtLeastOnceProjectionOverMemory_runsBesideTheTransactionalFour() throws Exception {
    int before = AuditMemProjection.MEMORY.rowCount(AUDIT_MEM);
    String pid = "q-p-mode-" + System.nanoTime();
    createProduct(pid, "ModeWidget", 12.50);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(AuditMemProjection.MEMORY.rowCount(AUDIT_MEM))
                    .as("the at-least-once projection applied the new events to its own store")
                    .isGreaterThan(before)
                    .isGreaterThanOrEqualTo(1));
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(lastOffset(SUBSCRIPTION_CHECKPOINT, "audit_mem"))
                    .as("the at-least-once projection's checkpoint is the subscription checkpoint")
                    .isGreaterThanOrEqualTo(1L));
    assertThat(lastOffset(READ_MODEL_CHECKPOINT, "audit_mem"))
        .as("the at-least-once projection keeps no checkpoint in the read-model store")
        .isEqualTo(-1L);
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(productRows(pid))
                    .as("the TRANSACTIONAL_LOCAL four write where they should")
                    .isEqualTo(1L));
    // The product's row and the products checkpoint commit together, in the read-model store.
    assertThat(lastOffset(READ_MODEL_CHECKPOINT, "products"))
        .as("a projection routed to the read-model store checkpoints beside its read model")
        .isGreaterThanOrEqualTo(1L);
    assertThat(lastOffset(SUBSCRIPTION_CHECKPOINT, "products"))
        .as("a projection routed to the read-model store keeps no subscription checkpoint")
        .isEqualTo(-1L);

    assertThat(auditMemRows())
        .as("nothing of the at-least-once projection lands in the read-model store's database")
        .isZero();
  }

  /** Rows of {@code products_view} in the read-model store's database for one product id. */
  private long productRows(String productId) throws SQLException {
    if (regclass("public.products_view") == null) {
      return 0L;
    }
    try (var c = dataSource.getConnection();
        var ps = c.prepareStatement("SELECT count(*) FROM public.products_view WHERE id = ?")) {
      ps.setString(1, productId);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  /**
   * Rows of {@code audit_mem_view} in the read-model store's database; an absent table holds none.
   */
  private long auditMemRows() throws SQLException {
    if (regclass("public.audit_mem_view") == null) {
      return 0L;
    }
    try (var c = dataSource.getConnection();
        var ps = c.prepareStatement("SELECT count(*) FROM public.audit_mem_view");
        var rs = ps.executeQuery()) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
