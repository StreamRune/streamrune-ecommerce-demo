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
 * in the real app beside the four TRANSACTIONAL_LOCAL PostgreSQL projections: its rows land only in
 * its own store, its checkpoint lands in the processor's database, and the transactional four write
 * their rows into the processor's database. The framework's ProjectionProducer selected the single
 * JdbcProjectionRepository bean because the four declare TRANSACTIONAL_LOCAL.
 *
 * <p>Before it commits a projection's batch the processor makes sure that projection's {@code
 * <name>_view} table exists, whatever the projection's mode, so an empty {@code audit_mem_view} may
 * exist in its database; what must never land there is a row.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class ProjectionDeliveryModeE2EIT {

  private static final ProjectionName AUDIT_MEM = ProjectionName.of("audit_mem");

  @Inject DataSource dataSource;

  private void createProduct(String id, String name, double price, int stock) {
    String body =
        """
        {
          "productId": "%s",
          "name": "%s",
          "description": "test product",
          "category": "Test",
          "price": %s,
          "initialStock": %d
        }"""
            .formatted(id, name, price, stock);
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

  private long lastOffset(String projection) throws SQLException {
    try (var c = dataSource.getConnection();
        var ps =
            c.prepareStatement(
                "SELECT last_offset FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, projection);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : -1L;
      }
    }
  }

  @Test
  void anAtLeastOnceProjectionOverMemory_runsBesideTheTransactionalFour() throws SQLException {
    int before = AuditMemProjection.MEMORY.rowCount(AUDIT_MEM);
    String pid = "q-p-mode-" + System.nanoTime();
    createProduct(pid, "ModeWidget", 12.50, 10);

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
                assertThat(lastOffset("audit_mem"))
                    .as("the at-least-once projection's checkpoint is in the processor's database")
                    .isGreaterThanOrEqualTo(1L));
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(productRows(pid))
                    .as("the TRANSACTIONAL_LOCAL four write where they should")
                    .isEqualTo(1L));

    assertThat(auditMemRows())
        .as("nothing of the at-least-once projection lands in the processor's database")
        .isZero();
  }

  /** Rows of {@code products_view} in the processor's database for one product id. */
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

  /** Rows of {@code audit_mem_view} in the processor's database; an absent table holds none. */
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
