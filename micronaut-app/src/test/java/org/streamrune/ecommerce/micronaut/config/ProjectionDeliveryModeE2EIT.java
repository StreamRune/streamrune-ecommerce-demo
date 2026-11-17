package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.ecommerce.micronaut.AbstractIntegrationTest;

/**
 * An at-least-once projection over an in-memory store ({@link AuditMemProjection}) boots and runs
 * in the real app beside the four TRANSACTIONAL_LOCAL PostgreSQL projections. The framework's
 * ProjectionFactory routes the four to the default read-model store, {@code postgres}, because they
 * write through its repository: their rows and their checkpoints ({@code readmodel_checkpoint})
 * commit together in the app's database. The at-least-once projection writes to a repository that
 * belongs to no store, so it is routed to the event store's subscription checkpoint: its checkpoint
 * lands in {@code subscription_checkpoint}, its rows land only in its own store, and none of them
 * lands in the read-model store's database.
 *
 * <p>The subscription checkpoint's processor creates no {@code <name>_view} table, so {@code
 * audit_mem_view} is normally absent; the check counts its rows, and an absent table holds none.
 *
 * <p>The in-memory store is static, and every Micronaut test context in this module runs the
 * projection, some of them over another database. Its rows are keyed by global offset, so the store
 * is checked at this database's checkpoint against this database's event at that offset, not by its
 * size.
 */
@MicronautTest(transactional = false)
class ProjectionDeliveryModeE2EIT extends AbstractIntegrationTest {

  private static final ProjectionName AUDIT_MEM = ProjectionName.of("audit_mem");

  /** The default read-model store's checkpoints: the transactional four commit theirs here. */
  private static final String READ_MODEL_CHECKPOINT = "readmodel_checkpoint";

  /** The event store's subscription checkpoints: the unrouted at-least-once projection's. */
  private static final String SUBSCRIPTION_CHECKPOINT = "subscription_checkpoint";

  /**
   * Long enough for a fresh test context to take over leadership: when the previous context's
   * leases were not released at its shutdown, this context's projections stand by until those
   * leases expire (the lease TTL plus one standby retry) before they process anything.
   */
  private static final Duration PROPAGATION = Duration.ofSeconds(60);

  @Inject
  @Client("/")
  HttpClient client;

  @Inject DataSource dataSource;

  @Test
  void anAtLeastOnceInMemoryProjectionRunsBesideTheTransactionalFour_andWritesOnlyToMemory()
      throws Exception {
    String pid = "m-p-mode-" + System.nanoTime();
    String body =
        """
        {
          "productId": "%s",
          "name": "Mode probe",
          "description": "test product",
          "category": "Test",
          "price": 9.99
        }"""
            .formatted(pid);
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/products", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);

    await()
        .atMost(PROPAGATION)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(productRows(pid))
                    .as("the TRANSACTIONAL_LOCAL four write where they should")
                    .isEqualTo(1L));
    // The product's row and the products checkpoint commit together, so this checkpoint is at or
    // past the event the POST appended.
    long productsCheckpoint = lastOffset(READ_MODEL_CHECKPOINT, "products");
    assertThat(lastOffset(SUBSCRIPTION_CHECKPOINT, "products"))
        .as("a projection routed to the read-model store keeps no subscription checkpoint")
        .isEqualTo(-1L);

    await()
        .atMost(PROPAGATION)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(lastOffset(SUBSCRIPTION_CHECKPOINT, "audit_mem"))
                    .as("the at-least-once projection's checkpoint is the subscription checkpoint")
                    .isGreaterThanOrEqualTo(productsCheckpoint));
    long checkpoint = lastOffset(SUBSCRIPTION_CHECKPOINT, "audit_mem");
    assertThat(lastOffset(READ_MODEL_CHECKPOINT, "audit_mem"))
        .as("the at-least-once projection keeps no checkpoint in the read-model store")
        .isEqualTo(-1L);

    assertThat(
            AuditMemProjection.MEMORY.findById(AUDIT_MEM, String.valueOf(checkpoint), String.class))
        .as("the at-least-once projection applied the event at its checkpoint to its own store")
        .contains(eventTypeAt(checkpoint));
    assertThat(auditMemRows())
        .as("nothing of the at-least-once projection lands in the read-model store's database")
        .isZero();
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

  /** The type of this database's event at {@code globalOffset}. */
  private String eventTypeAt(long globalOffset) throws SQLException {
    try (var c = dataSource.getConnection();
        var ps =
            c.prepareStatement("SELECT event_type FROM event_stream WHERE global_offset = ?")) {
      ps.setLong(1, globalOffset);
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).as("an event at offset %d", globalOffset).isTrue();
        return rs.getString(1);
      }
    }
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
