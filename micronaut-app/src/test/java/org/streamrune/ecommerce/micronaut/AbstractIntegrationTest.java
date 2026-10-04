package org.streamrune.ecommerce.micronaut;

import io.micronaut.test.support.TestPropertyProvider;
import java.util.Map;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Shared base for the Micronaut HTTP integration tests. Spins up a real PostgreSQL container once
 * for the whole suite (singleton-container pattern), loads the framework + crypto schema via {@code
 * scripts/init-db.sql}, and points the Micronaut {@code datasources.default} at it through {@link
 * TestPropertyProvider}.
 *
 * <p>Because the container URL is identical for every subclass, Micronaut Test caches a single
 * application context across test classes — the embedded server, event store, crypto engine, and
 * projection runner are started once. Projection auto-discovery is left at its default (enabled) so
 * the {@code customers}/{@code products}/{@code orders} projections actually run.
 *
 * <p>{@code PER_CLASS} lifecycle is required for {@link TestPropertyProvider}: Micronaut Test reads
 * {@link #getProperties()} from a test instance constructed in {@code @BeforeAll}, which is only
 * possible when the test instance is shared across the class. Without it, the property provider is
 * never consulted and the datasource falls back to the production {@code application.yml} URL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationTest implements TestPropertyProvider {

  /**
   * Singleton container started once for the entire test suite. {@code withReuse(true)} keeps the
   * datasource URL stable so Micronaut Test can cache and reuse one application context.
   */
  protected static final PostgreSQLContainer<?> POSTGRES;

  static {
    POSTGRES =
        new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("streamrune_ecommerce")
            .withUsername("postgres")
            .withPassword("postgres")
            .withCopyFileToContainer(
                MountableFile.forHostPath("../scripts/init-db.sql"),
                "/docker-entrypoint-initdb.d/init-db.sql")
            .withReuse(true);
    POSTGRES.start();
  }

  @Override
  public Map<String, String> getProperties() {
    if (!POSTGRES.isRunning()) {
      POSTGRES.start();
    }
    return Map.of(
        "datasources.default.url",
        POSTGRES.getJdbcUrl(),
        "datasources.default.username",
        POSTGRES.getUsername(),
        "datasources.default.password",
        POSTGRES.getPassword(),
        "datasources.default.driver-class-name",
        "org.postgresql.Driver",
        // Disable the outbox poller for tests that don't supply a RabbitMQ container.
        // The framework's OutboxPoller factory requires both OutboxStore + OutboxPublisher; with
        // the publisher absent (RabbitMqFactory gated on this property) the poller is not created.
        "streamrune.outbox.enabled",
        "false",
        // Demo schema is owned by scripts/init-db.sql (loaded via Testcontainers
        // initdb); disable Flyway auto-init so the library-default EventStoreFactory
        // does not attempt to run migrations against the pre-created schema.
        "streamrune.event-store.schema.auto-initialize",
        "false",
        // The compensation retry sweeper the framework starts for the order-fulfillment SagaRunner
        // bean re-drives a stuck COMPENSATING saga every second instead of every minute, so
        // SagaRecoveryAdminIT can watch it finish one.
        "streamrune.saga.compensation-retry-interval",
        "1s");
  }
}
