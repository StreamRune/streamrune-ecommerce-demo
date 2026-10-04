package org.streamrune.ecommerce.quarkus;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Starts a real PostgreSQL (Testcontainers) for {@code @QuarkusTest} runs and overrides the
 * datasource config to point at it. The schema — including the crypto-shredding tables ({@code
 * encryption_keys}, {@code forgotten_subjects}) the GDPR flow needs — is loaded from {@code
 * scripts/init-db.sql} via the container's docker-entrypoint-initdb.d hook, exactly as the Spring
 * app's {@code AbstractIntegrationTest} does.
 *
 * <p>The container starts before the Quarkus application context, so {@code
 * JdbcProjectionRepository} (which auto-creates {@code *_view} tables on first write) and the wired
 * event store see a fully provisioned database.
 */
public class PostgresTestResource implements QuarkusTestResourceLifecycleManager {

  private PostgreSQLContainer<?> postgres;

  @Override
  public Map<String, String> start() {
    postgres =
        new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("streamrune_ecommerce")
            .withUsername("postgres")
            .withPassword("postgres")
            .withCopyFileToContainer(
                MountableFile.forHostPath("../scripts/init-db.sql"),
                "/docker-entrypoint-initdb.d/init-db.sql");
    postgres.start();
    return Map.of(
        "quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
        "quarkus.datasource.username", postgres.getUsername(),
        "quarkus.datasource.password", postgres.getPassword());
  }

  @Override
  public void stop() {
    if (postgres != null) {
      postgres.stop();
    }
  }
}
