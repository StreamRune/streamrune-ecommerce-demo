package org.streamrune.ecommerce.quarkus;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Starts a real PostgreSQL (Testcontainers) for {@code @QuarkusTest} runs and overrides the
 * datasource config to point at it. The database starts empty: the application creates the schema
 * itself at startup ({@code streamrune.event-store.schema.auto-initialize=true} in {@code
 * application.properties}) — the event-store tables and the crypto-shredding tables ({@code
 * encryption_keys}, {@code forgotten_subjects}, {@code erased_key_generations}) the GDPR flow needs
 * — as it does outside the tests.
 *
 * <p>The container starts before the Quarkus application context, so the datasource the event store
 * factory migrates is reachable when the first bean asks for it.
 */
public class PostgresTestResource implements QuarkusTestResourceLifecycleManager {

  private PostgreSQLContainer<?> postgres;

  @Override
  public Map<String, String> start() {
    postgres =
        new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("streamrune_ecommerce")
            .withUsername("postgres")
            .withPassword("postgres");
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
