package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.postgres.PgAdvisoryLocker;

/**
 * The command bus locks an aggregate in PostgreSQL ({@link PgAdvisoryLocker}): the lock holds
 * across every instance of the app on one database, as in the Spring app, whose {@code
 * TwoInstanceSagaIT} shows a command on one backend waiting for the lock a command on the other
 * holds. The framework's in-process locker steps aside for the app's.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class AggregateLockerWiringIT {

  @Inject AggregateLocker locker;

  @Inject DataSource dataSource;

  @Test
  void theCommandBusLocksAnAggregateInPostgreSql() throws Exception {
    assertThat(locker)
        .as("the app's locker, not the framework's in-process default")
        .isInstanceOf(PgAdvisoryLocker.class);

    long before = advisoryLocks();
    try (var held =
        locker.acquireLock(
            StreamId.of(AggregateType.of("product"), AggregateId.of("locker-wiring")),
            Duration.ofSeconds(1))) {
      assertThat(advisoryLocks())
          .as("the lock is an advisory lock every instance on the database sees")
          .isGreaterThan(before);
    }
  }

  /** The advisory locks PostgreSQL has granted, on every connection. */
  private long advisoryLocks() throws Exception {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted")) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
