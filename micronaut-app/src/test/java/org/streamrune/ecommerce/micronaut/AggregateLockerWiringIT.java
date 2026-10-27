package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
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
 * holds. The framework's in-process locker steps aside for the app's, whose pool reports its meters
 * to Micrometer.
 */
@MicronautTest(transactional = false)
class AggregateLockerWiringIT extends AbstractIntegrationTest {

  /** The name of the locker's pool, the {@code pool} tag of its meters. */
  private static final String LOCK_POOL = "streamrune-advisory-lock";

  @Inject AggregateLocker locker;

  @Inject DataSource dataSource;

  @Inject MeterRegistry meterRegistry;

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

  /**
   * The lock pool reports itself to Micrometer as {@code hikaricp.connections.*}, tagged {@code
   * pool=streamrune-advisory-lock}: its maximum is the application pool's size, a held lock is one
   * of its active connections, and the gauge of the commands waiting for one and the counter of the
   * waits that ran out are there to show its saturation.
   */
  @Test
  void theLockPoolReportsItsConnectionsToMicrometer() throws Exception {
    int applicationPoolSize = 10;
    assertThat(lockPoolGauge("hikaricp.connections.max").value()).isEqualTo(applicationPoolSize);
    assertThat(lockPoolGauge("hikaricp.connections.pending")).isNotNull();
    assertThat(meterRegistry.find("hikaricp.connections.timeout").tag("pool", LOCK_POOL).counter())
        .as("the waits for a lock connection that ran out")
        .isNotNull();

    try (var held =
        locker.acquireLock(
            StreamId.of(AggregateType.of("product"), AggregateId.of("lock-pool-meters")),
            Duration.ofSeconds(1))) {
      // Hikari samples the pool's state at most once a second.
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(lockPoolGauge("hikaricp.connections.active").value())
                      .as("the lock connection the held lock takes")
                      .isGreaterThanOrEqualTo(1));
    }
  }

  /** A gauge of the lock pool, which must exist. */
  private Gauge lockPoolGauge(String name) {
    Gauge gauge = meterRegistry.find(name).tag("pool", LOCK_POOL).gauge();
    assertThat(gauge).as("%s tagged pool=%s", name, LOCK_POOL).isNotNull();
    return gauge;
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
