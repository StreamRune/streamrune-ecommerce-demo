package org.streamrune.ecommerce.micronaut.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import javax.sql.DataSource;

/**
 * The connections the aggregate lock is taken on ({@code StreamRuneFactory#aggregateLocker}): a
 * Hikari pool of their own, apart from the application's pool, whose meters reach Micrometer as
 * {@code hikaricp.connections.*} with the tag {@code pool=streamrune-advisory-lock}. A command
 * holds one of its connections from its lock to its end, so {@code hikaricp.connections.active} is
 * the commands in flight and {@code hikaricp.connections.pending} the commands waiting for a
 * connection. The pool is saturated when {@code active} stands at {@code hikaricp.connections.max}
 * and {@code pending} is above zero; a wait that runs out is counted in {@code
 * hikaricp.connections.timeout}, and its command's attempt fails with a {@code LockException}.
 *
 * <p>The pool has the settings of the one {@code PgAdvisoryLocker.withDedicatedPool} builds, which
 * that locker keeps to itself and reports nowhere: the app builds the pool here and hands it to
 * {@code new PgAdvisoryLocker(DataSource)}. A locker built on a pool it is given does not close the
 * pool, so the application closes it ({@link #close()}). The Spring and Quarkus apps build the same
 * pool.
 */
public final class AdvisoryLockPool implements AutoCloseable {

  /** The pool's name, the value of the {@code pool} tag of its meters. */
  public static final String NAME = "streamrune-advisory-lock";

  private final HikariDataSource connections;

  /**
   * Opens the pool: {@code size} connections to the database at {@code url}, its meters in {@code
   * meterRegistry}.
   */
  public AdvisoryLockPool(
      String url, String username, String password, int size, MeterRegistry meterRegistry) {
    HikariConfig config = new HikariConfig();
    config.setPoolName(NAME);
    config.setJdbcUrl(url);
    config.setUsername(username);
    config.setPassword(password);
    config.setMaximumPoolSize(size);
    // When every connection is taken, a command waits this long for one, then fails with a
    // LockException.
    config.setConnectionTimeout(Duration.ofSeconds(10).toMillis());
    config.setKeepaliveTime(Duration.ofMinutes(5).toMillis());
    config.setMaxLifetime(Duration.ofMinutes(30).toMillis());
    config.setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory(meterRegistry));
    connections = new HikariDataSource(config);
  }

  /** The pool, for the locker. */
  public DataSource dataSource() {
    return connections;
  }

  /** Closes the pool's connections and removes its meters. */
  @Override
  public void close() {
    connections.close();
  }
}
