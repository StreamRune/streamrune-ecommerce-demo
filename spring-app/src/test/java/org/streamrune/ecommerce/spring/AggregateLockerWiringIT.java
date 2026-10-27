package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.postgres.PgAdvisoryLocker;

/**
 * The command bus locks an aggregate in PostgreSQL ({@link PgAdvisoryLocker}) on a pool of the
 * locker's own, whose meters reach Micrometer and {@code /actuator/metrics}, as in the Quarkus and
 * Micronaut apps. {@code TwoInstanceSagaIT} shows a command on one backend waiting for the lock a
 * command on the other holds.
 */
class AggregateLockerWiringIT extends AbstractIntegrationTest {

  /** The name of the locker's pool, the {@code pool} tag of its meters. */
  private static final String LOCK_POOL = "streamrune-advisory-lock";

  @Autowired private AggregateLocker locker;

  @Autowired private MeterRegistry meterRegistry;

  @Test
  void theCommandBusLocksAnAggregateInPostgreSql() {
    assertThat(locker)
        .as("the app's locker, not the framework's in-process default")
        .isInstanceOf(PgAdvisoryLocker.class);
  }

  /**
   * The lock pool reports itself to Micrometer as {@code hikaricp.connections.*}, tagged {@code
   * pool=streamrune-advisory-lock}: its maximum is the application pool's size, a held lock is one
   * of its active connections, and the gauge of the commands waiting for one and the counter of the
   * waits that ran out are there to show its saturation. The metrics endpoint answers the query
   * Chapter 15 of the tutorial sends.
   */
  @Test
  void theLockPoolReportsItsConnectionsToMicrometer() throws Exception {
    assertThat(lockPoolGauge("hikaricp.connections.max").value()).isEqualTo(10);
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

    JsonNode lockPool = metric("hikaricp.connections.active?tag=pool:" + LOCK_POOL);
    assertThat(lockPool.path("name").asText()).isEqualTo("hikaricp.connections.active");
    List<String> pools = new ArrayList<>();
    for (JsonNode tag : metric("hikaricp.connections.active").path("availableTags")) {
      if (tag.path("tag").asText().equals("pool")) {
        tag.path("values").forEach(value -> pools.add(value.asText()));
      }
    }
    assertThat(pools)
        .as("the pools that report the name: the lock pool and the application's")
        .contains(LOCK_POOL)
        .hasSize(2);
  }

  /** The answer of {@code /actuator/metrics/} to {@code query}. */
  private JsonNode metric(String query) throws Exception {
    String body =
        client
            .get()
            .uri("/actuator/metrics/" + query)
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(String.class)
            .returnResult()
            .getResponseBody();
    return objectMapper.readTree(body);
  }

  /** A gauge of the lock pool, which must exist. */
  private Gauge lockPoolGauge(String name) {
    Gauge gauge = meterRegistry.find(name).tag("pool", LOCK_POOL).gauge();
    assertThat(gauge).as("%s tagged pool=%s", name, LOCK_POOL).isNotNull();
    return gauge;
  }
}
