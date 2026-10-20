package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.metrics.MetricNames;

/**
 * The app reports the framework's meters at {@code /actuator/metrics}, as the Quarkus and Micronaut
 * apps do at {@code /q/metrics} and {@code /metrics} (their smoke suites check the same meters).
 * The two dead-letter gauges are the ones the demo's {@code DeadLetterRetryRunner} samples; {@code
 * saga.faulted_backlog} and {@code projections.processed} stand for the saga and projection meters.
 */
class StreamRuneMetricsIT extends AbstractIntegrationTest {

  @Test
  void theFrameworksMetersAreListedAtTheMetricsEndpoint() throws Exception {
    String body =
        client
            .get()
            .uri("/actuator/metrics")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(String.class)
            .returnResult()
            .getResponseBody();

    List<String> names = new ArrayList<>();
    for (JsonNode name : objectMapper.readTree(body).path("names")) {
      names.add(name.asText());
    }
    assertThat(names)
        .contains(
            MetricNames.DLQ_AWAITING_OPERATOR,
            MetricNames.DLQ_PENDING,
            MetricNames.SAGA_FAULTED_BACKLOG,
            MetricNames.PROJECTIONS_PROCESSED,
            MetricNames.COMMANDS_DISPATCHED);
  }
}
