package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;

/**
 * The answer to each refusal the command bus reports. {@code CommandRefusalStatusIT} forces three
 * of them through the app's routes; the fourth, the bus's admission budget, applies only to {@code
 * executeAsync}, which no route calls, so its answer is checked here.
 */
class CommandRefusalExceptionHandlersTest {

  @Test
  void anOverloadedBusIsAnsweredLikeEveryOtherRefusal() {
    assertRefused(
        new CommandRefusalExceptionHandlers.CommandBusOverloaded()
            .handle(null, new CommandBusOverloadedException("budget used up")),
        "COMMAND_BUS_OVERLOADED");
    assertRefused(
        new CommandRefusalExceptionHandlers.CircuitBreakerOpen()
            .handle(null, new CircuitBreakerOpenException("open")),
        "CIRCUIT_BREAKER_OPEN");
    assertRefused(
        new CommandRefusalExceptionHandlers.CommandBusClosed()
            .handle(null, new CommandBusClosedException("closed")),
        "COMMAND_BUS_CLOSED");
    assertRefused(
        new CommandRefusalExceptionHandlers.AggregateLock()
            .handle(null, new LockException("timeout")),
        "AGGREGATE_LOCK_UNAVAILABLE");
    assertRefused(
        new CommandRefusalExceptionHandlers.AggregateVersionConflict()
            .handle(null, new OptimisticLockException("conflict")),
        "AGGREGATE_VERSION_CONFLICT");
  }

  private static void assertRefused(HttpResponse<Map<String, String>> answer, String reason) {
    assertThat(answer.getStatus().getCode()).isEqualTo(503);
    assertThat(answer.getHeaders().get("Retry-After")).isEqualTo("5");
    assertThat(answer.getContentType()).contains(MediaType.APPLICATION_JSON_TYPE);
    assertThat(answer.body()).containsEntry("reason", reason).containsKey("message");
  }
}
