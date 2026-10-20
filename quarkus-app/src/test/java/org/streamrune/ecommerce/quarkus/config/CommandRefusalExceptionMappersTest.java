package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
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
class CommandRefusalExceptionMappersTest {

  @Test
  void anOverloadedBusIsAnsweredLikeEveryOtherRefusal() {
    assertRefused(
        new CommandRefusalExceptionMappers.CommandBusOverloaded()
            .toResponse(new CommandBusOverloadedException("budget used up")),
        "COMMAND_BUS_OVERLOADED");
    assertRefused(
        new CommandRefusalExceptionMappers.CircuitBreakerOpen()
            .toResponse(new CircuitBreakerOpenException("open")),
        "CIRCUIT_BREAKER_OPEN");
    assertRefused(
        new CommandRefusalExceptionMappers.CommandBusClosed()
            .toResponse(new CommandBusClosedException("closed")),
        "COMMAND_BUS_CLOSED");
    assertRefused(
        new CommandRefusalExceptionMappers.AggregateLock().toResponse(new LockException("timeout")),
        "AGGREGATE_LOCK_UNAVAILABLE");
    assertRefused(
        new CommandRefusalExceptionMappers.AggregateVersionConflict()
            .toResponse(new OptimisticLockException("conflict")),
        "AGGREGATE_VERSION_CONFLICT");
  }

  private static void assertRefused(Response answer, String reason) {
    assertThat(answer.getStatus()).isEqualTo(503);
    assertThat(answer.getHeaderString("Retry-After")).isEqualTo("5");
    assertThat(answer.getMediaType().toString()).isEqualTo("application/json");
    @SuppressWarnings("unchecked")
    Map<String, String> body = (Map<String, String>) answer.getEntity();
    assertThat(body).containsEntry("reason", reason).containsKey("message");
  }
}
