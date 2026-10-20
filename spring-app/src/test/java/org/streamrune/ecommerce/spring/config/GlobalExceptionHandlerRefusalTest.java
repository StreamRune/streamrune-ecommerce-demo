package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
class GlobalExceptionHandlerRefusalTest {

  private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

  @Test
  void anOverloadedBusIsAnsweredLikeEveryOtherRefusal() {
    assertRefused(
        handler.handleCommandRefused(new CommandBusOverloadedException("budget used up")),
        "COMMAND_BUS_OVERLOADED");
    assertRefused(
        handler.handleCommandRefused(new CircuitBreakerOpenException("open")),
        "CIRCUIT_BREAKER_OPEN");
    assertRefused(
        handler.handleCommandRefused(new CommandBusClosedException("closed")),
        "COMMAND_BUS_CLOSED");
    assertRefused(
        handler.handleCommandRefused(new LockException("timeout")), "AGGREGATE_LOCK_UNAVAILABLE");
    assertRefused(
        handler.handleCommandRefused(new OptimisticLockException("conflict")),
        "AGGREGATE_VERSION_CONFLICT");
  }

  private static void assertRefused(ResponseEntity<?> answer, String reason) {
    assertThat(answer.getStatusCode().value()).isEqualTo(503);
    assertThat(answer.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("5");
    assertThat(answer.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
    assertThat(answer.getBody())
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("reason", reason)
        .containsKey("message");
  }
}
