package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.CommandDeadLetteredException;

/**
 * The answer to each refusal the command bus reports, and to a command it dead-lettered. {@code
 * CommandRefusalStatusIT} forces three of the refusals and both dead-lettered answers through the
 * app's routes; the fourth refusal, the bus's admission budget, applies only to {@code
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

  @Test
  void aDeadLetteredCommandIsAnsweredByWhetherItHasAKey_notByItsCause() {
    ResponseEntity<?> queued =
        handler.handleCommandRefused(
            deadLettered(IdempotencyKey.scopedTo("admin-1", "k-1"), new LockException("timeout")));
    assertRefused(queued, "QUEUED_FOR_REPLAY");
    assertThat(queued.getBody())
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("commandId", "cmd-dl-1");

    ResponseEntity<?> awaiting =
        handler.handleCommandRefused(
            deadLettered(null, new EventStoreException("connection reset")));
    assertThat(awaiting.getStatusCode().value()).isEqualTo(500);
    assertThat(awaiting.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNull();
    assertThat(awaiting.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
    assertThat(awaiting.getBody())
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("reason", "AWAITING_OPERATOR")
        .containsEntry("commandId", "cmd-dl-1")
        .containsKey("message");
  }

  private static CommandDeadLetteredException deadLettered(
      IdempotencyKey key, RuntimeException cause) {
    return new CommandDeadLetteredException(
        new ProductCommand.AdjustStock("p-1", 5, "restock"),
        CommandId.of("cmd-dl-1"),
        StreamId.of(AggregateType.of("product"), AggregateId.of("p-1")),
        key,
        cause);
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
