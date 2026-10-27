package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import java.util.Map;
import org.junit.jupiter.api.Test;
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

  @Test
  void aDeadLetteredCommandIsAnsweredByWhetherItHasAKey_notByItsCause() {
    HttpResponse<Map<String, String>> queued =
        new CommandRefusalExceptionHandlers.CommandDeadLettered()
            .handle(
                null,
                deadLettered(
                    IdempotencyKey.scopedTo("admin-1", "k-1"), new LockException("timeout")));
    assertRefused(queued, "QUEUED_FOR_REPLAY");
    assertThat(queued.body()).containsEntry("commandId", "cmd-dl-1");

    HttpResponse<Map<String, String>> awaiting =
        new CommandRefusalExceptionHandlers.CommandDeadLettered()
            .handle(null, deadLettered(null, new EventStoreException("connection reset")));
    assertThat(awaiting.getStatus().getCode()).isEqualTo(500);
    assertThat(awaiting.getHeaders().get("Retry-After")).isNull();
    assertThat(awaiting.getContentType()).contains(MediaType.APPLICATION_JSON_TYPE);
    assertThat(awaiting.body())
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

  private static void assertRefused(HttpResponse<Map<String, String>> answer, String reason) {
    assertThat(answer.getStatus().getCode()).isEqualTo(503);
    assertThat(answer.getHeaders().get("Retry-After")).isEqualTo("5");
    assertThat(answer.getContentType()).contains(MediaType.APPLICATION_JSON_TYPE);
    assertThat(answer.body()).containsEntry("reason", reason).containsKey("message");
  }
}
