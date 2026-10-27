package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
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

  @Test
  void aDeadLetteredCommandIsAnsweredByWhatHappensToItsEntry_notByItsCause() {
    Response queued =
        new CommandRefusalExceptionMappers.CommandDeadLettered()
            .toResponse(
                deadLettered(
                    IdempotencyKey.scopedTo("admin-1", "k-1"), new LockException("timeout")));
    assertThat(queued.getStatus()).isEqualTo(503);
    assertThat(queued.getHeaderString("Retry-After"))
        .as("no sooner than the retry runner looks for due entries")
        .isEqualTo("60");
    assertThat(queued.getMediaType().toString()).isEqualTo("application/json");
    assertThat(body(queued))
        .containsEntry("reason", "QUEUED_FOR_REPLAY")
        .containsEntry("commandId", "cmd-dl-1")
        .containsKey("message");

    assertAwaitingOperator(
        new CommandRefusalExceptionMappers.CommandDeadLettered()
            .toResponse(deadLettered(null, new EventStoreException("connection reset"))));
    assertAwaitingOperator(
        new CommandRefusalExceptionMappers.CommandDeadLettered()
            .toResponse(
                deadLettered(
                    IdempotencyKey.scopedTo("admin-1", "k-1"),
                    CommandDeadLetteredException.PendingOutcome.AWAITING_OPERATOR,
                    new LockException("timeout"))));
  }

  /** 500 without {@code Retry-After}, the entry's {@code commandId} in the body. */
  private static void assertAwaitingOperator(Response awaiting) {
    assertThat(awaiting.getStatus()).isEqualTo(500);
    assertThat(awaiting.getHeaderString("Retry-After")).isNull();
    assertThat(awaiting.getMediaType().toString()).isEqualTo("application/json");
    assertThat(body(awaiting))
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

  /** A keyed command whose entry has {@code outcome}, as a re-send under its key is answered. */
  private static CommandDeadLetteredException deadLettered(
      IdempotencyKey key,
      CommandDeadLetteredException.PendingOutcome outcome,
      RuntimeException cause) {
    return new CommandDeadLetteredException(
        new ProductCommand.AdjustStock("p-1", 5, "restock"),
        CommandId.of("cmd-dl-1"),
        StreamId.of(AggregateType.of("product"), AggregateId.of("p-1")),
        key,
        outcome,
        cause);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> body(Response answer) {
    return (Map<String, String>) answer.getEntity();
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
