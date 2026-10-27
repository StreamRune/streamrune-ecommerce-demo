package org.streamrune.ecommerce.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
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
import org.streamrune.ecommerce.commands.CommandRefusal.Reason;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.CommandDeadLetteredException;
import org.streamrune.runtime.LockAcquisitionException;

/**
 * The refusals the apps answer {@code 503}, the dead-lettered commands whose outcome is pending,
 * and the failures that are neither.
 */
class CommandRefusalTest {

  private static final StreamId PRODUCT =
      StreamId.of(AggregateType.of("product"), AggregateId.of("p-1"));

  @Test
  void eachRefusalNamesItsReasonAndTheWait() {
    assertThat(reasonOf(new CircuitBreakerOpenException("open")))
        .isEqualTo(Reason.CIRCUIT_BREAKER_OPEN);
    assertThat(reasonOf(new CommandBusOverloadedException("full")))
        .isEqualTo(Reason.COMMAND_BUS_OVERLOADED);
    assertThat(reasonOf(new CommandBusClosedException("closed")))
        .isEqualTo(Reason.COMMAND_BUS_CLOSED);
    assertThat(reasonOf(new LockException("timeout"))).isEqualTo(Reason.AGGREGATE_LOCK_UNAVAILABLE);
    assertThat(reasonOf(new OptimisticLockException("conflict")))
        .isEqualTo(Reason.AGGREGATE_VERSION_CONFLICT);
    assertThat(
            reasonOf(
                new LockAcquisitionException(
                    StreamId.of(AggregateType.of("product"), AggregateId.of("p-1")),
                    "Timeout acquiring lock")))
        .as("the striped locker's subtype")
        .isEqualTo(Reason.AGGREGATE_LOCK_UNAVAILABLE);
  }

  @Test
  void theBodyNamesTheReasonFirstAndSaysWhatHappened() {
    CommandRefusal refusal =
        CommandRefusal.of(new CommandBusOverloadedException("full")).orElseThrow();

    assertThat(refusal.body()).containsOnlyKeys("reason", "message");
    assertThat(List.copyOf(refusal.body().keySet())).containsExactly("reason", "message");
    assertThat(refusal.body().get("reason")).isEqualTo("COMMAND_BUS_OVERLOADED");
    assertThat(refusal.body().get("message"))
        .isEqualTo("The command bus admits no more commands now.");
    assertThat(refusal.status()).isEqualTo(503);
    assertThat(refusal.retryLater()).isTrue();
    assertThat(refusal.retryAfterSeconds()).isEqualTo(CommandRefusal.RETRY_AFTER_SECONDS);
    assertThat(CommandRefusal.RETRY_AFTER_SECONDS).isEqualTo(5);
  }

  @Test
  void aDeadLetteredKeyedCommandIsQueuedForReplay_whateverItFailedOn() {
    for (RuntimeException cause :
        List.of(
            new LockException("timeout"),
            new OptimisticLockException("conflict"),
            new EventStoreException("store down"))) {
      CommandRefusal answer =
          CommandRefusal.of(deadLettered(IdempotencyKey.scopedTo("admin-1", "k-1"), cause))
              .orElseThrow();

      assertThat(answer.reason())
          .as("dead-lettered after %s", cause)
          .isEqualTo(Reason.QUEUED_FOR_REPLAY);
      assertThat(answer.status())
          .as("not applied yet: wait, or send it again with the key")
          .isEqualTo(503);
      assertThat(answer.retryLater()).isTrue();
      assertThat(answer.retryAfterSeconds())
          .as("no sooner than the retry runner looks for due entries")
          .isEqualTo(CommandRefusal.QUEUED_RETRY_AFTER_SECONDS)
          .isEqualTo(60);
      assertThat(List.copyOf(answer.body().keySet()))
          .containsExactly("reason", "commandId", "message");
      assertThat(answer.body().get("commandId")).isEqualTo("cmd-dl-1");
      assertThat(answer.body().get("message")).contains("same key");
    }
  }

  @Test
  void aDeadLetteredUnkeyedCommandAwaitsAnOperator_andIsNotSentAgain() {
    for (RuntimeException cause :
        List.of(new LockException("timeout"), new EventStoreException("store down"))) {
      CommandRefusal answer = CommandRefusal.of(deadLettered(null, cause)).orElseThrow();

      assertThat(answer.reason())
          .as("dead-lettered after %s", cause)
          .isEqualTo(Reason.AWAITING_OPERATOR);
      assertThat(answer.status()).isEqualTo(500);
      assertThat(answer.retryLater()).as("no Retry-After: a resend may apply it twice").isFalse();
      assertThat(List.copyOf(answer.body().keySet()))
          .containsExactly("reason", "commandId", "message");
      assertThat(answer.body().get("commandId")).isEqualTo("cmd-dl-1");
      assertThat(answer.body().get("message")).contains("operator");
    }
  }

  /**
   * A keyed command whose entry the retry schedule does not replay — its replays are used up, or it
   * is older than the command-inbox window — awaits an operator as an unkeyed one does: the answer
   * follows the entry's outcome, not the key.
   */
  @Test
  void aDeadLetteredKeyedCommandWhoseEntryTheScheduleDoesNotReplayAwaitsAnOperator() {
    CommandRefusal answer =
        CommandRefusal.of(
                new CommandDeadLetteredException(
                    new ProductCommand.AdjustStock("p-1", 5, "restock"),
                    CommandId.of("cmd-dl-1"),
                    PRODUCT,
                    IdempotencyKey.scopedTo("admin-1", "k-1"),
                    CommandDeadLetteredException.PendingOutcome.AWAITING_OPERATOR,
                    new EventStoreException("store down")))
            .orElseThrow();

    assertThat(answer.reason()).isEqualTo(Reason.AWAITING_OPERATOR);
    assertThat(answer.status()).isEqualTo(500);
    assertThat(answer.retryLater()).as("no Retry-After: an operator decides").isFalse();
    assertThat(List.copyOf(answer.body().keySet()))
        .containsExactly("reason", "commandId", "message");
    assertThat(answer.body().get("commandId")).isEqualTo("cmd-dl-1");
    assertThat(answer.body().get("message")).contains("operator");
  }

  @Test
  void aGenuineFailureOrAnotherRejectionIsNoRefusal() {
    assertThat(CommandRefusal.of(new EventStoreException("store down"))).isEmpty();
    assertThat(CommandRefusal.of(new IllegalStateException("misconfigured"))).isEmpty();
    assertThat(CommandRefusal.of(new IllegalArgumentException("bad request"))).isEmpty();
    assertThat(CommandRefusal.of(null)).isEmpty();
  }

  private static Reason reasonOf(RuntimeException failure) {
    return CommandRefusal.of(failure).orElseThrow().reason();
  }

  /**
   * What the bus throws once the entry of a stock adjustment that failed on {@code cause} is
   * stored.
   */
  private static CommandDeadLetteredException deadLettered(
      IdempotencyKey key, RuntimeException cause) {
    return new CommandDeadLetteredException(
        new ProductCommand.AdjustStock("p-1", 5, "restock"),
        CommandId.of("cmd-dl-1"),
        PRODUCT,
        key,
        cause);
  }
}
