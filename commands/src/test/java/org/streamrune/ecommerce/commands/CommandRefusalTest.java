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
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.commands.CommandRefusal.Reason;
import org.streamrune.runtime.LockAcquisitionException;

/** The refusals the apps answer {@code 503}, and the failures they do not. */
class CommandRefusalTest {

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
    assertThat(CommandRefusal.RETRY_AFTER_SECONDS).isEqualTo(5);
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
}
