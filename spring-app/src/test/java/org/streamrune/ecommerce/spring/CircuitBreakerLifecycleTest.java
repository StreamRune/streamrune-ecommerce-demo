package org.streamrune.ecommerce.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;

class CircuitBreakerLifecycleTest {

  /** What the bus passes to before() and onError(): the command, no result yet. */
  private static final CommandContext CTX =
      new CommandContext(
          new PaymentCommand.CapturePayment("pay-1"),
          "CapturePayment",
          CommandId.of("test-id"),
          PaymentState.TYPE,
          AggregateId.of("pay-1"),
          null,
          Instant.now());

  /** What the bus passes to after() once the command succeeded: the same context with a result. */
  private static final CommandContext SUCCEEDED =
      new CommandContext(
          CTX.command(),
          CTX.commandType(),
          CTX.commandId(),
          CTX.aggregateType(),
          CTX.aggregateId(),
          new CommandResult(
              List.of(),
              StreamId.of(PaymentState.TYPE, AggregateId.of("pay-1")),
              new Version(1),
              List.of()),
          CTX.timestamp());

  @Test
  void closed_to_open_to_halfOpen_to_closed() {
    AtomicLong clock = new AtomicLong(0);
    var cb =
        new CircuitBreakerCommandInterceptor(
            3,
            Duration.ofSeconds(30),
            CircuitBreakerCommandInterceptor.DEFAULT_PROBE_TIMEOUT,
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock::get);

    // CLOSED: commands pass through.
    assertTrue(cb.before(CTX));
    cb.after(SUCCEEDED);
    assertEquals("CLOSED", cb.circuitState());

    // Three infrastructure failures in a row open the circuit.
    failOnce(cb);
    failOnce(cb);
    assertEquals("CLOSED", cb.circuitState()); // still under threshold
    failOnce(cb);
    assertEquals("OPEN", cb.circuitState());

    // OPEN before the cooldown elapses: fail fast with no handler call.
    var rejected = assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
    assertTrue(rejected.getMessage().contains("CapturePayment"));

    // Advance past the cooldown: the next request becomes the probe (HALF_OPEN).
    clock.addAndGet(Duration.ofSeconds(31).toNanos());
    assertTrue(cb.before(CTX));
    assertEquals("HALF_OPEN", cb.circuitState());

    // Probe succeeds: after() closes the circuit and resets the counter.
    cb.after(SUCCEEDED);
    assertEquals("CLOSED", cb.circuitState());
  }

  /** One command the bus admits and then sees fail on infrastructure. */
  private static void failOnce(CircuitBreakerCommandInterceptor cb) {
    assertTrue(cb.before(CTX));
    cb.onError(CTX, new RuntimeException("gateway down"));
  }
}
