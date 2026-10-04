package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;

/**
 * End-to-end proof of the saga-timeout sweep against the demo's real {@link
 * org.streamrune.postgres.PostgresSagaStore} (Testcontainers PostgreSQL).
 *
 * <p>The demo's {@link OrderFulfillmentSaga} is a {@code SagaOrchestrator} with no timeout, and the
 * app does not wire a {@link SagaTimeoutRunner}. This test drives the framework runner directly
 * against the demo's persisted saga state, using a {@link SagaDecider} that reuses the demo saga's
 * {@code evolve}/{@code handle}/{@code compensate} logic and adds a {@code timeout()}.
 *
 * <p>Time is made deterministic by injecting a fixed {@link Clock} into the runner and exercising
 * its package-private {@code pollOnce()} (the framework feature mirrored from {@code
 * DeadLetterRetryRunner}). {@code pollOnce()} computes the cutoff as {@code clock.now() - timeout};
 * a saga is swept only when its stored {@code updated_at} is strictly before that cutoff. We anchor
 * both clocks to the saga row's actual {@code updated_at} so the boundary is exact — no {@code
 * Thread.sleep} on wall-clock time.
 *
 * <p>Non-vacuity contrast: a saga that has NOT yet timed out (cutoff before its {@code updated_at})
 * yields no compensation; the same saga once past the cutoff yields exactly the compensation
 * commands its state dictates.
 */
class SagaTimeoutE2EIT extends AbstractIntegrationTest {

  private static final Duration TIMEOUT = Duration.ofMinutes(30);

  @Autowired private SagaStore sagaStore;
  @Autowired private DataSource dataSource;

  /** Records every command dispatched, so the test can assert the exact compensation set. */
  private static final class RecordingCommandBus implements CommandBus {
    final List<Command> dispatched = new CopyOnWriteArrayList<>();

    @Override
    public <C extends Command> CommandResult execute(C command) {
      dispatched.add(command);
      // The runner only needs execute() not to throw. Return an empty, well-formed result.
      return new CommandResult(
          List.of(),
          org.streamrune.core.types.StreamId.of(
              OrderState.TYPE, org.streamrune.core.types.AggregateId.of("saga-timeout-test")),
          org.streamrune.core.types.Version.initial(),
          List.of());
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      // SagaCommandDispatch now dispatches under an idempotency key; the interface default
      // throws UnsupportedOperationException, which would fault the sweep. Same recording +
      // result logic as the 1-arg version — this fake has no real inbox to key against.
      return execute(command);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      // SagaTimeoutRunner.Builder.build() now fails fast with IllegalStateException
      // unless the CommandBus reports idempotent-execution support, since timeout compensation
      // dispatches exclusively through the keyed execute(command, key) overload with no unkeyed
      // fallback. This fake has no real CommandInbox but DOES implement the keyed overload above
      // (recording + returning the same well-formed result as the 1-arg version), so it honestly
      // supports it.
      return true;
    }
  }

  /**
   * Adapts the demo's {@link OrderFulfillmentSaga} to the {@link SagaDecider} contract the {@link
   * SagaTimeoutRunner} consumes, reusing its real {@code evolve}/{@code handle}/{@code compensate}
   * and adding a {@link #timeout()}. Routing methods (start/correlate) are irrelevant to the
   * timeout sweep, so they are not part of the decider contract.
   */
  private static final class TimeoutDecider implements SagaDecider<OrderFulfillmentState> {
    private final OrderFulfillmentSaga delegate = new OrderFulfillmentSaga();

    @Override
    public Class<OrderFulfillmentState> stateType() {
      return delegate.stateType();
    }

    @Override
    public OrderFulfillmentState initialState(SagaId sagaId) {
      return delegate.initialState(sagaId);
    }

    @Override
    public OrderFulfillmentState evolve(OrderFulfillmentState state, EventEnvelope event) {
      return delegate.evolve(state, event);
    }

    @Override
    public List<SagaCommand> handle(OrderFulfillmentState state, EventEnvelope event) {
      return delegate.handle(state, event);
    }

    @Override
    public List<SagaCommand> compensate(
        OrderFulfillmentState state, Throwable failure, SagaCommand failedCommand) {
      return delegate.compensate(state, failure, failedCommand);
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(TIMEOUT);
    }
  }

  private SagaTimeoutRunner<OrderFulfillmentState> runnerWithClock(
      CommandBus commandBus, Clock clock) {
    return SagaTimeoutRunner.<OrderFulfillmentState>builder()
        .decider(new TimeoutDecider())
        .sagaStore(sagaStore)
        // SagaRunner persists under the state class simple name; the sweep must query the same.
        .sagaType(SagaType.fromClass(OrderFulfillmentState.class))
        .commandBus(commandBus)
        .clock(clock)
        .build();
  }

  /**
   * Seeds a saga stuck mid-fulfilment (payment captured, awaiting inventory) and returns its id.
   */
  private SagaId seedAwaitingInventorySaga() {
    SagaId sagaId = new SagaId("fulfillment-to-" + System.nanoTime());
    String orderId = "to-order-" + System.nanoTime();
    String paymentId = "pay-" + orderId;
    OrderFulfillmentState state =
        new OrderFulfillmentState(
            sagaId,
            orderId,
            "to-cust-" + System.nanoTime(),
            paymentId,
            new Money(new BigDecimal("42.00"), "USD"),
            List.of(),
            OrderFulfillmentStatus.AWAITING_INVENTORY);
    // RUNNING: the saga is mid-fulfilment (awaiting inventory), not yet in a terminal status.
    sagaStore.create(
        sagaId, SagaType.fromClass(OrderFulfillmentState.class), state, SagaStatus.RUNNING);
    return sagaId;
  }

  /** Reads the DB-assigned {@code updated_at} for a saga row (the value the sweep compares). */
  private Instant updatedAt(SagaId sagaId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT updated_at FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("seeded saga row must exist").isTrue();
        return rs.getTimestamp("updated_at").toInstant();
      }
    }
  }

  /**
   * Returns the commands dispatched for THIS saga only. The runner sweeps every timed-out saga of
   * the type, and the shared Testcontainers DB may hold sagas from other tests; filtering by this
   * saga's payment/order ids keeps the assertion deterministic regardless of that background state.
   */
  private static List<Command> compensationsFor(
      RecordingCommandBus bus, OrderFulfillmentState saga) {
    return bus.dispatched.stream()
        .filter(
            c ->
                (c instanceof PaymentCommand.RefundPayment r
                        && r.paymentId().equals(saga.paymentId()))
                    || (c instanceof OrderCommand.CancelOrder o
                        && o.orderId().equals(saga.orderId())))
        .toList();
  }

  @Test
  void timedOutSagaFiresCompensationDeterministically() throws Exception {
    SagaId sagaId = seedAwaitingInventorySaga();
    OrderFulfillmentState saga =
        sagaStore
            .load(
                sagaId,
                SagaType.fromClass(OrderFulfillmentState.class),
                OrderFulfillmentState.class)
            .orElseThrow()
            .state();
    Instant updatedAt = updatedAt(sagaId);

    // ---- (1) NOT yet timed out: clock such that (now - TIMEOUT) is BEFORE the saga's updated_at.
    // cutoff = now - TIMEOUT = (updatedAt + TIMEOUT - 1s) - TIMEOUT = updatedAt - 1s  < updatedAt
    var bus1 = new RecordingCommandBus();
    Clock beforeTimeout = Clock.fixed(updatedAt.plus(TIMEOUT).minusSeconds(1), ZoneOffset.UTC);
    SagaTimeoutRunnerTestAccess.pollOnce(runnerWithClock(bus1, beforeTimeout));
    assertThat(compensationsFor(bus1, saga))
        .as("saga not past its timeout must not be compensated (non-vacuity guard)")
        .isEmpty();

    // ---- (2) Timed out: clock such that (now - TIMEOUT) is AFTER the saga's updated_at.
    // cutoff = now - TIMEOUT = (updatedAt + TIMEOUT + 60s) - TIMEOUT = updatedAt + 60s > updatedAt
    var bus2 = new RecordingCommandBus();
    Clock pastTimeout = Clock.fixed(updatedAt.plus(TIMEOUT).plusSeconds(60), ZoneOffset.UTC);
    SagaTimeoutRunnerTestAccess.pollOnce(runnerWithClock(bus2, pastTimeout));

    // AWAITING_INVENTORY compensation = RefundPayment + CancelOrder (see OrderFulfillmentSaga).
    List<Command> mine = compensationsFor(bus2, saga);
    assertThat(mine)
        .as("timed-out AWAITING_INVENTORY saga must dispatch exactly refund + cancel")
        .hasSize(2);
    assertThat(mine.get(0)).isInstanceOf(PaymentCommand.RefundPayment.class);
    assertThat(mine.get(1)).isInstanceOf(OrderCommand.CancelOrder.class);

    PaymentCommand.RefundPayment refund = (PaymentCommand.RefundPayment) mine.get(0);
    OrderCommand.CancelOrder cancel = (OrderCommand.CancelOrder) mine.get(1);
    assertThat(refund.paymentId()).isEqualTo(saga.paymentId());
    assertThat(cancel.orderId()).isEqualTo(saga.orderId());
  }
}
