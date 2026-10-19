package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;

/**
 * End-to-end proof of the saga-timeout sweep against the demo's real {@link
 * org.streamrune.postgres.PostgresSagaStore} (Testcontainers PostgreSQL).
 *
 * <p>The demo's {@link OrderFulfillmentSaga} is a {@code SagaDecider} too, with a deadline, and the
 * app wires a {@link SagaTimeoutRunner} for it ({@code orderFulfillmentTimeoutRunner}, polling
 * every 10 seconds against a 15-minute deadline, which no saga of this test reaches). This test
 * drives a second runner of its own against the demo's persisted saga state, built from the app's
 * saga class with a deadline of {@link #TIMEOUT} and a fixed clock. The runner is bound to the
 * app's saga runner, as the app binds its own: a timeout runner names the {@link SagaRunner} whose
 * step lock it shares, and the framework refuses one that names nothing. It also follows that
 * runner's lease: it claims only while the app's saga subscription leads the saga type in this
 * process, so the test waits for the subscription to report {@link LeaseRole#LEADING} before its
 * first sweep.
 *
 * <p>Time is made deterministic by injecting a fixed {@link Clock} into the runner and exercising
 * its package-private {@code pollOnce()} (the framework feature mirrored from {@code
 * DeadLetterRetryRunner}). {@code pollOnce()} computes the cutoff as {@code clock.now() - timeout};
 * a saga is swept only when its stored {@code updated_at} is strictly before that cutoff. We anchor
 * both clocks to the saga row's actual {@code updated_at} so the boundary is exact — no {@code
 * Thread.sleep} on wall-clock time.
 *
 * <p>Non-vacuity contrast: a saga that has NOT yet timed out (cutoff before its {@code updated_at})
 * yields no compensation; the same saga once past the cutoff yields exactly its compensation, the
 * order's cancellation. What the cancellation undoes is {@code SagaDeadlineUndoE2EIT}'s subject.
 */
class SagaTimeoutE2EIT extends AbstractIntegrationTest {

  private static final Duration TIMEOUT = Duration.ofMinutes(30);

  @Autowired private SagaStore sagaStore;
  @Autowired private DataSource dataSource;
  @Autowired private SagaRunner<?> orderFulfillmentSagaRunner;

  @Autowired
  @Qualifier("orderFulfillmentSagaSubscription")
  private PollingEventSubscription orderFulfillmentSagaSubscription;

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
      // SagaCommandDispatch dispatches under an idempotency key; the interface default
      // throws UnsupportedOperationException, which would fault the sweep. Same recording +
      // result logic as the 1-arg version — this fake has no real inbox to key against.
      return execute(command);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      // SagaTimeoutRunner.Builder.build() fails fast with IllegalStateException
      // unless the CommandBus reports idempotent-execution support, since timeout compensation
      // dispatches exclusively through the keyed execute(command, key) overload with no unkeyed
      // fallback. This fake has no real CommandInbox but DOES implement the keyed overload above
      // (recording + returning the same well-formed result as the 1-arg version), so it honestly
      // supports it.
      return true;
    }
  }

  private SagaTimeoutRunner<OrderFulfillmentState> runnerWithClock(
      CommandBus commandBus, Clock clock) {
    return SagaTimeoutRunner.<OrderFulfillmentState>builder()
        .decider(new OrderFulfillmentSaga(TIMEOUT))
        .sagaStore(sagaStore)
        // The saga type is the state class: the saga runner stores its rows under it, and the
        // sweep selects by it.
        .sagaType(SagaType.fromClass(OrderFulfillmentState.class))
        .commandBus(commandBus)
        .clock(clock)
        // Shares the saga runner's step lock: in this process the claim at a saga's deadline
        // waits for a delivery that is dispatching that saga's forward commands.
        .sagaRunner(orderFulfillmentSagaRunner)
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
                switch (c) {
                  case OrderCommand.CancelOrder o -> o.orderId().equals(saga.orderId());
                  case PaymentCommand.CancelPayment p -> p.paymentId().equals(saga.paymentId());
                  case InventoryCommand.ReleaseStock r -> r.orderId().equals(saga.orderId());
                  default -> false;
                })
        .toList();
  }

  @Test
  void timedOutSagaFiresCompensationDeterministically() throws Exception {
    // The test's runner follows the app's saga lease: a sweep on a process that does not lead
    // claims nothing, and the non-vacuity contrast below would pass for the wrong reason.
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> orderFulfillmentSagaSubscription.leaseRole() == LeaseRole.LEADING);
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

    // The compensation in every state is the order's cancellation (see OrderFulfillmentSaga); its
    // OrderCancelled starts the undo of the stock and the payment.
    assertThat(compensationsFor(bus2, saga))
        .as("a timed-out saga dispatches exactly the order's cancellation")
        .containsExactly(
            new OrderCommand.CancelOrder(saga.orderId(), OrderFulfillmentSaga.COMPENSATION_REASON));
  }
}
