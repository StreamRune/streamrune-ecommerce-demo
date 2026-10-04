package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.payment.PaymentDecider;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;

/**
 * The payment process manager turns only a gateway outcome into a payment decision. A command it
 * cannot dispatch makes {@code onEvents} throw, so the subscription delivers the event again; a
 * second delivery of a settled payment calls neither the gateway nor the bus.
 *
 * <p>The bus is a fake that runs the real {@link PaymentDecider} against an in-memory payment
 * stream and throws the failures a test scripts for it. The gateway breaker opens after a single
 * failure, so a failure it should not have counted shows as an open breaker.
 */
class PaymentProcessManagerTest {

  private static final String PAYMENT = "pay-o-1";
  private static final StreamId STREAM = StreamId.of(PaymentState.TYPE, AggregateId.of(PAYMENT));
  private static final CorrelationId SAGA = CorrelationId.of("fulfillment-o-1");

  private final PaymentStreams store = new PaymentStreams();
  private final ScriptedBus bus = new ScriptedBus(store);
  private final CountingGateway gateway = new CountingGateway();
  private final PaymentGatewayCircuitBreaker breaker =
      new PaymentGatewayCircuitBreaker(1, Duration.ofMinutes(5));
  private final PaymentProcessManager manager =
      new PaymentProcessManager(gateway, breaker, bus, store);

  @Test
  void aCaptureTheStoreRefusesIsDeliveredAgainAndNeverFailsThePayment() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.failNext(PaymentCommand.CapturePayment.class, new EventStoreException("connection reset"));

    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .as("the dispatch failure reaches the subscription, which delivers the event again")
        .isInstanceOf(EventStoreException.class);
    assertThat(bus.attempted()).noneMatch(PaymentCommand.FailPayment.class::isInstance);
    assertThat(breaker.state())
        .as("the gateway answered; its breaker counts no failure")
        .isEqualTo(PaymentGatewayCircuitBreaker.State.CLOSED);
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated");

    manager.onEvents(List.of(initiated));

    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentCaptured");
    assertThat(gateway.calls()).as("the second delivery asks the gateway again").isEqualTo(2);
    assertThat(gateway.hasCaptured(PAYMENT)).isTrue();
    assertThat(gateway.charges())
        .as("the payment id is the gateway's idempotency key: one charge")
        .isEqualTo(1);
  }

  @Test
  void aCaptureRefusedByTheCommandBusBreakerIsDeliveredAgainAndNeverFailsThePayment() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.failNext(
        PaymentCommand.CapturePayment.class, new CircuitBreakerOpenException("circuit open"));

    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(bus.attempted()).noneMatch(PaymentCommand.FailPayment.class::isInstance);
    assertThat(breaker.state()).isEqualTo(PaymentGatewayCircuitBreaker.State.CLOSED);

    manager.onEvents(List.of(initiated));

    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentCaptured");
  }

  @Test
  void aFailPaymentTheBusRefusesIsDeliveredAgainInsteadOfLeavingThePaymentPending() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    gateway.setFailureInjected(true);
    bus.failNext(PaymentCommand.FailPayment.class, new CircuitBreakerOpenException("circuit open"));

    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .as("a FailPayment the bus refused is not swallowed")
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated");
    assertThat(breaker.state())
        .as("the gateway failure itself was counted")
        .isEqualTo(PaymentGatewayCircuitBreaker.State.OPEN);

    manager.onEvents(List.of(initiated));

    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentFailed");
    assertThat(gateway.calls())
        .as("the open gateway breaker fails the payment without calling the gateway")
        .isEqualTo(1);
  }

  @Test
  void aSecondDeliveryOfACapturedPaymentCallsNeitherTheGatewayNorTheBus() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    manager.onEvents(List.of(initiated));
    int dispatched = bus.attempted().size();

    manager.onEvents(List.of(initiated));

    assertThat(gateway.calls()).isEqualTo(1);
    assertThat(bus.attempted()).hasSize(dispatched);
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentCaptured");
  }

  @Test
  void aSecondDeliveryOfAFailedPaymentDoesNotChargeItWhenTheGatewayHasRecovered() {
    // A breaker that stays closed after one failure, so only the payment's state can stop the
    // second gateway call.
    var tolerantManager =
        new PaymentProcessManager(
            gateway, new PaymentGatewayCircuitBreaker(3, Duration.ofMinutes(5)), bus, store);
    EventEnvelope initiated = store.initiate(PAYMENT);
    gateway.setFailureInjected(true);
    tolerantManager.onEvents(List.of(initiated));
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentFailed");
    gateway.setFailureInjected(false);

    tolerantManager.onEvents(List.of(initiated));

    assertThat(gateway.calls()).isEqualTo(1);
    assertThat(gateway.charges()).isZero();
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentFailed");
  }

  @Test
  void aBatchThatFailsPartWayIsSafeToDeliverAgainInFull() {
    EventEnvelope first = store.initiate("pay-o-a");
    EventEnvelope second = store.initiate("pay-o-b");
    bus.failNext(PaymentCommand.CapturePayment.class, null);
    bus.failNext(PaymentCommand.CapturePayment.class, new EventStoreException("connection reset"));

    assertThatThrownBy(() -> manager.onEvents(List.of(first, second)))
        .isInstanceOf(EventStoreException.class);
    manager.onEvents(List.of(first, second));

    assertThat(store.types(StreamId.of(PaymentState.TYPE, AggregateId.of("pay-o-a"))))
        .containsExactly("PaymentInitiated", "PaymentCaptured");
    assertThat(store.types(StreamId.of(PaymentState.TYPE, AggregateId.of("pay-o-b"))))
        .containsExactly("PaymentInitiated", "PaymentCaptured");
    assertThat(gateway.charges()).isEqualTo(2);
  }

  @Test
  void aRejectionBecauseThePaymentIsAlreadySettledIsNotAnError() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.settleFirst(PAYMENT);

    manager.onEvents(List.of(initiated));

    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentFailed");
  }

  @Test
  void aRejectionOfAPaymentThatIsStillPendingIsNotSwallowed() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.failNext(
        PaymentCommand.CapturePayment.class, new DomainException("rejected for another reason"));

    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .isInstanceOf(DomainException.class);
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated");
  }

  @Test
  void theCommandsCarryTheSagaCorrelationAndAKeyThatIsTheSameOnEveryDelivery() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.failNext(PaymentCommand.CapturePayment.class, new EventStoreException("connection reset"));
    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .isInstanceOf(EventStoreException.class);
    manager.onEvents(List.of(initiated));

    assertThat(bus.keys())
        .containsExactly(
            new IdempotencyKey("payment-capture:" + PAYMENT),
            new IdempotencyKey("payment-capture:" + PAYMENT));
    assertThat(bus.correlations()).containsOnly(SAGA);
  }

  /** Counts every gateway call and every real charge (a call for a new payment that succeeded). */
  private static final class CountingGateway extends PaymentGatewaySimulator {
    private int calls;
    private int charges;

    @Override
    public void processPayment(String paymentId) {
      calls++;
      boolean known = hasCaptured(paymentId);
      super.processPayment(paymentId);
      if (!known) {
        charges++;
      }
    }

    int calls() {
      return calls;
    }

    int charges() {
      return charges;
    }
  }

  /** Payment streams in memory; only {@code readStream} is used by the manager. */
  private static final class PaymentStreams implements EventStore {
    private final Map<StreamId, List<EventEnvelope>> streams = new HashMap<>();
    private long offset;

    EventEnvelope initiate(String paymentId) {
      append(
          StreamId.of(PaymentState.TYPE, AggregateId.of(paymentId)),
          new PaymentEvent.PaymentInitiated(
              paymentId,
              paymentId.substring("pay-".length()),
              new Money(new BigDecimal("10.00"), "USD")));
      return streams.get(StreamId.of(PaymentState.TYPE, AggregateId.of(paymentId))).getFirst();
    }

    void append(StreamId streamId, PaymentEvent event) {
      List<EventEnvelope> stream = streams.computeIfAbsent(streamId, id -> new ArrayList<>());
      stream.add(
          new EventEnvelope(
              GlobalOffset.of(++offset),
              streamId,
              new Version(stream.size() + 1L),
              EventType.fromClass(event.getClass()),
              event,
              new EventMetadata(
                  EventId.of("evt-" + UUID.randomUUID()),
                  CommandId.of("cmd-" + UUID.randomUUID()),
                  null,
                  null,
                  SAGA,
                  null,
                  null,
                  Instant.now(),
                  Map.of())));
    }

    PaymentState state(StreamId streamId, PaymentDecider decider) {
      PaymentState state = decider.initialState();
      for (EventEnvelope envelope : streams.getOrDefault(streamId, List.of())) {
        state = decider.evolve(state, (PaymentEvent) envelope.event());
      }
      return state;
    }

    List<String> types(StreamId streamId) {
      return streams.getOrDefault(streamId, List.of()).stream()
          .map(e -> e.eventType().name())
          .toList();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return streams.getOrDefault(streamId, List.of()).stream()
          .filter(e -> e.version().value() > afterVersion.value())
          .limit(maxCount)
          .toList();
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AppendResult append(StreamId streamId, List<EventEnvelope> events, Version expected) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Runs the real decider against {@link PaymentStreams}, after throwing whatever a test scripted
   * for the command type (a scripted {@code null} lets that attempt through).
   */
  private static final class ScriptedBus implements CommandBus {
    private final PaymentStreams store;
    private final PaymentDecider decider = new PaymentDecider();
    private final Map<Class<?>, Deque<java.util.Optional<RuntimeException>>> failures =
        new HashMap<>();
    private final List<Command> attempted = new ArrayList<>();
    private final List<IdempotencyKey> keys = new ArrayList<>();
    private final List<CorrelationId> correlations = new ArrayList<>();
    private String settleFirst;

    ScriptedBus(PaymentStreams store) {
      this.store = store;
    }

    void failNext(Class<? extends Command> type, RuntimeException failure) {
      failures
          .computeIfAbsent(type, t -> new ArrayDeque<>())
          .add(java.util.Optional.ofNullable(failure));
    }

    /** Settles the payment (as failed) just before the manager's command reaches the decider. */
    void settleFirst(String paymentId) {
      this.settleFirst = paymentId;
    }

    List<Command> attempted() {
      return attempted;
    }

    List<IdempotencyKey> keys() {
      return keys;
    }

    List<CorrelationId> correlations() {
      return correlations;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return run(command, null);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return run(command, key);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    private CommandResult run(Command command, IdempotencyKey key) {
      attempted.add(command);
      keys.add(key);
      correlations.add(
          StreamRuneContext.CURRENT.isBound()
              ? StreamRuneContext.CURRENT.get().correlationId()
              : null);
      var scripted = failures.getOrDefault(command.getClass(), new ArrayDeque<>()).poll();
      if (scripted != null && scripted.isPresent()) {
        throw scripted.get();
      }
      PaymentCommand payment = (PaymentCommand) command;
      String paymentId =
          switch (payment) {
            case PaymentCommand.InitiatePayment c -> c.paymentId();
            case PaymentCommand.CapturePayment c -> c.paymentId();
            case PaymentCommand.RefundPayment c -> c.paymentId();
            case PaymentCommand.FailPayment c -> c.paymentId();
          };
      StreamId streamId = StreamId.of(PaymentState.TYPE, AggregateId.of(paymentId));
      if (paymentId.equals(settleFirst)) {
        settleFirst = null;
        store.append(streamId, new PaymentEvent.PaymentFailed(paymentId, "settled elsewhere"));
      }
      List<PaymentEvent> events = decider.decide(payment, store.state(streamId, decider));
      events.forEach(e -> store.append(streamId, e));
      return new CommandResult(List.copyOf(events), streamId, Version.initial(), List.of());
    }
  }
}
