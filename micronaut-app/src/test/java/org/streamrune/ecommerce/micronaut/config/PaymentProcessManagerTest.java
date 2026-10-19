package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.payment.PaymentDecider;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.SagaEnvelopes;

/**
 * The payment process manager turns only a gateway outcome into a payment decision. A command it
 * cannot dispatch makes {@code onEvents} throw, so the subscription delivers the event again; a
 * second delivery of a settled payment calls neither the gateway nor the bus.
 *
 * <p>Every {@code PaymentInitiated} here is one the fulfillment saga caused: it carries the saga's
 * origin and the correlation id saga dispatch binds, as the command bus stores it. The manager
 * dispatches inside {@code StreamRuneContext.continuing(event)}, so its command carries that origin
 * one hop further and the event's id as causation id, and runs with no user.
 *
 * <p>The bus is a fake that runs the real {@link PaymentDecider} against an in-memory payment
 * stream and throws the failures a test scripts for it. Like the shipped buses it first refuses a
 * reserved correlation id that is bound without its saga origin. The gateway breaker opens after a
 * single failure, so a failure it should not have counted shows as an open breaker. Three cases run
 * the framework's own bus, which is what decides whether a failed command is dead-lettered.
 */
class PaymentProcessManagerTest {

  private static final String PAYMENT = "pay-o-1";
  private static final StreamId STREAM = StreamId.of(PaymentState.TYPE, AggregateId.of(PAYMENT));
  private static final SagaId SAGA = SagaId.of("fulfillment-o-1");
  private static final SagaType SAGA_TYPE = SagaType.fromClass(OrderFulfillmentState.class);

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
  void aCaptureOfAPaymentThatFailedInTheMeantimeIsRecordedAndRefunded() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.settleFirst(PAYMENT);

    manager.onEvents(List.of(initiated));

    // The gateway took the money, so the capture is recorded; the payment is refunded in the same
    // append.
    assertThat(store.types(STREAM))
        .containsExactly("PaymentInitiated", "PaymentFailed", "PaymentCaptured", "PaymentRefunded");
  }

  @Test
  void aRejectionPropagates() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.failNext(PaymentCommand.CapturePayment.class, new DomainException("rejected"));

    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .as("no decider refuses a capture of this payment: a rejection is a fault")
        .isInstanceOf(DomainException.class);
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated");
  }

  // ---- A payment cancelled while the gateway charged it: the capture is recorded, and the payment
  // refunds it in the same append. One the gateway never charged is left alone. ----

  @Test
  void aVoidedPaymentTheGatewayCharged_isCapturedSoThatItIsRefunded() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    gateway.chargedEarlier(PAYMENT);
    store.append(
        STREAM, new PaymentEvent.PaymentVoided(PAYMENT, "o-1", "changed my mind"), byARequest());

    manager.onEvents(List.of(initiated));

    assertThat(bus.attempted()).containsExactly(new PaymentCommand.CapturePayment(PAYMENT));
    assertThat(bus.keys()).containsExactly(new IdempotencyKey("payment-capture:" + PAYMENT));
    assertThat(bus.causes().getFirst().causationId())
        .as("the capture continues PaymentInitiated")
        .isEqualTo(CausationId.of(initiated.metadata().eventId().value()));
    assertThat(gateway.calls()).as("the gateway is not asked again").isZero();
    assertThat(store.types(STREAM))
        .containsExactly("PaymentInitiated", "PaymentVoided", "PaymentCaptured", "PaymentRefunded");
  }

  @Test
  void aVoidedPaymentTheGatewayNeverCharged_isLeftAlone() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    store.append(
        STREAM, new PaymentEvent.PaymentVoided(PAYMENT, "o-1", "changed my mind"), byARequest());

    manager.onEvents(List.of(initiated));

    assertThat(bus.attempted()).isEmpty();
    assertThat(gateway.calls()).as("a voided payment is not charged").isZero();
    assertThat(store.types(STREAM)).containsExactly("PaymentInitiated", "PaymentVoided");
  }

  @Test
  void aFailedPaymentTheGatewayCharged_isCapturedSoThatItIsRefunded() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    gateway.chargedEarlier(PAYMENT);
    store.append(
        STREAM,
        new PaymentEvent.PaymentFailed(PAYMENT, "o-1", "gateway timeout"),
        PaymentStreams.causedBySagaOf(PAYMENT));

    manager.onEvents(List.of(initiated));

    assertThat(bus.attempted()).containsExactly(new PaymentCommand.CapturePayment(PAYMENT));
    assertThat(store.types(STREAM))
        .containsExactly("PaymentInitiated", "PaymentFailed", "PaymentCaptured", "PaymentRefunded");
  }

  @Test
  void aRefundIsSentToTheGateway() {
    gateway.chargedEarlier(PAYMENT);
    EventEnvelope refunded =
        new EventEnvelope(
            GlobalOffset.of(9),
            STREAM,
            new Version(3),
            EventType.fromClass(PaymentEvent.PaymentRefunded.class),
            new PaymentEvent.PaymentRefunded(
                PAYMENT, "o-1", new Money(new BigDecimal("10.00"), "USD"), "changed my mind"),
            byARequest());

    manager.onEvents(List.of(refunded));
    manager.onEvents(List.of(refunded));

    assertThat(gateway.hasRefunded(PAYMENT)).isTrue();
    assertThat(bus.attempted()).as("a refund at the gateway sends no command").isEmpty();
  }

  /** Metadata of an event a request's command left: no saga origin. */
  private static EventMetadata byARequest() {
    return initiatedByARequest().metadata();
  }

  @Test
  void theCommandsCarryAKeyThatIsTheSameOnEveryDelivery() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    bus.failNext(PaymentCommand.CapturePayment.class, new EventStoreException("connection reset"));
    assertThatThrownBy(() -> manager.onEvents(List.of(initiated)))
        .isInstanceOf(EventStoreException.class);
    manager.onEvents(List.of(initiated));

    assertThat(bus.keys())
        .containsExactly(
            new IdempotencyKey("payment-capture:" + PAYMENT),
            new IdempotencyKey("payment-capture:" + PAYMENT));
  }

  @Test
  void theCommandContinuesThePaymentInitiatedEvent() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    assertThat(initiated.metadata().sagaOrigin())
        .as("the event the saga's InitiatePayment produced")
        .isEqualTo(SagaOrigin.dispatchedBy(SAGA_TYPE, SAGA));

    manager.onEvents(List.of(initiated));

    assertThat(bus.attempted())
        .hasSize(1)
        .first()
        .isInstanceOf(PaymentCommand.CapturePayment.class);
    StreamRuneContext.DispatchCause cause = bus.causes().getFirst();
    assertThat(cause.causationId())
        .as("the command reacts to this PaymentInitiated")
        .isEqualTo(CausationId.of(initiated.metadata().eventId().value()));
    assertThat(cause.sagaOrigin())
        .as("the saga that caused PaymentInitiated, one hop further")
        .isEqualTo(new SagaOrigin(SAGA_TYPE, SAGA, 1));
    StreamRuneContext.RequestContext context = bus.contexts().getFirst();
    assertThat(context.correlationId())
        .as("one fulfillment, one correlation id")
        .isEqualTo(initiated.metadata().correlationId())
        .isEqualTo(CorrelationId.of("streamrune:saga:fulfillment-o-1"));
    assertThat(context.userId()).as("no user issued this command").isNull();
    assertThat(context.authority()).isNull();
    assertThat(context.baggage()).isEmpty();
    assertThat(bus.sagaDispatches())
        .as("a continuation is not a saga's own dispatch: it runs with no system principal")
        .containsExactly(false);

    // The reply the saga waits for carries the same origin, by which SagaRunner routes it.
    EventEnvelope captured = store.readStream(STREAM, initiated.version(), 1).getFirst();
    assertThat(captured.metadata().sagaOrigin()).isEqualTo(new SagaOrigin(SAGA_TYPE, SAGA, 1));
  }

  @Test
  void aFailedGatewayCallContinuesThePaymentInitiatedEventToo() {
    EventEnvelope initiated = store.initiate(PAYMENT);
    gateway.setFailureInjected(true);

    manager.onEvents(List.of(initiated));

    assertThat(bus.attempted()).hasSize(1).first().isInstanceOf(PaymentCommand.FailPayment.class);
    assertThat(bus.causes().getFirst().sagaOrigin()).isEqualTo(new SagaOrigin(SAGA_TYPE, SAGA, 1));
    assertThat(bus.contexts().getFirst().userId()).isNull();
  }

  // ---- The framework's own bus: a failed command dispatched inside
  // StreamRuneContext.continuing(event) is published to no dead letter queue, whether or not a
  // saga caused the event. An entry has no place for the causing event or for a saga origin, so
  // its replay would store the reply without them. The subscription's redelivery is the
  // command's only second attempt. ----

  @Test
  void aFailedDispatchPropagatesAndLeavesNoDeadLetter() {
    var real = new RealBus();
    try (real) {
      var realManager = new PaymentProcessManager(gateway, breaker, real.bus(), real.store());
      EventEnvelope initiated =
          real.stored(
              SagaEnvelopes.causedBy(
                  OrderFulfillmentState.class, SAGA, STREAM, paymentInitiated(PAYMENT)));
      real.store().refuseNextAppend();

      assertThatThrownBy(() -> realManager.onEvents(List.of(initiated)))
          .as("the store's failure reaches the subscription, which delivers the event again")
          .isInstanceOf(EventStoreException.class);
      assertThat(real.deadLetters().all())
          .as("a command dispatched in continuation of an event is not dead-lettered")
          .isEmpty();
      assertThat(real.types(STREAM)).containsExactly("PaymentInitiated");

      realManager.onEvents(List.of(initiated));

      assertThat(real.types(STREAM)).containsExactly("PaymentInitiated", "PaymentCaptured");
      EventMetadata captured =
          real.store().readStream(STREAM, initiated.version(), 1).getFirst().metadata();
      assertThat(captured.sagaOrigin()).isEqualTo(new SagaOrigin(SAGA_TYPE, SAGA, 1));
      assertThat(captured.causationId())
          .isEqualTo(CausationId.of(initiated.metadata().eventId().value()));
      assertThat(captured.correlationId())
          .isEqualTo(CorrelationId.of("streamrune:saga:fulfillment-o-1"));
      assertThat(captured.userId()).isNull();
      assertThat(real.deadLetters().all()).isEmpty();
      assertThat(gateway.charges()).isEqualTo(1);
    }
  }

  @Test
  void aFailedDispatchForAnEventNoSagaCausedLeavesNoDeadLetterEither() {
    // The rule does not depend on a saga: the manager continues a PaymentInitiated that carries no
    // origin (the demo writes no such event; only the saga initiates a payment), and the failed
    // capture is in no dead letter queue. A later delivery stores it once, as the event's effect.
    var real = new RealBus();
    try (real) {
      var realManager = new PaymentProcessManager(gateway, breaker, real.bus(), real.store());
      EventEnvelope initiated = real.stored(initiatedByARequest());
      real.store().refuseNextAppend();

      assertThatThrownBy(() -> realManager.onEvents(List.of(initiated)))
          .as("the store's failure reaches the subscription, which delivers the event again")
          .isInstanceOf(EventStoreException.class);
      assertThat(real.deadLetters().all())
          .as("a command dispatched in continuation of an event is not dead-lettered")
          .isEmpty();
      assertThat(real.types(STREAM)).containsExactly("PaymentInitiated");

      realManager.onEvents(List.of(initiated));
      realManager.onEvents(List.of(initiated));

      assertThat(real.types(STREAM))
          .as("the delivery that succeeds stores the capture once; one after it stores nothing")
          .containsExactly("PaymentInitiated", "PaymentCaptured");
      EventMetadata captured =
          real.store().readStream(STREAM, initiated.version(), 1).getFirst().metadata();
      assertThat(captured.causationId())
          .isEqualTo(CausationId.of(initiated.metadata().eventId().value()));
      assertThat(captured.sagaOrigin()).as("no saga caused PaymentInitiated").isNull();
      assertThat(captured.correlationId()).isEqualTo(CorrelationId.of("corr-of-a-request"));
      assertThat(captured.userId()).isNull();
      assertThat(real.deadLetters().all()).isEmpty();
      assertThat(gateway.charges()).isEqualTo(1);
    }
  }

  @Test
  void theSameFailureOutsideAContinuationIsDeadLettered() {
    // What the two cases above rest on: the store's refusal is a failure the bus dead-letters. The
    // same capture, dispatched with no continuation around it, leaves an entry.
    var real = new RealBus();
    try (real) {
      real.stored(initiatedByARequest());
      real.store().refuseNextAppend();

      assertThatThrownBy(
              () ->
                  real.bus()
                      .execute(
                          new PaymentCommand.CapturePayment(PAYMENT),
                          new IdempotencyKey("payment-capture:" + PAYMENT)))
          .isInstanceOf(EventStoreException.class);

      assertThat(real.deadLetters().all())
          .singleElement()
          .satisfies(
              entry ->
                  assertThat(entry.commandType())
                      .isEqualTo(PaymentCommand.CapturePayment.class.getName()));
    }
  }

  /** A {@code PaymentInitiated} as a request's command leaves it: no saga origin. */
  private static EventEnvelope initiatedByARequest() {
    return new EventEnvelope(
        GlobalOffset.initial(),
        STREAM,
        Version.initial(),
        EventType.fromClass(PaymentEvent.PaymentInitiated.class),
        paymentInitiated(PAYMENT),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("corr-of-a-request"),
            null,
            null,
            Instant.now(),
            Map.of()));
  }

  private static PaymentEvent.PaymentInitiated paymentInitiated(String paymentId) {
    return new PaymentEvent.PaymentInitiated(
        paymentId, paymentId.substring("pay-".length()), new Money(new BigDecimal("10.00"), "USD"));
  }

  /**
   * The framework's {@code VirtualThreadCommandBus} with the payment decider registered as the
   * application registers it, over an in-memory event store that can refuse one append, with a
   * command inbox and a dead letter queue.
   */
  private static final class RealBus implements AutoCloseable {
    private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    private final RefusingStore store =
        new RefusingStore(new InMemoryEventStore().withCommandInbox(inbox));
    private final InMemoryDeadLetterQueue deadLetters = new InMemoryDeadLetterQueue();
    private final VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .deadLetterQueue(deadLetters)
            .register(
                PaymentState.TYPE,
                PaymentCommand.class,
                cmd ->
                    AggregateId.of(
                        switch (cmd) {
                          case PaymentCommand.InitiatePayment c -> c.paymentId();
                          case PaymentCommand.CapturePayment c -> c.paymentId();
                          case PaymentCommand.FailPayment c -> c.paymentId();
                          case PaymentCommand.CancelPayment c -> c.paymentId();
                        }),
                new PaymentDecider())
            .build();

    VirtualThreadCommandBus bus() {
      return bus;
    }

    RefusingStore store() {
      return store;
    }

    InMemoryDeadLetterQueue deadLetters() {
      return deadLetters;
    }

    /** Appends the envelope and returns it as the store hands it to a subscription. */
    EventEnvelope stored(EventEnvelope envelope) {
      store.append(envelope.streamId(), List.of(envelope), Version.initial());
      return store.readStream(envelope.streamId(), Version.initial(), 1).getFirst();
    }

    List<String> types(StreamId streamId) {
      return store.readStream(streamId, Version.initial(), 100).stream()
          .map(e -> e.eventType().name())
          .toList();
    }

    @Override
    public void close() {
      bus.close();
    }
  }

  /** An event store that fails one append with a store error and delegates everything else. */
  private static final class RefusingStore implements EventStore {
    private final InMemoryEventStore delegate;
    private boolean refuseNext;

    RefusingStore(InMemoryEventStore delegate) {
      this.delegate = delegate;
    }

    void refuseNextAppend() {
      refuseNext = true;
    }

    private void refuseOnce() {
      if (refuseNext) {
        refuseNext = false;
        throw new EventStoreException("connection reset");
      }
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      return delegate.load(streamId);
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      return delegate.load(streamId, expectedSnapshotVersion);
    }

    @Override
    public AppendResult append(StreamId streamId, List<EventEnvelope> events, Version expected) {
      refuseOnce();
      return delegate.append(streamId, events, expected);
    }

    @Override
    public IdempotentAppendResult appendWithKey(
        StreamId streamId,
        List<EventEnvelope> events,
        Version expected,
        IdempotencyKey idempotencyKey,
        String commandType) {
      refuseOnce();
      return delegate.appendWithKey(streamId, events, expected, idempotencyKey, commandType);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      delegate.saveSnapshot(streamId, version, state);
    }

    @Override
    public void saveSnapshot(
        StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
      delegate.saveSnapshot(streamId, version, state, snapshotVersion);
    }

    @Override
    public GlobalOffset lastGlobalOffset() {
      return delegate.lastGlobalOffset();
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return delegate.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return delegate.readStream(streamId, afterVersion, maxCount);
    }
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

    /** Captures the payment as an earlier call did, counted as neither a call nor a charge. */
    void chargedEarlier(String paymentId) {
      super.processPayment(paymentId);
    }

    int charges() {
      return charges;
    }
  }

  /** Payment streams in memory; only {@code readStream} is used by the manager. */
  private static final class PaymentStreams implements EventStore {
    private final Map<StreamId, List<EventEnvelope>> streams = new HashMap<>();
    private long offset;

    /**
     * Stores the {@code PaymentInitiated} of the payment as the saga's {@code InitiatePayment}
     * leaves it: with the origin of the saga of the payment's order.
     */
    EventEnvelope initiate(String paymentId) {
      StreamId streamId = StreamId.of(PaymentState.TYPE, AggregateId.of(paymentId));
      append(streamId, paymentInitiated(paymentId), causedBySagaOf(paymentId));
      return streams.get(streamId).getFirst();
    }

    static EventMetadata causedBySagaOf(String paymentId) {
      return SagaEnvelopes.metadataCausedBy(
          OrderFulfillmentState.class,
          SagaId.of("fulfillment-" + paymentId.substring("pay-".length())));
    }

    void append(StreamId streamId, PaymentEvent event, EventMetadata metadata) {
      List<EventEnvelope> stream = streams.computeIfAbsent(streamId, id -> new ArrayList<>());
      stream.add(
          new EventEnvelope(
              GlobalOffset.of(++offset),
              streamId,
              new Version(stream.size() + 1L),
              EventType.fromClass(event.getClass()),
              event,
              metadata));
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
   * for the command type (a scripted {@code null} lets that attempt through). It records the
   * request context and the dispatch cause each command ran under, and stores the command's events
   * with the metadata a command bus derives from the two.
   */
  private static final class ScriptedBus implements CommandBus {
    private final PaymentStreams store;
    private final PaymentDecider decider = new PaymentDecider();
    private final Map<Class<?>, Deque<java.util.Optional<RuntimeException>>> failures =
        new HashMap<>();
    private final List<Command> attempted = new ArrayList<>();
    private final List<IdempotencyKey> keys = new ArrayList<>();
    private final List<StreamRuneContext.RequestContext> contexts = new ArrayList<>();
    private final List<StreamRuneContext.DispatchCause> causes = new ArrayList<>();
    private final List<Boolean> sagaDispatches = new ArrayList<>();
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

    List<StreamRuneContext.RequestContext> contexts() {
      return contexts;
    }

    List<StreamRuneContext.DispatchCause> causes() {
      return causes;
    }

    List<Boolean> sagaDispatches() {
      return sagaDispatches;
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
      StreamRuneContext.requireNoForeignReservedCorrelationId();
      StreamRuneContext.RequestContext context = StreamRuneContext.capture();
      StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();
      attempted.add(command);
      keys.add(key);
      contexts.add(context);
      causes.add(cause);
      sagaDispatches.add(StreamRuneContext.isSagaDispatch());
      var scripted = failures.getOrDefault(command.getClass(), new ArrayDeque<>()).poll();
      if (scripted != null && scripted.isPresent()) {
        throw scripted.get();
      }
      PaymentCommand payment = (PaymentCommand) command;
      String paymentId =
          switch (payment) {
            case PaymentCommand.InitiatePayment c -> c.paymentId();
            case PaymentCommand.CapturePayment c -> c.paymentId();
            case PaymentCommand.FailPayment c -> c.paymentId();
            case PaymentCommand.CancelPayment c -> c.paymentId();
          };
      StreamId streamId = StreamId.of(PaymentState.TYPE, AggregateId.of(paymentId));
      if (paymentId.equals(settleFirst)) {
        settleFirst = null;
        store.append(
            streamId,
            new PaymentEvent.PaymentFailed(paymentId, "o-settled", "settled elsewhere"),
            PaymentStreams.causedBySagaOf(paymentId));
      }
      List<PaymentEvent> events = decider.decide(payment, store.state(streamId, decider));
      events.forEach(e -> store.append(streamId, e, metadataOf(context, cause)));
      return new CommandResult(List.copyOf(events), streamId, Version.initial(), List.of());
    }

    /** What a command bus writes: the bound context's ids and the bound cause, nothing else. */
    private static EventMetadata metadataOf(
        StreamRuneContext.RequestContext context, StreamRuneContext.DispatchCause cause) {
      return new EventMetadata(
          EventId.of("evt-" + UUID.randomUUID()),
          CommandId.of("cmd-" + UUID.randomUUID()),
          context == null ? null : context.traceId(),
          null,
          context == null ? CorrelationId.of("corr-" + UUID.randomUUID()) : context.correlationId(),
          cause == null ? null : cause.causationId(),
          context == null ? null : context.userId(),
          Instant.now(),
          Map.of(),
          cause == null ? null : cause.sagaOrigin());
    }
  }
}
