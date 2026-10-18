package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SubscriptionHealthContributor;

/**
 * Micronaut lifecycle bean that wires and starts the e-commerce-specific polling subscriptions: the
 * order-fulfillment saga and the payment process manager, and the saga's timeout runner.
 *
 * <p>Both subscriptions are app-specific (the framework has no knowledge of {@link
 * OrderFulfillmentSaga} or {@link PaymentProcessManager}), so they cannot be auto-started by {@link
 * org.streamrune.micronaut.StreamRuneLifecycle}. This bean mirrors the Spring wiring in {@code
 * StreamRuneConfig}: build a {@link PollingEventSubscription}, call {@code start()}, and stop it on
 * shutdown.
 *
 * <p>The saga runner itself is a bean ({@code StreamRuneFactory#orderFulfillmentSagaRunner}), so
 * that the framework starts its compensation retry sweeper and arms its key-age guard; this bean
 * only subscribes it to the event stream, with {@code sagaRunner(...)}: the subscription takes the
 * runner's lease ({@code "saga:" + saga type}) and holds it while it runs, so with more than one
 * backend on the database one of them delivers the saga's events, and the others stand by. The
 * timeout runner ({@code StreamRuneFactory#orderFulfillmentTimeoutRunner}) and the compensation
 * retry sweeper follow that lease: they act on the backend whose saga subscription leads. The
 * timeout runner is started after the saga subscription and stopped after it: closing the
 * subscription first gives the lease back, so the runner does not stop while this backend leads.
 *
 * <p>The payment process manager's subscription delivers under a lease named after it, {@code
 * payment-process-manager}, over the same leadership ({@code null} when single-active-consumer
 * leadership is off: every backend then delivers). Both subscriptions register with the {@link
 * SubscriptionHealthContributor}, so the health check lists them with their {@code role}: {@code
 * LEADING} or {@code STANDBY}.
 *
 * <p>It also starts and closes the demo's own {@link DeadLetterRetryRunner} (see {@code
 * StreamRuneFactory#deadLetterRetryRunner}): an application-supplied runner replaces the
 * framework's, and the framework starts only runners it built itself.
 *
 * <p>Startup ordering: Micronaut fires {@link StartupEvent} once all beans are ready, which is
 * after {@code StreamRuneLifecycle} has started the projection runner and outbox poller. All
 * injected collaborators (EventStore, OffsetStore, the saga runner) are fully initialised before
 * these subscriptions start. The framework applies the inbox retention window to the saga runner in
 * its own {@code StartupEvent} listener; {@link #getOrder()} places this listener after every
 * framework listener, so the runner's key-age guard is armed before its subscription delivers the
 * first event.
 */
@Singleton
public class EcommerceSubscriptionLifecycle
    implements ApplicationEventListener<StartupEvent>, Ordered {

  private static final Logger LOG = LoggerFactory.getLogger(EcommerceSubscriptionLifecycle.class);

  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner;
  private final SagaTimeoutRunner<OrderFulfillmentState> orderFulfillmentTimeoutRunner;
  private final PaymentProcessManager paymentProcessManager;
  private final DeadLetterRetryRunner deadLetterRetryRunner;
  private final SubscriptionLeadership leadership;
  private final SubscriptionHealthContributor health;

  private PollingEventSubscription orderFulfillmentSagaSubscription;
  private PollingEventSubscription paymentProcessSubscription;

  public EcommerceSubscriptionLifecycle(
      EventStore eventStore,
      OffsetStore offsetStore,
      SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner,
      SagaTimeoutRunner<OrderFulfillmentState> orderFulfillmentTimeoutRunner,
      PaymentProcessManager paymentProcessManager,
      DeadLetterRetryRunner deadLetterRetryRunner,
      @Nullable SubscriptionLeadership leadership,
      SubscriptionHealthContributor health) {
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.orderFulfillmentSagaRunner = orderFulfillmentSagaRunner;
    this.orderFulfillmentTimeoutRunner = orderFulfillmentTimeoutRunner;
    this.paymentProcessManager = paymentProcessManager;
    this.deadLetterRetryRunner = deadLetterRetryRunner;
    this.leadership = leadership;
    this.health = health;
  }

  /**
   * Last among the {@code StartupEvent} listeners: after the framework's configurers, its startup
   * validators and its own runner starters, so the demo's subscriptions start on a validated,
   * configured app.
   */
  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    // Order-fulfillment saga subscription — mirrors Spring's orderFulfillmentSagaSubscription bean.
    orderFulfillmentSagaSubscription =
        PollingEventSubscription.builder()
            .subscriptionName("order-fulfillment-saga")
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(java.time.Duration.ofMillis(500)))
            // Delivers under the saga runner's lease, and takes it.
            .sagaRunner(orderFulfillmentSagaRunner)
            .healthContributor(health)
            .build();
    orderFulfillmentSagaSubscription.start();
    LOG.info("StreamRune: started order-fulfillment-saga subscription");

    // The saga's deadline: compensates a fulfillment that has not ended in time.
    orderFulfillmentTimeoutRunner.start();
    LOG.info("StreamRune: started the order-fulfillment saga's timeout runner");

    // Payment process manager subscription — mirrors Spring's paymentProcessSubscription bean.
    paymentProcessSubscription =
        PollingEventSubscription.builder()
            .subscriptionName("payment-process-manager")
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(java.time.Duration.ofMillis(500)))
            .listener(paymentProcessManager)
            .leadership(leadership)
            .healthContributor(health)
            .build();
    paymentProcessSubscription.start();
    LOG.info("StreamRune: started payment-process-manager subscription");

    // The demo's own dead-letter retry runner: the framework does not start an application's.
    deadLetterRetryRunner.start();
    LOG.info("StreamRune: started the demo's DeadLetterRetryRunner");
  }

  @PreDestroy
  public void close() {
    closeQuietly(deadLetterRetryRunner, "DeadLetterRetryRunner");
    closeQuietly(paymentProcessSubscription, "payment-process-manager subscription");
    paymentProcessSubscription = null;
    // The saga subscription first: closing it gives the saga's lease back, and the timeout runner
    // that follows the lease then stops on a backend that does not lead.
    closeQuietly(orderFulfillmentSagaSubscription, "order-fulfillment-saga subscription");
    orderFulfillmentSagaSubscription = null;
    closeQuietly(orderFulfillmentTimeoutRunner::stop, "order-fulfillment saga's timeout runner");
  }

  private static void closeQuietly(AutoCloseable closeable, String name) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
      LOG.info("StreamRune: stopped {}", name);
    } catch (Exception e) {
      LOG.warn("StreamRune: failed to stop {}", name, e);
    }
  }
}
