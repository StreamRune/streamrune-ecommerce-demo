package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;

/**
 * Micronaut lifecycle bean that wires and starts the e-commerce-specific polling subscriptions: the
 * order-fulfillment saga and the payment process manager.
 *
 * <p>Both subscriptions are app-specific (the framework has no knowledge of {@link
 * OrderFulfillmentSaga} or {@link PaymentProcessManager}), so they cannot be auto-started by {@link
 * org.streamrune.micronaut.StreamRuneLifecycle}. This bean mirrors the Spring wiring in {@code
 * StreamRuneConfig}: build a {@link PollingEventSubscription}, call {@code start()}, and stop it on
 * shutdown.
 *
 * <p>The saga runner itself is a bean ({@code StreamRuneFactory#orderFulfillmentSagaRunner}), so
 * that the framework starts its compensation retry sweeper and arms its key-age guard; this bean
 * only subscribes it to the event stream.
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
  private final PaymentProcessManager paymentProcessManager;
  private final DeadLetterRetryRunner deadLetterRetryRunner;

  private PollingEventSubscription orderFulfillmentSagaSubscription;
  private PollingEventSubscription paymentProcessSubscription;

  public EcommerceSubscriptionLifecycle(
      EventStore eventStore,
      OffsetStore offsetStore,
      SagaRunner<OrderFulfillmentState> orderFulfillmentSagaRunner,
      PaymentProcessManager paymentProcessManager,
      DeadLetterRetryRunner deadLetterRetryRunner) {
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.orderFulfillmentSagaRunner = orderFulfillmentSagaRunner;
    this.paymentProcessManager = paymentProcessManager;
    this.deadLetterRetryRunner = deadLetterRetryRunner;
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
            .listener(orderFulfillmentSagaRunner.asEventListener())
            .build();
    orderFulfillmentSagaSubscription.start();
    LOG.info("StreamRune: started order-fulfillment-saga subscription");

    // Payment process manager subscription — mirrors Spring's paymentProcessSubscription bean.
    paymentProcessSubscription =
        PollingEventSubscription.builder()
            .subscriptionName("payment-process-manager")
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .config(SubscriptionConfig.pollingOnly(java.time.Duration.ofMillis(500)))
            .listener(paymentProcessManager)
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
    closeQuietly(orderFulfillmentSagaSubscription, "order-fulfillment-saga subscription");
    orderFulfillmentSagaSubscription = null;
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
