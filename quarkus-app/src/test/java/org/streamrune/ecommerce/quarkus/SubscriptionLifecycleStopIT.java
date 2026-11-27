package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.junit.jupiter.api.Test;

/**
 * The app stops its own subscriptions when the container fires
 * {@code @BeforeDestroyed(ApplicationScoped.class)}, the stop event that still finds the {@code
 * DataSource} open after a refused start, and a {@code ShutdownEvent} after it stops nothing a
 * second time.
 *
 * <p>The profile gives this class an app of its own: it stops that app's subscriptions.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
@TestProfile(SubscriptionLifecycleStopIT.OwnAppProfile.class)
class SubscriptionLifecycleStopIT {

  private static final SimpleFormatter FORMATTER = new SimpleFormatter();
  private static final String LEADERSHIP_STOPPED = "StreamRune: stopped SubscriptionLeadership";
  private static final String BUS_STOPPED = "StreamRune: stopped VirtualThreadCommandBus";
  private static final String TIMEOUT_RUNNER_STOPPED =
      "StreamRune: stopped order-fulfillment saga's timeout runner";
  private static final String SAGA_STOPPED =
      "StreamRune: stopped order-fulfillment-saga subscription";
  private static final String RETRY_RUNNER_STOPPED = "StreamRune: stopped DeadLetterRetryRunner";

  /** A profile no other test class uses, so that the app is started for this class alone. */
  public static class OwnAppProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      // Only makes the profile distinct, so that this class gets an app of its own.
      return Map.of("streamrune.outbox.flush-interval", "700ms");
    }
  }

  @Inject BeanManager beanManager;

  @Test
  void beforeDestroyedStopsTheSubscriptions_andTheShutdownEventAfterItStopsNothingTwice() {
    List<String> messages = new CopyOnWriteArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            if (record.getMessage() != null) {
              messages.add(FORMATTER.formatMessage(record));
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Logger root = Logger.getLogger("");
    root.addHandler(handler);
    try {
      beanManager
          .getEvent()
          .select(Object.class, BeforeDestroyed.Literal.APPLICATION)
          .fire(new Object());

      assertThat(count(messages, SAGA_STOPPED))
          .as("the saga subscription is stopped by the before-destroyed event")
          .isEqualTo(1);
      assertThat(count(messages, RETRY_RUNNER_STOPPED)).isEqualTo(1);
      assertThat(count(messages, TIMEOUT_RUNNER_STOPPED)).isEqualTo(1);
      assertStoppedBeforeTheFramework(messages);

      beanManager.getEvent().select(ShutdownEvent.class).fire(new ShutdownEvent());
      beanManager
          .getEvent()
          .select(Object.class, BeforeDestroyed.Literal.APPLICATION)
          .fire(new Object());

      assertThat(count(messages, SAGA_STOPPED))
          .as("a later shutdown event stops nothing again")
          .isEqualTo(1);
      assertThat(count(messages, RETRY_RUNNER_STOPPED)).isEqualTo(1);
      assertThat(count(messages, TIMEOUT_RUNNER_STOPPED)).isEqualTo(1);
    } finally {
      root.removeHandler(handler);
    }
  }

  /** The demo's subscriptions stop while the lease holder and the command bus are still open. */
  private static void assertStoppedBeforeTheFramework(List<String> messages) {
    int saga = indexOf(messages, SAGA_STOPPED);
    assertThat(saga).isNotNegative();
    assertThat(indexOf(messages, LEADERSHIP_STOPPED)).isGreaterThan(saga);
    assertThat(indexOf(messages, BUS_STOPPED)).isGreaterThan(saga);
  }

  private static int indexOf(List<String> messages, String text) {
    for (int i = 0; i < messages.size(); i++) {
      if (messages.get(i).contains(text)) {
        return i;
      }
    }
    return -1;
  }

  private static long count(List<String> messages, String text) {
    return messages.stream().filter(m -> m.contains(text)).count();
  }
}
