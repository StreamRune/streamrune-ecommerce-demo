package org.streamrune.ecommerce.spring.config;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.StreamRuneContext;

/**
 * A demo-only knob that widens the window between a saga's deadline and its forward commands: it
 * sleeps a random time between zero and {@code max} before every command a saga dispatches ({@link
 * StreamRuneContext#isSagaDispatch()}), and lets every other command through at once.
 *
 * <p>With it, a saga's deadline can pass while one of its forward commands is on the way to the
 * event store, which is the moment the saga runner's forward fence is for: the command is stored
 * before the compensation claim, or refused at its commit. {@link StreamRuneConfig#commandBus}
 * registers it only when {@code app.chaos.saga-command-delay-max} is above zero (the default is
 * {@code 0ms}); the two-backend Compose profile and {@code TwoInstanceSagaIT} turn it on. Do not
 * copy it into an application: it slows every saga down on purpose.
 */
public final class SagaCommandDelayInterceptor implements CommandInterceptor {

  private final long maxNanos;

  /**
   * An interceptor that delays each saga command by up to {@code max}.
   *
   * @param max the longest delay; positive
   */
  public SagaCommandDelayInterceptor(Duration max) {
    Objects.requireNonNull(max, "max is required");
    if (max.isZero() || max.isNegative()) {
      throw new IllegalArgumentException("max must be positive, got " + max);
    }
    this.maxNanos = max.toNanos();
  }

  /** Sleeps a random 0..{@code max} when a saga dispatched the command; never refuses one. */
  @Override
  public boolean before(CommandContext ctx) {
    if (StreamRuneContext.isSagaDispatch()) {
      try {
        Thread.sleep(Duration.ofNanos(ThreadLocalRandom.current().nextLong(maxNanos + 1)));
      } catch (InterruptedException e) {
        // Shutting down: keep the interrupt for the code after this interceptor and go on.
        Thread.currentThread().interrupt();
      }
    }
    return true;
  }
}
