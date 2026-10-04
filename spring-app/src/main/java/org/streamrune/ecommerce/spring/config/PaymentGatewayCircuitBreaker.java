package org.streamrune.ecommerce.spring.config;

import java.time.Duration;
import java.time.Instant;

/**
 * A small circuit breaker around the payment gateway call.
 *
 * <p>The demo also has a {@code CircuitBreakerCommandInterceptor} on the command bus, but that one
 * guards <em>commands</em> and resets on any successful command — and the order-fulfillment saga
 * compensates a failed payment with a <em>successful</em> {@code FailPayment}/{@code CancelOrder},
 * which would keep resetting it. The textbook place for a breaker is around the flaky dependency
 * itself: this breaker counts only gateway-call outcomes, so consecutive gateway failures open it
 * regardless of what the saga does on the command bus.
 *
 * <p>State machine: CLOSED → OPEN (after {@code failureThreshold} consecutive failures) → HALF_OPEN
 * (after {@code cooldown}) → CLOSED (probe success) or OPEN (probe failure). While OPEN the {@link
 * PaymentProcessManager} fast-fails the payment without calling the gateway.
 *
 * <p>Single-threaded use (driven by one polling subscription); methods are {@code synchronized} for
 * safe reads from the admin endpoint.
 */
public class PaymentGatewayCircuitBreaker {

  public enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int failureThreshold;
  private final Duration cooldown;

  private State state = State.CLOSED;
  private int consecutiveFailures = 0;
  private Instant openedAt = null;

  public PaymentGatewayCircuitBreaker(int failureThreshold, Duration cooldown) {
    this.failureThreshold = failureThreshold;
    this.cooldown = cooldown;
  }

  /**
   * Whether a gateway call may be attempted now. CLOSED always allows; OPEN allows a single probe
   * once the cooldown has elapsed (transitioning to HALF_OPEN); HALF_OPEN allows the probe.
   */
  public synchronized boolean allowRequest() {
    if (state == State.OPEN
        && openedAt != null
        && !Instant.now().isBefore(openedAt.plus(cooldown))) {
      state = State.HALF_OPEN;
    }
    return state != State.OPEN;
  }

  /** Records a successful gateway call: closes the breaker and clears the failure count. */
  public synchronized void recordSuccess() {
    consecutiveFailures = 0;
    state = State.CLOSED;
    openedAt = null;
  }

  /** Records a failed gateway call: opens the breaker once the threshold is reached. */
  public synchronized void recordFailure() {
    consecutiveFailures++;
    if (consecutiveFailures >= failureThreshold || state == State.HALF_OPEN) {
      state = State.OPEN;
      openedAt = Instant.now();
    }
  }

  /** Current state, applying the cooldown so a long-open breaker reports HALF_OPEN when due. */
  public synchronized State state() {
    if (state == State.OPEN
        && openedAt != null
        && !Instant.now().isBefore(openedAt.plus(cooldown))) {
      state = State.HALF_OPEN;
    }
    return state;
  }
}
