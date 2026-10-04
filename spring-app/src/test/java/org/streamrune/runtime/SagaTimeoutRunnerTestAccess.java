package org.streamrune.runtime;

import java.time.Instant;
import org.streamrune.core.saga.SagaState;

/**
 * Test-only bridge into {@link SagaTimeoutRunner}'s package-private sweep methods. Lives in the
 * {@code org.streamrune.runtime} package so it can reach {@code pollOnce()} / {@code
 * processBatch(Instant)}, which the framework keeps package-private precisely so tests can drive a
 * single deterministic sweep without the background poll loop or wall-clock sleeps.
 *
 * <p>{@code SagaTimeoutE2EIT} uses {@link #pollOnce} to fire one clock-driven sweep against the
 * demo's real PostgreSQL saga store.
 */
public final class SagaTimeoutRunnerTestAccess {

  private SagaTimeoutRunnerTestAccess() {}

  /** Runs exactly one clock-driven sweep ({@code cutoff = clock.now() - timeout}). */
  public static <S extends SagaState> void pollOnce(SagaTimeoutRunner<S> runner) {
    runner.pollOnce();
  }

  /** Runs exactly one sweep against an explicit cutoff instant. */
  public static <S extends SagaState> void processBatch(
      SagaTimeoutRunner<S> runner, Instant cutoff) {
    runner.processBatch(cutoff);
  }
}
