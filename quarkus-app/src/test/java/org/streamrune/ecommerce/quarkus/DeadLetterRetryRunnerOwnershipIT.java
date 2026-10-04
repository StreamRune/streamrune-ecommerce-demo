package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.streamrune.quarkus.StreamRuneLifecycle;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * The demo supplies its own {@link DeadLetterRetryRunner} (its policy and its five sealed command
 * roots), so the framework must neither build its default runner nor start the demo's, and the
 * demo's {@code EcommerceSubscriptionLifecycle} starts it. Before the framework honoured that on
 * Quarkus, this app ran two runners: the framework's (started, framework policy) and the demo's
 * (never started, used only by the admin replay endpoint).
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class DeadLetterRetryRunnerOwnershipIT {

  @Inject DeadLetterRetryRunner deadLetterRetryRunner;
  @Inject StreamRuneLifecycle streamRuneLifecycle;

  @Test
  void theDemosOwnRunnerIsTheOneStarted_andTheFrameworkBuiltNone() {
    assertThat(deadLetterRetryRunner.isStarted())
        .as("the demo's runner is started by EcommerceSubscriptionLifecycle")
        .isTrue();
    assertThat(streamRuneLifecycle.deadLetterRetryRunner())
        .as("the framework built and started no runner of its own")
        .isNull();
  }
}
