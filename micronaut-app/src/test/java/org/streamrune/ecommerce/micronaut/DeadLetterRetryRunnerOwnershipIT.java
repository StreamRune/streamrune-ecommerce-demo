package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * The demo supplies its own {@link DeadLetterRetryRunner} (its policy and its five sealed command
 * roots), so the framework's runner backs off ({@code @Requires(missingBeans)}) and the framework's
 * lifecycle does not start the demo's: {@code EcommerceSubscriptionLifecycle} does. Before it did,
 * no runner polled the dead-letter queue in this app at all.
 */
@MicronautTest(transactional = false)
class DeadLetterRetryRunnerOwnershipIT extends AbstractIntegrationTest {

  @Inject ApplicationContext context;
  @Inject DeadLetterRetryRunner deadLetterRetryRunner;

  @Test
  void theDemosOwnRunnerIsTheOnlyOne_andItIsStarted() {
    assertThat(context.getBeansOfType(DeadLetterRetryRunner.class)).hasSize(1);
    assertThat(deadLetterRetryRunner.isStarted())
        .as("the demo's runner is started by EcommerceSubscriptionLifecycle")
        .isTrue();
  }
}
