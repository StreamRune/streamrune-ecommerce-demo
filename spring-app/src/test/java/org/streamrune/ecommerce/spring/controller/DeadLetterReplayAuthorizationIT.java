package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.queries.dto.ProductView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * Replaying a dead-lettered {@code @RequireRole("ADMIN")} command. The demo's {@code
 * HeaderUserRoleResolver} reads the role from the request, and a replay has no request: the
 * framework therefore never asks it there. Because the resolver declares {@code
 * requiresRequestContext()}, the authorization interceptor completes the already-authorized command
 * under the recorded dead-letter-replay system principal, as the identity the entry recorded, and
 * logs that it did. Asking the resolver instead could only answer {@code GUEST} and would deny the
 * command on every attempt until its retry ladder ran out.
 *
 * <p>Each entry is written the way the command bus writes one after an authorized command failed on
 * transient infrastructure: the command, its original caller and correlation id, an infrastructure
 * error. The bus never dead-letters an authorization rejection, so every real entry has already
 * passed the role check once. {@code DiscontinueProduct} is the guarded command because it needs
 * nothing but an existing product.
 */
class DeadLetterReplayAuthorizationIT extends AbstractIntegrationTest {

  @Autowired private DeadLetterQueue deadLetterQueue;
  @Autowired private DeadLetterRetryRunner deadLetterRetryRunner;
  @Autowired private EventStore eventStore;

  @Test
  void aDeadLetteredAdminCommandIsReplayedOffTheRequestThread() {
    String pid = "p-dlq-auth-" + System.nanoTime();
    createProduct(pid);
    CommandId commandId = deadLetterDiscontinue(pid, UserId.of("admin-1"));

    assertThat(StreamRuneContext.CURRENT.isBound())
        .as("the replay runs where the retry runner's poll thread runs it: with no request at all")
        .isFalse();
    assertThat(deadLetterRetryRunner.retry(commandId)).isTrue();

    assertThat(deadLetterQueue.find(commandId))
        .as("a successful replay discards the entry")
        .isEmpty();
    assertThat(discontinuedEvents(pid)).isEqualTo(1);
    awaitStatus(pid, "DISCONTINUED");
  }

  @Test
  void theAdminRetryEndpointReplaysADeadLetteredAdminCommand() {
    String pid = "p-dlq-auth-http-" + System.nanoTime();
    createProduct(pid);
    CommandId commandId = deadLetterDiscontinue(pid, UserId.of("admin-1"));

    client
        .post()
        .uri("/api/admin/dead-letters/" + commandId.value() + "/retry")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "ops-1")
        .exchange()
        .expectStatus()
        .isOk();

    assertThat(deadLetterQueue.find(commandId))
        .as("the operator's retry replayed the command and discarded the entry")
        .isEmpty();
    assertThat(discontinuedEvents(pid)).isEqualTo(1);
  }

  @Test
  void aDeadLetteredAdminCommandWithNoRecordedCallerIsStillRefused() {
    String pid = "p-dlq-auth-anon-" + System.nanoTime();
    createProduct(pid);
    CommandId commandId = deadLetterDiscontinue(pid, null);

    assertThat(deadLetterRetryRunner.retry(commandId)).isTrue();

    var entry = deadLetterQueue.find(commandId);
    assertThat(entry)
        .as("a replay with no identity is refused (Authentication required) and the entry is kept")
        .isPresent();
    assertThat(entry.get().dlqAttempts()).isPositive();
    assertThat(discontinuedEvents(pid)).isZero();
  }

  private void createProduct(String pid) {
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"productId": "%s", "name": "Replayed", "description": "dead-letter replay",
             "category": "Test", "price": 5.00, "initialStock": 3}"""
                .formatted(pid))
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  private CommandId deadLetterDiscontinue(String pid, UserId originalCaller) {
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());
    deadLetterQueue.publish(
        new DeadLetterPublishRequest(
            "{\"productId\":\"" + pid + "\"}",
            ProductCommand.DiscontinueProduct.class.getName(),
            commandId,
            StreamId.of(ProductState.TYPE, AggregateId.of(pid)),
            IllegalStateException.class.getName(),
            "transient downstream failure",
            3,
            Instant.now(),
            CorrelationId.of("corr-" + UUID.randomUUID()),
            originalCaller,
            null,
            null));
    return commandId;
  }

  private long discontinuedEvents(String pid) {
    return eventStore
        .readStream(
            StreamId.of(ProductState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> e.event() instanceof ProductEvent.ProductDiscontinued)
        .count();
  }

  private void awaitStatus(String pid, String status) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(
                        client
                            .get()
                            .uri("/api/products/" + pid)
                            .exchange()
                            .expectStatus()
                            .isOk()
                            .expectBody(ProductView.class)
                            .returnResult()
                            .getResponseBody())
                    .extracting(v -> v.status().name())
                    .isEqualTo(status));
  }
}
