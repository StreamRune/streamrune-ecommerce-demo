package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.ecommerce.spring.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * A payment the gateway captured is not failed because the app could not record the capture. The
 * event store refuses the payment's {@code PaymentCaptured} (injected in the database by {@link
 * AppendFailureInjection}) after the gateway has taken the money. The payment process manager must
 * not turn that into {@code FailPayment} — the saga would cancel an order the customer paid for —
 * but leave {@code PaymentInitiated} to be delivered again. Once the store accepts the event, the
 * capture is recorded and the saga confirms the order.
 */
class PaymentCaptureRetryIT extends AbstractIntegrationTest {

  @Autowired private DataSource dataSource;
  @Autowired private VirtualThreadCommandBus commandBus;
  @Autowired private EventStore eventStore;
  @Autowired private DeadLetterQueue deadLetterQueue;
  @Autowired private DeadLetterRetryRunner deadLetterRetryRunner;
  @Autowired private PaymentGatewaySimulator paymentGateway;
  @Autowired private PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker;

  @Test
  void aCaptureTheStoreRefusesIsDeliveredAgainAndTheOrderIsConfirmed() throws Exception {
    String orderId = "pm-retry-" + System.nanoTime();
    String productId = "pm-prod-" + System.nanoTime();
    String paymentId = "pay-" + orderId;
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 5));

    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(PaymentState.TYPE, paymentId, "PaymentCaptured");
      placeOrder(orderId, "pm-cust-" + System.nanoTime(), productId);

      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(captureDeadLetters(paymentId)).isNotEmpty());
      assertThat(paymentGateway.hasCaptured(paymentId)).as("the gateway took the money").isTrue();
      assertThat(eventTypes(PaymentState.TYPE, paymentId))
          .as("a capture the store refused is not turned into a failed payment")
          .containsExactly("PaymentInitiated");
      assertThat(paymentGatewayCircuitBreaker.state())
          .as("the gateway answered; its breaker counts no failure")
          .isEqualTo(PaymentGatewayCircuitBreaker.State.CLOSED);
      failures.allowAppend(PaymentState.TYPE, paymentId, "PaymentCaptured");
    }

    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> assertThat(eventTypes(OrderState.TYPE, orderId)).contains("OrderConfirmed"));
    assertThat(eventTypes(PaymentState.TYPE, paymentId))
        .containsExactly("PaymentInitiated", "PaymentCaptured");
    assertThat(eventTypes(OrderState.TYPE, orderId)).doesNotContain("OrderCancelled");

    // The bus dead-lettered the refused captures too. They carry the capture's idempotency key, so
    // replaying them now is an inbox hit: the entries go away and nothing is captured twice.
    for (var entry : captureDeadLetters(paymentId)) {
      deadLetterRetryRunner.retry(entry.commandId());
    }
    assertThat(captureDeadLetters(paymentId)).isEmpty();
    assertThat(eventTypes(PaymentState.TYPE, paymentId))
        .containsExactly("PaymentInitiated", "PaymentCaptured");
  }

  private List<DeadLetterQueue.DeadLetterEntry> captureDeadLetters(String paymentId) {
    return deadLetterQueue.read(1000).stream()
        .filter(e -> e.aggregateId().value().equals(paymentId))
        .filter(e -> e.commandType().equals(PaymentCommand.CapturePayment.class.getName()))
        .toList();
  }

  private List<String> eventTypes(AggregateType type, String id) {
    return eventStore
        .readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 100)
        .stream()
        .map(e -> e.eventType().name())
        .toList();
  }

  private void placeOrder(String orderId, String customerId, String productId) {
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "%s", "customerId": "%s",
             "lines": [{"productId": "%s", "quantity": 1, "unitPrice": 9.99}]}"""
                .formatted(orderId, customerId, productId))
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }
}
