package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.micronaut.config.PaymentGatewayCircuitBreaker;
import org.streamrune.ecommerce.micronaut.config.PaymentGatewaySimulator;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * A payment the gateway captured is not failed because the app could not record the capture. The
 * event store refuses the payment's {@code PaymentCaptured} (injected in the database by {@link
 * AppendFailureInjection}) after the gateway has taken the money. The payment process manager must
 * not turn that into {@code FailPayment} — the saga would cancel an order the customer paid for —
 * but leave {@code PaymentInitiated} to be delivered again. Once the store accepts the event, the
 * capture is recorded and the saga confirms the order.
 *
 * <p>That redelivery is the capture's only second attempt. The command is dispatched inside {@code
 * StreamRuneContext.continuing(PaymentInitiated)}, and the command bus publishes a failed command
 * of a continuation to no dead letter queue. {@code PaymentInitiated} is an event the fulfillment
 * saga caused, so the recorded {@code PaymentCaptured} names the saga as its origin, one hop from
 * the saga's own command, and {@code PaymentInitiated} as its cause; that origin is what brings it
 * to the saga.
 */
@MicronautTest(transactional = false)
class PaymentCaptureRetryIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject DataSource dataSource;
  @Inject VirtualThreadCommandBus commandBus;
  @Inject EventStore eventStore;
  @Inject DeadLetterQueue deadLetterQueue;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject PaymentGatewayCircuitBreaker paymentGatewayCircuitBreaker;

  @Test
  void aCaptureTheStoreRefusesIsDeliveredAgainAndTheOrderIsConfirmed() throws Exception {
    String orderId = "m-pm-retry-" + System.nanoTime();
    String productId = "m-pm-prod-" + System.nanoTime();
    String paymentId = "pay-" + orderId;
    createProduct(productId);
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 5));

    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(PaymentState.TYPE, paymentId, "PaymentCaptured");
      placeOrder(orderId, "m-pm-cust-" + System.nanoTime(), productId);

      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(failures.refusals())
                      .as("appends of PaymentCaptured the store refused")
                      .isPositive());
      assertThat(paymentGateway.hasCaptured(paymentId)).as("the gateway took the money").isTrue();
      assertThat(eventTypes(PaymentState.TYPE, paymentId))
          .as("a capture the store refused is not turned into a failed payment")
          .containsExactly("PaymentInitiated");
      assertThat(paymentGatewayCircuitBreaker.state())
          .as("the gateway answered; its breaker counts no failure")
          .isEqualTo(PaymentGatewayCircuitBreaker.State.CLOSED);
      assertThat(captureDeadLetters(paymentId))
          .as("a command dispatched in continuation of an event is not dead-lettered")
          .isEmpty();
      failures.allowAppend(PaymentState.TYPE, paymentId, "PaymentCaptured");
    }

    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> assertThat(eventTypes(OrderState.TYPE, orderId)).contains("OrderConfirmed"));
    assertThat(eventTypes(PaymentState.TYPE, paymentId))
        .containsExactly("PaymentInitiated", "PaymentCaptured");
    assertThat(eventTypes(OrderState.TYPE, orderId)).doesNotContain("OrderCancelled");

    // Nothing but the subscription's redelivery ran the capture again: the dead letter queue never
    // held it.
    assertThat(captureDeadLetters(paymentId)).isEmpty();

    // What brings the capture to the saga: the origin the bus stored with it.
    EventEnvelope initiated = events(PaymentState.TYPE, paymentId).get(0);
    EventMetadata captured = events(PaymentState.TYPE, paymentId).get(1).metadata();
    SagaId sagaId = new SagaId("fulfillment-" + orderId);
    assertThat(initiated.metadata().sagaOrigin())
        .isEqualTo(
            SagaOrigin.dispatchedBy(SagaType.fromClass(OrderFulfillmentState.class), sagaId));
    assertThat(captured.sagaOrigin())
        .isEqualTo(new SagaOrigin(SagaType.fromClass(OrderFulfillmentState.class), sagaId, 1));
    assertThat(captured.causationId())
        .isEqualTo(CausationId.of(initiated.metadata().eventId().value()));
    assertThat(captured.correlationId())
        .isEqualTo(initiated.metadata().correlationId())
        .isEqualTo(CorrelationId.of("streamrune:saga:" + sagaId.value()));
    assertThat(captured.userId()).as("no user issued the capture").isNull();
  }

  /**
   * A gateway that answers no call does not fail the payment: the outcome is unknown, so the
   * payment stays {@code PENDING}, {@code PaymentInitiated} is delivered again, and the order is
   * not cancelled. After three calls without an answer the gateway breaker opens. Once the gateway
   * answers, the payment is captured and the saga confirms the order.
   */
  @Test
  void aGatewayOutageHoldsThePayment_andTheOrderIsConfirmedOnceTheGatewayAnswers()
      throws Exception {
    String orderId = "m-pm-outage-" + System.nanoTime();
    String productId = "m-pm-prod-" + System.nanoTime();
    String paymentId = "pay-" + orderId;
    createProduct(productId);
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 5));

    paymentGateway.setOutage(true);
    try {
      placeOrder(orderId, "m-pm-cust-" + System.nanoTime(), productId);

      await()
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(paymentGatewayCircuitBreaker.state())
                      .as("three deliveries without an answer open the gateway breaker")
                      .isEqualTo(PaymentGatewayCircuitBreaker.State.OPEN));
      assertThat(eventTypes(PaymentState.TYPE, paymentId))
          .as("a call without an answer neither fails nor captures the payment")
          .containsExactly("PaymentInitiated");
      assertThat(eventTypes(OrderState.TYPE, orderId)).doesNotContain("OrderCancelled");
    } finally {
      paymentGateway.setOutage(false);
      // The breaker lets a probe through 30 seconds after it opened; the test closes it at once.
      paymentGatewayCircuitBreaker.recordSuccess();
    }

    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> assertThat(eventTypes(OrderState.TYPE, orderId)).contains("OrderConfirmed"));
    assertThat(eventTypes(PaymentState.TYPE, paymentId))
        .containsExactly("PaymentInitiated", "PaymentCaptured");
    assertThat(paymentGateway.hasCaptured(paymentId)).isTrue();
  }

  /**
   * The customer cancels while the capture cannot be recorded: the cancellation voids the pending
   * payment, though the gateway has taken the money. The redelivered {@code PaymentInitiated} finds
   * the void and the gateway's capture, and records the capture, which the payment refunds in the
   * same append; the refund reaches the gateway.
   */
  @Test
  void aCaptureRedeliveredAfterTheOrderWasCancelled_isRecordedAndRefunded() throws Exception {
    String orderId = "m-pm-cancel-" + System.nanoTime();
    String customerId = "m-pm-cust-" + System.nanoTime();
    String productId = "m-pm-prod-" + System.nanoTime();
    String paymentId = "pay-" + orderId;
    createProduct(productId);
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 5));

    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(PaymentState.TYPE, paymentId, "PaymentCaptured");
      placeOrder(orderId, customerId, productId);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(failures.refusals()).isPositive();
                assertThat(paymentGateway.hasCaptured(paymentId)).isTrue();
              });

      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(() -> assertThat(cancel(orderId, customerId)).isEqualTo(200));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(eventTypes(PaymentState.TYPE, paymentId))
                      .containsExactly("PaymentInitiated", "PaymentVoided"));
      failures.allowAppend(PaymentState.TYPE, paymentId, "PaymentCaptured");
    }

    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> {
              assertThat(eventTypes(PaymentState.TYPE, paymentId))
                  .containsExactly(
                      "PaymentInitiated", "PaymentVoided", "PaymentCaptured", "PaymentRefunded");
              assertThat(paymentGateway.hasRefunded(paymentId))
                  .as("the refund reached the gateway")
                  .isTrue();
            });
    assertThat(eventTypes(OrderState.TYPE, orderId)).contains("OrderCancelled");
    assertThat(eventTypes(OrderState.TYPE, orderId)).doesNotContain("OrderConfirmed");
    assertThat(eventTypes(InventoryState.TYPE, productId))
        .as("the order reserved no stock")
        .doesNotContain("StockReserved");
  }

  private int cancel(String orderId, String customerId) {
    try {
      return client
          .toBlocking()
          .exchange(
              HttpRequest.POST(
                      "/api/orders/" + orderId + "/cancel",
                      """
                      {"reason": "changed my mind"}""")
                  .contentType(MediaType.APPLICATION_JSON)
                  .header("X-User-Role", "CUSTOMER")
                  .header("X-User-Id", customerId))
          .getStatus()
          .getCode();
    } catch (HttpClientResponseException refused) {
      return refused.getStatus().getCode();
    }
  }

  private List<DeadLetterQueue.DeadLetterEntry> captureDeadLetters(String paymentId) {
    return deadLetterQueue.read(1000).stream()
        .filter(e -> e.aggregateId().value().equals(paymentId))
        .filter(e -> e.commandType().equals(PaymentCommand.CapturePayment.class.getName()))
        .toList();
  }

  private List<String> eventTypes(AggregateType type, String id) {
    return events(type, id).stream().map(e -> e.eventType().name()).toList();
  }

  private List<EventEnvelope> events(AggregateType type, String id) {
    return eventStore.readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 100);
  }

  /** Creates the product in the catalogue, which prices an order's line. */
  private void createProduct(String productId) {
    commandBus.execute(
        new ProductCommand.CreateProduct(
            productId, "Product", null, "General", new Money(new BigDecimal("9.99"), "USD"), 0));
  }

  private void placeOrder(String orderId, String customerId, String productId) {
    String body =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": 1}]}"""
            .formatted(orderId, customerId, productId);
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/orders", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "CUSTOMER")
                    .header("X-User-Id", customerId));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
  }
}
