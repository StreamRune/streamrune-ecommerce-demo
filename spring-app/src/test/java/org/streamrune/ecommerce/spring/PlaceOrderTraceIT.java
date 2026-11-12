package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * One order, traced through the components the app builds itself and the ones the Spring
 * integration builds: the command bus (its {@code command/PlaceOrder} span, with the lock and the
 * event store under it), the saga runner and the subscription that feeds it, the projection runner
 * and the payment process manager's subscription. The spans reach {@link RecordedSpans} through the
 * OpenTelemetry SDK of Spring Boot, as they reach an OTLP exporter in a run that exports them.
 */
class PlaceOrderTraceIT extends AbstractIntegrationTest {

  private static final AttributeKey<String> AGGREGATE_ID =
      AttributeKey.stringKey("streamrune.aggregate.id");

  private static final AttributeKey<String> STREAM_ID =
      AttributeKey.stringKey("streamrune.stream.id");

  private static final AttributeKey<String> SAGA_ID = AttributeKey.stringKey("streamrune.saga.id");

  private static final AttributeKey<String> PROJECTION_NAME =
      AttributeKey.stringKey("streamrune.projection.name");

  private static final String SAGA_SPAN_PREFIX =
      "saga/org.streamrune.ecommerce.commands.saga.OrderFulfillmentState/";

  @Autowired private RecordedSpans recorded;

  @Autowired private VirtualThreadCommandBus commandBus;

  @Test
  void aPlacedOrderIsTracedFromItsCommandToItsSaga() {
    String product = "s-trace-p-" + System.nanoTime();
    String customer = "s-trace-c-" + System.nanoTime();
    String order = "s-trace-o-" + System.nanoTime();
    commandBus.execute(
        new ProductCommand.CreateProduct(
            product,
            "Traced product",
            null,
            "General",
            new Money(new BigDecimal("4.50"), "USD"),
            0));
    commandBus.execute(new InventoryCommand.ReceiveShipment(product, 5));
    recorded.clear();

    client
        .post()
        .uri("/api/orders")
        .header("X-User-Id", customer)
        .header("X-User-Role", "CUSTOMER")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "%s", "customerId": "%s",
             "lines": [{"productId": "%s", "quantity": 1}]}"""
                .formatted(order, customer, product))
        .exchange()
        .expectStatus()
        .isOk();

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              List<SpanData> spans = recorded.all();
              SpanData command = placeOrder(spans, order).orElseThrow();
              assertThat(command.getKind()).isEqualTo(SpanKind.INTERNAL);
              assertThat(command.getParentSpanContext().isValid())
                  .as("the command span has the HTTP request's span as its parent")
                  .isTrue();
              assertThat(childrenOf(spans, command))
                  .as("the spans under command/PlaceOrder")
                  .contains("lock/acquire", "eventstore/load", "eventstore/append");
              assertThat(
                      spans.stream()
                          .filter(s -> s.getName().equals("lock/acquire"))
                          .filter(s -> isChildOf(s, command))
                          .map(s -> s.getAttributes().get(STREAM_ID)))
                  .as("the lock of the order's stream, taken by the app's PgAdvisoryLocker")
                  .containsExactly("order:" + order);

              List<SpanData> linkedSteps =
                  spans.stream()
                      .filter(s -> s.getName().equals(SAGA_SPAN_PREFIX + "step"))
                      .filter(s -> ("fulfillment-" + order).equals(s.getAttributes().get(SAGA_ID)))
                      .filter(s -> linksTo(s, command))
                      .toList();
              assertThat(linkedSteps)
                  .as("the saga's steps linked to the command, one of which dispatches a command")
                  .anyMatch(
                      step ->
                          spans.stream()
                              .anyMatch(
                                  s -> s.getName().startsWith("command/") && isChildOf(s, step)));
              assertThat(spans)
                  .as("the subscription that delivers the order's events to the saga")
                  .anyMatch(
                      s ->
                          s.getName().equals("subscription/order-fulfillment-saga/deliver")
                              && s.getKind() == SpanKind.CONSUMER);
              assertThat(spans)
                  .as("the payment process manager's subscription")
                  .anyMatch(
                      s -> s.getName().equals("subscription/payment-process-manager/deliver"));
              assertThat(spans)
                  .as("the projection runner's orders projection")
                  .anyMatch(
                      s ->
                          s.getName().equals("projection/orders/process")
                              && "orders".equals(s.getAttributes().get(PROJECTION_NAME)));
            });
  }

  private static Optional<SpanData> placeOrder(List<SpanData> spans, String order) {
    return spans.stream()
        .filter(s -> s.getName().equals("command/PlaceOrder"))
        .filter(s -> order.equals(s.getAttributes().get(AGGREGATE_ID)))
        .findFirst();
  }

  private static List<String> childrenOf(List<SpanData> spans, SpanData parent) {
    return spans.stream().filter(s -> isChildOf(s, parent)).map(SpanData::getName).toList();
  }

  private static boolean isChildOf(SpanData span, SpanData parent) {
    return span.getParentSpanId().equals(parent.getSpanId())
        && span.getTraceId().equals(parent.getTraceId());
  }

  private static boolean linksTo(SpanData span, SpanData target) {
    for (LinkData link : span.getLinks()) {
      if (link.getSpanContext().getSpanId().equals(target.getSpanId())) {
        return true;
      }
    }
    return false;
  }
}
