package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.spring.config.SeedDataRunner;
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

  private static final AttributeKey<String> REJECTION_TYPE =
      AttributeKey.stringKey("streamrune.command.rejection.type");

  private static final String SAGA_SPAN_PREFIX =
      "saga/org.streamrune.ecommerce.commands.saga.OrderFulfillmentState/";

  @Autowired private RecordedSpans recorded;

  @Autowired private VirtualThreadCommandBus commandBus;

  @Autowired private SeedDataRunner seedDataRunner;

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

  /**
   * A start on a seeded database sends the six creation commands again, and their deciders refuse
   * them: that is how the seed finds its data there. Each refusal is the domain's answer, not a
   * failure, so its span keeps the unset status, records no exception and names the rejection by
   * its type alone (chapter 17, "Starting again on a seeded database").
   */
  @Test
  void theSeedsRefusalsOnASeededDatabaseAreRejectedCommandsNotFailedSpans() {
    seedDataRunner.seed(); // the database is seeded from here on, whichever test ran first
    recorded.clear();

    assertThat(seedDataRunner.seed()).as("a seeded database takes no seed command").isZero();

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              List<SpanData> rejected =
                  recorded.all().stream()
                      .filter(s -> s.getName().startsWith("command/"))
                      .filter(s -> s.getAttributes().get(REJECTION_TYPE) != null)
                      .toList();
              assertThat(rejected)
                  .extracting(SpanData::getName)
                  .containsExactlyInAnyOrder(
                      "command/CreateProduct",
                      "command/CreateProduct",
                      "command/CreateProduct",
                      "command/RegisterCustomer",
                      "command/RegisterCustomer",
                      "command/PlaceOrder");
              assertThat(rejected)
                  .allSatisfy(
                      span -> {
                        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
                        assertThat(span.getEvents()).as("no exception event").isEmpty();
                        assertThat(span.getAttributes().get(REJECTION_TYPE))
                            .isEqualTo(DomainException.class.getName());
                        assertThat(
                                recorded.all().stream()
                                    .filter(s -> s.getTraceId().equals(span.getTraceId())))
                            .as("no span of the refused command's trace is a failed one")
                            .noneMatch(s -> s.getStatus().getStatusCode() == StatusCode.ERROR);
                      });
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
