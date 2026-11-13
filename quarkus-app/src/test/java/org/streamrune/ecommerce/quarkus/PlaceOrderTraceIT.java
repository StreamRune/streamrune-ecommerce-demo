package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * One order, traced through the components the app builds itself (the saga runner, the
 * subscriptions of {@code EcommerceSubscriptionLifecycle}) and the ones the Quarkus integration
 * builds (the command bus with the app's {@code PgAdvisoryLocker}, the event store, the projection
 * runner), as in the Spring and Micronaut apps. The spans reach {@link RecordedSpans} through the
 * SDK of quarkus-opentelemetry, which this test's profile switches on.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
@TestProfile(PlaceOrderTraceIT.TracingProfile.class)
class PlaceOrderTraceIT {

  /**
   * The SDK on, exporting nowhere: the spans reach {@link RecordedSpans} and no exporter. The other
   * tests run with the SDK off, as the application does unless {@code QUARKUS_OTEL_SDK_DISABLED} is
   * {@code false}, and StreamRune traces nothing in them.
   */
  public static class TracingProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("quarkus.otel.sdk.disabled", "false", "quarkus.otel.traces.exporter", "none");
    }
  }

  private static final AttributeKey<String> AGGREGATE_ID =
      AttributeKey.stringKey("streamrune.aggregate.id");

  private static final AttributeKey<String> STREAM_ID =
      AttributeKey.stringKey("streamrune.stream.id");

  private static final AttributeKey<String> SERVICE_INSTANCE_ID =
      AttributeKey.stringKey("service.instance.id");

  private static final AttributeKey<String> SAGA_ID = AttributeKey.stringKey("streamrune.saga.id");

  private static final String SAGA_STEP =
      "saga/org.streamrune.ecommerce.commands.saga.OrderFulfillmentState/step";

  @Inject RecordedSpans recorded;

  @Inject VirtualThreadCommandBus commandBus;

  @Test
  void aPlacedOrderIsTracedFromItsCommandToItsSaga() {
    String product = "q-trace-p-" + System.nanoTime();
    String customer = "q-trace-c-" + System.nanoTime();
    String order = "q-trace-o-" + System.nanoTime();
    commandBus.execute(
        new ProductCommand.CreateProduct(
            product, "Traced product", null, "General", new Money(new BigDecimal("4.50"), "USD")));
    commandBus.execute(new InventoryCommand.ReceiveShipment(product, 5));
    recorded.clear();

    given()
        .header("X-User-Id", customer)
        .header("X-User-Role", "CUSTOMER")
        .contentType("application/json")
        .body(
            """
            {"orderId": "%s", "customerId": "%s",
             "lines": [{"productId": "%s", "quantity": 1}]}"""
                .formatted(order, customer, product))
        .when()
        .post("/api/orders")
        .then()
        .statusCode(200);

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              List<SpanData> spans = recorded.all();
              SpanData command =
                  spans.stream()
                      .filter(s -> s.getName().equals("command/PlaceOrder"))
                      .filter(s -> order.equals(s.getAttributes().get(AGGREGATE_ID)))
                      .findFirst()
                      .orElseThrow(() -> new AssertionError("no command/PlaceOrder span"));
              assertNamesItsInstance(command);
              assertThat(spans.stream().filter(s -> isChildOf(s, command)).map(SpanData::getName))
                  .as("the spans under command/PlaceOrder")
                  .contains("lock/acquire", "eventstore/load", "eventstore/append");
              assertThat(
                      spans.stream()
                          .filter(s -> s.getName().equals("lock/acquire"))
                          .filter(s -> isChildOf(s, command))
                          .map(s -> s.getAttributes().get(STREAM_ID)))
                  .as("the lock of the order's stream, taken by the app's PgAdvisoryLocker")
                  .containsExactly("order:" + order);
              assertThat(spans)
                  .as("the saga's first step, linked to the command that placed the order")
                  .anyMatch(
                      s ->
                          s.getName().equals(SAGA_STEP)
                              && ("fulfillment-" + order).equals(s.getAttributes().get(SAGA_ID))
                              && linksTo(s, command));
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
            });
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

  /**
   * The resource of a span names the instance that sent it, {@code service.instance.id}: the host
   * name where {@code HOSTNAME} is set (in Docker the container's, so two backends differ), else an
   * id the application draws at its start. StreamRune's own attributes never name the process.
   */
  private static void assertNamesItsInstance(SpanData span) {
    String instance = span.getResource().getAttribute(SERVICE_INSTANCE_ID);
    String hostname = System.getenv("HOSTNAME");
    if (hostname != null && !hostname.isBlank()) {
      assertThat(instance).as("service.instance.id").isEqualTo(hostname);
    } else {
      assertThat(instance).as("service.instance.id").isNotBlank();
      assertThat(UUID.fromString(instance)).isNotNull();
    }
  }
}
