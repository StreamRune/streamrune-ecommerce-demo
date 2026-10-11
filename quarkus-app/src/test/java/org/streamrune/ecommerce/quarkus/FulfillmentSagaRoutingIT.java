package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Whose saga an event reaches, against the running Quarkus app. The correlation id of a request is
 * whatever the client sends in {@code X-Correlation-Id}; the fulfillment saga routes an event by
 * the order the event is about, so a caller who names another order's saga in that header does not
 * reach it.
 *
 * <p>Two customers place an order each, and the saga of each order waits for its payment. The first
 * customer then confirms their own order with the second customer's saga id as the correlation id.
 * The second saga must still be waiting for its payment afterwards, and its row must be the row
 * that was stored: a row records every event the runner hands its saga, so an untouched row is what
 * shows that the event was never routed there, whatever the saga would have made of it.
 *
 * <p><b>How the two sagas are kept waiting.</b> A fulfillment passes {@code AWAITING_PAYMENT} in
 * well under a second, so the test stores each saga's row in that state before the order is placed
 * ({@link #sagaAwaitingItsPayment}): the order's own {@code OrderPlaced} then finds its saga
 * started and starts nothing, the saga stays where it is and the order stays {@code CREATED} for as
 * long as the test needs. Holding a step of a real saga instead (refusing the append of {@code
 * PaymentCaptured}, as {@code PaymentCaptureRetryIT} does) makes the payment process manager retry
 * a failing command, and those failures open the command bus's circuit breaker for the requests
 * this test sends.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class FulfillmentSagaRoutingIT {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final SagaType FULFILLMENT = SagaType.fromClass(OrderFulfillmentState.class);

  private static final Money PRICE = new Money(new BigDecimal("9.99"), "USD");

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "q-route-operator";

  @TestHTTPResource("/")
  URI server;

  @Inject VirtualThreadCommandBus commandBus;
  @Inject EventStore eventStore;
  @Inject SagaStore sagaStore;

  private final HttpClient http = HttpClient.newHttpClient();

  @Test
  void anEventOfOneOrderDoesNotReachTheSagaItsCorrelationIdNames() throws Exception {
    String product = uniqueId("p");
    commandBus.execute(new InventoryCommand.ReceiveShipment(product, 5));
    String alice = uniqueId("alice");
    String bob = uniqueId("bob");
    String aliceOrder = uniqueId("o-alice");
    String bobOrder = uniqueId("o-bob");
    String laterOrder = uniqueId("o-later");

    SagaId aliceSaga = sagaAwaitingItsPayment(aliceOrder, alice, product);
    SagaId bobSaga = sagaAwaitingItsPayment(bobOrder, bob, product);
    try {
      placeOrder(bobOrder, bob, product);
      placeOrder(aliceOrder, alice, product);
      awaitOrderReadableBy(aliceOrder, alice);

      // Alice confirms her own order and names Bob's saga in X-Correlation-Id.
      HttpResponse<String> confirmed =
          send(
              "POST",
              "/api/orders/" + aliceOrder + "/confirm",
              alice,
              "CUSTOMER",
              bobSaga.value(),
              "");
      assertThat(confirmed.statusCode()).isEqualTo(200);
      assertThat(correlationIdOf(OrderState.TYPE, aliceOrder, "OrderConfirmed"))
          .as("the correlation id the stored event carries is the one the client sent")
          .isEqualTo(bobSaga.value());

      // The saga subscription reads the global stream in order: once the saga of an order
      // placed afterwards exists, the subscription is past Alice's OrderConfirmed.
      placeOrder(laterOrder, alice, product);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> assertThat(fulfillmentStatus("fulfillment-" + laterOrder)).isNotNull());

      assertThat(fulfillmentStatus(bobSaga.value()))
          .as("Bob's saga after Alice's OrderConfirmed named it in its correlation id")
          .isEqualTo("AWAITING_PAYMENT");
      // The event went to the saga of the order it is about, and that saga did not ask for a
      // confirmation yet: it stays where it is too.
      assertThat(fulfillmentStatus(aliceSaga.value())).isEqualTo("AWAITING_PAYMENT");
      assertThat(eventTypes(OrderState.TYPE, bobOrder)).containsExactly("OrderPlaced");

      // A saga's row records every event the runner hands it, whatever the saga makes of it:
      // each one raises the row's version and becomes its last applied offset. Bob's row is
      // the row that was stored, and the event Alice's row applied last is her OrderConfirmed.
      LoadedSaga<OrderFulfillmentState> bobRow = sagaRow(bobSaga);
      assertThat(bobRow.version()).as("the version of Bob's saga row").isEqualTo(1);
      assertThat(bobRow.lastAppliedOffset())
          .as("the offset of the last event applied to Bob's saga row")
          .isNull();
      assertThat(sagaRow(aliceSaga).lastAppliedOffset())
          .as("the offset of the last event applied to Alice's saga row")
          .isEqualTo(offsetOf(OrderState.TYPE, aliceOrder, "OrderConfirmed"));
    } finally {
      sagaStore.delete(aliceSaga, FULFILLMENT);
      sagaStore.delete(bobSaga, FULFILLMENT);
    }

    // The order placed last has a saga of its own, which runs to its end: nothing is left
    // running.
    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(fulfillmentStatus("fulfillment-" + laterOrder)).isEqualTo("COMPLETED"));
  }

  /**
   * Stores the saga of an order as it is between {@code InitiatePayment} and the capture: started,
   * waiting for its payment. Stored before the order is placed, it is the saga the order's {@code
   * OrderPlaced} finds, so nothing drives it on.
   */
  private SagaId sagaAwaitingItsPayment(String order, String customer, String product) {
    SagaId sagaId = new SagaId("fulfillment-" + order);
    OrderFulfillmentState state =
        new OrderFulfillmentState(
            sagaId,
            order,
            customer,
            "pay-" + order,
            PRICE,
            List.of(new OrderEvent.OrderLine(product, 1, PRICE)),
            OrderFulfillmentStatus.AWAITING_PAYMENT);
    sagaStore.create(sagaId, FULFILLMENT, state, SagaStatus.STARTED);
    return sagaId;
  }

  // --- reads ---

  /** The fulfillment status of one saga, read as an ADMIN; {@code null} while it has no row. */
  private String fulfillmentStatus(String sagaId) throws Exception {
    HttpResponse<String> saga =
        send("GET", "/api/saga/fulfillments/" + sagaId, OPERATOR, "ADMIN", null, null);
    if (saga.statusCode() == 404) {
      return null;
    }
    assertThat(saga.statusCode()).isEqualTo(200);
    return JSON.readTree(saga.body()).path("fulfillmentStatus").asText();
  }

  /** Waits for the order's read-model row: until it is there, its own customer is refused. */
  private void awaitOrderReadableBy(String order, String customer) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        send("GET", "/api/orders/" + order, customer, "CUSTOMER", null, null)
                            .statusCode())
                    .isEqualTo(200));
  }

  /** The stored row of one fulfillment saga. */
  private LoadedSaga<OrderFulfillmentState> sagaRow(SagaId sagaId) {
    return sagaStore.load(sagaId, FULFILLMENT, OrderFulfillmentState.class).orElseThrow();
  }

  private long offsetOf(AggregateType type, String id, String eventType) {
    return events(type, id).stream()
        .filter(e -> e.eventType().name().equals(eventType))
        .findFirst()
        .orElseThrow()
        .globalOffset()
        .value();
  }

  private List<String> eventTypes(AggregateType type, String id) {
    return events(type, id).stream().map(e -> e.eventType().name()).toList();
  }

  private String correlationIdOf(AggregateType type, String id, String eventType) {
    return events(type, id).stream()
        .filter(e -> e.eventType().name().equals(eventType))
        .findFirst()
        .orElseThrow()
        .metadata()
        .correlationId()
        .value();
  }

  private List<EventEnvelope> events(AggregateType type, String id) {
    return eventStore.readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 100);
  }

  // --- requests ---

  private void placeOrder(String order, String customer, String product) throws Exception {
    String body =
        """
        {"orderId": "%s", "customerId": "%s",
         "lines": [{"productId": "%s", "quantity": 1, "unitPrice": 9.99}]}"""
            .formatted(order, customer, product);
    assertThat(send("POST", "/api/orders", customer, "CUSTOMER", null, body).statusCode())
        .isEqualTo(200);
  }

  /**
   * One request as the given caller.
   *
   * @param correlationId the {@code X-Correlation-Id} header, or {@code null} to send none
   * @param body the JSON body, an empty string for a request without one, {@code null} for a GET
   */
  private HttpResponse<String> send(
      String method, String path, String userId, String role, String correlationId, String body)
      throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(server().resolve(path))
            .header("Accept", "application/json")
            .header("X-User-Id", userId)
            .header("X-User-Role", role);
    if (correlationId != null) {
      request.header("X-Correlation-Id", correlationId);
    }
    if (body == null || body.isEmpty()) {
      request.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      request
          .header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(body));
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private URI server() {
    return server;
  }

  private static String uniqueId(String prefix) {
    return "q-route-" + prefix + "-" + System.nanoTime();
  }
}
