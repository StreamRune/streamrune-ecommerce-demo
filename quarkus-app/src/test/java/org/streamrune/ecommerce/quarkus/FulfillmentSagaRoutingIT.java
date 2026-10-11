package org.streamrune.ecommerce.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Whose saga an event reaches, against the running Quarkus app. The correlation id of a request is
 * whatever the client sends in {@code X-Correlation-Id}; the fulfillment saga routes an event by
 * the order the event is about, so a caller who names another order's saga in that header does not
 * reach it.
 *
 * <p>Two customers place an order each. Both payments are held before their capture ({@link
 * AppendFailureInjection}), so both sagas wait in {@code AWAITING_PAYMENT} and both orders stay
 * {@code CREATED}. The first customer then confirms their own order with the second customer's saga
 * id as the correlation id. The second saga must still be waiting for its payment afterwards, and
 * must run to its own end once the payment is captured.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class FulfillmentSagaRoutingIT {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** An operator: an identity with the ADMIN role. */
  private static final String OPERATOR = "q-route-operator";

  @TestHTTPResource("/")
  URI server;

  @Inject DataSource dataSource;
  @Inject VirtualThreadCommandBus commandBus;
  @Inject EventStore eventStore;

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
    String bobSaga = "fulfillment-" + bobOrder;

    try (var failures = new AppendFailureInjection(dataSource)) {
      for (String order : List.of(bobOrder, aliceOrder, laterOrder)) {
        failures.failAppend(PaymentState.TYPE, "pay-" + order, "PaymentCaptured");
      }
      try {
        placeOrder(bobOrder, bob, product);
        awaitFulfillmentStatus(bobSaga, "AWAITING_PAYMENT");
        placeOrder(aliceOrder, alice, product);
        awaitOrderReadableBy(aliceOrder, alice);

        // Alice confirms her own order and names Bob's saga in X-Correlation-Id.
        HttpResponse<String> confirmed =
            send("POST", "/api/orders/" + aliceOrder + "/confirm", alice, "CUSTOMER", bobSaga, "");
        assertThat(confirmed.statusCode()).isEqualTo(200);
        assertThat(correlationIdOf(OrderState.TYPE, aliceOrder, "OrderConfirmed"))
            .as("the correlation id the stored event carries is the one the client sent")
            .isEqualTo(bobSaga);

        // The saga subscription reads the global stream in order: once the saga of an order
        // placed afterwards exists, the subscription is past Alice's OrderConfirmed.
        placeOrder(laterOrder, alice, product);
        awaitFulfillmentStatus("fulfillment-" + laterOrder, "AWAITING_PAYMENT");

        assertThat(fulfillmentStatus(bobSaga))
            .as("Bob's saga after Alice's OrderConfirmed named it in its correlation id")
            .isEqualTo("AWAITING_PAYMENT");
        assertThat(eventTypes(OrderState.TYPE, bobOrder)).containsExactly("OrderPlaced");
      } finally {
        for (String order : List.of(bobOrder, aliceOrder, laterOrder)) {
          failures.allowAppend(PaymentState.TYPE, "pay-" + order, "PaymentCaptured");
        }
      }
    }

    // Bob's saga goes on to its own end: the payment is captured, the stock reserved, the order
    // confirmed.
    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> {
              assertThat(eventTypes(OrderState.TYPE, bobOrder)).contains("OrderConfirmed");
              assertThat(fulfillmentStatus(bobSaga)).isEqualTo("COMPLETED");
            });
    assertThat(eventTypes(PaymentState.TYPE, "pay-" + bobOrder))
        .containsExactly("PaymentInitiated", "PaymentCaptured");

    // Alice's two sagas reach an end as well, so the test leaves nothing running. The order she
    // confirmed by hand is not confirmed a second time: its saga's ConfirmOrder is refused and the
    // saga compensates.
    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> {
              assertThat(sagaStatus("fulfillment-" + aliceOrder))
                  .isIn("COMPLETED", "COMPENSATED", "FAILED");
              assertThat(sagaStatus("fulfillment-" + laterOrder))
                  .isIn("COMPLETED", "COMPENSATED", "FAILED");
            });
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

  /** The framework's status of one saga, from the saga list; {@code null} while it has no row. */
  private String sagaStatus(String sagaId) throws Exception {
    HttpResponse<String> list =
        send("GET", "/api/saga/fulfillments?limit=100000", OPERATOR, "ADMIN", null, null);
    assertThat(list.statusCode()).isEqualTo(200);
    for (JsonNode row : JSON.readTree(list.body())) {
      if (row.path("sagaId").asText().equals(sagaId)) {
        return row.path("status").asText();
      }
    }
    return null;
  }

  private void awaitFulfillmentStatus(String sagaId, String status) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(fulfillmentStatus(sagaId)).isEqualTo(status));
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
