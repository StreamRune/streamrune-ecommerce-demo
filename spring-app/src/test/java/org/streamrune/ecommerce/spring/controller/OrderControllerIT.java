package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.commands.order.OrderCancellationProcessManager;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentIds;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * HTTP integration test for the Spring order flow. Exercises the full CQRS round-trip (POST command
 * → event → projection → GET query) and the {@code @RequireRole("ADMIN")} authorization on
 * ship/deliver, over real HTTP against a running Spring Boot app backed by real PostgreSQL and
 * RabbitMQ (Testcontainers).
 *
 * <p><b>Saga-aware.</b> The Spring app runs the global {@code OrderFulfillment} saga (wired
 * unconditionally in {@code StreamRuneConfig}): every {@code OrderPlaced} kicks off InitiatePayment
 * → CapturePayment → ReserveStock → <b>ConfirmOrder</b>, i.e. the saga itself drives an order to
 * {@code CONFIRMED}. If the product has no inventory, {@code ReserveStock} fails and the saga
 * <b>compensates by cancelling the order</b>. So these tests must (1) stock the product first via
 * {@code ReceiveShipment} so the reservation succeeds and the order is never cancelled, and (2)
 * treat the saga as the confirmation mechanism — they {@link #awaitOrderStatus await} the
 * saga-driven {@code CONFIRMED} state rather than confirming manually (a manual {@code
 * ConfirmOrder} on an order the saga already confirmed would race and fail with "Can only confirm
 * CREATED orders"). The ship/deliver steps run after the saga has confirmed, so the ADMIN-role
 * enforcement they assert is exercised exactly as before.
 *
 * <p>All test classes extend {@link AbstractIntegrationTest} and share a single Spring context
 * (singleton-container pattern). No {@code @TestPropertySource} or other annotation that would
 * change the context key may be added here.
 */
class OrderControllerIT extends AbstractIntegrationTest {

  @Autowired VirtualThreadCommandBus commandBus;
  @Autowired EventStore eventStore;
  @Autowired DataSource dataSource;
  @Autowired SagaStore sagaStore;
  @Autowired PaymentGatewaySimulator paymentGateway;

  private OrderUndoProbe probe;

  @BeforeEach
  void probe() {
    probe = new OrderUndoProbe(eventStore, sagaStore, dataSource);
  }

  /** One {@code audit_log} row, as the command bus's audit interceptor wrote it. */
  private record AuditRow(String outcome, String userId, String errorMessage) {}

  /** The audit rows written for {@code commandType} on {@code aggregateId}, oldest first. */
  private List<AuditRow> auditRows(String aggregateId, String commandType) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT outcome, user_id, error_message FROM audit_log"
                    + " WHERE aggregate_id = ? AND command_type = ? ORDER BY id")) {
      ps.setString(1, aggregateId);
      ps.setString(2, commandType);
      try (var rs = ps.executeQuery()) {
        var rows = new ArrayList<AuditRow>();
        while (rs.next()) {
          rows.add(new AuditRow(rs.getString(1), rs.getString(2), rs.getString(3)));
        }
        return rows;
      }
    }
  }

  /**
   * Stocks the inventory aggregate for {@code productId} so the saga's {@code ReserveStock}
   * succeeds. Inventory lives in its own {@code inventory:<productId>} stream, fed by {@code
   * ReceiveShipment} — creating a product does not stock it. Issued directly on the command bus,
   * mirroring the Spring {@code SeedDataRunner}'s {@code ReceiveShipment} seeding (the app also
   * serves {@code POST /api/inventory/{productId}/receive}).
   */
  private void stockProduct(String productId, int quantity) {
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, quantity));
  }

  private void placeOrder(String orderId, String customerId, String productId) {
    String body =
        """
        {
          "orderId": "%s",
          "customerId": "%s",
          "lines": [
            {"productId": "%s", "quantity": 1, "unitPrice": 9.99}
          ]
        }"""
            .formatted(orderId, customerId, productId);
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  private OrderView fetchOrder(String orderId) {
    return client
        .get()
        .uri("/api/orders/" + orderId)
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(OrderView.class)
        .returnResult()
        .getResponseBody();
  }

  /**
   * Fetches an order without asserting on status, returning {@code null} when the projection has
   * not yet populated (404) so {@link #awaitOrderStatus} can poll safely.
   */
  private OrderView fetchOrderOrNull(String orderId) {
    return client
        .get()
        .uri("/api/orders/" + orderId)
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectBody(OrderView.class)
        .returnResult()
        .getResponseBody();
  }

  /**
   * Awaits the saga (or a prior command) driving the order to {@code expected} in the projection.
   * Uses {@link #fetchOrderOrNull} so transient 404s (projection not yet populated) are treated as
   * retryable assertion failures rather than propagated NullPointerExceptions.
   */
  private void awaitOrderStatus(String orderId, OrderStatus expected) {
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              OrderView v = fetchOrderOrNull(orderId);
              assertThat(v).isNotNull();
              assertThat(v.status()).isEqualTo(expected);
            });
  }

  /** Places an order of the given products and quantities, in the map's order, at 9.99 each. */
  private void placeOrder(String orderId, String customerId, Map<String, Integer> lines) {
    String body =
        """
        {"orderId": "%s", "customerId": "%s", "lines": [%s]}"""
            .formatted(
                orderId,
                customerId,
                lines.entrySet().stream()
                    .map(
                        l ->
                            """
                            {"productId": "%s", "quantity": %d, "unitPrice": 9.99}"""
                                .formatted(l.getKey(), l.getValue()))
                    .collect(Collectors.joining(", ")));
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  /** Cancels the order with the reason "changed my mind" and returns the HTTP status. */
  private int cancel(String orderId, String role, String userId) {
    return client
        .post()
        .uri("/api/orders/" + orderId + "/cancel")
        .header("X-User-Role", role)
        .header("X-User-Id", userId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"reason": "changed my mind"}""")
        .exchange()
        .returnResult(Void.class)
        .getStatus()
        .value();
  }

  /**
   * Cancels as the order's customer as soon as the order answers: its read-model row, which the
   * customer's access is checked against, appears a moment after the order is placed, and until
   * then the cancellation is refused with 403.
   */
  private void cancelAsCustomerAsSoonAsTheOrderAnswers(String orderId, String customerId) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(cancel(orderId, "CUSTOMER", customerId)).isEqualTo(200));
  }

  /** The order's total, as its {@code OrderPlaced} recorded it. */
  private Money orderTotal(String orderId) {
    return ((OrderEvent.OrderPlaced) probe.events(OrderState.TYPE, orderId).getFirst().event())
        .total();
  }

  private void shipOrderAsAdmin(String orderId) {
    client
        .post()
        .uri("/api/orders/" + orderId + "/ship")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  @Test
  void placeOrderReturns2xxAndProjectionEventuallyExposesIt() {
    String oid = "o-create-" + System.nanoTime();
    String cid = "o-cust-" + System.nanoTime();
    String pid = "o-prod-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              OrderView v = fetchOrder(oid);
              assertThat(v).isNotNull();
              assertThat(v.orderId()).isEqualTo(oid);
              assertThat(v.customerId()).isEqualTo(cid);
              assertThat(v.lines()).hasSize(1);
              assertThat(v.lines().get(0).productId()).isEqualTo(pid);
            });
  }

  @Test
  void listOrdersReturnsAllPlaced() {
    String cid = "o-list-cust-" + System.nanoTime();
    String oid1 = "o-list-1-" + System.nanoTime();
    String oid2 = "o-list-2-" + System.nanoTime();
    String pid1 = "prod-list-a-" + System.nanoTime();
    String pid2 = "prod-list-b-" + System.nanoTime();
    stockProduct(pid1, 5);
    stockProduct(pid2, 5);
    placeOrder(oid1, cid, pid1);
    placeOrder(oid2, cid, pid2);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> orders =
                  client
                      .get()
                      .uri("/api/orders")
                      .header("X-User-Role", "CUSTOMER")
                      .header("X-User-Id", cid)
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(orders).isNotNull();
              List<String> ids = orders.stream().map(o -> (String) o.get("orderId")).toList();
              assertThat(ids).containsExactlyInAnyOrder(oid1, oid2);
            });
  }

  @Test
  void listOrdersByCustomerIdFiltersCorrectly() {
    String cid = "o-filter-cust-" + System.nanoTime();
    String oid = "o-filter-" + System.nanoTime();
    String pid = "prod-filter-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> orders =
                  client
                      .get()
                      .uri("/api/orders?customerId=" + cid)
                      .header("X-User-Role", "CUSTOMER")
                      .header("X-User-Id", cid)
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(orders).isNotNull();
              assertThat(orders.stream().map(o -> (String) o.get("orderId")).toList())
                  .containsExactly(oid);
            });
  }

  @Test
  void getMissingOrderReturns404() {
    client
        .get()
        .uri("/api/orders/does-not-exist-" + System.nanoTime())
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  @Test
  void placeOrderWithInvalidPayloadReturns400() {
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "", "customerId": "", "lines": []}""")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  /**
   * An otherwise valid order whose orderId carries a control character (a JSON {@code \n}) is
   * refused by {@code AggregateId.of} in the command bus's id extractor, before any interceptor
   * runs or anything is written; the {@code IllegalArgumentException} maps to 400 and its message
   * names the rule without echoing the id.
   */
  @Test
  void placeOrderWithAControlCharacterInTheOrderIdReturns400() {
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "o-ctl\\nINFO forged-SECRET", "customerId": "c-ctl", "lines": [{"productId": "p-ctl", "quantity": 1, "unitPrice": 10}]}""")
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .value(
            body ->
                assertThat(body)
                    .contains("aggregateId must not contain control characters")
                    .doesNotContain("SECRET"));
  }

  /**
   * An order id longer than the 255-character stream-id columns is refused by {@code
   * AggregateId.of} before any interceptor runs, so a command on it answers 400 instead of failing
   * the event-store append with an SQL error (a 500). The order id comes from the path here, which
   * no {@code PlaceOrder} constructor sees.
   */
  @Test
  void confirmOrderWithAnOrderIdLongerThan255CharactersReturns400() {
    String orderId = "o-long-SECRET-" + "x".repeat(256 - 14);
    client
        .post()
        .uri("/api/orders/" + orderId + "/confirm")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .value(
            body ->
                assertThat(body)
                    .contains("aggregateId must be at most 255 characters, got 256")
                    .doesNotContain("SECRET"));
  }

  /**
   * An order id of 244 to 255 characters is a valid order stream id, but the saga that {@code
   * OrderPlaced} starts is identified by {@code "fulfillment-" + orderId}, which a {@code SagaId}
   * refuses above 255 characters. Accepted at {@code PlaceOrder}, the order would be placed and its
   * saga could never start. The {@code OrderCommand.PlaceOrder} the controller builds refuses it
   * first: 400 with the rule, without echoing the id, and nothing is written to the order's stream.
   */
  @Test
  void placeOrderWithAnOrderIdTooLongForTheSagaIdReturns400AndWritesNothing() {
    for (int length : new int[] {244, 255}) {
      String orderId = "o-saga-SECRET-" + "x".repeat(length - 14);
      client
          .post()
          .uri("/api/orders")
          .header("X-User-Role", "ADMIN")
          .header("X-User-Id", "test-admin")
          .contentType(MediaType.APPLICATION_JSON)
          .bodyValue(
              """
              {"orderId": "%s", "customerId": "c-saga-long", "lines": [{"productId": "p-saga-long", "quantity": 1, "unitPrice": 10}]}"""
                  .formatted(orderId))
          .exchange()
          .expectStatus()
          .isBadRequest()
          .expectBody(String.class)
          .value(
              body ->
                  assertThat(body)
                      .contains("orderId must be at most 243 characters, got " + orderId.length())
                      .doesNotContain("SECRET"));

      assertThat(
              eventStore.readStream(
                  StreamId.of(OrderState.TYPE, AggregateId.of(orderId)),
                  Version.initial(),
                  Integer.MAX_VALUE))
          .isEmpty();
    }
  }

  /**
   * The longest order id {@code PlaceOrder} accepts, 243 characters, is the longest whose saga id
   * {@code "fulfillment-" + orderId} fits 255: the saga starts, takes the payment, reserves the
   * stock and confirms the order.
   */
  @Test
  void placeOrderWithAnOrderIdAtTheLengthBoundIsDrivenBySagaToConfirmed() {
    String oid = "o-bound-" + System.nanoTime();
    oid = oid + "x".repeat(243 - oid.length());
    String cid = "o-bound-cust-" + System.nanoTime();
    String pid = "prod-bound-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
  }

  /**
   * An order line whose productId carries a control character passes {@code @NotBlank} but is
   * refused when the controller builds the {@code OrderCommand.OrderLine}: the saga would address
   * the inventory stream by that productId, which the inventory extractor refuses, but only after
   * the payment was captured. The refusal answers 400 with the rule, without echoing the id, and
   * nothing is written to the order's stream, so no saga starts and no payment is taken.
   */
  @Test
  void placeOrderWithAControlCharacterInAProductIdReturns400AndWritesNothing() {
    String orderId = "o-pctl-" + System.nanoTime();
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"orderId": "%s", "customerId": "c-pctl", "lines": [{"productId": "p-ok", "quantity": 1, "unitPrice": 10}, {"productId": "p-ctl\\nINFO forged-SECRET", "quantity": 1, "unitPrice": 10}]}"""
                .formatted(orderId))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .value(
            body ->
                assertThat(body)
                    .contains("productId must not contain control characters")
                    .doesNotContain("SECRET"));

    assertThat(
            eventStore.readStream(
                StreamId.of(OrderState.TYPE, AggregateId.of(orderId)),
                Version.initial(),
                Integer.MAX_VALUE))
        .isEmpty();
  }

  @Test
  void confirmOrderIsDrivenBySagaToConfirmed() {
    // The OrderFulfillment saga is the confirmation mechanism: with the product stocked, placing an
    // order drives it through payment + reservation to CONFIRMED with no manual /confirm call.
    String oid = "o-confirm-" + System.nanoTime();
    String cid = "o-conf-cust-" + System.nanoTime();
    String pid = "prod-conf-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
  }

  // ---- Every cancellation of an order is undone through its OrderCancelled: the order
  // cancellation process manager releases each line's stock and voids or refunds the payment. ----

  @Test
  void cancellingAConfirmedOrderRefundsItsPaymentAndReleasesItsStock() {
    String oid = "o-cancel-" + System.nanoTime();
    String cid = "o-cancel-cust-" + System.nanoTime();
    String pid = "prod-cancel-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, Map.of(pid, 2));
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);

    assertThat(cancel(oid, "CUSTOMER", cid)).isEqualTo(200);

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(probe.paymentEvents(oid))
                    .containsExactly("PaymentInitiated", "PaymentCaptured", "PaymentRefunded"));
    var refunded =
        (PaymentEvent.PaymentRefunded)
            probe.events(PaymentState.TYPE, PaymentIds.forOrder(oid)).getLast().event();
    assertThat(refunded.amount()).as("a full refund").isEqualTo(orderTotal(oid));
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(probe.stockEventsOf(pid, oid))
                  .containsExactly("StockReserved:2", "StockReleased:2");
              assertThat(probe.inventory(pid).available()).isEqualTo(5);
              assertThat(paymentGateway.hasRefunded(PaymentIds.forOrder(oid)))
                  .as("the refund reached the gateway")
                  .isTrue();
            });
  }

  @Test
  void cancellingRightAfterPlacing_leavesNoChargeAndNoReservation() {
    String oid = "o-cancel-early-" + System.nanoTime();
    String cid = "o-cancel-early-cust-" + System.nanoTime();
    String pid = "prod-cancel-early-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, Map.of(pid, 1));

    cancelAsCustomerAsSoonAsTheOrderAnswers(oid, cid);

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              assertThat(probe.sagaStatus(oid))
                  .isIn(SagaStatus.COMPENSATED, SagaStatus.COMPLETED, SagaStatus.FAILED);
              List<String> payment = probe.paymentEvents(oid);
              assertThat(payment.getLast())
                  .as("the payment ends voided or refunded, never only captured: %s", payment)
                  .isIn("PaymentVoided", "PaymentRefunded");
              assertThat(probe.inventory(pid).reservationOf(oid))
                  .as("the order's net reservation")
                  .isEmpty();
              assertThat(probe.inventory(pid).available()).isEqualTo(5);
            });
  }

  @Test
  void cancellingTwiceRefundsOnce() throws SQLException {
    String oid = "o-cancel-twice-" + System.nanoTime();
    String cid = "o-cancel-twice-cust-" + System.nanoTime();
    String pid = "prod-cancel-twice-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, Map.of(pid, 1));
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);

    assertThat(cancel(oid, "CUSTOMER", cid)).isEqualTo(200);
    assertThat(cancel(oid, "CUSTOMER", cid)).isEqualTo(200);

    long secondCancellation = probe.events(OrderState.TYPE, oid).getLast().globalOffset().value();
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(probe.checkpoint(OrderCancellationProcessManager.SUBSCRIPTION_NAME))
                    .as("the undo of both cancellations ran")
                    .isGreaterThanOrEqualTo(secondCancellation));
    assertThat(probe.eventTypes(OrderState.TYPE, oid))
        .filteredOn("OrderCancelled"::equals)
        .hasSize(2);
    assertThat(probe.paymentEvents(oid))
        .containsExactly("PaymentInitiated", "PaymentCaptured", "PaymentRefunded");
    assertThat(probe.stockEventsOf(pid, oid)).containsExactly("StockReserved:1", "StockReleased:1");
  }

  @Test
  void aMultiLineOrderReservesEveryLine_andItsShipmentCommitsEveryLine() {
    String oid = "o-multi-" + System.nanoTime();
    String cid = "o-multi-cust-" + System.nanoTime();
    String first = "prod-multi-a-" + System.nanoTime();
    String second = "prod-multi-b-" + System.nanoTime();
    stockProduct(first, 5);
    stockProduct(second, 5);
    var lines = new LinkedHashMap<String, Integer>();
    lines.put(first, 2);
    lines.put(second, 3);
    placeOrder(oid, cid, lines);
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    assertThat(probe.stockEventsOf(first, oid)).containsExactly("StockReserved:2");
    assertThat(probe.stockEventsOf(second, oid)).containsExactly("StockReserved:3");

    shipOrderAsAdmin(oid);

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(probe.stockEventsOf(first, oid))
                  .containsExactly("StockReserved:2", "ReservationConfirmed:2");
              assertThat(probe.stockEventsOf(second, oid))
                  .containsExactly("StockReserved:3", "ReservationConfirmed:3");
            });
    assertThat(probe.inventory(first).committed()).isEqualTo(2);
    assertThat(probe.inventory(second).committed()).isEqualTo(3);
  }

  /**
   * A customer cancels while the saga's reservation step is still running: the second product's
   * {@code StockReserved} is refused until the cancellation's undo has run, so the undo releases
   * the first product only. The reservation the step then stores at the second product is released
   * by the undo of the saga's own cancellation, which a cancelled order accepts.
   */
  @Test
  void aReservationStoredAfterTheCustomersCancellation_isReleasedByTheSagasCancellation()
      throws Exception {
    String oid = "o-late-reserve-" + System.nanoTime();
    String cid = "o-late-reserve-cust-" + System.nanoTime();
    String first = "prod-late-a-" + System.nanoTime();
    String second = "prod-late-b-" + System.nanoTime();
    stockProduct(first, 5);
    stockProduct(second, 5);
    var lines = new LinkedHashMap<String, Integer>();
    lines.put(first, 1);
    lines.put(second, 1);

    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(InventoryState.TYPE, second, "StockReserved");
      placeOrder(oid, cid, lines);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(probe.stockEventsOf(first, oid)).containsExactly("StockReserved:1");
                assertThat(failures.refusals()).isPositive();
              });

      cancelAsCustomerAsSoonAsTheOrderAnswers(oid, cid);

      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(probe.stockEventsOf(first, oid))
                    .containsExactly("StockReserved:1", "StockReleased:1");
                assertThat(probe.paymentEvents(oid)).contains("PaymentRefunded");
              });
      assertThat(probe.stockEventsOf(second, oid))
          .as("nothing is reserved at the second product yet, so nothing is released there")
          .isEmpty();
      failures.allowAppend(InventoryState.TYPE, second, "StockReserved");
    }

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              assertThat(probe.stockEventsOf(second, oid))
                  .containsExactly("StockReserved:1", "StockReleased:1");
              assertThat(probe.sagaStatus(oid)).isEqualTo(SagaStatus.COMPENSATED);
            });
    assertThat(
            probe.events(OrderState.TYPE, oid).stream()
                .map(e -> e.event())
                .filter(OrderEvent.OrderCancelled.class::isInstance)
                .map(e -> ((OrderEvent.OrderCancelled) e).reason())
                .toList())
        .as("the customer's cancellation, then the saga's")
        .containsExactly("changed my mind", OrderFulfillmentSaga.COMPENSATION_REASON);
    assertThat(probe.inventory(second).available()).isEqualTo(5);
    assertThat(probe.inventory(first).available()).isEqualTo(5);
  }

  /**
   * An {@code ADMIN} may cancel an order id nobody placed; its {@code OrderCancelled} names no
   * lines and its undo does nothing. The id is 255 characters long, so a payment id built from it
   * would not fit an id; a cancellation process manager that tried would be refused on every
   * delivery, and no later cancellation would be undone.
   */
  @Test
  void aCancellationOfAnOrderNobodyPlaced_leavesTheUndoRunning() {
    String prefix = "o-unplaced-" + System.nanoTime() + "-";
    String unplaced = prefix + "x".repeat(255 - prefix.length());
    assertThat(cancel(unplaced, "ADMIN", "test-admin")).isEqualTo(200);

    String oid = "o-after-unplaced-" + System.nanoTime();
    String cid = "o-after-unplaced-cust-" + System.nanoTime();
    String pid = "prod-after-unplaced-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, Map.of(pid, 1));
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    assertThat(cancel(oid, "CUSTOMER", cid)).isEqualTo(200);

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(() -> assertThat(probe.paymentEvents(oid)).contains("PaymentRefunded"));
    assertThat(probe.eventTypes(OrderState.TYPE, unplaced)).containsExactly("OrderCancelled");
  }

  /**
   * A shipped order is on its way: its cancellation answers 400 with the rule, and the order's
   * stream ends with {@code OrderShipped}.
   */
  @Test
  void cancellingAShippedOrderAnswers400() {
    String oid = "o-cancel-shipped-" + System.nanoTime();
    String cid = "o-cancel-shipped-cust-" + System.nanoTime();
    String pid = "prod-cancel-shipped-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    shipOrderAsAdmin(oid);

    client
        .post()
        .uri("/api/orders/" + oid + "/cancel")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"reason": "too late"}""")
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody(String.class)
        .value(body -> assertThat(body).contains("Cannot cancel SHIPPED orders"));

    var events =
        eventStore.readStream(
            StreamId.of(OrderState.TYPE, AggregateId.of(oid)),
            Version.initial(),
            Integer.MAX_VALUE);
    assertThat(events.get(events.size() - 1).event()).isInstanceOf(OrderEvent.OrderShipped.class);
  }

  @Test
  void shipOrderRequiresAdminRole() {
    String oid = "o-ship-" + System.nanoTime();
    String cid = "o-ship-cust-" + System.nanoTime();
    String pid = "prod-ship-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    // The saga drives the order to CONFIRMED (the precondition ShipOrder requires).
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    shipOrderAsAdmin(oid);
  }

  /**
   * A refused privileged command is audited: the audit interceptor runs before the authorization
   * interceptor, so the refusal reaches its {@code onError()} and leaves a {@code FAILURE} row that
   * names the caller and the missing role.
   */
  @Test
  void shipOrderWithoutAdminRoleReturns403AndIsAuditedAsAFailure() throws SQLException {
    String oid = "o-ship-noadmin-" + System.nanoTime();
    String cid = "o-ship-noadmin-cust-" + System.nanoTime();
    String pid = "prod-ship-noadmin-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    // CUSTOMER lacks the ADMIN role ShipOrder requires: AuthorizationException -> 403
    // (GlobalExceptionHandler). The order is CONFIRMED, so the state machine would accept the
    // command: the 403 is the authorization interceptor's.
    client
        .post()
        .uri("/api/orders/" + oid + "/ship")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .exchange()
        .expectStatus()
        .isForbidden();

    List<AuditRow> rows = auditRows(oid, "ShipOrder");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).outcome()).isEqualTo("FAILURE");
    assertThat(rows.get(0).userId()).isEqualTo(cid);
    assertThat(rows.get(0).errorMessage()).contains("Required role: ADMIN");
  }

  @Test
  void deliverOrderRequiresAdminRole() {
    String oid = "o-deliver-" + System.nanoTime();
    String cid = "o-deliver-cust-" + System.nanoTime();
    String pid = "prod-deliver-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    // Full sequence: CREATED -> CONFIRMED (saga) -> SHIPPED -> DELIVERED.
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    shipOrderAsAdmin(oid);
    client
        .post()
        .uri("/api/orders/" + oid + "/deliver")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }
}
