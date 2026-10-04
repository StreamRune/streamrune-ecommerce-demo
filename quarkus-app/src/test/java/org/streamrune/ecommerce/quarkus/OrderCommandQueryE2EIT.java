package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * HTTP end-to-end parity suite for the Quarkus order flow, mirroring the Spring app's {@code
 * OrderControllerIT}. Exercises the full CQRS round-trip (POST command → event → projection → GET
 * query) and the {@code @RequireRole("ADMIN")} authorization on ship/deliver, over real HTTP
 * against a running Quarkus app backed by a real PostgreSQL (Testcontainers).
 *
 * <p>The authorization assertions are the load-bearing part: they prove the Quarkus {@code
 * OrderCommandController} binds the request context (via {@code
 * StreamRuneRequestFilter.withContext}) so the command bus's authorization interceptor observes the
 * {@code X-User-Role}. Without that bind the role-gated commands would fail closed for everyone
 * (the bug this suite guards against).
 *
 * <p><b>Saga-aware.</b> The Quarkus app runs the global {@code OrderFulfillment} saga (wired in
 * {@code EcommerceSubscriptionLifecycle}): every {@code OrderPlaced} kicks off InitiatePayment →
 * CapturePayment → ReserveStock → <b>ConfirmOrder</b>, i.e. the saga itself drives an order to
 * {@code CONFIRMED}. If the product has no inventory, {@code ReserveStock} fails and the saga
 * <b>compensates by cancelling the order</b>. So these tests must (1) stock the product first via
 * {@code ReceiveShipment} so the reservation succeeds and the order is never cancelled, and (2)
 * treat the saga as the confirmation mechanism — they {@link #awaitOrderStatus await} the
 * saga-driven {@code CONFIRMED} state rather than confirming manually (a manual {@code
 * ConfirmOrder} on an order the saga already confirmed would race and fail with "Can only confirm
 * CREATED orders"). This mirrors how the Spring app's {@code SagaControllerIT} stocks the product
 * and lets the saga drive the order. The ship/deliver steps run after the saga has confirmed, so
 * the ADMIN-role enforcement they assert is exercised exactly as before.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class OrderCommandQueryE2EIT {

  @Inject VirtualThreadCommandBus commandBus;
  @Inject EventStore eventStore;
  @Inject DataSource dataSource;

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
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(ContentType.JSON)
        .body(body)
        .when()
        .post("/api/orders")
        .then()
        .statusCode(200);
  }

  private OrderView fetchOrder(String orderId) {
    return given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "test-user")
        .when()
        .get("/api/orders/" + orderId)
        .then()
        .statusCode(200)
        .extract()
        .as(OrderView.class);
  }

  /**
   * Awaits the saga (or a prior command) driving the order to {@code expected} in the projection.
   */
  private void awaitOrderStatus(String orderId, OrderStatus expected) {
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchOrder(orderId).status()).isEqualTo(expected));
  }

  private void shipOrderAsAdmin(String orderId) {
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .when()
        .post("/api/orders/" + orderId + "/ship")
        .then()
        .statusCode(200);
  }

  @Test
  void placeOrderReturns2xxAndProjectionEventuallyExposesIt() {
    String oid = "q-o-create-" + System.nanoTime();
    String cid = "q-o-cust-" + System.nanoTime();
    String pid = "q-o-prod-" + System.nanoTime();
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
    String cid = "q-o-list-cust-" + System.nanoTime();
    String oid1 = "q-o-list-1-" + System.nanoTime();
    String oid2 = "q-o-list-2-" + System.nanoTime();
    String pid1 = "q-prod-list-a-" + System.nanoTime();
    String pid2 = "q-prod-list-b-" + System.nanoTime();
    stockProduct(pid1, 5);
    stockProduct(pid2, 5);
    placeOrder(oid1, cid, pid1);
    placeOrder(oid2, cid, pid2);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              List<String> ids =
                  given()
                      .when()
                      .get("/api/orders")
                      .then()
                      .statusCode(200)
                      .extract()
                      .jsonPath()
                      .getList("orderId", String.class);
              assertThat(ids).contains(oid1, oid2);
            });
  }

  @Test
  void getMissingOrderReturns404() {
    given().when().get("/api/orders/does-not-exist-" + System.nanoTime()).then().statusCode(404);
  }

  @Test
  void placeOrderWithInvalidPayloadReturns400() {
    // Blank orderId: the command bus rejects the blank aggregate id with an
    // IllegalArgumentException, mapped to 400 by IllegalArgumentExceptionMapper.
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "test-user")
        .contentType(ContentType.JSON)
        .body(
            """
            {"orderId": "", "customerId": "", "lines": []}""")
        .when()
        .post("/api/orders")
        .then()
        .statusCode(400);
  }

  @Test
  void placeOrderWithAControlCharacterInTheOrderIdReturns400() {
    // An otherwise valid order whose orderId carries a control character (a JSON
    // \n) is refused by AggregateId.of in the command bus's id extractor, before any interceptor
    // runs or anything is written; mapped to 400 by IllegalArgumentExceptionMapper, and the message
    // names the rule without echoing the id.
    String body =
        given()
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", "test-user")
            .contentType(ContentType.JSON)
            .body(
                """
                {"orderId": "o-ctl\\nINFO forged-SECRET", "customerId": "c-ctl", "lines": [{"productId": "p-ctl", "quantity": 1, "unitPrice": 10}]}""")
            .when()
            .post("/api/orders")
            .then()
            .statusCode(400)
            .extract()
            .asString();
    assertThat(body)
        .contains("aggregateId must not contain control characters")
        .doesNotContain("SECRET");
  }

  @Test
  void confirmOrderWithAnOrderIdLongerThan255CharactersReturns400() {
    // An order id longer than the 255-character stream-id columns is refused by AggregateId.of
    // before any interceptor runs, so a command on it answers 400 instead of failing the
    // event-store append with an SQL error (a 500). The order id comes from the path here, which no
    // PlaceOrder constructor sees.
    String orderId = "q-o-long-SECRET-" + "x".repeat(256 - 16);
    String body =
        given()
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", "test-user")
            .contentType(ContentType.JSON)
            .when()
            .post("/api/orders/" + orderId + "/confirm")
            .then()
            .statusCode(400)
            .extract()
            .asString();
    assertThat(body)
        .contains("aggregateId must be at most 255 characters, got 256")
        .doesNotContain("SECRET");
  }

  @Test
  void placeOrderWithAnOrderIdTooLongForTheSagaIdReturns400AndWritesNothing() {
    // An order id of 244 to 255 characters is a valid order stream id, but the saga that
    // OrderPlaced starts is identified by "fulfillment-" + orderId, which a SagaId refuses above
    // 255 characters. Accepted at PlaceOrder, the order would be placed and its saga could never
    // start. The OrderCommand.PlaceOrder the controller builds refuses it first: 400 with the
    // rule, without echoing the id, and nothing is written to the order's stream.
    for (int length : new int[] {244, 255}) {
      String orderId = "q-o-saga-SECRET-" + "x".repeat(length - 16);
      String body =
          given()
              .header("X-User-Role", "CUSTOMER")
              .header("X-User-Id", "test-user")
              .contentType(ContentType.JSON)
              .body(
                  """
                  {"orderId": "%s", "customerId": "c-saga-long", "lines": [{"productId": "p-saga-long", "quantity": 1, "unitPrice": 10}]}"""
                      .formatted(orderId))
              .when()
              .post("/api/orders")
              .then()
              .statusCode(400)
              .extract()
              .asString();
      assertThat(body)
          .contains("orderId must be at most 243 characters, got " + length)
          .doesNotContain("SECRET");

      assertThat(
              eventStore.readStream(
                  StreamId.of(OrderState.TYPE, AggregateId.of(orderId)),
                  Version.initial(),
                  Integer.MAX_VALUE))
          .isEmpty();
    }
  }

  @Test
  void placeOrderWithAnOrderIdAtTheLengthBoundIsDrivenBySagaToConfirmed() {
    // The longest order id PlaceOrder accepts, 243 characters, is the longest whose saga id
    // "fulfillment-" + orderId fits 255: the saga starts, takes the payment, reserves the stock
    // and confirms the order.
    String oid = "q-o-bound-" + System.nanoTime();
    oid = oid + "x".repeat(243 - oid.length());
    String cid = "q-o-bound-cust-" + System.nanoTime();
    String pid = "q-prod-bound-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
  }

  @Test
  void placeOrderWithAControlCharacterInAProductIdReturns400AndWritesNothing() {
    // The request DTO carries no validation; the OrderCommand.OrderLine the controller builds
    // refuses a productId with a control character. The saga would address the inventory
    // stream by that productId, which the inventory extractor refuses, but only after the payment
    // was captured. Mapped to 400 by IllegalArgumentExceptionMapper without echoing the id, and
    // nothing
    // is written to the order's stream, so no saga starts and no payment is taken.
    String orderId = "q-o-pctl-" + System.nanoTime();
    String body =
        given()
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", "test-user")
            .contentType(ContentType.JSON)
            .body(
                """
                {"orderId": "%s", "customerId": "c-pctl", "lines": [{"productId": "p-ok", "quantity": 1, "unitPrice": 10}, {"productId": "p-ctl\\nINFO forged-SECRET", "quantity": 1, "unitPrice": 10}]}"""
                    .formatted(orderId))
            .when()
            .post("/api/orders")
            .then()
            .statusCode(400)
            .extract()
            .asString();
    assertThat(body)
        .contains("productId must not contain control characters")
        .doesNotContain("SECRET");

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
    String oid = "q-o-confirm-" + System.nanoTime();
    String cid = "q-o-conf-cust-" + System.nanoTime();
    String pid = "q-prod-conf-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
  }

  @Test
  void cancelOrderReturns2xx() {
    String oid = "q-o-cancel-" + System.nanoTime();
    String cid = "q-o-cancel-cust-" + System.nanoTime();
    String pid = "q-prod-cancel-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);
    // Let the saga settle the order at CONFIRMED first, then cancel (CancelOrder is legal from any
    // non-DELIVERED state) — avoids racing the saga's in-flight ConfirmOrder on the same stream.
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);

    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(ContentType.JSON)
        .body(
            """
            {"reason": "test cancellation"}""")
        .when()
        .post("/api/orders/" + oid + "/cancel")
        .then()
        .statusCode(200);
  }

  @Test
  void shipOrderRequiresAdminRole() {
    String oid = "q-o-ship-" + System.nanoTime();
    String cid = "q-o-ship-cust-" + System.nanoTime();
    String pid = "q-prod-ship-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    // The saga drives the order to CONFIRMED (the precondition ShipOrder requires).
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    shipOrderAsAdmin(oid);
  }

  /**
   * A refused privileged command is audited: the framework orders the audit interceptor before the
   * authorization interceptor, so the refusal reaches its {@code onError()} and leaves a {@code
   * FAILURE} row that names the caller and the missing role, as on the Spring and Micronaut apps.
   */
  @Test
  void shipOrderWithoutAdminRoleReturns400AndIsAuditedAsAFailure() throws SQLException {
    String oid = "q-o-ship-noadmin-" + System.nanoTime();
    String cid = "q-o-ship-noadmin-cust-" + System.nanoTime();
    String pid = "q-prod-ship-noadmin-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    // CUSTOMER lacks the ADMIN role ShipOrder requires: AuthorizationException -> 400 (a
    // DomainException subclass mapped by DomainExceptionMapper). The order is CONFIRMED, so a 400
    // here proves the authz gate (not the state machine) rejected the command.
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(ContentType.JSON)
        .when()
        .post("/api/orders/" + oid + "/ship")
        .then()
        .statusCode(400);

    List<AuditRow> rows = auditRows(oid, "ShipOrder");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).outcome()).isEqualTo("FAILURE");
    assertThat(rows.get(0).userId()).isEqualTo(cid);
    assertThat(rows.get(0).errorMessage()).contains("Required role: ADMIN");
  }

  @Test
  void deliverOrderRequiresAdminRole() {
    String oid = "q-o-deliver-" + System.nanoTime();
    String cid = "q-o-deliver-cust-" + System.nanoTime();
    String pid = "q-prod-deliver-" + System.nanoTime();
    stockProduct(pid, 5);
    placeOrder(oid, cid, pid);

    // Full sequence: CREATED -> CONFIRMED (saga) -> SHIPPED -> DELIVERED.
    awaitOrderStatus(oid, OrderStatus.CONFIRMED);
    shipOrderAsAdmin(oid);
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .when()
        .post("/api/orders/" + oid + "/deliver")
        .then()
        .statusCode(200);
  }
}
