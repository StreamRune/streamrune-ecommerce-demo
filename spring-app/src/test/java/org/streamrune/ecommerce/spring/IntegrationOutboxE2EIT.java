package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * End-to-end proof of the transactional outbox path in the Spring demo app:
 *
 * <ol>
 *   <li>Placing an order of a stocked product, which the fulfillment saga confirms, writes an
 *       {@code outbox_events} row for the {@code OrderConfirmed} in the SAME transaction as the
 *       event.
 *   <li>The OutboxPoller picks up the entry and delivers it to RabbitMQ.
 *   <li>The received payload contains NO PII (no customer name, email, or customerId).
 *   <li>No customer events are ever published — CustomerEvent.* is excluded from the mapper
 *       allowlist.
 * </ol>
 *
 * <p>Postgres and RabbitMQ containers are inherited from {@link AbstractIntegrationTest}. A
 * test-only {@link TestRabbitListener} collects messages from the {@code streamrune.integration}
 * queue via {@code @RabbitListener}, letting the test assert on the messages received by the full
 * pipeline (event → outbox entry → RabbitMQ).
 */
class IntegrationOutboxE2EIT extends AbstractIntegrationTest {

  // -----------------------------------------------------------------------
  // Injected dependencies (testRabbitListener inherited from AbstractIntegrationTest)
  // -----------------------------------------------------------------------

  @Autowired private DataSource dataSource;
  @Autowired private VirtualThreadCommandBus commandBus;

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private void registerCustomer(String customerId, String name, String email) {
    String body =
        """
        {
          "customerId": "%s",
          "name": "%s",
          "email": "%s",
          "address": "1 Outbox Lane",
          "phone": "555-9999"
        }"""
            .formatted(customerId, name, email);
    client
        .post()
        .uri("/api/customers")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  /** Creates the product in the catalogue, which prices the order's line. */
  private void createProduct(String productId) {
    commandBus.execute(
        new ProductCommand.CreateProduct(
            productId,
            "Outbox product",
            null,
            "General",
            new Money(new BigDecimal("9.99"), "USD"),
            0));
  }

  /** Stocks the product, so that the saga reserves the line and confirms the order. */
  private void stockProduct(String productId) {
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 5));
  }

  private void placeOrder(String orderId, String customerId, String productId) {
    String body =
        """
        {
          "orderId": "%s",
          "customerId": "%s",
          "lines": [{"productId": "%s", "quantity": 1}]
        }"""
            .formatted(orderId, customerId, productId);
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", customerId)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  /**
   * Returns all outbox_events rows whose payload_type matches the given prefix, as raw JSON
   * strings.
   */
  private List<String> outboxPayloads(String payloadTypePrefix) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT payload::text FROM outbox_events WHERE payload_type LIKE ?")) {
      ps.setString(1, payloadTypePrefix + "%");
      try (ResultSet rs = ps.executeQuery()) {
        List<String> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(rs.getString(1));
        }
        return rows;
      }
    }
  }

  /** Returns the count of outbox_events rows whose payload_type matches the given prefix. */
  private int outboxCount(String payloadTypePrefix) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT COUNT(*) FROM outbox_events WHERE payload_type LIKE ?")) {
      ps.setString(1, payloadTypePrefix + "%");
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt(1) : 0;
      }
    }
  }

  // -----------------------------------------------------------------------
  // Test
  // -----------------------------------------------------------------------

  @Test
  void outboxRoutesPiiSafeOrderConfirmedToRabbitMq() throws Exception {
    // Unique IDs to avoid cross-test contamination in the shared Testcontainers DB
    String customerId = "outbox-cust-" + System.nanoTime();
    String orderId = "outbox-ord-" + System.nanoTime();
    String productId = "outbox-prod-" + System.nanoTime();

    // Known PII — the assertions below prove none of it leaks into the outbox/broker
    String customerName = "Alice PIINeedle";
    String customerEmail = "alice.needle@example.com";

    // (1) Register the customer so the customer stream exists and the order can reference it.
    //     CustomerRegistered is NOT in the outbox allowlist — this step must NOT produce an outbox
    //     row for Customer events.
    registerCustomer(customerId, customerName, customerEmail);

    // (2) Place the order of a stocked product (OrderPlaced is excluded from the mapper — carries
    //     customerId).
    createProduct(productId);
    stockProduct(productId);
    placeOrder(orderId, customerId, productId);

    // (3) The fulfillment saga takes the payment, reserves the stock and confirms the order. Its
    //     OrderConfirmed IS in the allowlist; the outbox entry is written in the SAME transaction
    //     as the event_stream row.

    // ---- (a) Outbox row exists for OrderConfirmed -------------------------------------------
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(outboxCount("Order"))
                    .as("at least one Order outbox_events row must exist after OrderConfirmed")
                    .isGreaterThanOrEqualTo(1));

    // ---- (b) Outbox payload contains NO PII --------------------------------------------------
    List<String> orderOutboxPayloads = outboxPayloads("Order");
    assertThat(orderOutboxPayloads)
        .as("order outbox payloads must be present (non-vacuity check)")
        .isNotEmpty();
    for (String payload : orderOutboxPayloads) {
      assertThat(payload)
          .as("outbox payload must not contain customer name (PII)")
          .doesNotContain(customerName);
      assertThat(payload)
          .as("outbox payload must not contain customer email (PII)")
          .doesNotContain(customerEmail);
      assertThat(payload)
          .as("outbox payload must not contain raw customerId (PII)")
          .doesNotContain(customerId);
      assertThat(payload)
          .as("outbox payload must not contain customer address (PII)")
          .doesNotContain("1 Outbox Lane");
      assertThat(payload)
          .as("outbox payload must not contain customer phone (PII)")
          .doesNotContain("555-9999");
    }

    // ---- (c) RabbitMQ listener received the OrderConfirmed integration event -----------------
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              List<ReceivedMessage> orderMessages =
                  testRabbitListener.messages.stream()
                      .filter(m -> m.payloadType().startsWith("Order"))
                      .toList();
              assertThat(orderMessages)
                  .as("RabbitMQ must have received at least one Order integration event")
                  .isNotEmpty();
              // The body must contain the orderId (proves it's the right order)
              assertThat(orderMessages)
                  .as("the saga's OrderConfirmed of this order reaches RabbitMQ")
                  .anySatisfy(
                      m -> {
                        assertThat(m.payloadType()).isEqualTo("OrderConfirmed");
                        assertThat(m.body()).contains(orderId);
                      });
              // The body must NOT contain PII
              for (ReceivedMessage msg : orderMessages) {
                assertThat(msg.body())
                    .as("RabbitMQ message body must not contain customer name (PII)")
                    .doesNotContain(customerName);
                assertThat(msg.body())
                    .as("RabbitMQ message body must not contain customer email (PII)")
                    .doesNotContain(customerEmail);
                assertThat(msg.body())
                    .as("RabbitMQ message body must not contain raw customerId (PII)")
                    .doesNotContain(customerId);
                assertThat(msg.body())
                    .as("RabbitMQ message body must not contain customer address (PII)")
                    .doesNotContain("1 Outbox Lane");
                assertThat(msg.body())
                    .as("RabbitMQ message body must not contain customer phone (PII)")
                    .doesNotContain("555-9999");
              }
            });

    // ---- (d) NO customer outbox rows or customer messages on RabbitMQ -----------------------
    assertThat(outboxCount("Customer"))
        .as("CustomerEvent.* must NOT produce any outbox_events rows")
        .isZero();

    List<ReceivedMessage> customerMessages =
        testRabbitListener.messages.stream()
            .filter(m -> m.payloadType().startsWith("Customer"))
            .toList();
    assertThat(customerMessages)
        .as("NO customer-typed messages must ever reach RabbitMQ")
        .isEmpty();
  }
}
