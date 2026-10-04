package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.quarkus.config.RabbitMqProducer;

/**
 * End-to-end proof of the transactional outbox path in the Quarkus demo app, mirroring the Spring
 * app's {@code IntegrationOutboxE2EIT}:
 *
 * <ol>
 *   <li>Placing and confirming an order writes an {@code outbox_events} row in the SAME transaction
 *       as the domain events.
 *   <li>The OutboxPoller picks up the entry and delivers it to RabbitMQ.
 *   <li>The received payload contains NO PII (no customer name, email, address, phone, or
 *       customerId).
 *   <li>No customer events are ever published — CustomerEvent.* is excluded from the mapper
 *       allowlist.
 * </ol>
 *
 * <p>A RabbitMQ container is started by {@link RabbitMqTestResource} and its coordinates are
 * injected into the Quarkus config, which causes {@link
 * org.streamrune.ecommerce.quarkus.config.RabbitMqProducer} to open a channel and create an {@link
 * org.streamrune.core.outbox.OutboxPublisher} bean, which the framework's OutboxPoller picks up and
 * starts automatically.
 *
 * <p>A dedicated test AMQP consumer subscribes to {@code streamrune.integration} on each test run
 * to collect messages and assert on the full pipeline.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
// restrictToAnnotatedClass: a bare @QuarkusTestResource is GLOBAL in Quarkus — it would start a
// RabbitMQ container for every @QuarkusTest boot in this module (contradicting
// RabbitMqTestResource's
// javadoc and making unrelated tests depend on RabbitMQ starting). Restricting it to this class
// keeps
// the broker scoped to the one test that needs it.
@QuarkusTestResource(value = RabbitMqTestResource.class, restrictToAnnotatedClass = true)
@TestProfile(IntegrationOutboxE2EIT.OutboxEnabledProfile.class)
class IntegrationOutboxE2EIT {

  /**
   * Forces {@code streamrune.outbox.enabled=true} for this test's dedicated Quarkus context so the
   * framework's OutboxPoller is produced and started. {@code %test.streamrune.outbox.enabled=false}
   * (application.properties) disables the outbox for every other test; {@link RabbitMqTestResource}
   * also returns this key, but a test-resource config override is not reliably re-applied when
   * QuarkusTest restarts the app for this class (it runs after other @QuarkusTest classes in the
   * same JVM), whereas a {@link QuarkusTestProfile} override is. Without it the poller silently
   * does not start and no integration event ever reaches RabbitMQ.
   */
  public static class OutboxEnabledProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("streamrune.outbox.enabled", "true");
    }
  }

  @Inject DataSource dataSource;

  @ConfigProperty(name = "rabbitmq.host")
  String rabbitHost;

  @ConfigProperty(name = "rabbitmq.port")
  int rabbitPort;

  @ConfigProperty(name = "rabbitmq.username")
  String rabbitUsername;

  @ConfigProperty(name = "rabbitmq.password")
  String rabbitPassword;

  /** Captured AMQP message from the {@code streamrune.integration} queue. */
  record ReceivedMessage(String payloadType, String body) {}

  private final List<ReceivedMessage> receivedMessages = new CopyOnWriteArrayList<>();

  private Connection testConnection;
  private Channel testChannel;

  @BeforeEach
  void subscribeToIntegrationQueue() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbitHost);
    factory.setPort(rabbitPort);
    factory.setUsername(rabbitUsername);
    factory.setPassword(rabbitPassword);

    testConnection = factory.newConnection();
    testChannel = testConnection.createChannel();

    // Ensure the integration topology exists before consuming. RabbitMqProducer declares the same
    // exchange/queue/binding when the OutboxPoller starts, but under heavy parallel Testcontainers
    // load (the full build spins Postgres containers for three apps plus this RabbitMQ) that
    // startup
    // can lag this @BeforeEach, so a bare basicConsume races it and fails with 404 NOT_FOUND. These
    // declares are idempotent and use identical parameters, so they coexist with the app's own.
    testChannel.exchangeDeclare(RabbitMqProducer.EXCHANGE, "topic", /* durable= */ true);
    testChannel.queueDeclare(
        RabbitMqProducer.QUEUE,
        /* durable= */ true,
        /* exclusive= */ false,
        /* autoDelete= */ false,
        /* arguments= */ null);
    testChannel.queueBind(RabbitMqProducer.QUEUE, RabbitMqProducer.EXCHANGE, "integration.#");

    DeliverCallback deliverCallback =
        (consumerTag, delivery) -> {
          String type =
              delivery.getProperties() != null && delivery.getProperties().getType() != null
                  ? delivery.getProperties().getType()
                  : "unknown";
          String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
          receivedMessages.add(new ReceivedMessage(type, body));
        };
    testChannel.basicConsume(
        RabbitMqProducer.QUEUE, /* autoAck= */ true, deliverCallback, consumerTag -> {});
  }

  @AfterEach
  void closeTestConsumer() {
    try {
      if (testChannel != null) testChannel.close();
    } catch (Exception ignored) {
    }
    try {
      if (testConnection != null) testConnection.close();
    } catch (Exception ignored) {
    }
    receivedMessages.clear();
  }

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
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType("application/json")
        .body(body)
        .when()
        .post("/api/customers")
        .then()
        .statusCode(200);
  }

  private void placeOrder(String orderId, String customerId, String productId) {
    String body =
        """
        {
          "orderId": "%s",
          "customerId": "%s",
          "lines": [{"productId": "%s", "quantity": 1, "unitPrice": 9.99}]
        }"""
            .formatted(orderId, customerId, productId);
    given()
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", customerId)
        .contentType("application/json")
        .body(body)
        .when()
        .post("/api/orders")
        .then()
        .statusCode(200);
  }

  private void confirmOrder(String orderId) {
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "system")
        .contentType("application/json")
        .when()
        .post("/api/orders/" + orderId + "/confirm")
        .then()
        .statusCode(200);
  }

  private List<String> outboxPayloads(String payloadTypePrefix) throws Exception {
    try (var conn = dataSource.getConnection();
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

  private int outboxCount(String payloadTypePrefix) throws Exception {
    try (var conn = dataSource.getConnection();
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

    // (2) Place the order (OrderPlaced is excluded from the mapper — carries customerId).
    placeOrder(orderId, customerId, productId);

    // (3) Confirm the order directly. This emits OrderConfirmed, which IS in the allowlist.
    //     The outbox entry is written in the SAME transaction as the event_stream row.
    confirmOrder(orderId);

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

    // ---- (c) RabbitMQ consumer received the OrderConfirmed integration event -----------------
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              List<ReceivedMessage> orderMessages =
                  receivedMessages.stream()
                      .filter(m -> m.payloadType().startsWith("Order"))
                      .toList();
              assertThat(orderMessages)
                  .as("RabbitMQ must have received at least one Order integration event")
                  .isNotEmpty();
              // The body must contain the orderId (proves it's the right order)
              assertThat(orderMessages)
                  .extracting(ReceivedMessage::body)
                  .anySatisfy(body -> assertThat(body).contains(orderId));
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
        receivedMessages.stream().filter(m -> m.payloadType().startsWith("Customer")).toList();
    assertThat(customerMessages)
        .as("NO customer-typed messages must ever reach RabbitMQ")
        .isEmpty();
  }
}
