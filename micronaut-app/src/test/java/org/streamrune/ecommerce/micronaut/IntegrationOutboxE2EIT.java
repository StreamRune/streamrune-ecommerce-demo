package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.MountableFile;

/**
 * End-to-end proof of the transactional outbox path in the Micronaut demo app, mirroring the Spring
 * and Quarkus {@code IntegrationOutboxE2EIT}:
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
 * <p>This test manages its own containers (both PostgreSQL and RabbitMQ) independently of {@link
 * AbstractIntegrationTest}, which supplies only Postgres and disables the outbox. The test enables
 * {@code streamrune.outbox.enabled=true} and supplies rabbitmq coordinates so that {@link
 * org.streamrune.ecommerce.micronaut.config.RabbitMqFactory} opens a channel and creates an {@link
 * org.streamrune.core.outbox.OutboxPublisher} bean, which the framework's OutboxPoller then picks
 * up and starts automatically via {@link org.streamrune.micronaut.StreamRuneLifecycle}.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IntegrationOutboxE2EIT implements TestPropertyProvider {

  // ---------------------------------------------------------------------------
  // Containers — started once for this test class.
  // ---------------------------------------------------------------------------

  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("streamrune_ecommerce")
          .withUsername("postgres")
          .withPassword("postgres")
          .withCopyFileToContainer(
              MountableFile.forHostPath("../scripts/init-db.sql"),
              "/docker-entrypoint-initdb.d/init-db.sql");

  private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:3-management");

  static {
    POSTGRES.start();
    RABBIT.start();
  }

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    // Datasource
    props.put("datasources.default.url", POSTGRES.getJdbcUrl());
    props.put("datasources.default.username", POSTGRES.getUsername());
    props.put("datasources.default.password", POSTGRES.getPassword());
    props.put("datasources.default.driver-class-name", "org.postgresql.Driver");
    // Outbox + RabbitMQ
    props.put("streamrune.outbox.enabled", "true");
    props.put("rabbitmq.host", RABBIT.getHost());
    props.put("rabbitmq.port", String.valueOf(RABBIT.getAmqpPort()));
    props.put("rabbitmq.username", RABBIT.getAdminUsername());
    props.put("rabbitmq.password", RABBIT.getAdminPassword());
    return props;
  }

  // ---------------------------------------------------------------------------
  // Injected beans
  // ---------------------------------------------------------------------------

  @Inject
  @Client("/")
  HttpClient client;

  @Inject DataSource dataSource;

  // ---------------------------------------------------------------------------
  // Test AMQP consumer
  // ---------------------------------------------------------------------------

  /** Captured AMQP messages from the {@code streamrune.integration} queue. */
  record ReceivedMessage(String payloadType, String body) {}

  private final List<ReceivedMessage> receivedMessages = new CopyOnWriteArrayList<>();

  private Connection testConnection;
  private Channel testChannel;

  @BeforeEach
  void subscribeToIntegrationQueue() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(RABBIT.getHost());
    factory.setPort(RABBIT.getAmqpPort());
    factory.setUsername(RABBIT.getAdminUsername());
    factory.setPassword(RABBIT.getAdminPassword());

    testConnection = factory.newConnection();
    testChannel = testConnection.createChannel();

    // Queue is already declared by RabbitMqFactory at app startup; just consume from it.
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
        "streamrune.integration", /* autoAck= */ true, deliverCallback, consumerTag -> {});
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

  // ---------------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------------

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
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/customers", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "CUSTOMER")
                    .header("X-User-Id", customerId));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
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

  private void confirmOrder(String orderId) {
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/orders/" + orderId + "/confirm", null)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "system"));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
  }

  // ---------------------------------------------------------------------------
  // DB helpers
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Test
  // ---------------------------------------------------------------------------

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
