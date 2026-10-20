package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.streamrune.ecommerce.micronaut.config.RabbitMqFactory;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.BackgroundRelayHealthContributor.ComponentHealth;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * The Micronaut app starts and serves commands while RabbitMQ is down, and its outbox drains once
 * the broker answers.
 *
 * <p>The application context of this class has the outbox on and a broker port nothing listens on,
 * so every connection to it is refused, and a database of its own ({@code broker_down}) on the
 * suite's PostgreSQL, so no entry another class wrote is waiting in its outbox. The app must answer
 * {@code /health} with {@code UP} and accept a command whose event the outbox carries. The entry
 * stays {@code PENDING}: the relay's attempt fails as a transport failure, recorded in {@code
 * last_error} and not counted in {@code attempts}, the relay stays alive and {@code UP}, and {@code
 * streamrune.outbox.pending} counts the entry. Then RabbitMQ starts on that very port, and the
 * entry is delivered and reaches the queue once. The store counts the delivery itself as the
 * entry's one attempt; a refused attempt counted against the entry would show above it.
 *
 * <p>The relay retries a transport failure on a backoff that grows towards 60 seconds while the
 * failures last, so the delivery after the broker starts waits for the entry's next attempt, which
 * the test allows for.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OutboxBrokerDownStartupIT implements TestPropertyProvider {

  private static final String DATABASE = "broker_down";

  private final int brokerPort = freePort();

  private final RabbitMQContainer rabbit = new RabbitOnPort(brokerPort);

  @Override
  public Map<String, String> getProperties() {
    createDatabase();
    var postgres = AbstractIntegrationTest.POSTGRES;
    Map<String, String> props = new HashMap<>();
    props.put("datasources.default.url", databaseUrl());
    props.put("datasources.default.username", postgres.getUsername());
    props.put("datasources.default.password", postgres.getPassword());
    props.put("datasources.default.driver-class-name", "org.postgresql.Driver");
    props.put("streamrune.outbox.enabled", "true");
    props.put("streamrune.outbox.flush-interval-ms", "500");
    props.put("rabbitmq.host", "localhost");
    props.put("rabbitmq.port", String.valueOf(brokerPort));
    props.put("rabbitmq.username", rabbit.getAdminUsername());
    props.put("rabbitmq.password", rabbit.getAdminPassword());
    return props;
  }

  @Inject
  @Client("/")
  HttpClient client;

  @Inject DataSource dataSource;

  @Inject BackgroundRelayHealthContributor relayHealth;

  @Inject MeterRegistry meters;

  @AfterAll
  void stopBroker() {
    rabbit.stop();
  }

  @Test
  void theAppServesWithoutTheBrokerAndDeliversTheOutboxOnceTheBrokerAnswers() throws Exception {
    HttpResponse<String> health =
        client.toBlocking().exchange(HttpRequest.GET("/health"), String.class);
    assertThat(health.getStatus().getCode()).isEqualTo(200);
    assertThat(health.body()).contains("\"status\":\"UP\"");

    String productId = "m-broker-down-" + System.nanoTime();
    HttpResponse<String> created =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST(
                        "/api/products",
                        """
                        {"productId": "%s", "name": "Lamp", "description": "while the broker is down",
                         "category": "Home", "price": 20.00, "initialStock": 3}"""
                            .formatted(productId))
                    .contentType(MediaType.APPLICATION_JSON_TYPE)
                    .header("X-User-Id", "admin-1")
                    .header("X-User-Role", "ADMIN"),
                String.class);
    assertThat(created.getStatus().getCode()).isEqualTo(200);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              OutboxRow row = outboxRow(productId);
              assertThat(row).as("the product's outbox entry").isNotNull();
              assertThat(row.status()).isEqualTo("PENDING");
              assertThat(row.lastError())
                  .as("the relay tried and the broker was not there")
                  .contains("RabbitMQ is not reachable at localhost:" + brokerPort)
                  .contains("Connection refused");
              assertThat(row.attempts()).as("a transport failure is not counted").isZero();
            });

    ComponentHealth relay =
        relayHealth.components().stream()
            .filter(component -> component.name().equals("outbox-relay"))
            .findFirst()
            .orElseThrow();
    assertThat(relay.status()).isEqualTo(BackgroundRelayHealthContributor.Status.UP);
    assertThat(relay.started()).isTrue();
    assertThat(relay.alive()).isTrue();
    assertThat(relay.consecutiveFailures()).isZero();
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              Gauge pending = meters.find("streamrune.outbox.pending").gauge();
              assertThat(pending).as("the backlog gauge").isNotNull();
              assertThat(pending.value())
                  .as("the backlog gauge counts the waiting entry")
                  .isGreaterThanOrEqualTo(1);
            });

    rabbit.start();

    await()
        .atMost(Duration.ofSeconds(120))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> assertThat(outboxRow(productId).status()).isEqualTo("DELIVERED"));
    OutboxRow delivered = outboxRow(productId);
    assertThat(delivered.attempts())
        .as("the delivery is the one attempt counted; none of the refused ones is")
        .isEqualTo(1);

    assertThat(messagesWithId(delivered.entryId()))
        .as("the broker received the entry once")
        .isEqualTo(1);
  }

  private record OutboxRow(String entryId, String status, int attempts, String lastError) {}

  /**
   * The outbox entry of the product's {@code ProductCreated}, or {@code null} while there is none.
   */
  private OutboxRow outboxRow(String productId) throws Exception {
    try (var conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT entry_id, status, attempts, last_error FROM outbox_events"
                    + " WHERE aggregate_type = 'product' AND aggregate_id = ?"
                    + " AND payload_type = 'ProductCreated'")) {
      ps.setString(1, productId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next()
            ? new OutboxRow(
                rs.getString("entry_id"),
                rs.getString("status"),
                rs.getInt("attempts"),
                rs.getString("last_error"))
            : null;
      }
    }
  }

  /** Takes every message off the integration queue and counts those with this message id. */
  private int messagesWithId(String messageId) throws Exception {
    var factory = new ConnectionFactory();
    factory.setHost(rabbit.getHost());
    factory.setPort(rabbit.getAmqpPort());
    factory.setUsername(rabbit.getAdminUsername());
    factory.setPassword(rabbit.getAdminPassword());
    int matching = 0;
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {
      GetResponse message;
      while ((message = channel.basicGet(RabbitMqFactory.QUEUE, true)) != null) {
        Map<String, Object> headers = message.getProps().getHeaders();
        if (messageId.equals(message.getProps().getMessageId())
            && messageId.equals(String.valueOf(headers.get("X-Outbox-Entry-Id")))) {
          matching++;
        }
      }
    }
    return matching;
  }

  private static void createDatabase() {
    var postgres = AbstractIntegrationTest.POSTGRES;
    try (var conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + DATABASE + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + DATABASE);
    } catch (java.sql.SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String databaseUrl() {
    var postgres = AbstractIntegrationTest.POSTGRES;
    return "jdbc:postgresql://"
        + postgres.getHost()
        + ":"
        + postgres.getMappedPort(5432)
        + "/"
        + DATABASE;
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** RabbitMQ on a host port chosen before it starts: the port the app was given. */
  private static final class RabbitOnPort extends RabbitMQContainer {

    RabbitOnPort(int hostPort) {
      super("rabbitmq:3-management");
      addFixedExposedPort(hostPort, 5672);
      withStartupTimeout(Duration.ofSeconds(120));
    }
  }
}
