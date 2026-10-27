package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.streamrune.ecommerce.spring.config.RabbitMqConfig;
import org.streamrune.spring.StreamRuneHealthIndicator;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * The Spring app starts and serves commands while RabbitMQ is down, and its outbox drains once the
 * broker answers.
 *
 * <p>The app is given a broker port nothing listens on, so every connection to it is refused. It
 * runs in a Spring context of its own, built with {@link SpringApplicationBuilder} on a database of
 * its own ({@code broker_down}), so the cached test context, whose relay publishes to the shared
 * broker, cannot deliver this app's entries; its listener containers do not start, so the test's
 * own read of the queue is the only consumer. The app must start, answer {@code /actuator/health}
 * with {@code UP} and accept a command whose event the outbox carries. The entry stays {@code
 * PENDING}: the relay's attempt fails as a transport failure, recorded in {@code last_error} and
 * not counted in {@code attempts}, the relay stays alive and {@code UP}, and {@code
 * streamrune.outbox.pending} counts the entry. Then RabbitMQ starts on that very port, and the
 * entry is delivered and reaches the queue once. The store counts the delivery itself as the
 * entry's one attempt; a refused attempt counted against the entry would show above it.
 *
 * <p>The relay retries a transport failure on a backoff that grows towards 60 seconds while the
 * failures last, so the delivery after the broker starts waits for the entry's next attempt, which
 * the test allows for.
 *
 * <p>The app starts without the dead-letter retry runner's warning that its command bus answers a
 * keyed command that fails again under its key by another policy or inbox window than the runner
 * replays by ({@link #RUNNER_AND_BUS_DIFFER}): the bus the app builds takes the runner's.
 */
@ExtendWith(OutputCaptureExtension.class)
class OutboxBrokerDownStartupIT {

  /** How the framework's warning that the retry runner and its command bus differ begins. */
  private static final String RUNNER_AND_BUS_DIFFER =
      "DeadLetterRetryRunner replays by maxRetries=";

  private static final String DATABASE = "broker_down";

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private ConfigurableApplicationContext app;

  private RabbitMQContainer rabbit;

  @AfterEach
  void stop() {
    if (app != null) {
      app.close();
    }
    if (rabbit != null) {
      rabbit.stop();
    }
  }

  @Test
  void theAppServesWithoutTheBrokerAndDeliversTheOutboxOnceTheBrokerAnswers(CapturedOutput output)
      throws Exception {
    createDatabase();
    int brokerPort = freePort();
    rabbit = new RabbitOnPort(brokerPort);

    app = start(brokerPort);
    assertThat(output.getAll())
        .as("the app's log from its start")
        .contains("Tomcat started on port")
        .as("the retry runner and the command bus answer a keyed re-send by the same rules")
        .doesNotContain(RUNNER_AND_BUS_DIFFER);

    HttpResponse<String> health = get("/actuator/health");
    assertThat(health.statusCode()).as("health: %s", health.body()).isEqualTo(200);
    assertThat(health.body()).contains("\"status\":\"UP\"");

    String productId = "broker-down-" + System.nanoTime();
    HttpResponse<String> created =
        send(
            "POST",
            "/api/products",
            """
            {"productId": "%s", "name": "Lamp", "description": "while the broker is down",
             "category": "Home", "price": 20.00, "initialStock": 3}"""
                .formatted(productId));
    assertThat(created.statusCode()).as("create: %s", created.body()).isEqualTo(200);

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

    Health relay = app.getBean(StreamRuneHealthIndicator.class).health();
    assertThat(relay.getStatus()).isEqualTo(Status.UP);
    assertThat(relay.getDetails().get("relay.outbox-relay"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("status", "UP")
        .containsEntry("started", true)
        .containsEntry("alive", true)
        .containsEntry("consecutiveFailures", 0);
    MeterRegistry meters = app.getBean(MeterRegistry.class);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              var pending = meters.find("streamrune.outbox.pending").gauge();
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

  /** Starts the app with the broker at {@code localhost:brokerPort}. */
  private ConfigurableApplicationContext start(int brokerPort) {
    var postgres = AbstractIntegrationTest.POSTGRES;
    return new SpringApplicationBuilder(SpringEcommerceApplication.class)
        .run(
            "--server.port=0",
            "--spring.datasource.url=" + databaseUrl(),
            "--spring.datasource.username=" + postgres.getUsername(),
            "--spring.datasource.password=" + postgres.getPassword(),
            "--spring.rabbitmq.host=localhost",
            "--spring.rabbitmq.port=" + brokerPort,
            "--spring.rabbitmq.username=" + rabbit.getAdminUsername(),
            "--spring.rabbitmq.password=" + rabbit.getAdminPassword(),
            "--spring.rabbitmq.listener.simple.auto-startup=false",
            "--streamrune.outbox.flush-interval-ms=500",
            "--streamrune.projections.auto-discovery.enabled=false",
            "--streamrune.polling-interval-ms=500",
            "--streamrune.sse.keep-alive-interval=1h");
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> send(String method, String path, String json) throws Exception {
    return http.send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .header("X-User-Id", "admin-1")
            .header("X-User-Role", "ADMIN")
            .method(method, HttpRequest.BodyPublishers.ofString(json))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private URI uri(String path) {
    return URI.create(
        "http://localhost:" + app.getEnvironment().getRequiredProperty("local.server.port") + path);
  }

  private record OutboxRow(String entryId, String status, int attempts, String lastError) {}

  /**
   * The outbox entry of the product's {@code ProductCreated}, or {@code null} while there is none.
   */
  private static OutboxRow outboxRow(String productId) throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    try (var conn =
            DriverManager.getConnection(
                databaseUrl(), postgres.getUsername(), postgres.getPassword());
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
      while ((message = channel.basicGet(RabbitMqConfig.QUEUE, true)) != null) {
        Map<String, Object> headers = message.getProps().getHeaders();
        if (messageId.equals(message.getProps().getMessageId())
            && messageId.equals(String.valueOf(headers.get("X-Outbox-Entry-Id")))) {
          matching++;
        }
      }
    }
    return matching;
  }

  private static void createDatabase() throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    try (var conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + DATABASE + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + DATABASE);
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

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
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
