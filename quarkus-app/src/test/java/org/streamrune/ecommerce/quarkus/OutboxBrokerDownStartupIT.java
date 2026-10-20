package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.quarkus.config.RabbitMqProducer;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * The Quarkus app starts and serves commands while RabbitMQ is down, and its outbox drains once the
 * broker answers.
 *
 * <p>The profile gives the app the outbox and a broker port nothing listens on, so every connection
 * to it is refused; the app of this profile has its own PostgreSQL ({@link PostgresTestResource}).
 * The app must answer {@code /q/health} with {@code UP} and accept a command whose event the outbox
 * carries. The entry stays {@code PENDING}: the relay's attempt fails as a transport failure,
 * recorded in {@code last_error} and not counted in {@code attempts}, the relay stays {@code UP},
 * and {@code streamrune.outbox.pending} counts the entry. Then RabbitMQ starts on that very port,
 * and the entry is delivered and reaches the queue once. The store counts the delivery itself as
 * the entry's one attempt; a refused attempt counted against the entry would show above it.
 *
 * <p>The relay retries a transport failure on a backoff that grows towards 60 seconds while the
 * failures last, so the delivery after the broker starts waits for the entry's next attempt, which
 * the test allows for.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
@TestProfile(OutboxBrokerDownStartupIT.BrokerDownProfile.class)
class OutboxBrokerDownStartupIT {

  /** The outbox on, its relay polling every 500 ms, and a broker port nothing listens on. */
  public static class BrokerDownProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "streamrune.outbox.enabled", "true",
          "streamrune.outbox.flush-interval-ms", "500",
          "rabbitmq.host", "localhost",
          "rabbitmq.port", String.valueOf(freePort()));
    }
  }

  @ConfigProperty(name = "rabbitmq.port")
  int brokerPort;

  @Inject DataSource dataSource;

  @Inject MeterRegistry meters;

  private RabbitMQContainer rabbit;

  @AfterEach
  void stopBroker() {
    if (rabbit != null) {
      rabbit.stop();
    }
  }

  @Test
  void theAppServesWithoutTheBrokerAndDeliversTheOutboxOnceTheBrokerAnswers() throws Exception {
    rabbit = new RabbitOnPort(brokerPort);

    given().when().get("/q/health").then().statusCode(200);

    String productId = "q-broker-down-" + System.nanoTime();
    given()
        .header("X-User-Id", "admin-1")
        .header("X-User-Role", "ADMIN")
        .contentType(ContentType.JSON)
        .body(
            """
            {"productId": "%s", "name": "Lamp", "description": "while the broker is down",
             "category": "Home", "price": 20.00, "initialStock": 3}"""
                .formatted(productId))
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);

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

    JsonPath health = given().when().get("/q/health").then().statusCode(200).extract().jsonPath();
    assertThat(health.getString("status")).isEqualTo("UP");
    Map<String, Object> streamRune = streamRuneCheck(health);
    assertThat(streamRune).containsEntry("status", "UP");
    assertThat(streamRune.get("data"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("relay.outbox-relay.status", "UP")
        .containsEntry("relay.outbox-relay.alive", true)
        .containsEntry("relay.outbox-relay.consecutiveFailures", 0);
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

  @SuppressWarnings("unchecked")
  private static Map<String, Object> streamRuneCheck(JsonPath health) {
    List<Map<String, Object>> checks = health.getList("checks");
    return checks.stream()
        .filter(check -> "streamrune".equals(check.get("name")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no streamrune check in " + health.prettify()));
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
      while ((message = channel.basicGet(RabbitMqProducer.QUEUE, true)) != null) {
        Map<String, Object> headers = message.getProps().getHeaders();
        if (messageId.equals(message.getProps().getMessageId())
            && messageId.equals(String.valueOf(headers.get("X-Outbox-Entry-Id")))) {
          matching++;
        }
      }
    }
    return matching;
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
