package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.Container.ExecResult;

/**
 * The Spring app's outbox publisher keeps publisher confirms across a broker restart.
 *
 * <p>The broker application is stopped and started again inside the running container ({@code
 * rabbitmqctl stop_app} / {@code start_app}), so every client connection is closed by the broker
 * and the AMQP port stays the same. An outbox entry written while the broker is down must end up
 * {@code DELIVERED}, which the relay records only after the broker confirms the publish. A
 * publisher whose channel came back without confirm mode would still put the message on the queue,
 * but the confirm would never arrive: the entry would time out, stay claimed for the lease and be
 * published again, over and over, without ever reaching {@code DELIVERED}.
 */
class OutboxBrokerRestartIT extends AbstractIntegrationTest {

  @Autowired private DataSource dataSource;

  @Test
  void anEntryWrittenWhileTheBrokerIsDownIsConfirmedAndDeliveredAfterTheRestart() throws Exception {
    String orderId = "restart-ord-" + System.nanoTime();
    String entryId = "restart-entry-" + System.nanoTime();

    rabbitmqctl("stop_app");
    try {
      // Give the clients time to see the closed connections before the entry exists, so the
      // relay's first attempt meets a closed channel and nothing is handed to the broker.
      Thread.sleep(2_000);
      seedPendingOrderConfirmed(entryId, orderId);
      // Longer than the demo's 5-second outbox poll interval, so the relay attempts the entry
      // while the broker is down; a refused publish stays retryable.
      Thread.sleep(6_000);
      assertThat(statusOf(entryId))
          .as("an entry the broker never received is not delivered")
          .isNotEqualTo("DELIVERED");
    } finally {
      rabbitmqctl("start_app");
    }

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(statusOf(entryId))
                    .as("the broker confirmed the publish after the restart")
                    .isEqualTo("DELIVERED"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () ->
                assertThat(testRabbitListener.messages)
                    .extracting(ReceivedMessage::body)
                    .as("the consumer received the entry written while the broker was down")
                    .anySatisfy(body -> assertThat(body).contains(orderId)));
  }

  private static void rabbitmqctl(String command) throws Exception {
    ExecResult result = RABBIT.execInContainer("rabbitmqctl", command);
    assertThat(result.getExitCode())
        .as("rabbitmqctl %s: %s %s", command, result.getStdout(), result.getStderr())
        .isZero();
  }

  private void seedPendingOrderConfirmed(String entryId, String orderId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type,"
                    + " aggregate_id) VALUES (?, ?::jsonb, 'OrderConfirmed', 'order', ?)")) {
      ps.setString(1, entryId);
      ps.setString(2, "{\"orderId\":\"" + orderId + "\"}");
      ps.setString(3, orderId);
      ps.executeUpdate();
    }
  }

  private String statusOf(String entryId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }
}
