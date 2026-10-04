package org.streamrune.ecommerce.quarkus.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * Quarkus CDI producer for the RabbitMQ-backed {@link OutboxPublisher}.
 *
 * <p>Opens a dedicated AMQP channel and declares the {@code streamrune.events} topic exchange, the
 * {@code streamrune.integration} queue, and the {@code integration.#} binding that routes all
 * {@code integration.*} routing keys into that queue — matching the Spring app's topology exactly
 * so the notifications-service consumer works unchanged.
 *
 * <p>The producer is gated on {@code streamrune.outbox.enabled=true}. When the outbox is disabled
 * (e.g. in tests that don't supply a RabbitMQ container) the producer returns {@code null} and no
 * AMQP connection is opened. CDI allows {@code null} returns from dependent-scoped producers; the
 * framework's {@link org.streamrune.quarkus.StreamRuneProducers#outboxPoller} guard already skips
 * the poller when no {@link OutboxPublisher} bean resolves.
 */
@ApplicationScoped
public class RabbitMqProducer {

  /** Integration event exchange. Matches the Spring RabbitMqConfig.EXCHANGE constant. */
  public static final String EXCHANGE = "streamrune.events";

  /** Queue that the notifications-service (and any other consumer) subscribes to. */
  public static final String QUEUE = "streamrune.integration";

  /**
   * Routing key used when publishing outbox entries. The topic binding pattern {@code
   * integration.#} routes this key to {@link #QUEUE}. Matches Spring RabbitMqConfig.ROUTING_KEY.
   */
  public static final String ROUTING_KEY = "integration.event";

  @ConfigProperty(name = "streamrune.outbox.enabled", defaultValue = "false")
  boolean outboxEnabled;

  @ConfigProperty(name = "rabbitmq.host", defaultValue = "localhost")
  String host;

  @ConfigProperty(name = "rabbitmq.port", defaultValue = "5672")
  int port;

  @ConfigProperty(name = "rabbitmq.username", defaultValue = "guest")
  String username;

  @ConfigProperty(name = "rabbitmq.password", defaultValue = "guest")
  String password;

  private Connection connection;
  private Channel channel;

  /**
   * Produces the {@link OutboxPublisher} bean backed by {@link RabbitMqOutboxPublisher}.
   *
   * <p>Dependent-scoped so CDI permits returning {@code null} when the outbox is disabled.
   * Consumers that use {@code Instance<OutboxPublisher>} (e.g. the framework's {@code outboxPoller}
   * producer) check {@code isResolvable()} before calling {@code get()}.
   *
   * @return publisher, or {@code null} when {@code streamrune.outbox.enabled=false}
   */
  @Produces
  public OutboxPublisher outboxPublisher() throws IOException, TimeoutException {
    if (!outboxEnabled) {
      return null;
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);

    connection = factory.newConnection();
    channel = connection.createChannel();

    // Declare topology idempotently so the app can start standalone (before ops pre-creates it).
    channel.exchangeDeclare(EXCHANGE, "topic", /* durable= */ true);
    channel.queueDeclare(
        QUEUE,
        /* durable= */ true,
        /* exclusive= */ false,
        /* autoDelete= */ false,
        /* arguments= */ null);
    channel.queueBind(QUEUE, EXCHANGE, "integration.#");

    return RabbitMqOutboxPublisher.builder()
        .channel(channel)
        .exchange(EXCHANGE)
        .routingKey(ROUTING_KEY)
        .confirmTimeout(Duration.ofSeconds(30))
        .build();
  }

  @PreDestroy
  void close() {
    if (channel != null) {
      try {
        channel.close();
      } catch (Exception ignored) {
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception ignored) {
      }
    }
  }
}
