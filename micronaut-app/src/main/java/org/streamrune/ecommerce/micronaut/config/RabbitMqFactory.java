package org.streamrune.ecommerce.micronaut.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * Micronaut factory for the RabbitMQ-backed {@link OutboxPublisher}.
 *
 * <p>Opens a dedicated AMQP channel and declares the {@code streamrune.events} topic exchange, the
 * {@code streamrune.integration} queue, and the {@code integration.#} binding — matching the Spring
 * and Quarkus topology exactly so the notifications-service consumer works unchanged.
 *
 * <p>The publisher bean is gated on {@code streamrune.outbox.enabled=true} via {@link
 * io.micronaut.context.annotation.Requires}, so tests that do not start a RabbitMQ container (and
 * therefore do not set that property) will not create the AMQP connection and the framework's
 * {@link org.streamrune.micronaut.StreamRuneMicronautModule#outboxPoller} won't fire either (it
 * requires an {@link OutboxPublisher} bean to be present).
 */
@Factory
public class RabbitMqFactory {

  /** Integration event exchange. Matches the Spring/Quarkus topology constant. */
  public static final String EXCHANGE = "streamrune.events";

  /** Queue that the notifications-service (and any other consumer) subscribes to. */
  public static final String QUEUE = "streamrune.integration";

  /**
   * Routing key used when publishing outbox entries. The topic binding pattern {@code
   * integration.#} routes this key to {@link #QUEUE}.
   */
  public static final String ROUTING_KEY = "integration.event";

  @Value("${rabbitmq.host:localhost}")
  String host;

  @Value("${rabbitmq.port:5672}")
  int port;

  @Value("${rabbitmq.username:guest}")
  String username;

  @Value("${rabbitmq.password:guest}")
  String password;

  private Connection connection;
  private Channel channel;

  /**
   * Provides a raw AMQP {@link Channel} with the integration topology pre-declared.
   *
   * <p>Opening the connection + channel happens here so that other beans (the {@link
   * OutboxPublisher} and potentially a test consumer) can share the same channel object. The
   * channel is stored as a field so {@link #close()} can tear it down on context shutdown.
   *
   * @return a connected, topology-initialised channel
   * @throws IOException if the AMQP channel cannot be opened or topology declarations fail
   * @throws TimeoutException if connecting to the broker times out
   */
  @Singleton
  @Requires(property = "streamrune.outbox.enabled", value = "true")
  public Channel rabbitMqChannel() throws IOException, TimeoutException {
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

    return channel;
  }

  /**
   * Provides the {@link OutboxPublisher} bean backed by {@link RabbitMqOutboxPublisher}.
   *
   * <p>Gated on {@code streamrune.outbox.enabled=true} (same as {@link #rabbitMqChannel}) so this
   * bean is absent when the outbox is disabled. The framework's OutboxPoller factory requires an
   * {@link OutboxPublisher} bean; when absent the poller is not created and no polling happens.
   *
   * @param ch the AMQP channel with topology already declared
   * @return the publisher
   */
  @Singleton
  @Requires(property = "streamrune.outbox.enabled", value = "true")
  public OutboxPublisher outboxPublisher(Channel ch) throws IOException {
    return RabbitMqOutboxPublisher.builder()
        .channel(ch)
        .exchange(EXCHANGE)
        .routingKey(ROUTING_KEY)
        .confirmTimeout(Duration.ofSeconds(30))
        .build();
  }

  /** Closes the AMQP channel and connection on context shutdown. */
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
