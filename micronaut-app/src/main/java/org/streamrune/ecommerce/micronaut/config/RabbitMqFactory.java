package org.streamrune.ecommerce.micronaut.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;

/**
 * Micronaut factory for the RabbitMQ-backed {@link OutboxPublisher}.
 *
 * <p>The publisher is a {@link LazyRabbitMqOutboxPublisher}: it opens its own AMQP connection and
 * channel on its first publish, declares the {@code streamrune.events} topic exchange, the {@code
 * streamrune.integration} queue, and the {@code integration.#} binding — matching the Spring and
 * Quarkus topology exactly so the notifications-service consumer works unchanged — and then
 * publishes with broker confirms. Nothing is opened at startup, so the application starts and
 * serves commands while the broker is down; the relay retries the outbox entries until the broker
 * answers.
 *
 * <p>The publisher bean is gated on {@code streamrune.outbox.enabled=true} via {@link
 * io.micronaut.context.annotation.Requires}, so tests that do not start a RabbitMQ container (and
 * therefore do not set that property) get no publisher and the framework's {@link
 * org.streamrune.micronaut.StreamRuneMicronautModule#outboxPoller} won't fire either (it requires
 * an {@link OutboxPublisher} bean to be present).
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

  /** Client-provided name of the publisher's connection, shown in the RabbitMQ management UI. */
  public static final String OUTBOX_CONNECTION_NAME = "streamrune-outbox-publisher";

  @Value("${rabbitmq.host:localhost}")
  String host;

  @Value("${rabbitmq.port:5672}")
  int port;

  @Value("${rabbitmq.username:guest}")
  String username;

  @Value("${rabbitmq.password:guest}")
  String password;

  /**
   * Provides the {@link OutboxPublisher} bean backed by {@link LazyRabbitMqOutboxPublisher}.
   *
   * <p>The connection uses the amqp-client's automatic recovery (its default): after a broker
   * restart or a network failure the client reconnects and reopens the publisher's channel in
   * confirm mode with its listeners. A connection attempt is bounded to 10 s for the connect and 10
   * s for the handshake ({@link LazyRabbitMqOutboxPublisher#connectBound()}): the relay's deadline
   * bounds nothing while the publisher connects, and the client's own 60-second connection timeout
   * would let a broker whose packets are dropped hold the relay about 70 seconds per cycle. The
   * 30-second confirm timeout plus the publisher's default 30-second publish-block budget give a
   * 60-second in-flight horizon, below the 2-minute claim lease of {@code
   * StreamRuneFactory#outboxStore}. Closed on context shutdown, which closes the connection if one
   * was opened.
   *
   * @return the publisher, not yet connected
   */
  @Singleton
  @Bean(preDestroy = "close")
  @Requires(property = "streamrune.outbox.enabled", value = "true")
  public LazyRabbitMqOutboxPublisher outboxPublisher() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);
    // Bounded: the relay's deadline bounds nothing while the publisher connects.
    factory.setConnectionTimeout((int) LazyRabbitMqOutboxPublisher.CONNECTION_TIMEOUT.toMillis());
    factory.setHandshakeTimeout((int) LazyRabbitMqOutboxPublisher.HANDSHAKE_TIMEOUT.toMillis());
    factory.setAutomaticRecoveryEnabled(true);
    return new LazyRabbitMqOutboxPublisher(
        factory,
        OUTBOX_CONNECTION_NAME,
        RabbitMqFactory::declareTopology,
        EXCHANGE,
        ROUTING_KEY,
        Duration.ofSeconds(30));
  }

  /**
   * Declares the topology idempotently on the publisher's channel before its first publish, so the
   * app can start standalone (before ops pre-creates it). The client's topology recovery repeats
   * the declarations after a reconnect.
   */
  static void declareTopology(Channel channel) throws IOException {
    channel.exchangeDeclare(EXCHANGE, "topic", /* durable= */ true);
    channel.queueDeclare(
        QUEUE,
        /* durable= */ true,
        /* exclusive= */ false,
        /* autoDelete= */ false,
        /* arguments= */ null);
    channel.queueBind(QUEUE, EXCHANGE, "integration.#");
  }
}
