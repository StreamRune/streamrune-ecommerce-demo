package org.streamrune.ecommerce.quarkus.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.io.IOException;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;

/**
 * Quarkus CDI producer for the RabbitMQ-backed {@link OutboxPublisher}.
 *
 * <p>The publisher is a {@link LazyRabbitMqOutboxPublisher}: it opens its own AMQP connection and
 * channel on its first publish, declares the {@code streamrune.events} topic exchange, the {@code
 * streamrune.integration} queue, and the {@code integration.#} binding that routes all {@code
 * integration.*} routing keys into that queue — matching the Spring app's topology exactly so the
 * notifications-service consumer works unchanged — and then publishes with broker confirms. Nothing
 * is opened at startup, so the application starts and serves commands while the broker is down; the
 * relay retries the outbox entries until the broker answers.
 *
 * <p>The producer is gated on {@code streamrune.outbox.enabled=true}. When the outbox is disabled
 * (e.g. in tests that don't supply a RabbitMQ container) the producer returns {@code null}. CDI
 * allows {@code null} returns from dependent-scoped producers; the framework's {@link
 * org.streamrune.quarkus.StreamRuneProducers#outboxPoller} guard already skips the poller when no
 * {@link OutboxPublisher} bean resolves.
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

  /** Client-provided name of the publisher's connection, shown in the RabbitMQ management UI. */
  public static final String OUTBOX_CONNECTION_NAME = "streamrune-outbox-publisher";

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

  private LazyRabbitMqOutboxPublisher publisher;

  /**
   * Produces the {@link OutboxPublisher} bean backed by {@link LazyRabbitMqOutboxPublisher}.
   *
   * <p>Dependent-scoped so CDI permits returning {@code null} when the outbox is disabled.
   * Consumers that use {@code Instance<OutboxPublisher>} (e.g. the framework's {@code outboxPoller}
   * producer) check {@code isResolvable()} before calling {@code get()}. Every injection point gets
   * the same publisher, which {@link #close()} closes on shutdown.
   *
   * <p>The connection uses the amqp-client's automatic recovery (its default): after a broker
   * restart or a network failure the client reconnects and reopens the publisher's channel in
   * confirm mode with its listeners. A connection attempt is bounded to 10 s for the connect and 10
   * s for the handshake ({@link LazyRabbitMqOutboxPublisher#connectBound()}): the relay's deadline
   * bounds nothing while the publisher connects, and the client's own 60-second connection timeout
   * would let a broker whose packets are dropped hold the relay about 70 seconds per cycle. The
   * 30-second confirm timeout plus the publisher's default 30-second publish-block budget give a
   * 60-second in-flight horizon, below the 2-minute claim lease of {@code
   * StreamRuneProducer#outboxStore}.
   *
   * @return publisher, or {@code null} when {@code streamrune.outbox.enabled=false}
   */
  @Produces
  public synchronized OutboxPublisher outboxPublisher() {
    if (!outboxEnabled) {
      return null;
    }
    if (publisher == null) {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(host);
      factory.setPort(port);
      factory.setUsername(username);
      factory.setPassword(password);
      // Bounded: the relay's deadline bounds nothing while the publisher connects.
      factory.setConnectionTimeout((int) LazyRabbitMqOutboxPublisher.CONNECTION_TIMEOUT.toMillis());
      factory.setHandshakeTimeout((int) LazyRabbitMqOutboxPublisher.HANDSHAKE_TIMEOUT.toMillis());
      factory.setAutomaticRecoveryEnabled(true);
      publisher =
          new LazyRabbitMqOutboxPublisher(
              factory,
              OUTBOX_CONNECTION_NAME,
              RabbitMqProducer::declareTopology,
              EXCHANGE,
              ROUTING_KEY,
              Duration.ofSeconds(30));
    }
    return publisher;
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

  @PreDestroy
  synchronized void close() {
    if (publisher != null) {
      publisher.close();
    }
  }
}
