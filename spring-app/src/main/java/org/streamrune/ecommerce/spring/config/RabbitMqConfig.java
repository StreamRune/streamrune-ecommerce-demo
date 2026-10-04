package org.streamrune.ecommerce.spring.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * RabbitMQ topology and outbox publisher for the e-commerce demo.
 *
 * <p>Declares the {@code streamrune.events} topic exchange, the {@code streamrune.integration}
 * queue, and a binding with routing key {@code integration.#} so any routing key prefixed with
 * {@code integration.} (or exactly {@code integration}) lands in the queue. The notifications
 * service consumer subscribes to {@code streamrune.integration}. Spring AMQP declares these three
 * beans when its own connection is first opened; {@link #outboxPublisher} declares the same
 * topology on the publisher's channel, because the first publish may come before that.
 *
 * <p>The {@link OutboxPublisher} bean is consumed by the auto-configured {@link
 * org.streamrune.runtime.OutboxPoller} once {@code streamrune.outbox.enabled=true} is set in
 * application.yml. It publishes on its own AMQP connection, not on Spring AMQP's (see {@link
 * #outboxRabbitConnection}).
 */
@Configuration(proxyBeanMethods = false)
public class RabbitMqConfig {

  /** Integration event exchange. The outbox publisher publishes here. */
  public static final String EXCHANGE = "streamrune.events";

  /** Queue that notifications-service (and any other consumer) subscribes to. */
  public static final String QUEUE = "streamrune.integration";

  /**
   * Routing key used when publishing outbox entries. The topic binding pattern {@code
   * integration.#} routes this key to the {@link #integrationQueue()}.
   */
  public static final String ROUTING_KEY = "integration.event";

  /** Client-provided name of the publisher's connection, shown in the RabbitMQ management UI. */
  public static final String OUTBOX_CONNECTION_NAME = "streamrune-outbox-publisher";

  @Bean
  public TopicExchange integrationExchange() {
    // durable=true, auto-delete=false
    return new TopicExchange(EXCHANGE, true, false);
  }

  @Bean
  public Queue integrationQueue() {
    // durable=true so messages survive broker restarts
    return new Queue(QUEUE, true);
  }

  @Bean
  public Binding integrationBinding(TopicExchange integrationExchange, Queue integrationQueue) {
    // "integration.#" matches "integration" and "integration.<anything>"
    return BindingBuilder.bind(integrationQueue).to(integrationExchange).with("integration.#");
  }

  /**
   * The outbox publisher's own AMQP connection, opened with the plain RabbitMQ Java client and
   * automatic recovery switched on.
   *
   * <p>The publisher is deliberately not given a channel from Spring AMQP's {@code
   * ConnectionFactory}. Spring AMQP turns the client's automatic recovery off and, after the
   * connection drops, hands out a fresh channel behind the same proxy, without confirm mode and
   * without the publisher's listeners. Publishes on that channel still reach the queue, but no
   * confirm ever comes back: every entry times out, stays claimed for the lease and is published
   * again, over and over, until the application restarts.
   *
   * <p>With automatic recovery the client reconnects by itself after a broker restart or a network
   * failure (every 5 seconds by default) and reopens the same channel object with confirm mode and
   * the confirm and return listeners restored, so the publisher keeps working without a restart.
   * While the connection is down, a publish fails before anything is handed to the broker and the
   * relay tries the entry again on its next poll. A publish already sent when the connection drops
   * gets no confirm: it waits out the confirm timeout, stays claimed until the lease expires and is
   * then published again, so the consumer may receive it twice (it deduplicates by entry id).
   *
   * <p>Host, port, credentials and virtual host come from the same {@code spring.rabbitmq.*}
   * properties Spring AMQP uses. TLS settings under {@code spring.rabbitmq.ssl} are not applied
   * here; a TLS deployment configures the factory itself.
   *
   * <p>Spring closes the connection on shutdown, after the outbox poller that uses it has stopped.
   * Gated on {@code streamrune.outbox.enabled=true}: the connection is opened eagerly at startup,
   * so it must not be created when no broker is available (e.g., in tests that disable the outbox).
   */
  @Bean(destroyMethod = "close")
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public Connection outboxRabbitConnection(RabbitProperties rabbit)
      throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rabbit.determineHost());
    factory.setPort(rabbit.determinePort());
    factory.setUsername(rabbit.determineUsername());
    factory.setPassword(rabbit.determinePassword());
    String virtualHost = rabbit.determineVirtualHost();
    if (virtualHost != null) {
      factory.setVirtualHost(virtualHost);
    }
    // The client's default, written out because this is the whole point of a separate connection.
    factory.setAutomaticRecoveryEnabled(true);
    return factory.newConnection(OUTBOX_CONNECTION_NAME);
  }

  /**
   * {@link OutboxPublisher} backed by {@link RabbitMqOutboxPublisher}.
   *
   * <p>Opens a channel on {@link #outboxRabbitConnection} that only this publisher uses (see the
   * Javadoc on the publisher class); {@link RabbitMqOutboxPublisher.Builder#build()} puts it into
   * confirm mode.
   *
   * <p>The channel declares the exchange, the queue and the binding before the publisher is built,
   * with the same arguments as the beans above. Spring AMQP declares those beans lazily, when its
   * own connection is first opened, and nothing in this application opens it before the outbox's
   * first publish. A publish to an exchange that does not exist makes the broker close the channel,
   * and a channel closed that way is not reopened by automatic recovery: every later publish would
   * fail. The declarations are idempotent, and the client's topology recovery repeats them after a
   * reconnect, so a broker that comes back without its definitions gets them again. The Quarkus and
   * Micronaut apps declare the topology the same way.
   *
   * <p>The 30-second confirm timeout, combined with the publisher's default 30-second {@code
   * publishBlockBudget}, gives an in-flight horizon (see {@link
   * RabbitMqOutboxPublisher#inFlightHorizon()}) of 60 seconds; {@link StreamRuneConfig#outboxStore}
   * sets the outbox claim lease to 2 minutes so it strictly exceeds that horizon plus the
   * framework's recommended margin. A broker timeout therefore makes the poller retry rather than
   * lose the message, and a lease reclaim can never race a still-in-flight publish of the same
   * entry.
   */
  @Bean
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public OutboxPublisher outboxPublisher(Connection outboxRabbitConnection) throws IOException {
    Channel channel = outboxRabbitConnection.createChannel();
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
}
