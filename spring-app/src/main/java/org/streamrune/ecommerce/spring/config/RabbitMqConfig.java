package org.streamrune.ecommerce.spring.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * RabbitMQ topology and outbox publisher for the e-commerce demo.
 *
 * <p>Declares the {@code streamrune.events} topic exchange, the {@code streamrune.integration}
 * queue, and a binding with routing key {@code integration.#} so any routing key prefixed with
 * {@code integration.} (or exactly {@code integration}) lands in the queue. The notifications
 * service consumer subscribes to {@code streamrune.integration}. Spring AMQP declares these three
 * beans when its own connection is first opened; the outbox publisher declares the same topology on
 * its own channel ({@link #declareTopology}), because its first publish may come before that.
 *
 * <p>The {@link OutboxPublisher} bean is consumed by the auto-configured {@link
 * org.streamrune.runtime.OutboxPoller} once {@code streamrune.outbox.enabled=true} is set in
 * application.yml. It publishes on its own AMQP connection, not on Spring AMQP's (see {@link
 * #outboxPublisher}), and opens that connection on its first publish: the application starts and
 * serves commands while the broker is down.
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
   * The outbox publisher: a {@link LazyRabbitMqOutboxPublisher} that opens its own AMQP connection,
   * with the plain RabbitMQ Java client and automatic recovery switched on, on its first publish,
   * and then publishes through a {@link RabbitMqOutboxPublisher} on a channel only it uses.
   *
   * <p>Nothing is opened at startup, so the application starts and serves commands while the broker
   * is down. A command's integration events wait in the outbox: each publish attempt fails with a
   * transport failure, which the relay retries on a growing backoff without counting it against the
   * entry, and the entries are delivered once the broker answers. The relay's {@code WARN} line and
   * {@code streamrune.outbox.pending} show the outage, and the publisher logs {@code Outbox
   * publisher connected to RabbitMQ at <host>:<port>} when it connects.
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
   * relay tries the entry again later. A publish already sent when the connection drops gets no
   * confirm: it waits out the confirm timeout, stays claimed until the lease expires and is then
   * published again, so the consumer may receive it twice (it deduplicates by entry id).
   *
   * <p>Host, port, credentials and virtual host come from the same {@code spring.rabbitmq.*}
   * properties Spring AMQP uses. TLS settings under {@code spring.rabbitmq.ssl} are not applied
   * here; a TLS deployment configures the factory itself.
   *
   * <p>The 30-second confirm timeout, combined with the publisher's default 30-second {@code
   * publishBlockBudget}, gives an in-flight horizon (see {@link
   * RabbitMqOutboxPublisher#inFlightHorizon()}) of 60 seconds; {@link StreamRuneConfig#outboxStore}
   * sets the outbox claim lease to 2 minutes so it strictly exceeds that horizon plus the
   * framework's recommended margin. A broker timeout therefore makes the poller retry rather than
   * lose the message, and a lease reclaim can never race a still-in-flight publish of the same
   * entry.
   *
   * <p>Spring closes the publisher, and with it the connection, on shutdown, after the outbox
   * poller that uses it has stopped. Gated on {@code streamrune.outbox.enabled=true}, which the
   * relay needs.
   */
  @Bean(destroyMethod = "close")
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public LazyRabbitMqOutboxPublisher outboxPublisher(RabbitProperties rabbit) {
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
    return new LazyRabbitMqOutboxPublisher(
        factory,
        OUTBOX_CONNECTION_NAME,
        RabbitMqConfig::declareTopology,
        EXCHANGE,
        ROUTING_KEY,
        Duration.ofSeconds(30));
  }

  /**
   * Declares the exchange, the queue and the binding on the publisher's channel, with the same
   * arguments as the beans above, before its first publish. Spring AMQP declares those beans
   * lazily, when its own connection is first opened, and nothing in this application opens it
   * before the outbox's first publish. A publish to an exchange that does not exist makes the
   * broker close the channel, and a channel closed that way is not reopened by automatic recovery:
   * every later publish would fail. The declarations are idempotent, and the client's topology
   * recovery repeats them after a reconnect, so a broker that comes back without its definitions
   * gets them again. The Quarkus and Micronaut apps declare the topology the same way.
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
