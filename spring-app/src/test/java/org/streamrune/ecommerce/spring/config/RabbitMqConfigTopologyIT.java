package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The outbox publisher's own connection declares the integration topology, so its first publish
 * works on a broker where nothing else has declared the exchange, the queue or the binding yet.
 *
 * <p>Spring AMQP declares the topology beans only when its own connection is first opened, and the
 * application may publish before anything opens it. A publish to a missing exchange makes the
 * broker close the publisher's channel, and a channel closed that way is never reopened. The test
 * runs the configuration's two bean methods directly against a fresh broker, without a Spring
 * context and therefore without Spring AMQP's declarations.
 */
@Testcontainers
class RabbitMqConfigTopologyIT {

  @Container static final RabbitMQContainer BROKER = new RabbitMQContainer("rabbitmq:3-management");

  @Test
  void theFirstPublishOnAFreshBrokerIsConfirmedAndRoutedToTheIntegrationQueue() throws Exception {
    RabbitProperties rabbit = new RabbitProperties();
    rabbit.setHost(BROKER.getHost());
    rabbit.setPort(BROKER.getAmqpPort());
    rabbit.setUsername(BROKER.getAdminUsername());
    rabbit.setPassword(BROKER.getAdminPassword());
    RabbitMqConfig config = new RabbitMqConfig();

    try (Connection publisherConnection = config.outboxRabbitConnection(rabbit)) {
      OutboxPublisher publisher = config.outboxPublisher(publisherConnection);
      // Returns only when the broker confirmed the message and routed it to a queue.
      publisher.publish(
          OutboxEntry.pending(
              OutboxEntryId.of("topology-entry-1"),
              "{\"orderId\":\"topology-ord-1\"}",
              "OrderConfirmed",
              StreamId.of(OrderState.TYPE, AggregateId.of("topology-ord-1"))));
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(BROKER.getHost());
    factory.setPort(BROKER.getAmqpPort());
    factory.setUsername(BROKER.getAdminUsername());
    factory.setPassword(BROKER.getAdminPassword());
    try (Connection consumerConnection = factory.newConnection();
        Channel channel = consumerConnection.createChannel()) {
      GetResponse message = channel.basicGet(RabbitMqConfig.QUEUE, true);
      assertThat(message).as("the published entry waits in the integration queue").isNotNull();
      assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).contains("topology-ord-1");
    }
  }
}
