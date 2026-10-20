package org.streamrune.ecommerce.spring.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;

/**
 * The publisher's connection attempt is bounded: the relay's deadline bounds nothing while the
 * publisher connects, so the factory's connection and handshake timeouts are what keep a broker
 * that drops the packets from holding the relay. {@code spring.rabbitmq.connection-timeout} wins
 * when it is set.
 */
class RabbitMqConfigTest {

  @Test
  void theConnectionAttemptIsBoundedToTwentySeconds() {
    try (LazyRabbitMqOutboxPublisher publisher =
        new RabbitMqConfig().outboxPublisher(new RabbitProperties())) {
      assertThat(publisher.connectBound()).isEqualTo(Duration.ofSeconds(20));
    }
  }

  @Test
  void theConfiguredConnectionTimeoutIsHonoured() {
    RabbitProperties rabbit = new RabbitProperties();
    rabbit.setConnectionTimeout(Duration.ofSeconds(3));

    try (LazyRabbitMqOutboxPublisher publisher = new RabbitMqConfig().outboxPublisher(rabbit)) {
      assertThat(publisher.connectBound()).isEqualTo(Duration.ofSeconds(13));
    }
  }
}
