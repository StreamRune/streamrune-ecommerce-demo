package org.streamrune.ecommerce.micronaut.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;

/**
 * The publisher's connection attempt is bounded: the relay's deadline bounds nothing while the
 * publisher connects, so the factory's connection and handshake timeouts are what keep a broker
 * that drops the packets from holding the relay.
 */
class RabbitMqFactoryTest {

  @Test
  void theConnectionAttemptIsBoundedToTwentySeconds() {
    RabbitMqFactory factory = new RabbitMqFactory();
    factory.host = "localhost";
    factory.port = 5672;
    factory.username = "guest";
    factory.password = "guest";

    try (LazyRabbitMqOutboxPublisher publisher = factory.outboxPublisher()) {
      assertThat(publisher.connectBound()).isEqualTo(Duration.ofSeconds(20));
    }
  }
}
