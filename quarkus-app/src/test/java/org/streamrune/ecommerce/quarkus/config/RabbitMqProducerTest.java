package org.streamrune.ecommerce.quarkus.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher;

/**
 * The publisher's connection attempt is bounded: the relay's deadline bounds nothing while the
 * publisher connects, so the factory's connection and handshake timeouts are what keep a broker
 * that drops the packets from holding the relay.
 */
class RabbitMqProducerTest {

  @Test
  void theConnectionAttemptIsBoundedToTwentySeconds() {
    RabbitMqProducer producer = new RabbitMqProducer();
    producer.outboxEnabled = true;
    producer.host = "localhost";
    producer.port = 5672;
    producer.username = "guest";
    producer.password = "guest";

    var publisher = (LazyRabbitMqOutboxPublisher) producer.outboxPublisher();
    try {
      assertThat(publisher.connectBound()).isEqualTo(Duration.ofSeconds(20));
    } finally {
      producer.close();
    }
  }
}
