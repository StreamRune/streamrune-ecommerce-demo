package org.streamrune.ecommerce.notifications;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class IntegrationEventListenerIT {

  @Container static final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3-management");

  @DynamicPropertySource
  static void rabbitProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.rabbitmq.host", rabbit::getHost);
    registry.add("spring.rabbitmq.port", rabbit::getAmqpPort);
    registry.add("spring.rabbitmq.username", rabbit::getAdminUsername);
    registry.add("spring.rabbitmq.password", rabbit::getAdminPassword);
  }

  @Autowired RabbitTemplate rabbitTemplate;
  @Autowired IntegrationEventListener listener;

  @BeforeEach
  void clearState() throws Exception {
    // Reset the listener's internal state between tests via reflection
    var receivedField = IntegrationEventListener.class.getDeclaredField("received");
    receivedField.setAccessible(true);
    var seenField = IntegrationEventListener.class.getDeclaredField("seenEntryIds");
    seenField.setAccessible(true);
    synchronized (listener) {
      ((java.util.Deque<?>) receivedField.get(listener)).clear();
      ((java.util.Set<?>) seenField.get(listener)).clear();
    }
  }

  private void publishEvent(String entryId, String type, String body) {
    MessageProperties props = new MessageProperties();
    props.setType(type);
    props.setMessageId(entryId);
    props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
    Message message = new Message(body.getBytes(java.nio.charset.StandardCharsets.UTF_8), props);
    rabbitTemplate.send("streamrune.events", "integration.event", message);
  }

  @Test
  void consumesPublishedIntegrationEvent() {
    publishEvent("entry-1", "OrderConfirmedIntegrationEvent", "{\"orderId\":\"o-1\"}");

    await().atMost(10, SECONDS).until(() -> !listener.recent().isEmpty());

    var items = listener.recent();
    assertThat(items).hasSize(1);
    var received = items.get(0);
    assertThat(received.type()).isEqualTo("OrderConfirmedIntegrationEvent");
    assertThat(received.body()).contains("o-1");
    assertThat(received.entryId()).isEqualTo("entry-1");
    assertThat(received.receivedAt()).isNotNull();
  }

  @Test
  void dedupesByEntryId() {
    // Publish the same entryId twice — should appear only once
    publishEvent("entry-dup", "OrderConfirmedIntegrationEvent", "{\"orderId\":\"o-dup\"}");
    publishEvent("entry-dup", "OrderConfirmedIntegrationEvent", "{\"orderId\":\"o-dup\"}");

    // Also publish a different entryId — should be added separately
    publishEvent("entry-other", "OrderShippedIntegrationEvent", "{\"orderId\":\"o-other\"}");

    await().atMost(10, SECONDS).until(() -> listener.recent().size() >= 2);

    var items = listener.recent();
    // entry-dup counted exactly once, entry-other also present = 2 total
    assertThat(items).hasSize(2);
    assertThat(items)
        .extracting(IntegrationEventListener.Received::entryId)
        .containsExactlyInAnyOrder("entry-dup", "entry-other");
  }
}
