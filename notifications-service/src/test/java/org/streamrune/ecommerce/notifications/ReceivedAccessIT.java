package org.streamrune.ecommerce.notifications;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Who reads what the notifications service received, over real HTTP. {@code GET /received} shows
 * the body of every integration event, and a body names an order and may hold a customer's cancel
 * reason: it answers an {@code ADMIN}, the role together with an identity, and nobody else. {@code
 * GET /health} says that the service runs, to everyone, and says nothing more.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ReceivedAccessIT {

  @Container static final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3-management");

  @DynamicPropertySource
  static void rabbitProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.rabbitmq.host", rabbit::getHost);
    registry.add("spring.rabbitmq.port", rabbit::getAmqpPort);
    registry.add("spring.rabbitmq.username", rabbit::getAdminUsername);
    registry.add("spring.rabbitmq.password", rabbit::getAdminPassword);
  }

  @LocalServerPort int port;
  @Autowired RabbitTemplate rabbitTemplate;
  @Autowired IntegrationEventListener listener;

  private final HttpClient http = HttpClient.newHttpClient();

  @Test
  void whatWasReceivedIsReadByAnAdminAndByNobodyElse() throws Exception {
    publish(
        "entry-access-1",
        "OrderCancelled",
        "{\"orderId\": \"order-access-1\", \"reason\": \"changed my mind\"}");
    await().atMost(10, SECONDS).until(() -> !listener.recent().isEmpty());

    String[][] refused = {
      {null, null},
      {null, "ADMIN"},
      {"admin-1", null},
      {"   ", "ADMIN"},
      {"cust-alice", "CUSTOMER"},
      {"admin-1", "admin"},
      {"admin-1", "ADMINISTRATOR"}
    };
    for (String[] caller : refused) {
      HttpResponse<String> answer = get("/received", caller[0], caller[1]);
      assertThat(answer.statusCode())
          .as("GET /received as id=%s role=%s", caller[0], caller[1])
          .isEqualTo(403);
      assertThat(answer.body())
          .as("the refusal sent to id=%s role=%s", caller[0], caller[1])
          .doesNotContain("order-access-1", "changed my mind", "entry-access-1");
    }

    HttpResponse<String> admin = get("/received", "admin-1", "ADMIN");
    assertThat(admin.statusCode()).isEqualTo(200);
    assertThat(admin.body()).contains("order-access-1", "changed my mind", "entry-access-1");
  }

  @Test
  void theHealthEndpointAnswersEveryoneAndCarriesNoBody() throws Exception {
    publish("entry-access-2", "OrderConfirmed", "{\"orderId\": \"order-access-2\"}");
    await()
        .atMost(10, SECONDS)
        .until(
            () -> listener.recent().stream().anyMatch(r -> r.entryId().equals("entry-access-2")));

    HttpResponse<String> anonymous = get("/health", null, null);
    assertThat(anonymous.statusCode()).isEqualTo(200);
    assertThat(anonymous.body()).isEmpty();

    HttpResponse<String> admin = get("/health", "admin-1", "ADMIN");
    assertThat(admin.statusCode()).isEqualTo(200);
    assertThat(admin.body()).isEmpty();
  }

  private HttpResponse<String> get(String path, String userId, String role) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET();
    if (userId != null) {
      request.header("X-User-Id", userId);
    }
    if (role != null) {
      request.header("X-User-Role", role);
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private void publish(String entryId, String type, String body) {
    MessageProperties properties = new MessageProperties();
    properties.setType(type);
    properties.setMessageId(entryId);
    properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
    Message message = new Message(body.getBytes(StandardCharsets.UTF_8), properties);
    rabbitTemplate.send("streamrune.events", "integration.event", message);
  }
}
