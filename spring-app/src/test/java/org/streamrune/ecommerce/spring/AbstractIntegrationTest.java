package org.streamrune.ecommerce.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * Shared base class for HTTP integration tests. Spins up real PostgreSQL and RabbitMQ containers
 * once for all subclasses (singleton container pattern) and exposes a {@link WebTestClient} bound
 * to the running Spring application's random port.
 *
 * <p>Because the container URLs are the same for every subclass, Spring's test context cache serves
 * a single application context to all test classes — avoiding the overhead of starting N separate
 * Spring Boot instances.
 *
 * <p>The database starts empty. The application creates the schema itself, as it does outside the
 * tests: {@code streamrune.event-store.schema.auto-initialize=true} in {@code application.yml} has
 * the event store factory apply the framework's event-store and crypto migration series when the
 * context starts.
 *
 * <p>RabbitMQ is included so {@link
 * org.streamrune.ecommerce.spring.config.RabbitMqConfig#outboxRabbitConnection} can open the outbox
 * publisher's AMQP connection at context startup. Tests that do not exercise the outbox are
 * unaffected — the poller runs but finds no outbox entries for the events those tests emit (e.g.
 * {@code CustomerEvent.*} is excluded from the outbox mapper's allowlist).
 *
 * <p>A shared {@link TestRabbitListener} is registered in the Spring context via {@link
 * RabbitListenerConfig} so that {@link IntegrationOutboxE2EIT} does not need its own
 * {@code @Import}, which would produce a different context key and cause Spring Boot 4.0's
 * context-restart path to invoke {@code OutboxPoller.start()} a second time.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(AbstractIntegrationTest.RabbitListenerConfig.class)
public abstract class AbstractIntegrationTest {

  // ---------------------------------------------------------------------------
  // Shared RabbitMQ test listener — always in the context so all subclasses
  // share the same Spring context key (required for Spring Boot 4.0 context cache)
  // ---------------------------------------------------------------------------

  /** Captured AMQP message from the {@code streamrune.integration} queue. */
  public record ReceivedMessage(String payloadType, String body) {}

  /**
   * Singleton listener bean that receives every message published to the {@code
   * streamrune.integration} queue. Tests that do not exercise the outbox will see an empty list;
   * {@link IntegrationOutboxE2EIT} asserts on the accumulated messages.
   */
  public static class TestRabbitListener {
    public final List<ReceivedMessage> messages = new CopyOnWriteArrayList<>();

    @RabbitListener(queues = "streamrune.integration")
    public void onMessage(Message message) {
      String type =
          message.getMessageProperties() != null
              ? message.getMessageProperties().getType()
              : "unknown";
      String body = new String(message.getBody(), StandardCharsets.UTF_8);
      messages.add(new ReceivedMessage(type, body));
    }
  }

  /** Registers the shared {@link TestRabbitListener} bean into every test context. */
  @TestConfiguration
  public static class RabbitListenerConfig {
    @Bean
    public TestRabbitListener testRabbitListener() {
      return new TestRabbitListener();
    }
  }

  /**
   * Singleton PostgreSQL container started once for the entire test suite. {@code withReuse(true)}
   * tells Testcontainers not to stop the container between test classes, keeping the Spring context
   * key (datasource URL) stable so Spring can cache and reuse a single application context.
   */
  static final PostgreSQLContainer<?> POSTGRES;

  /**
   * Singleton RabbitMQ container. Required because {@code RabbitMqConfig.outboxRabbitConnection}
   * opens an AMQP connection eagerly at startup; if no broker is available the context fails to
   * load. Shared across all subclasses so the Spring context key remains stable (the broker URL is
   * part of the key).
   */
  static final RabbitMQContainer RABBIT;

  static {
    POSTGRES =
        new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("streamrune_ecommerce")
            .withUsername("postgres")
            .withPassword("postgres")
            .withReuse(true);
    POSTGRES.start();

    RABBIT = new RabbitMQContainer("rabbitmq:3-management");
    RABBIT.start();
  }

  @DynamicPropertySource
  static void registerProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("streamrune.projections.auto-discovery.enabled", () -> "false");
    // RabbitMQ broker for the outbox publisher (opened eagerly at startup by RabbitMqConfig).
    registry.add("spring.rabbitmq.host", RABBIT::getHost);
    registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
    registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
    registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
  }

  @LocalServerPort protected int port;

  @Autowired protected ObjectMapper objectMapper;

  /** Receives messages from the {@code streamrune.integration} RabbitMQ queue. */
  @Autowired protected TestRabbitListener testRabbitListener;

  protected WebTestClient client;

  @BeforeEach
  void buildClient() {
    client =
        WebTestClient.bindToServer()
            .baseUrl("http://localhost:" + port)
            .responseTimeout(Duration.ofSeconds(30))
            .build();
  }
}
