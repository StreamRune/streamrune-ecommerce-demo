package org.streamrune.ecommerce.quarkus;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.time.Duration;
import java.util.Map;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * Starts a real RabbitMQ broker (Testcontainers) for {@code @QuarkusTest} runs that exercise the
 * transactional outbox → RabbitMQ path. Overrides the {@code rabbitmq.*} config keys that {@link
 * org.streamrune.ecommerce.quarkus.config.RabbitMqProducer} reads, and enables the outbox poller so
 * the {@link org.streamrune.quarkus.StreamRuneProducers#outboxPoller} producer creates and starts
 * the poller.
 *
 * <p>This resource is NOT included in any global test resource annotation — only the {@code
 * IntegrationOutboxE2EIT} opts into it via {@code @QuarkusTestResource}. Other tests (which do not
 * declare this resource) will not start a RabbitMQ container; their {@code
 * streamrune.outbox.enabled} remains {@code false} from the default, so {@link
 * org.streamrune.ecommerce.quarkus.config.RabbitMqProducer#outboxPublisher()} returns {@code null}
 * and no AMQP connection is attempted.
 */
public class RabbitMqTestResource implements QuarkusTestResourceLifecycleManager {

  private RabbitMQContainer rabbit;

  @Override
  public Map<String, String> start() {
    // Generous startup timeout: under the full build's parallel Testcontainers load (Postgres for
    // three apps plus this broker) RabbitMQ has been observed taking ~40s to log "Server startup
    // complete", close to the 60s default. 120s keeps container startup from flaking the build.
    rabbit =
        new RabbitMQContainer("rabbitmq:3-management").withStartupTimeout(Duration.ofSeconds(120));
    rabbit.start();
    return Map.of(
        "rabbitmq.host", rabbit.getHost(),
        "rabbitmq.port", String.valueOf(rabbit.getAmqpPort()),
        "rabbitmq.username", rabbit.getAdminUsername(),
        "rabbitmq.password", rabbit.getAdminPassword(),
        "streamrune.outbox.enabled", "true");
  }

  @Override
  public void stop() {
    if (rabbit != null) {
      rabbit.stop();
    }
  }
}
