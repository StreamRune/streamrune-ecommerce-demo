package org.streamrune.ecommerce.commands.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher.BatchResult;
import org.streamrune.core.outbox.OutboxPublisher.FailureKind;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.commands.integration.LazyRabbitMqOutboxPublisher.BrokerUnreachableException;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * The publisher without a broker: a port nothing listens on refuses every connection. That a
 * reachable broker gets the entries is shown by each app's broker-down startup test, which starts
 * RabbitMQ on the port the app was given.
 */
class LazyRabbitMqOutboxPublisherTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("product"), AggregateId.of("p-1"));

  private final AtomicInteger declarations = new AtomicInteger();

  private final LazyRabbitMqOutboxPublisher publisher =
      new LazyRabbitMqOutboxPublisher(
          refusingFactory(),
          "outbox-test",
          channel -> declarations.incrementAndGet(),
          "streamrune.events",
          "integration.event",
          Duration.ofSeconds(30));

  @AfterEach
  void closePublisher() {
    publisher.close();
    Thread.interrupted();
  }

  @Test
  void creatingThePublisherOpensNothing() {
    assertThat(publisher.isConnected()).isFalse();
    assertThat(declarations).hasValue(0);
  }

  @Test
  void aBrokerThatCannotBeReachedFailsEveryEntryAsATransportFailure() {
    List<OutboxEntry> batch = List.of(entry("e-1"), entry("e-2"));

    BatchResult result = publisher.publishBatch(batch, Instant.now().plusSeconds(30));

    assertThat(result.confirmed()).isEmpty();
    assertThat(result.failed()).containsOnlyKeys(OutboxEntryId.of("e-1"), OutboxEntryId.of("e-2"));
    for (Exception failure : result.failed().values()) {
      assertThat(failure)
          .isInstanceOf(BrokerUnreachableException.class)
          .hasMessageContaining("RabbitMQ is not reachable at localhost:")
          .hasMessageContaining("nothing was published")
          .hasCauseInstanceOf(ConnectException.class);
      assertThat(publisher.classifyFailure(failure)).isEqualTo(FailureKind.TRANSPORT);
    }
    assertThat(publisher.isConnected()).isFalse();
    assertThat(declarations).as("no channel, so no topology").hasValue(0);
  }

  @Test
  void theSingleEntryPathThrowsTheSameTransportFailure() {
    assertThatThrownBy(() -> publisher.publish(entry("e-1")))
        .isInstanceOf(BrokerUnreachableException.class)
        .satisfies(
            e ->
                assertThat(publisher.classifyFailure((Exception) e))
                    .isEqualTo(FailureKind.TRANSPORT));
  }

  @Test
  void aFailureTheBrokerConnectionDidNotCauseIsATransportFailureBeforeTheFirstConnection() {
    assertThat(publisher.classifyFailure(new IOException("anything")))
        .isEqualTo(FailureKind.TRANSPORT);
  }

  @Test
  void theHorizonIsTheRabbitMqPublishersOneBeforeAnyConnection() {
    assertThat(publisher.inFlightHorizon())
        .isEqualTo(RabbitMqOutboxPublisher.DEFAULT_PUBLISH_BLOCK_BUDGET.plusSeconds(30));
  }

  @Test
  void aStoppingRelayAttemptsNothing() {
    Thread.currentThread().interrupt();

    BatchResult result = publisher.publishBatch(List.of(entry("e-1")), null);

    assertThat(Thread.currentThread().isInterrupted()).as("the interrupt is kept").isTrue();
    assertThat(result.confirmed()).isEmpty();
    assertThat(result.failed()).as("an entry never attempted is in neither bucket").isEmpty();
  }

  @Test
  void aBatchPastItsDeadlineAttemptsNothing() {
    BatchResult result =
        publisher.publishBatch(List.of(entry("e-1")), Instant.now().minusMillis(1));

    assertThat(result.confirmed()).isEmpty();
    assertThat(result.failed()).isEmpty();
  }

  @Test
  void aClosedPublisherDoesNotConnect() {
    publisher.close();

    BatchResult result = publisher.publishBatch(List.of(entry("e-1")), null);

    assertThat(result.failed().get(OutboxEntryId.of("e-1")))
        .isInstanceOf(BrokerUnreachableException.class)
        .hasMessage("the outbox publisher is closed");
  }

  private static OutboxEntry entry(String id) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{}", "ProductCreated", STREAM);
  }

  /** A factory for a local port that nothing listens on: every connection is refused. */
  private static ConnectionFactory refusingFactory() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setPort(freePort());
    return factory;
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
