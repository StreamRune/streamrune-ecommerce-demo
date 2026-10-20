package org.streamrune.ecommerce.commands.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
  void aBrokerWhosePacketsAreDroppedFailsWithinTheConnectionTimeout() {
    // TEST-NET-1 (RFC 5737): nothing answers there, so a connect waits out its timeout (or the
    // network refuses the route at once).
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("192.0.2.1");
    factory.setPort(5672);
    factory.setConnectionTimeout(300);
    factory.setHandshakeTimeout(300);

    try (var dropped = publisherFor(factory)) {
      assertFailsWithinTheBound(dropped);
    }
  }

  @Test
  void aBrokerThatNeverAnswersTheHandshakeFailsWithinTheHandshakeTimeout() throws Exception {
    try (ServerSocket silent = new ServerSocket(0)) {
      // Accepts every connection and never writes a byte: the AMQP handshake waits for an answer.
      List<Socket> held = new CopyOnWriteArrayList<>();
      Thread.ofVirtual()
          .start(
              () -> {
                while (!silent.isClosed()) {
                  try {
                    held.add(silent.accept());
                  } catch (IOException e) {
                    return;
                  }
                }
              });
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
      factory.setPort(silent.getLocalPort());
      factory.setConnectionTimeout(300);
      factory.setHandshakeTimeout(300);

      try (var unanswered = publisherFor(factory)) {
        assertFailsWithinTheBound(unanswered);
      } finally {
        for (Socket socket : held) {
          socket.close();
        }
      }
    }
  }

  @Test
  void theConnectBoundIsTheFactorysConnectionAndHandshakeTimeouts() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setConnectionTimeout((int) LazyRabbitMqOutboxPublisher.CONNECTION_TIMEOUT.toMillis());
    factory.setHandshakeTimeout((int) LazyRabbitMqOutboxPublisher.HANDSHAKE_TIMEOUT.toMillis());

    try (var bounded = publisherFor(factory)) {
      assertThat(bounded.connectBound()).isEqualTo(Duration.ofSeconds(20));
    }
  }

  @Test
  void theConnectBoundFitsInsideTheInFlightHorizon() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setConnectionTimeout((int) LazyRabbitMqOutboxPublisher.CONNECTION_TIMEOUT.toMillis());
    factory.setHandshakeTimeout((int) LazyRabbitMqOutboxPublisher.HANDSHAKE_TIMEOUT.toMillis());

    try (var bounded = publisherFor(factory)) {
      // A connect that fails marks its batch for retry inside the lease the horizon promised, so
      // the bound must never exceed the horizon.
      assertThat(bounded.connectBound()).isLessThanOrEqualTo(bounded.inFlightHorizon());
    }
  }

  @Test
  void aClosedPublisherDoesNotConnect() {
    publisher.close();
    assertThat(publisher.isConnected()).isFalse();

    BatchResult result = publisher.publishBatch(List.of(entry("e-1")), null);

    assertThat(result.failed().get(OutboxEntryId.of("e-1")))
        .isInstanceOf(BrokerUnreachableException.class)
        .hasMessage("the outbox publisher is closed");
  }

  /** The attempt fails every entry as a transport failure, well within a bound of 0.6 s. */
  private static void assertFailsWithinTheBound(LazyRabbitMqOutboxPublisher publisher) {
    long start = System.nanoTime();
    BatchResult result =
        publisher.publishBatch(List.of(entry("e-1")), Instant.now().plusSeconds(60));
    Duration took = Duration.ofNanos(System.nanoTime() - start);

    assertThat(took)
        .as("the attempt is bounded by the factory's timeouts")
        .isLessThan(Duration.ofSeconds(5));
    Exception failure = result.failed().get(OutboxEntryId.of("e-1"));
    assertThat(failure).isInstanceOf(BrokerUnreachableException.class);
    assertThat(publisher.classifyFailure(failure)).isEqualTo(FailureKind.TRANSPORT);
    assertThat(result.confirmed()).isEmpty();
  }

  private LazyRabbitMqOutboxPublisher publisherFor(ConnectionFactory factory) {
    return new LazyRabbitMqOutboxPublisher(
        factory,
        "outbox-test",
        channel -> declarations.incrementAndGet(),
        "streamrune.events",
        "integration.event",
        Duration.ofSeconds(30));
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
