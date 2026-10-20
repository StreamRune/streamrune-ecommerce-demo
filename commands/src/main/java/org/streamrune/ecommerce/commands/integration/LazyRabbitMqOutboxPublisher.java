package org.streamrune.ecommerce.commands.integration;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;

/**
 * The outbox's RabbitMQ publisher, connected on its first publish instead of at startup, so the
 * application starts and serves commands while the broker is down. The three apps wire it as their
 * {@link OutboxPublisher}.
 *
 * <p>A command's integration events are written to the outbox in the command's own transaction; the
 * relay ({@code OutboxPoller}) hands them to this publisher later. Nothing about a command depends
 * on the broker, so neither does the application's startup: the constructor opens nothing. The
 * first {@link #publishBatch} opens the connection, opens the publisher's channel, declares the
 * topology on it and builds a {@link RabbitMqOutboxPublisher} on that channel, which publishes this
 * batch and every later one.
 *
 * <p>While the broker cannot be reached, every entry of a batch fails with a {@link
 * BrokerUnreachableException} and nothing is handed to a broker. {@link #classifyFailure} reports
 * it as {@link FailureKind#TRANSPORT}: the relay keeps the entries {@code PENDING} with the error
 * in {@code last_error}, counts no attempt against the retry ladder and tries again on a backoff
 * that grows towards its 60-second cap. The relay logs one {@code WARN} per poll cycle that names
 * the error, and {@code streamrune.outbox.pending} holds the backlog. Once the broker answers, the
 * next attempt connects and the entries are delivered in order, each stream's entries one after the
 * other. This publisher logs one {@code INFO} line when it connects.
 *
 * <p>Once connected, the connection stays open for the life of the application. It is an
 * amqp-client connection with automatic recovery (the {@link ConnectionFactory}'s default): after a
 * broker restart or a network failure the client reconnects by itself and reopens the same channel
 * in confirm mode, with the confirm and return listeners the publisher registered, so a later
 * outage needs nothing from this class. Automatic recovery covers a connection that was established
 * once; a first connection that fails is this class's to retry, on the relay's next attempt.
 *
 * <p>The in-flight horizon is the one the {@link RabbitMqOutboxPublisher} reports once built — its
 * {@link RabbitMqOutboxPublisher#DEFAULT_PUBLISH_BLOCK_BUDGET default publish-block budget} plus
 * the confirm timeout — and is known before the first connection, because the relay reads it when
 * it is built, to check the claim lease against it.
 *
 * <p>The relay calls a publisher from its one poll thread; {@link #close} may come from another
 * thread, at shutdown.
 */
public final class LazyRabbitMqOutboxPublisher implements OutboxPublisher, AutoCloseable {

  /** Declares the exchanges, queues and bindings the publisher's channel publishes to. */
  @FunctionalInterface
  public interface Topology {

    /**
     * Declares the topology on {@code channel}, before the first publish. The declarations must be
     * idempotent: the client's topology recovery repeats them after every reconnect.
     *
     * @param channel the publisher's channel
     * @throws IOException if a declaration fails
     */
    void declare(Channel channel) throws IOException;
  }

  /**
   * The broker could not be reached, so nothing of the batch was published: the connection, the
   * publisher's channel or a topology declaration failed, or the publisher is closed. The cause is
   * the client's own exception, e.g. {@code java.net.ConnectException: Connection refused}.
   */
  public static final class BrokerUnreachableException extends IOException {

    BrokerUnreachableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(LazyRabbitMqOutboxPublisher.class);

  /** The outcome of a batch this publisher did not start: nothing reached a broker. */
  private static final BatchResult NOTHING_ATTEMPTED = new BatchResult(Set.of(), Map.of());

  private final ConnectionFactory connectionFactory;
  private final String connectionName;
  private final Topology topology;
  private final String exchange;
  private final String routingKey;
  private final Duration confirmTimeout;

  /** Held while a connection is opened, so that one poll thread at a time opens one. */
  private final ReentrantLock connecting = new ReentrantLock();

  private volatile Connection connection;
  private volatile RabbitMqOutboxPublisher delegate;
  private volatile boolean closed;

  /**
   * Creates the publisher; nothing is opened until the first publish.
   *
   * @param connectionFactory the factory of the publisher's own connection: host, port, credentials
   *     and virtual host set, automatic recovery on
   * @param connectionName the client-provided connection name the broker's management UI shows
   * @param topology declares, on the publisher's channel, what it publishes to
   * @param exchange the exchange every entry is published to
   * @param routingKey the routing key every entry is published with
   * @param confirmTimeout how long a publish waits for the broker's confirm
   */
  public LazyRabbitMqOutboxPublisher(
      ConnectionFactory connectionFactory,
      String connectionName,
      Topology topology,
      String exchange,
      String routingKey,
      Duration confirmTimeout) {
    this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
    this.connectionName = Objects.requireNonNull(connectionName, "connectionName");
    this.topology = Objects.requireNonNull(topology, "topology");
    this.exchange = Objects.requireNonNull(exchange, "exchange");
    this.routingKey = Objects.requireNonNull(routingKey, "routingKey");
    this.confirmTimeout = Objects.requireNonNull(confirmTimeout, "confirmTimeout");
  }

  @Override
  public void publish(OutboxEntry entry) throws Exception {
    connected().publish(entry);
  }

  @Override
  public BatchResult publishBatch(List<OutboxEntry> entries) {
    return publishBatch(entries, null);
  }

  /**
   * Publishes the batch through the {@link RabbitMqOutboxPublisher}, connecting first when there is
   * no connection yet. A connection that cannot be opened fails every entry with the same {@link
   * BrokerUnreachableException}. When the thread is interrupted (the relay stops) or the deadline
   * has passed before a connection is opened, no entry is attempted and none is in either bucket,
   * as the {@link OutboxPublisher} contract asks of entries a batch never reached.
   */
  @Override
  public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
    if (entries.isEmpty()) {
      return NOTHING_ATTEMPTED;
    }
    RabbitMqOutboxPublisher publisher = delegate;
    if (publisher == null) {
      if (Thread.currentThread().isInterrupted()
          || (deadline != null && !Instant.now().isBefore(deadline))) {
        return NOTHING_ATTEMPTED;
      }
      try {
        publisher = connected();
      } catch (BrokerUnreachableException unreachable) {
        if (Thread.currentThread().isInterrupted()) {
          // The relay is stopping: an interrupted connect is no evidence about the broker.
          return NOTHING_ATTEMPTED;
        }
        Map<OutboxEntryId, Exception> failed = LinkedHashMap.newLinkedHashMap(entries.size());
        for (OutboxEntry entry : entries) {
          failed.put(entry.id(), unreachable);
        }
        return new BatchResult(Set.of(), failed);
      }
    }
    return publisher.publishBatch(entries, deadline);
  }

  /**
   * {@link FailureKind#TRANSPORT} for a {@link BrokerUnreachableException} (nothing was handed to a
   * broker); every other failure comes from the {@link RabbitMqOutboxPublisher} and is classified
   * by it.
   */
  @Override
  public FailureKind classifyFailure(Exception failure) {
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 50; depth++, cause = cause.getCause()) {
      if (cause instanceof BrokerUnreachableException) {
        return FailureKind.TRANSPORT;
      }
    }
    RabbitMqOutboxPublisher publisher = delegate;
    return publisher != null ? publisher.classifyFailure(failure) : FailureKind.TRANSPORT;
  }

  /**
   * The {@link RabbitMqOutboxPublisher}'s horizon: its default publish-block budget plus the
   * confirm timeout.
   */
  @Override
  public Duration inFlightHorizon() {
    return RabbitMqOutboxPublisher.DEFAULT_PUBLISH_BLOCK_BUDGET.plus(confirmTimeout);
  }

  /** Whether the publisher has connected to the broker. */
  public boolean isConnected() {
    return delegate != null;
  }

  /**
   * Closes the connection, if one was opened; afterwards every publish fails with a {@link
   * BrokerUnreachableException}. The application calls it at shutdown, after the relay stopped.
   */
  @Override
  public void close() {
    closed = true;
    Connection opened = connection;
    if (opened != null) {
      closeQuietly(opened);
    }
  }

  /** The publisher on an open connection, connecting first when there is none yet. */
  private RabbitMqOutboxPublisher connected() throws BrokerUnreachableException {
    RabbitMqOutboxPublisher publisher = delegate;
    if (publisher != null) {
      return publisher;
    }
    connecting.lock();
    try {
      if (delegate != null) {
        return delegate;
      }
      if (closed) {
        throw new BrokerUnreachableException("the outbox publisher is closed", null);
      }
      Connection opened = null;
      try {
        opened = connectionFactory.newConnection(connectionName);
        Channel channel = opened.createChannel();
        if (channel == null) {
          throw new IOException("the connection has no channel left for the outbox publisher");
        }
        topology.declare(channel);
        publisher =
            RabbitMqOutboxPublisher.builder()
                .channel(channel)
                .exchange(exchange)
                .routingKey(routingKey)
                .confirmTimeout(confirmTimeout)
                .build();
      } catch (IOException | TimeoutException | RuntimeException e) {
        if (opened != null) {
          closeQuietly(opened);
        }
        throw new BrokerUnreachableException(
            "RabbitMQ is not reachable at " + address() + ", nothing was published: " + describe(e),
            e);
      }
      // Published before closed is read, and close() sets closed before it reads the connection:
      // whichever of the two runs second sees the other's write, so a connection opened while the
      // publisher was being closed is closed here or there.
      connection = opened;
      if (closed) {
        closeQuietly(opened);
        throw new BrokerUnreachableException("the outbox publisher is closed", null);
      }
      delegate = publisher;
      LOG.info("Outbox publisher connected to RabbitMQ at {}", address());
      return publisher;
    } finally {
      connecting.unlock();
    }
  }

  /** The failure and, when it wraps one, the cause the client reported it for. */
  private static String describe(Exception failure) {
    Throwable root = failure;
    for (int depth = 0; root.getCause() != null && root.getCause() != root && depth < 50; depth++) {
      root = root.getCause();
    }
    return root == failure ? failure.toString() : failure + " (" + root + ")";
  }

  private String address() {
    return connectionFactory.getHost() + ":" + connectionFactory.getPort();
  }

  private static void closeQuietly(Connection opened) {
    try {
      opened.close();
    } catch (IOException | RuntimeException e) {
      LOG.debug("Closing the outbox publisher's RabbitMQ connection failed", e);
    }
  }
}
