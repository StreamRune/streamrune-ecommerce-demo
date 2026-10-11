package org.streamrune.ecommerce.queries.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.test.InMemoryEventStore;

/**
 * The reader of the open live feed: where a connection starts, what it is sent, and when its reader
 * ends. The intervals are a few milliseconds, so a test waits for what the standard intervals take
 * seconds to do.
 */
class LiveFeedTest {

  private static final Duration POLL = Duration.ofMillis(10);
  private static final Duration KEEP_ALIVE = Duration.ofMillis(60);

  private final InMemoryEventStore store = new InMemoryEventStore();
  private final LiveFeed feed = new LiveFeed(POLL, KEEP_ALIVE);

  @AfterEach
  void endTheReaders() {
    feed.close();
  }

  @Test
  void aConnectionStartsAtTheHeadAndIsSentEveryEventStoredAfterIt() {
    register("c-before-1");
    register("c-before-2");
    RecordingClient client = new RecordingClient();

    feed.follow(store, client);
    awaitTrue(() -> client.signals.contains("comment: connected"));
    register("c-after-1");
    awaitTrue(() -> client.signals.size() == 2);
    register("c-after-2");
    awaitTrue(() -> client.signals.size() == 3);

    // The comment first, then the two events stored afterwards, one frame each, in order: nothing
    // of the two events the store held when the client connected.
    assertEquals(
        List.of("comment: connected", "event 3 CustomerRegistered", "event 4 CustomerRegistered"),
        firstSignals(client, 3));
  }

  @Test
  void aQuietConnectionIsSentAKeepAliveComment() {
    RecordingClient client = new RecordingClient();

    feed.follow(store, client);

    awaitTrue(() -> client.signals.contains("comment: keepalive"));
    assertEquals("comment: connected", client.signals.get(0));
  }

  @Test
  void theReaderEndsWhenItsClientIsGoneAndSendsItNothingMore() {
    RecordingClient client = new RecordingClient();
    feed.follow(store, client);
    awaitTrue(() -> feed.openConnections() == 1);

    client.gone = true;

    awaitTrue(() -> feed.openConnections() == 0);
    assertTrue(client.signals.stream().noneMatch(s -> s.equals("ended") || s.startsWith("failed")));
  }

  @Test
  void closingTheFeedEndsEveryReaderAndCompletesItsClient() {
    RecordingClient first = new RecordingClient();
    RecordingClient second = new RecordingClient();
    feed.follow(store, first);
    feed.follow(store, second);
    awaitTrue(() -> feed.openConnections() == 2);

    feed.close();

    awaitTrue(() -> feed.openConnections() == 0);
    assertEquals("ended", first.signals.getLast());
    assertEquals("ended", second.signals.getLast());
  }

  @Test
  void aFeedThatIsOpenedAgainServesClientsAgain() {
    feed.close();
    feed.open();
    RecordingClient client = new RecordingClient();

    feed.follow(store, client);
    awaitTrue(() -> client.signals.contains("comment: connected"));
    register("c-after");

    awaitTrue(() -> client.signals.contains("event 1 CustomerRegistered"));
  }

  @Test
  void aReaderThatCannotReadTheStoreFailsItsClient() {
    // A store that knows its head and fails every read of the global stream.
    EventStore broken =
        (EventStore)
            Proxy.newProxyInstance(
                EventStore.class.getClassLoader(),
                new Class<?>[] {EventStore.class},
                (proxy, method, arguments) -> {
                  if (method.getName().equals("lastGlobalOffset")) {
                    return GlobalOffset.initial();
                  }
                  throw new IllegalStateException("the store is down");
                });
    RecordingClient client = new RecordingClient();

    feed.follow(broken, client);

    awaitTrue(() -> client.signals.contains("failed: the store is down"));
    awaitTrue(() -> feed.openConnections() == 0);
  }

  // --- helpers ---

  /** A client that records what the reader sent it, one line per signal. */
  private static final class RecordingClient implements LiveFeed.Client {

    final List<String> signals = new CopyOnWriteArrayList<>();
    volatile boolean gone;

    @Override
    public boolean isGone() {
      return gone;
    }

    @Override
    public void comment(String text) {
      signals.add("comment:" + text);
    }

    @Override
    public void event(long globalOffset, Map<String, Object> frame) {
      signals.add("event " + globalOffset + " " + frame.get("eventType"));
    }

    @Override
    public void failed(Exception cause) {
      signals.add("failed: " + cause.getMessage());
    }

    @Override
    public void ended() {
      signals.add("ended");
    }
  }

  /** The first signals of a client, without the keep-alive comments a slow test run adds. */
  private static List<String> firstSignals(RecordingClient client, int count) {
    List<String> signals = new ArrayList<>(client.signals);
    signals.removeIf(signal -> signal.equals("comment: keepalive"));
    return signals.subList(0, count);
  }

  private void register(String customerId) {
    StreamId stream = StreamId.of(CustomerState.TYPE, AggregateId.of(customerId));
    EventEnvelope event =
        new EventEnvelope(
            GlobalOffset.initial(),
            stream,
            new Version(1),
            new EventType("CustomerRegistered"),
            new CustomerEvent.CustomerRegistered(
                customerId, "Alice", "alice@example.com", "1 Main St", "555-0100"),
            new EventMetadata(
                IdGenerator.generateEventId(),
                IdGenerator.generateCommandId(),
                null,
                null,
                CorrelationId.of("corr-" + customerId),
                null,
                UserId.of(customerId),
                Instant.parse("2026-10-04T10:15:30Z")));
    store.append(stream, List.of(event), Version.initial());
  }

  private static void awaitTrue(BooleanSupplier condition) {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("the condition did not hold within 10 seconds");
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while waiting", e);
      }
    }
  }
}
