package org.streamrune.ecommerce.queries.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
 * The reader of the open live feed: one for all the clients. Where a connection starts, what the
 * clients are sent, what the reader asks the store, how many clients it takes, and when it runs.
 * The intervals are a few milliseconds, so a test waits for what the standard intervals take
 * seconds to do.
 */
class LiveFeedTest {

  private static final Duration POLL = Duration.ofMillis(10);
  private static final Duration KEEP_ALIVE = Duration.ofMillis(60);
  private static final int PLACES = 3;

  /** How long a place waits for its client in these tests. */
  private static final Duration FOLLOW_TIMEOUT = Duration.ofMillis(400);

  private final InMemoryEventStore store = new InMemoryEventStore();

  /** The store the feed reads: {@link #store}, with every call counted and one of them held. */
  private final ObservedStore observed = new ObservedStore(store);

  private final LiveFeed feed =
      new LiveFeed(observed.asEventStore(), POLL, KEEP_ALIVE, PLACES, FOLLOW_TIMEOUT);

  /**
   * Ends the reader and checks, after whatever the test did, that the places add up: the places
   * taken are the pending ones and the registered clients, never fewer than none and never more
   * than the feed has. With the reader ended nothing changes between the three readings.
   */
  @AfterEach
  void endTheReaderAndCountThePlaces() {
    observed.failure.set(null);
    observed.release();
    feed.close();

    assertEquals(0, feed.registeredClients(), "clients registered on a closed feed");
    assertPlacesAddUp();
  }

  private void assertPlacesAddUp() {
    int taken = feed.openConnections();
    assertEquals(feed.pendingPlaces() + feed.registeredClients(), taken, "places taken");
    assertTrue(taken >= 0 && taken <= PLACES, "places taken: " + taken);
  }

  // --- where a connection starts ---

  @Test
  void aConnectionStartsAtTheHeadAndIsSentEveryEventStoredAfterIt() {
    register("c-before-1");
    register("c-before-2");
    RecordingClient client = new RecordingClient();

    feed.admit().follow(client);
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
  void aClientThatConnectsLaterStartsAtTheHeadOfThatMomentAndIsSentTheNextEvent() {
    RecordingClient early = new RecordingClient();
    feed.admit().follow(early);
    awaitTrue(() -> early.signals.contains("comment: connected"));
    register("c-1");
    awaitTrue(() -> early.signals.contains("event 1 CustomerRegistered"));

    RecordingClient late = new RecordingClient();
    feed.admit().follow(late);
    awaitTrue(() -> late.signals.contains("comment: connected"));
    register("c-2");
    awaitTrue(() -> late.signals.contains("event 2 CustomerRegistered"));
    awaitTrue(() -> early.signals.contains("event 2 CustomerRegistered"));

    assertEquals(
        List.of("comment: connected", "event 2 CustomerRegistered"), firstSignals(late, 2));
    assertEquals(
        List.of("comment: connected", "event 1 CustomerRegistered", "event 2 CustomerRegistered"),
        firstSignals(early, 3));
  }

  /**
   * The order the class comment of {@link LiveFeed} argues for, step by step. A client registers
   * while the reader is in the middle of reading an event. The reader takes that client in its next
   * round and reads the head for it only then: the event that was being read is at or below that
   * head and is not sent to the client, and the event stored after the client's opening comment is.
   */
  @Test
  void aClientThatRegistersWhileAnEventIsReadStartsAfterItAndMissesNothingStoredLater()
      throws Exception {
    RecordingClient early = new RecordingClient();
    feed.admit().follow(early);
    awaitTrue(() -> early.signals.contains("comment: connected"));

    // The reader stands in a read of the global stream that has not reached the store yet.
    observed.holdTheNextStreamRead();
    observed.awaitHeldRead();
    register("c-during");
    RecordingClient late = new RecordingClient();
    feed.admit().follow(late);
    observed.release();

    awaitTrue(() -> late.signals.contains("comment: connected"));
    register("c-after");
    awaitTrue(() -> late.signals.contains("event 2 CustomerRegistered"));
    awaitTrue(() -> early.signals.contains("event 2 CustomerRegistered"));

    assertEquals(
        List.of("comment: connected", "event 2 CustomerRegistered"), firstSignals(late, 2));
    assertEquals(
        List.of("comment: connected", "event 1 CustomerRegistered", "event 2 CustomerRegistered"),
        firstSignals(early, 3));
  }

  /**
   * The other half of the same order: the reader fixes the clients of a round before it reads the
   * head for them. A client that registers while that head is on its way back from the store is not
   * started at it: an event stored in between is above that head, and the client's own opening
   * comment comes later. The client gets a head of its own in the next round.
   */
  @Test
  void aClientThatRegistersWhileTheHeadIsReadIsNotStartedAtThatHead() throws Exception {
    // The reader has read the head for the first client and has not got the answer yet.
    observed.holdTheNextHeadRead();
    RecordingClient early = new RecordingClient();
    feed.admit().follow(early);
    observed.awaitHeldRead();
    register("c-during");
    RecordingClient late = new RecordingClient();
    feed.admit().follow(late);
    observed.release();

    awaitTrue(() -> late.signals.contains("comment: connected"));
    register("c-after");
    awaitTrue(() -> late.signals.contains("event 2 CustomerRegistered"));
    awaitTrue(() -> early.signals.contains("event 2 CustomerRegistered"));

    assertEquals(
        List.of("comment: connected", "event 2 CustomerRegistered"), firstSignals(late, 2));
    assertEquals(
        List.of("comment: connected", "event 1 CustomerRegistered", "event 2 CustomerRegistered"),
        firstSignals(early, 3));
  }

  // --- one reader for all the clients ---

  @Test
  void everyClientIsSentTheSameFramesInTheSameOrder() {
    List<RecordingClient> clients =
        List.of(new RecordingClient(), new RecordingClient(), new RecordingClient());
    clients.forEach(client -> feed.admit().follow(client));
    for (RecordingClient client : clients) {
      awaitTrue(() -> client.signals.contains("comment: connected"));
    }

    for (int i = 1; i <= 5; i++) {
      register("c-" + i);
    }
    for (RecordingClient client : clients) {
      awaitTrue(() -> client.signals.contains("event 5 CustomerRegistered"));
    }

    List<String> expected =
        List.of(
            "comment: connected",
            "event 1 CustomerRegistered",
            "event 2 CustomerRegistered",
            "event 3 CustomerRegistered",
            "event 4 CustomerRegistered",
            "event 5 CustomerRegistered");
    for (RecordingClient client : clients) {
      assertEquals(expected, firstSignals(client, 6));
    }
  }

  @Test
  void theStoreIsReadOncePerRoundHoweverManyClientsFollow() throws Exception {
    RecordingClient first = new RecordingClient();
    feed.admit().follow(first);
    awaitTrue(() -> first.signals.contains("comment: connected"));
    int roundsWithOneClient = streamReadsWhile(() -> sleep(POLL.multipliedBy(30)));

    RecordingClient second = new RecordingClient();
    RecordingClient third = new RecordingClient();
    feed.admit().follow(second);
    feed.admit().follow(third);
    awaitTrue(() -> third.signals.contains("comment: connected"));
    int headReadsBefore = observed.headReads.get();
    int roundsWithThreeClients = streamReadsWhile(() -> sleep(POLL.multipliedBy(30)));

    // Thirty polling intervals hold at most thirty-one reads of the stream, for one client and
    // for three: a reader per client would read three times as often.
    assertTrue(roundsWithOneClient <= 31, "stream reads with one client: " + roundsWithOneClient);
    assertTrue(
        roundsWithThreeClients <= 31, "stream reads with three clients: " + roundsWithThreeClients);
    assertTrue(roundsWithThreeClients >= 2, "the reader keeps reading: " + roundsWithThreeClients);
    // The head is read for clients that join, and nobody joined.
    assertEquals(headReadsBefore, observed.headReads.get());
    // The feed's own count is the number of calls the store saw, once the reader has ended.
    feed.close();
    assertEquals(observed.headReads.get() + observed.streamReads.get(), feed.storeReads());
  }

  @Test
  void clientsThatJoinInTheSameRoundShareOneReadOfTheHead() throws Exception {
    RecordingClient first = new RecordingClient();
    feed.admit().follow(first);
    awaitTrue(() -> first.signals.contains("comment: connected"));
    assertEquals(1, observed.headReads.get());

    // Both register while the reader stands in a read, so its next round takes them together.
    observed.holdTheNextStreamRead();
    observed.awaitHeldRead();
    RecordingClient second = new RecordingClient();
    RecordingClient third = new RecordingClient();
    feed.admit().follow(second);
    feed.admit().follow(third);
    observed.release();

    awaitTrue(() -> second.signals.contains("comment: connected"));
    awaitTrue(() -> third.signals.contains("comment: connected"));
    assertEquals(2, observed.headReads.get());
  }

  // --- a bounded number of clients ---

  @Test
  void theConnectionAfterTheLastPlaceIsRefusedAndTheOthersKeepReceiving() {
    List<RecordingClient> clients = new ArrayList<>();
    for (int i = 0; i < PLACES; i++) {
      RecordingClient client = new RecordingClient();
      clients.add(client);
      feed.admit().follow(client);
    }
    assertEquals(PLACES, feed.openConnections());

    LiveFeedFullException refused = assertThrows(LiveFeedFullException.class, feed::admit);

    assertTrue(refused.getMessage().contains(String.valueOf(PLACES)), refused.getMessage());
    assertEquals(PLACES, feed.openConnections());
    for (RecordingClient client : clients) {
      awaitTrue(() -> client.signals.contains("comment: connected"));
    }
    register("c-1");
    for (RecordingClient client : clients) {
      awaitTrue(() -> client.signals.contains("event 1 CustomerRegistered"));
    }
  }

  @Test
  void aPlaceIsFreeAgainWhenItsClientHasGone() {
    List<RecordingClient> clients = new ArrayList<>();
    for (int i = 0; i < PLACES; i++) {
      RecordingClient client = new RecordingClient();
      clients.add(client);
      feed.admit().follow(client);
    }
    assertThrows(LiveFeedFullException.class, feed::admit);

    clients.get(0).gone = true;

    awaitTrue(() -> feed.openConnections() == PLACES - 1);
    RecordingClient next = new RecordingClient();
    feed.admit().follow(next);
    awaitTrue(() -> next.signals.contains("comment: connected"));
  }

  // --- a place nobody follows ---

  @Test
  void aPlaceNobodyFollowsIsFreeAgainAfterTheFollowTimeout() throws Exception {
    // What a framework does that takes the place and never subscribes the response stream.
    for (int i = 0; i < PLACES; i++) {
      feed.admit();
    }
    assertEquals(PLACES, feed.pendingPlaces());
    assertEquals(PLACES, feed.openConnections());
    assertThrows(LiveFeedFullException.class, feed::admit);

    sleep(FOLLOW_TIMEOUT.multipliedBy(2));

    assertEquals(0, feed.openConnections());
    assertEquals(0, feed.pendingPlaces());
    // The feed is not full: it takes its clients again, all of them.
    List<RecordingClient> clients = new ArrayList<>();
    for (int i = 0; i < PLACES; i++) {
      RecordingClient client = new RecordingClient();
      clients.add(client);
      feed.admit().follow(client);
    }
    for (RecordingClient client : clients) {
      awaitTrue(() -> client.signals.contains("comment: connected"));
    }
    register("c-1");
    for (RecordingClient client : clients) {
      awaitTrue(() -> client.signals.contains("event 1 CustomerRegistered"));
    }
    assertEquals(PLACES, feed.registeredClients());
    assertPlacesAddUp();
  }

  @Test
  void aPlaceNobodyFollowsIsDroppedByTheReadersRoundToo() throws Exception {
    RecordingClient served = new RecordingClient();
    feed.admit().follow(served);
    awaitTrue(() -> served.signals.contains("comment: connected"));
    feed.admit();
    assertEquals(1, feed.pendingPlaces());

    // Nobody asks the feed for a place or for its count: the reader's rounds drop the place.
    awaitTrue(() -> feed.pendingPlaces() == 0);

    assertEquals(1, feed.registeredClients());
    assertEquals(1, feed.openConnections());
  }

  @Test
  void aClientFollowedOnAPlaceThatWasDroppedIsCompletedAtOnce() throws Exception {
    LiveFeed.Place abandoned = feed.admit();
    sleep(FOLLOW_TIMEOUT.multipliedBy(2));
    assertEquals(0, feed.openConnections());

    RecordingClient late = new RecordingClient();
    abandoned.follow(late);

    assertEquals(List.of("ended"), late.signals);
    assertEquals(0, feed.openConnections());
    assertEquals(0, feed.pendingPlaces());
    assertEquals(0, feed.registeredClients());
    assertFalse(feed.isReading());
  }

  @Test
  void aLateFollowDoesNotFreeAPlaceThatIsSomebodyElsesByNow() throws Exception {
    List<LiveFeed.Place> abandoned = new ArrayList<>();
    for (int i = 0; i < PLACES; i++) {
      abandoned.add(feed.admit());
    }
    sleep(FOLLOW_TIMEOUT.multipliedBy(2));
    // The same places, taken again by others, who follow them.
    List<RecordingClient> served = new ArrayList<>();
    for (int i = 0; i < PLACES; i++) {
      RecordingClient client = new RecordingClient();
      served.add(client);
      feed.admit().follow(client);
    }
    for (RecordingClient client : served) {
      awaitTrue(() -> client.signals.contains("comment: connected"));
    }

    List<RecordingClient> late = new ArrayList<>();
    for (LiveFeed.Place place : abandoned) {
      RecordingClient client = new RecordingClient();
      late.add(client);
      place.follow(client);
    }

    for (RecordingClient client : late) {
      assertEquals(List.of("ended"), client.signals);
    }
    // The late ones freed nothing and took nothing: the feed is as full as it was.
    assertEquals(PLACES, feed.openConnections());
    assertEquals(PLACES, feed.registeredClients());
    assertThrows(LiveFeedFullException.class, feed::admit);
    register("c-1");
    for (RecordingClient client : served) {
      awaitTrue(() -> client.signals.contains("event 1 CustomerRegistered"));
    }
    assertPlacesAddUp();
  }

  @Test
  void aFollowedPlaceIsNotDroppedWhileItsClientIsConnected() throws Exception {
    RecordingClient client = new RecordingClient();
    feed.admit().follow(client);
    awaitTrue(() -> client.signals.contains("comment: connected"));

    // Several follow timeouts, with the reader's rounds and a count in between.
    for (int i = 0; i < 4; i++) {
      sleep(FOLLOW_TIMEOUT);
      assertEquals(1, feed.openConnections());
    }

    register("c-1");
    awaitTrue(() -> client.signals.contains("event 1 CustomerRegistered"));
    assertEquals(1, feed.registeredClients());
    assertEquals(0, feed.pendingPlaces());
    assertTrue(client.signals.stream().noneMatch(s -> s.equals("ended") || s.startsWith("failed")));
    assertPlacesAddUp();
  }

  @Test
  void aPlaceThatIsFollowedTwiceServesOneClient() {
    LiveFeed.Place place = feed.admit();
    RecordingClient first = new RecordingClient();
    place.follow(first);

    assertThrows(IllegalStateException.class, () -> place.follow(new RecordingClient()));

    awaitTrue(() -> first.signals.contains("comment: connected"));
    assertEquals(1, feed.openConnections());
    assertEquals(1, feed.registeredClients());
    assertPlacesAddUp();
  }

  // --- a client that cannot keep up, a client that left ---

  @Test
  void aClientWhoseWriteFailsIsDroppedAloneAndTheOthersAreServed() {
    RecordingClient healthy = new RecordingClient();
    RecordingClient broken = new RecordingClient();
    broken.failsOnEvents = true;
    feed.admit().follow(healthy);
    feed.admit().follow(broken);
    awaitTrue(() -> healthy.signals.contains("comment: connected"));
    awaitTrue(() -> broken.signals.contains("comment: connected"));

    register("c-1");
    register("c-2");

    awaitTrue(() -> healthy.signals.contains("event 2 CustomerRegistered"));
    awaitTrue(() -> feed.openConnections() == 1);
    assertEquals(
        List.of("comment: connected", "event 1 CustomerRegistered", "event 2 CustomerRegistered"),
        firstSignals(healthy, 3));
    // Told once that its stream is over, and handed nothing afterwards.
    assertEquals(
        List.of("comment: connected", "failed: the buffer of this client is full"),
        firstSignals(broken, 2));
    assertEquals(2, withoutKeepAlives(broken).size());
  }

  @Test
  void aQuietFeedSendsEveryClientAKeepAliveComment() {
    RecordingClient first = new RecordingClient();
    RecordingClient second = new RecordingClient();

    feed.admit().follow(first);
    feed.admit().follow(second);

    awaitTrue(() -> first.signals.contains("comment: keepalive"));
    awaitTrue(() -> second.signals.contains("comment: keepalive"));
    assertEquals("comment: connected", first.signals.get(0));
    assertEquals("comment: connected", second.signals.get(0));
  }

  @Test
  void aClientThatIsGoneIsForgottenAndSentNothingMore() {
    RecordingClient staying = new RecordingClient();
    RecordingClient leaving = new RecordingClient();
    feed.admit().follow(staying);
    feed.admit().follow(leaving);
    awaitTrue(() -> feed.openConnections() == 2);
    awaitTrue(() -> leaving.signals.contains("comment: connected"));

    leaving.gone = true;

    awaitTrue(() -> feed.openConnections() == 1);
    int sentToTheOneThatLeft = leaving.signals.size();
    register("c-1");
    awaitTrue(() -> staying.signals.contains("event 1 CustomerRegistered"));
    assertEquals(sentToTheOneThatLeft, leaving.signals.size());
    assertTrue(
        leaving.signals.stream().noneMatch(s -> s.equals("ended") || s.startsWith("failed")));
  }

  // --- when the reader runs ---

  @Test
  void theReaderRunsWhileAClientIsConnectedAndReadsNothingOtherwise() throws Exception {
    assertFalse(feed.isReading());
    assertEquals(0, readerThreads());
    sleep(POLL.multipliedBy(5));
    assertEquals(0, feed.storeReads());

    RecordingClient client = new RecordingClient();
    feed.admit().follow(client);
    awaitTrue(() -> client.signals.contains("comment: connected"));
    assertTrue(feed.isReading());
    assertEquals(1, readerThreads());

    client.gone = true;

    awaitTrue(() -> !feed.isReading() && readerThreads() == 0);
    assertEquals(0, feed.openConnections());
    long readsWhenTheLastClientLeft = feed.storeReads();
    sleep(POLL.multipliedBy(5));
    assertEquals(readsWhenTheLastClientLeft, feed.storeReads());
  }

  @Test
  void aReaderThatStartsAgainStartsAtTheHead() {
    RecordingClient first = new RecordingClient();
    feed.admit().follow(first);
    awaitTrue(() -> first.signals.contains("comment: connected"));
    first.gone = true;
    awaitTrue(() -> !feed.isReading());
    register("c-while-nobody-watched");

    RecordingClient second = new RecordingClient();
    feed.admit().follow(second);
    awaitTrue(() -> second.signals.contains("comment: connected"));
    register("c-after");

    awaitTrue(() -> second.signals.contains("event 2 CustomerRegistered"));
    assertEquals(
        List.of("comment: connected", "event 2 CustomerRegistered"), firstSignals(second, 2));
  }

  @Test
  void closingTheFeedCompletesEveryClientAndEndsTheReader() {
    RecordingClient first = new RecordingClient();
    RecordingClient second = new RecordingClient();
    feed.admit().follow(first);
    feed.admit().follow(second);
    awaitTrue(() -> second.signals.contains("comment: connected"));

    feed.close();

    // close() returns when the reader has ended: nothing is left to wait for.
    assertFalse(feed.isReading());
    assertEquals(0, readerThreads());
    assertEquals(0, feed.openConnections());
    assertEquals("ended", first.signals.getLast());
    assertEquals("ended", second.signals.getLast());
  }

  @Test
  void aClientThatConnectsToAClosedFeedIsCompletedAtOnce() {
    feed.close();
    RecordingClient client = new RecordingClient();

    feed.admit().follow(client);

    assertEquals(List.of("ended"), client.signals);
    assertEquals(0, feed.openConnections());
    assertFalse(feed.isReading());
  }

  @Test
  void aPlaceTakenBeforeTheFeedClosedIsFreeOnceItsClientIsCompleted() {
    RecordingClient served = new RecordingClient();
    feed.admit().follow(served);
    awaitTrue(() -> served.signals.contains("comment: connected"));
    LiveFeed.Place taken = feed.admit();

    feed.close();
    // The served client was completed by the close; the pending place is not a client's yet.
    assertEquals("ended", served.signals.getLast());
    assertEquals(0, feed.registeredClients());
    RecordingClient late = new RecordingClient();
    taken.follow(late);

    assertEquals(List.of("ended"), late.signals);
    assertEquals(0, feed.openConnections());
    assertEquals(0, feed.pendingPlaces());
    assertFalse(feed.isReading());
  }

  @Test
  void aFeedThatIsOpenedAgainServesClientsAgain() {
    feed.close();
    feed.open();
    RecordingClient client = new RecordingClient();

    feed.admit().follow(client);
    awaitTrue(() -> client.signals.contains("comment: connected"));
    register("c-after");

    awaitTrue(() -> client.signals.contains("event 1 CustomerRegistered"));
  }

  @Test
  void aReaderThatCannotReadTheStoreFailsItsClientsAndTheNextClientStartsANewOne() {
    RecordingClient first = new RecordingClient();
    RecordingClient second = new RecordingClient();
    feed.admit().follow(first);
    feed.admit().follow(second);
    awaitTrue(() -> second.signals.contains("comment: connected"));

    observed.failure.set(new IllegalStateException("the store is down"));

    awaitTrue(() -> first.signals.contains("failed: the store is down"));
    awaitTrue(() -> second.signals.contains("failed: the store is down"));
    awaitTrue(() -> !feed.isReading() && readerThreads() == 0);
    assertEquals(0, feed.openConnections());

    observed.failure.set(null);
    RecordingClient next = new RecordingClient();
    feed.admit().follow(next);
    awaitTrue(() -> next.signals.contains("comment: connected"));
  }

  @Test
  void aReaderThatAnErrorEndsFailsItsClientsAndTheNextClientStartsANewOne() {
    RecordingClient first = new RecordingClient();
    RecordingClient second = new RecordingClient();
    feed.admit().follow(first);
    feed.admit().follow(second);
    awaitTrue(() -> second.signals.contains("comment: connected"));
    // The error is not swallowed: it reaches the handler of the reader's thread.
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.UncaughtExceptionHandler before = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, thrown) -> {
          if (thread.getName().equals(LiveFeed.READER_THREAD_NAME)) {
            uncaught.set(thrown);
          }
        });
    try {
      // What a read throws when an event's class cannot be loaded.
      NoClassDefFoundError error = new NoClassDefFoundError("org/example/AnEventThatIsGone");
      observed.failure.set(error);

      awaitTrue(() -> first.signals.contains("failed: The feed's reader has ended"));
      awaitTrue(() -> second.signals.contains("failed: The feed's reader has ended"));
      awaitTrue(() -> !feed.isReading() && readerThreads() == 0);
      awaitTrue(() -> uncaught.get() == error);
      assertEquals(0, feed.openConnections());
      assertEquals(0, feed.registeredClients());
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(before);
    }

    observed.failure.set(null);
    RecordingClient next = new RecordingClient();
    feed.admit().follow(next);
    awaitTrue(() -> next.signals.contains("comment: connected"));
    register("c-1");
    awaitTrue(() -> next.signals.contains("event 1 CustomerRegistered"));
    assertTrue(feed.isReading());
  }

  // --- helpers ---

  /** A client that records what the reader handed it, one line per signal. */
  private static final class RecordingClient implements LiveFeed.Client {

    final List<String> signals = new CopyOnWriteArrayList<>();
    volatile boolean gone;
    volatile boolean failsOnEvents;

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
      if (failsOnEvents) {
        throw new IllegalStateException("the buffer of this client is full");
      }
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

  /**
   * An event store that passes every call on to another one and counts the two calls the feed
   * makes. A test can hold the reader inside a read of the global stream, before that read reaches
   * the store, or inside a read of the head, after the store has answered it, and can make every
   * call fail, with an exception or with an error.
   */
  private static final class ObservedStore {

    final AtomicInteger headReads = new AtomicInteger();
    final AtomicInteger streamReads = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    private final EventStore delegate;
    private volatile String heldMethod;
    private volatile CountDownLatch held;
    private volatile CountDownLatch released = new CountDownLatch(0);

    ObservedStore(EventStore delegate) {
      this.delegate = delegate;
    }

    EventStore asEventStore() {
      return (EventStore)
          Proxy.newProxyInstance(
              EventStore.class.getClassLoader(),
              new Class<?>[] {EventStore.class},
              (proxy, method, arguments) -> {
                if (method.getName().equals("lastGlobalOffset")) {
                  headReads.incrementAndGet();
                } else if (method.getName().equals("readGlobalStream")) {
                  streamReads.incrementAndGet();
                  holdIfAskedTo("readGlobalStream");
                }
                Throwable failing = failure.get();
                if (failing != null) {
                  throw failing;
                }
                Object answer;
                try {
                  answer = method.invoke(delegate, arguments);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
                if (method.getName().equals("lastGlobalOffset")) {
                  holdIfAskedTo("lastGlobalOffset");
                }
                return answer;
              });
    }

    private void holdIfAskedTo(String method) throws InterruptedException {
      CountDownLatch reached = held;
      if (reached != null && method.equals(heldMethod)) {
        held = null;
        reached.countDown();
        released.await(10, TimeUnit.SECONDS);
      }
    }

    /**
     * The reader's next read of the global stream waits, before the store, for {@link #release}.
     */
    void holdTheNextStreamRead() {
      hold("readGlobalStream");
    }

    /** The reader's next read of the head waits, with the store's answer, for {@link #release}. */
    void holdTheNextHeadRead() {
      hold("lastGlobalOffset");
    }

    private void hold(String method) {
      released = new CountDownLatch(1);
      heldMethod = method;
      held = new CountDownLatch(1);
    }

    /** Returns once the reader stands in the read that is held. */
    void awaitHeldRead() throws InterruptedException {
      CountDownLatch reached = held;
      if (reached != null && !reached.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("the reader did not reach the held read within 10 seconds");
      }
    }

    void release() {
      released.countDown();
    }
  }

  /** How many times the reader reads the global stream while the given action runs. */
  private int streamReadsWhile(InterruptibleAction action) throws Exception {
    int before = observed.streamReads.get();
    action.run();
    return observed.streamReads.get() - before;
  }

  private interface InterruptibleAction {
    void run() throws Exception;
  }

  /** The number of live threads with the name of the feed's reader. */
  private static long readerThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.getName().equals(LiveFeed.READER_THREAD_NAME))
        .count();
  }

  /** The first signals of a client, without the keep-alive comments a slow test run adds. */
  private static List<String> firstSignals(RecordingClient client, int count) {
    return withoutKeepAlives(client).subList(0, count);
  }

  private static List<String> withoutKeepAlives(RecordingClient client) {
    List<String> signals = new ArrayList<>(client.signals);
    signals.removeIf(signal -> signal.equals("comment: keepalive"));
    return signals;
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

  private static void sleep(Duration duration) throws InterruptedException {
    Thread.sleep(duration);
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
