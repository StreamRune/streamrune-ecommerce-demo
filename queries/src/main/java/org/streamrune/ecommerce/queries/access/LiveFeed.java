package org.streamrune.ecommerce.queries.access;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;

/**
 * The reader behind the open live feed, {@code GET /api/events/sse}: one thread per application,
 * the same loop in the three applications. It polls the global stream once per {@link
 * #POLLING_INTERVAL} and hands the frame of every event it reads to every connected client. An
 * application takes a {@link Place} for a connection and hands it a {@link Client} that writes to
 * its own kind of response stream.
 *
 * <p><b>What a connection costs.</b> The feed is open to every caller, so what one connection costs
 * the application is what any number of anonymous callers can make it pay. The event store is asked
 * by the one reader: one read of the global stream per polling interval, however many clients are
 * connected, and one read of its head in an interval in which clients joined. An event is read, and
 * its {@code @Encrypted} fields are decrypted, once per application and not once per connection. A
 * connection costs its socket and the frames buffered for it, and the feed has {@link
 * #MAX_CONNECTIONS} places: {@link #admit} refuses the connection that would be one more, and the
 * application answers it {@code 503 Service Unavailable} with {@code Retry-After}.
 *
 * <p><b>When the reader runs.</b> It starts with the first client and ends when the last one has
 * left, so an application nobody watches reads nothing. Nothing is lost by stopping: a connection
 * starts at the head, so a reader that starts again has no position to keep. {@link #close} ends it
 * when the application stops.
 *
 * <p><b>The feed is live.</b> A connection starts at the head of the global stream. Nothing that
 * was stored before the client connected is sent to it, whatever the request carries, and no
 * connection makes the reader go back. A view that wants what happened earlier reads it from the
 * guarded endpoints: the lists of the read models, and for an {@code ADMIN} the event history,
 * {@code GET /api/events}.
 *
 * <p><b>The opening frame, and why no event after it is missed.</b> A client that has received the
 * comment frame {@code : connected} receives a frame for every event stored from then on, for as
 * long as it stays connected. The order of three steps makes that true:
 *
 * <ol>
 *   <li>{@link Place#follow} registers the client;
 *   <li>the reader, in its next round, takes the clients registered so far and <em>then</em> reads
 *       the head of the global stream, {@code EventStore.lastGlobalOffset()}; that offset, {@code
 *       H}, is where these clients start;
 *   <li>the reader sends them {@code : connected} and from then on hands them every event it reads
 *       whose offset is above {@code H}.
 * </ol>
 *
 * <p>The event store's contract for the head is that an event which becomes readable after {@code
 * lastGlobalOffset()} returned has a greater offset than the one returned. An event stored after
 * the client received {@code : connected} became readable after step 2, so its offset is above
 * {@code H}. The reader's own position is a head it read earlier or the offset of an event it has
 * read, so it is at or below {@code H}; from there it reads the global stream in offset order, as
 * every polling subscription does, so it reaches every event above {@code H}, and by then the
 * client is among the clients it hands frames to, because the client was registered before {@code
 * H} was read. Had the head been read first and the client registered afterwards, an event stored
 * between the two would be above that head and could be read and handed out before the client was
 * there to receive it. An event at or below {@code H} is never sent to the client: it was stored
 * before the client's {@code : connected}. Events stored while a client is away are not sent after
 * it reconnects; the client reads its lists again instead.
 *
 * <p><b>A client that cannot keep up.</b> The reader never waits for a client: a {@link Client}
 * takes a frame without blocking, and the response stream behind it buffers a bounded number of
 * frames for a client that reads slowly. A client whose buffer is full is dropped: its stream
 * fails, its place is free again and it reconnects. A slow or dead client therefore never holds up
 * the reader, which is all the clients share, and a client whose write fails is dropped alone.
 *
 * <p><b>A client that went away.</b> The reader forgets a client within one polling interval of
 * {@link Client#isGone} turning {@code true}. A server that learns of a closed connection only when
 * a write to it fails gets that write from the keep-alive: a comment frame after every {@link
 * #KEEP_ALIVE_INTERVAL} without an event.
 *
 * <p><b>An application that stops.</b> {@link #close} completes every client and returns when the
 * reader has ended, so no open stream holds a shutdown that waits for requests in flight, and no
 * read is under way when the data source closes.
 */
public final class LiveFeed implements AutoCloseable {

  /** How long the reader waits before it asks the store again. */
  public static final Duration POLLING_INTERVAL = Duration.ofSeconds(1);

  /** How long the feed stays without a frame before the reader sends a keep-alive comment. */
  public static final Duration KEEP_ALIVE_INTERVAL = Duration.ofSeconds(5);

  /**
   * How many clients the feed serves at once. A browser tab holds one connection, so a hundred is
   * far more than a demo is shown to, and it bounds what anonymous callers can hold: a hundred
   * sockets and the frames buffered for them.
   */
  public static final int MAX_CONNECTIONS = 100;

  /**
   * The {@code Retry-After} of the answer to a connection the feed has no place for, in seconds. A
   * place is free again within seconds of its client leaving.
   */
  public static final int RETRY_AFTER_SECONDS = 5;

  /** The text of the opening comment frame; the line on the wire is {@code : connected}. */
  public static final String CONNECTED_COMMENT = " connected";

  /** The text of the keep-alive comment frame; the line on the wire is {@code : keepalive}. */
  public static final String KEEP_ALIVE_COMMENT = " keepalive";

  /** The name of the reader's thread. */
  public static final String READER_THREAD_NAME = "live-feed-reader";

  private static final int BATCH_SIZE = 100;

  /** How long {@link #close} waits for a read that is under way. */
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

  /**
   * One connected client, as its application's response stream sees it. The reader calls every
   * method from its one thread, and no method may wait for the client: {@link #comment} and {@link
   * #event} hand the frame to a response stream that buffers it, and a stream whose buffer is full
   * fails itself and from then on answers {@link #isGone} with {@code true}.
   */
  public interface Client {

    /** Whether the client disconnected or was dropped; the reader then forgets it. */
    boolean isGone();

    /** Takes a comment frame: a line that starts with a colon and carries no event. */
    void comment(String text) throws Exception;

    /**
     * Takes the frame of one event.
     *
     * @param globalOffset the event's global offset, the frame's {@code id}
     * @param frame the event's {@link LiveFeedFrame}, the frame's {@code data}; one unmodifiable
     *     map for all the clients
     */
    void event(long globalOffset, Map<String, Object> frame) throws Exception;

    /** Ends the stream with an error: the feed sends this client nothing more. */
    void failed(Exception cause);

    /** Ends the stream normally: the feed is closing. */
    void ended();
  }

  /**
   * One of the feed's places, taken by {@link #admit} for one connection. It is free again when its
   * client has left, was dropped or was completed.
   */
  public final class Place {

    private boolean taken;

    private Place() {}

    /**
     * Registers the client and returns at once; the reader serves it from its next round on. On a
     * closed feed the client is completed at once.
     *
     * @param client the connected client
     */
    public void follow(Client client) {
      synchronized (lock) {
        if (taken) {
          throw new IllegalStateException("a place serves one client");
        }
        taken = true;
        if (!closed) {
          followers.add(new Follower(client));
          if (reader == null) {
            reader =
                Thread.ofPlatform()
                    .daemon()
                    .name(READER_THREAD_NAME)
                    .unstarted(LiveFeed.this::read);
            reader.start();
          }
          return;
        }
        places--;
      }
      client.ended();
    }
  }

  /** What one step of the reader hands a client. */
  private interface Frame {
    void handTo(Client client) throws Exception;
  }

  /** A registered client. Only the reader's thread reads and writes the two fields. */
  private static final class Follower {

    private static final long NOT_STARTED = -1;

    final Client client;

    /** The offset this client starts after: the head the reader read once it was registered. */
    long after = NOT_STARTED;

    /** Set when a write to the client failed; it is sent nothing more. */
    boolean dropped;

    Follower(Client client) {
      this.client = client;
    }
  }

  private final EventStore eventStore;
  private final Duration pollingInterval;
  private final Duration keepAliveInterval;
  private final int maxConnections;
  private final AtomicLong storeReads = new AtomicLong();

  /** Guards the four fields below. Never held during a store read or a call to a client. */
  private final Object lock = new Object();

  private final List<Follower> followers = new ArrayList<>();
  private int places;
  private Thread reader;
  private boolean closed;

  /**
   * A feed with the intervals and the number of places of the running applications.
   *
   * @param eventStore the store whose global stream the clients follow
   */
  public LiveFeed(EventStore eventStore) {
    this(eventStore, POLLING_INTERVAL, KEEP_ALIVE_INTERVAL, MAX_CONNECTIONS);
  }

  /** A feed with intervals and places of its own, for a test that cannot wait for the standard. */
  public LiveFeed(
      EventStore eventStore,
      Duration pollingInterval,
      Duration keepAliveInterval,
      int maxConnections) {
    this.eventStore = eventStore;
    this.pollingInterval = pollingInterval;
    this.keepAliveInterval = keepAliveInterval;
    this.maxConnections = maxConnections;
  }

  /**
   * Takes a place for one connection. An application calls this before it answers, so a connection
   * the feed has no place for gets a status and not a stream. It never waits and reads nothing, so
   * an event loop may call it.
   *
   * @return the place; hand its client to {@link Place#follow}
   * @throws LiveFeedFullException when every place is taken
   */
  public Place admit() {
    synchronized (lock) {
      if (places >= maxConnections) {
        throw new LiveFeedFullException(maxConnections);
      }
      places++;
      return new Place();
    }
  }

  /** The number of places taken: the clients the feed serves. */
  public int openConnections() {
    synchronized (lock) {
      return places;
    }
  }

  /** Whether the reader runs. */
  public boolean isReading() {
    synchronized (lock) {
      return reader != null;
    }
  }

  /** How many times the reader has asked the event store, for its head or for events. */
  public long storeReads() {
    return storeReads.get();
  }

  /**
   * Completes every client and waits until the reader has ended. A client that connects while the
   * feed is closed is completed at once.
   */
  @Override
  public void close() {
    Thread ending;
    synchronized (lock) {
      closed = true;
      ending = reader;
      lock.notifyAll();
    }
    if (ending != null) {
      try {
        ending.join(CLOSE_TIMEOUT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Serves clients again after a {@link #close}: an application that is started a second time. */
  public void open() {
    synchronized (lock) {
      closed = false;
    }
  }

  // --- the reader's thread ---

  private void read() {
    try {
      GlobalOffset position = null;
      long quietSince = System.nanoTime();
      while (true) {
        // The clients registered so far, and only then the head: steps 1 and 2 of the class
        // comment.
        List<Follower> round = clientsOfThisRound();
        if (round.isEmpty()) {
          return;
        }
        List<Follower> joining =
            round.stream().filter(follower -> follower.after == Follower.NOT_STARTED).toList();
        if (position == null || !joining.isEmpty()) {
          GlobalOffset head = head();
          if (position == null) {
            // The head, not the start: the reader never reads the store's history.
            position = head;
          }
          for (Follower follower : joining) {
            follower.after = head.value();
            hand(follower, client -> client.comment(CONNECTED_COMMENT));
          }
        }
        List<EventEnvelope> events = eventsAfter(position);
        for (EventEnvelope event : events) {
          long offset = event.globalOffset().value();
          Map<String, Object> frame = Collections.unmodifiableMap(LiveFeedFrame.of(event));
          for (Follower follower : round) {
            if (offset > follower.after) {
              hand(follower, client -> client.event(offset, frame));
            }
          }
          // readGlobalStream reads the events after the offset it is given.
          position = event.globalOffset();
        }
        if (!events.isEmpty()) {
          quietSince = System.nanoTime();
        } else if (System.nanoTime() - quietSince >= keepAliveInterval.toNanos()) {
          for (Follower follower : round) {
            hand(follower, client -> client.comment(KEEP_ALIVE_COMMENT));
          }
          quietSince = System.nanoTime();
        }
        // A full batch means there is more to read; otherwise the store is asked again after the
        // polling interval, whoever connects in between.
        if (events.size() < BATCH_SIZE) {
          pause();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      endAll(Client::ended);
    } catch (RuntimeException e) {
      // The store cannot be read: every client is told, and reconnects.
      endAll(client -> client.failed(e));
    }
  }

  /**
   * The clients the reader serves in one round, without the ones that left. Empty when the reader
   * is to end: the feed is closed (its clients are completed here), or no client is left.
   */
  private List<Follower> clientsOfThisRound() {
    List<Follower> registered;
    synchronized (lock) {
      registered = List.copyOf(followers);
    }
    List<Follower> left = registered.stream().filter(f -> f.dropped || f.client.isGone()).toList();
    List<Follower> completed;
    synchronized (lock) {
      for (Follower follower : left) {
        if (followers.remove(follower)) {
          places--;
        }
      }
      if (!closed) {
        if (followers.isEmpty()) {
          reader = null;
        }
        return List.copyOf(followers);
      }
      completed = takeAll();
    }
    completed.forEach(follower -> quietly(follower.client, Client::ended));
    return List.of();
  }

  private GlobalOffset head() {
    storeReads.incrementAndGet();
    return eventStore.lastGlobalOffset();
  }

  private List<EventEnvelope> eventsAfter(GlobalOffset position) {
    storeReads.incrementAndGet();
    return eventStore.readGlobalStream(position, BATCH_SIZE);
  }

  /** Hands one frame to one client. A client that cannot take it is dropped, alone. */
  private void hand(Follower follower, Frame frame) {
    if (follower.dropped || follower.client.isGone()) {
      return;
    }
    try {
      frame.handTo(follower.client);
    } catch (Exception e) {
      follower.dropped = true;
      quietly(follower.client, client -> client.failed(e));
    }
  }

  /** Waits one polling interval, or until the feed is closed. */
  private void pause() throws InterruptedException {
    synchronized (lock) {
      if (!closed) {
        lock.wait(Math.max(1, pollingInterval.toMillis()));
      }
    }
  }

  private void endAll(Consumer<Client> ending) {
    List<Follower> all;
    synchronized (lock) {
      all = takeAll();
    }
    all.forEach(follower -> quietly(follower.client, ending));
  }

  /** Forgets every client, frees their places and ends the reader. The caller holds the lock. */
  private List<Follower> takeAll() {
    List<Follower> all = List.copyOf(followers);
    followers.clear();
    places -= all.size();
    reader = null;
    return all;
  }

  /** A stream that cannot be ended is one the client has left already. */
  private static void quietly(Client client, Consumer<Client> ending) {
    try {
      ending.accept(client);
    } catch (RuntimeException alreadyOver) {
      // nothing to end
    }
  }
}
