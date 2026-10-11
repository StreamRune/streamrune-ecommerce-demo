package org.streamrune.ecommerce.queries.access;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;

/**
 * The reader behind the open live feed, {@code GET /api/events/sse}: one virtual thread per
 * connected client, the same loop in the three applications. An application hands {@link #follow} a
 * {@link Client} that writes to its own kind of response stream.
 *
 * <p><b>The feed is live.</b> A connection starts at the head of the global stream: the reader asks
 * the event store for its last global offset and sends the events stored after it. Nothing that was
 * stored before the client connected is read, decrypted or sent, whatever the request carries, so a
 * connection costs the store one small read per polling interval and never a replay of its history.
 * A view that wants what happened earlier reads it from the guarded endpoints: the lists of the
 * read models, and for an {@code ADMIN} the event history, {@code GET /api/events}.
 *
 * <p><b>The opening frame.</b> Once the reader knows the head it sends one comment frame. A client
 * that has received it receives a frame for every event stored from then on, for as long as it
 * stays connected. Events stored while a client is away are not sent after it reconnects; the
 * client reads its lists again instead.
 *
 * <p><b>A client that went away.</b> The reader ends within one polling interval of {@link
 * Client#isGone} turning {@code true}. A server that learns of a closed connection only when a
 * write to it fails gets that write from the keep-alive: a comment frame after every {@link
 * #KEEP_ALIVE_INTERVAL} without an event.
 *
 * <p><b>An application that stops.</b> {@link #close} ends every reader within one polling interval
 * and completes its client, so no open stream holds a shutdown that waits for requests in flight.
 */
public final class LiveFeed implements AutoCloseable {

  /** How long a reader that found no new event waits before it asks the store again. */
  public static final Duration POLLING_INTERVAL = Duration.ofSeconds(1);

  /** How long a connection stays without a frame before the reader sends a keep-alive comment. */
  public static final Duration KEEP_ALIVE_INTERVAL = Duration.ofSeconds(5);

  /** The text of the opening comment frame; the line on the wire is {@code : connected}. */
  public static final String CONNECTED_COMMENT = " connected";

  /** The text of the keep-alive comment frame; the line on the wire is {@code : keepalive}. */
  public static final String KEEP_ALIVE_COMMENT = " keepalive";

  private static final int BATCH_SIZE = 100;

  /** One connected client, as its application's response stream sees it. */
  public interface Client {

    /** Whether the client disconnected; the reader then ends without another signal. */
    boolean isGone();

    /** Writes a comment frame: a line that starts with a colon and carries no event. */
    void comment(String text) throws Exception;

    /**
     * Writes the frame of one event.
     *
     * @param globalOffset the event's global offset, the frame's {@code id}
     * @param frame the event's {@link LiveFeedFrame}, the frame's {@code data}
     */
    void event(long globalOffset, Map<String, Object> frame) throws Exception;

    /** Ends the stream with an error: the reader failed and sends nothing more. */
    void failed(Exception cause);

    /** Ends the stream normally: the feed is closing. */
    void ended();
  }

  private final Duration pollingInterval;
  private final Duration keepAliveInterval;
  private final Set<Thread> readers = ConcurrentHashMap.newKeySet();
  private volatile boolean closed;

  /** A feed with the intervals of the running applications. */
  public LiveFeed() {
    this(POLLING_INTERVAL, KEEP_ALIVE_INTERVAL);
  }

  /** A feed with intervals of its own, for a test that cannot wait for the standard ones. */
  public LiveFeed(Duration pollingInterval, Duration keepAliveInterval) {
    this.pollingInterval = pollingInterval;
    this.keepAliveInterval = keepAliveInterval;
  }

  /**
   * Starts the reader of one client on a virtual thread of its own and returns at once.
   *
   * @param eventStore the store whose global stream the client follows
   * @param client the connected client
   */
  public void follow(EventStore eventStore, Client client) {
    Thread.ofVirtual().name("live-feed").start(() -> read(eventStore, client));
  }

  /** The number of clients a reader is running for. */
  public int openConnections() {
    return readers.size();
  }

  /**
   * Ends every reader within one polling interval and completes its client. A client that connects
   * while the feed is closed is completed at once.
   */
  @Override
  public void close() {
    closed = true;
  }

  /** Serves clients again after a {@link #close}: an application that is started a second time. */
  public void open() {
    closed = false;
  }

  private void read(EventStore eventStore, Client client) {
    readers.add(Thread.currentThread());
    try {
      // The head, not the start: a connection is never served the store's history.
      GlobalOffset position = eventStore.lastGlobalOffset();
      client.comment(CONNECTED_COMMENT);
      long quietSince = System.nanoTime();
      while (!closed && !client.isGone()) {
        List<EventEnvelope> events = eventStore.readGlobalStream(position, BATCH_SIZE);
        for (EventEnvelope event : events) {
          client.event(event.globalOffset().value(), LiveFeedFrame.of(event));
          // readGlobalStream reads the events after the offset it is given.
          position = event.globalOffset();
        }
        if (!events.isEmpty()) {
          quietSince = System.nanoTime();
          continue;
        }
        if (System.nanoTime() - quietSince >= keepAliveInterval.toNanos()) {
          client.comment(KEEP_ALIVE_COMMENT);
          quietSince = System.nanoTime();
        }
        Thread.sleep(pollingInterval);
      }
      if (!client.isGone()) {
        client.ended();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      client.ended();
    } catch (Exception e) {
      client.failed(e);
    } finally {
      readers.remove(Thread.currentThread());
    }
  }
}
