package org.streamrune.ecommerce.quarkus.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.sse.OutboundSseEvent;
import java.util.List;
import java.util.Map;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.queries.access.LiveFeed;
import org.streamrune.ecommerce.queries.access.LiveFeedFrame;
import org.streamrune.ecommerce.queries.access.LiveFeedFullException;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/**
 * The demo's window on the event store: the global event list, one stream's history, and a live
 * feed of new events.
 *
 * <p><b>The list and the stream history are ADMIN only.</b> They return each event's payload as the
 * event store reads it, and the store decrypts {@code @Encrypted} fields on the way out. Until a
 * customer's key is erased, these two endpoints therefore show that customer's name, e-mail and
 * address in plain text. They are gated with the same {@code requireAdmin} check as the mutating
 * admin endpoints.
 *
 * <p><b>DEMO SHORTCUT, protect this in production.</b> The identity and the role are whatever the
 * client sent in {@code X-User-Id} and {@code X-User-Role} (the gateway stand-in chapter 9
 * describes), so the check keeps the tutorial honest but protects nothing against a caller who sets
 * the two headers. A real deployment authenticates the caller, derives the role from that identity,
 * and does not expose raw event payloads outside an operator's tooling.
 *
 * <p><b>The live feed is open and names no stream.</b> The browser's {@code EventSource} cannot
 * send the two headers, so the feed cannot ask who is calling and every connection is anonymous. A
 * frame is a {@link LiveFeedFrame}: the event's global offset, the type of its aggregate, the event
 * type and the timestamp. It holds no stream id, no aggregate id and no payload, for an {@code
 * ADMIN} as for everyone else, so no caller learns a customer id or an order id from it: the feed
 * says that a customer registered or an order was placed, and when, not which one. A view that
 * wants the detail reads it from the guarded endpoints, with the caller's headers.
 *
 * <p>Same paths, response shape and frame format as the Spring app's {@code
 * EventExplorerController}.
 *
 * <p><b>A connection starts at the head.</b> The feed sends a frame for every event stored while
 * the client is connected and nothing of what the store held before, so no caller makes the app
 * read its history. The frames are unnamed (no {@code event:} field), so {@code
 * EventSource.onmessage} receives every one of them.
 *
 * <p><b>One reader, a bounded number of connections.</b> {@link LiveFeed} reads: one reader for the
 * application asks the event store once per polling interval and hands every frame to every
 * connected client, so what the feed costs the store does not grow with the number of connections.
 * The reader runs while a client is connected. The feed has {@link LiveFeed#MAX_CONNECTIONS}
 * places; the connection that would be one more is answered {@code 503 Service Unavailable} with
 * {@code Retry-After} ({@link LiveFeedFullException}).
 */
@Path("/api/events")
@Produces(MediaType.APPLICATION_JSON)
public class EventExplorerController {

  /** Maximum number of frames buffered for a slow feed client before its stream is failed. */
  static final int SLOW_CLIENT_BUFFER = 256;

  @Inject EventStore eventStore;
  @Inject ObjectMapper objectMapper;
  @Inject StreamRuneRequestContextHolder requestContext;

  // One feed, and so one reader, for the application: Quarkus REST keeps a single instance of a
  // resource class. Built once the event store is injected.
  private LiveFeed liveFeed;

  @GET
  public List<Map<String, Object>> listEvents(
      @QueryParam("offset") String offset, @QueryParam("limit") String limit) {
    long after = RequestChecks.wholeNumber(offset, "offset", 0L);
    int count = RequestChecks.wholeNumber(limit, "limit", 50);
    OwnerOrAdminAccess.requireAdmin(RequestCaller.of(requestContext));
    if (count < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    return eventStore.readGlobalStream(new GlobalOffset(after), count).stream()
        .map(this::toMap)
        .toList();
  }

  @GET
  @Path("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathParam("aggregateType") String aggregateType,
      @PathParam("aggregateId") String aggregateId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.of(requestContext));
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  /**
   * The live feed: a frame for every event stored while the client is connected, open to every
   * caller. {@link LiveFeed} reads; this method writes what it reads as Server-Sent Events.
   *
   * <p>Frame wire format (matching Spring): {@code id} = globalOffset, no {@code event} name,
   * {@code data} = the JSON of a {@link LiveFeedFrame} (global offset, aggregate type, event type
   * and timestamp), without a stream id, an aggregate id or the event's payload.
   *
   * <p>The place is taken before the answer starts: when the feed has none left, {@link
   * LiveFeed#admit} throws {@link LiveFeedFullException} and {@code LiveFeedFullExceptionMapper}
   * answers {@code 503} with {@code Retry-After}. Taking a place and registering the client read
   * nothing and wait for nothing, so this method is safe on the event loop it is called on; the
   * store is read by the feed's own thread.
   *
   * <p>Uses a bounded Mutiny emitter with back-pressure buffering. The feed's reader hands a frame
   * to the emitter and goes on to the next client; it never waits for this one. A client that reads
   * too slowly has up to {@link #SLOW_CLIENT_BUFFER} frames buffered; beyond that its stream fails,
   * its place is free again and the client reconnects.
   */
  @GET
  @Path("/sse")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  public Multi<OutboundSseEvent> sseStream() {
    LiveFeed.Place place = liveFeed.admit();
    return Multi.createFrom()
        .<OutboundSseEvent>emitter(
            emitter -> place.follow(clientOf(emitter)), BackPressureStrategy.ERROR)
        .onOverflow()
        .buffer(SLOW_CLIENT_BUFFER);
  }

  /**
   * {@code HEAD} for the live feed: the headers of the feed's answer and nothing else. A framework
   * answers a {@code HEAD} it finds no handler for by running the {@code GET} handler and leaving
   * out the body; for a stream that never ends that would take one of the feed's places and
   * register a client that can never leave. This handler takes no place and reads nothing.
   */
  @HEAD
  @Path("/sse")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  public Response sseHead() {
    return Response.ok().type(MediaType.SERVER_SENT_EVENTS).build();
  }

  /** The client behind one response stream: what the feed's reader sends becomes its frames. */
  private LiveFeed.Client clientOf(MultiEmitter<? super OutboundSseEvent> emitter) {
    return new LiveFeed.Client() {
      @Override
      public boolean isGone() {
        return emitter.isCancelled();
      }

      @Override
      public void comment(String text) {
        emitter.emit(new SseFrame(null, null, text));
      }

      @Override
      public void event(long globalOffset, Map<String, Object> frame) throws Exception {
        emitter.emit(
            new SseFrame(
                String.valueOf(globalOffset), objectMapper.writeValueAsString(frame), null));
      }

      @Override
      public void failed(Exception cause) {
        emitter.fail(cause);
      }

      @Override
      public void ended() {
        emitter.complete();
      }
    };
  }

  /** The number of clients connected to the live feed. */
  public int openFeedConnections() {
    return liveFeed.openConnections();
  }

  /** Whether the feed's reader runs: it does while a client is connected. */
  public boolean feedReaderRuns() {
    return liveFeed.isReading();
  }

  /** How many times the feed's reader has asked the event store. */
  public long feedStoreReads() {
    return liveFeed.storeReads();
  }

  /** Builds the feed when the application starts; its reader starts with the first client. */
  @PostConstruct
  public void openFeed() {
    liveFeed = new LiveFeed(eventStore);
  }

  /** Completes the open feeds and ends the reader when the application stops. */
  @PreDestroy
  public void closeFeed() {
    liveFeed.close();
  }

  private Map<String, Object> toMap(EventEnvelope env) {
    return Map.of(
        "globalOffset", env.globalOffset().value(),
        "streamId", env.streamId().value(),
        "aggregateType", env.aggregateType().value(),
        "aggregateId", env.aggregateId().value(),
        "eventType", env.eventType().name(),
        "timestamp", env.metadata().timestamp().toString(),
        "payload", env.event());
  }

  /**
   * An unnamed SSE frame: an id and a data line, as the Spring app sends it, or a comment line
   * alone. Quarkus REST leaves out a field whose value is {@code null}.
   */
  private record SseFrame(String id, String data, String comment) implements OutboundSseEvent {

    @Override
    public Class<?> getType() {
      return String.class;
    }

    @Override
    public java.lang.reflect.Type getGenericType() {
      return String.class;
    }

    @Override
    public MediaType getMediaType() {
      return MediaType.TEXT_PLAIN_TYPE;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getComment() {
      return comment;
    }

    @Override
    public long getReconnectDelay() {
      return -1;
    }

    @Override
    public boolean isReconnectDelaySet() {
      return false;
    }

    @Override
    public Object getData() {
      return data;
    }
  }
}
