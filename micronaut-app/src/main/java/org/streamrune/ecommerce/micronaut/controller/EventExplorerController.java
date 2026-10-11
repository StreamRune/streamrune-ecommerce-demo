package org.streamrune.ecommerce.micronaut.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.sse.Event;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
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
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
 * read its history: {@link LiveFeed} reads, one reader per connection, and the reader ends when its
 * client leaves. The frames are unnamed (no {@code event:} field), so {@code EventSource.onmessage}
 * receives every one of them.
 */
@Controller("/api/events")
public class EventExplorerController {

  /** Maximum number of events buffered for a slow SSE client before the stream is terminated. */
  static final int SLOW_CLIENT_BUFFER = 256;

  @Inject EventStore eventStore;
  @Inject ObjectMapper objectMapper;

  private final LiveFeed liveFeed = new LiveFeed();

  @Get
  public List<Map<String, Object>> listEvents(
      @QueryValue(value = "offset", defaultValue = "0") long offset,
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @Get("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
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
   * <p>Uses a Flux with back-pressure buffering: a client that reads too slowly has up to {@link
   * #SLOW_CLIENT_BUFFER} frames buffered; beyond that the stream fails and the client reconnects.
   */
  @Get("/sse")
  @Produces(MediaType.TEXT_EVENT_STREAM)
  public Flux<Event<String>> sseStream() {
    return Flux.<Event<String>>create(
            sink -> liveFeed.follow(eventStore, clientOf(sink)), FluxSink.OverflowStrategy.ERROR)
        .onBackpressureBuffer(SLOW_CLIENT_BUFFER);
  }

  /** The client behind one response stream: what the feed's reader sends becomes its frames. */
  private LiveFeed.Client clientOf(FluxSink<Event<String>> sink) {
    return new LiveFeed.Client() {
      @Override
      public boolean isGone() {
        return sink.isCancelled();
      }

      @Override
      public void comment(String text) {
        // Micronaut writes one blank after the colon of every line itself.
        sink.next(Event.of("").comment(text.strip()));
      }

      @Override
      public void event(long globalOffset, Map<String, Object> frame) throws Exception {
        sink.next(
            Event.of(objectMapper.writeValueAsString(frame)).id(String.valueOf(globalOffset)));
      }

      @Override
      public void failed(Exception cause) {
        sink.error(cause);
      }

      @Override
      public void ended() {
        sink.complete();
      }
    };
  }

  /** The number of clients connected to the live feed. */
  public int openFeedConnections() {
    return liveFeed.openConnections();
  }

  /** Ends the open feeds when the application stops. */
  @PreDestroy
  void closeFeed() {
    liveFeed.close();
  }

  private Map<String, Object> toMap(EventEnvelope env) {
    // Convert the domain event (sealed interface / record) via Jackson so Micronaut's
    // serde-jackson only sees a plain Map<String, Object> and doesn't need @Serdeable on
    // domain event types that live in the shared domain module.
    TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
    Map<String, Object> payload = objectMapper.convertValue(env.event(), typeRef);
    return Map.of(
        "globalOffset", env.globalOffset().value(),
        "streamId", env.streamId().value(),
        "aggregateType", env.aggregateType().value(),
        "aggregateId", env.aggregateId().value(),
        "eventType", env.eventType().name(),
        "timestamp", env.metadata().timestamp().toString(),
        "payload", payload != null ? payload : Map.of());
  }
}
