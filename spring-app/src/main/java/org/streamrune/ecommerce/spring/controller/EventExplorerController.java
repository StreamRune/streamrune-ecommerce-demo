package org.streamrune.ecommerce.spring.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
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
 * address in plain text. They are gated with the same {@code requireAdmin} check as the admin
 * endpoints.
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
 * <p><b>A connection starts at the head.</b> The feed sends a frame for every event stored while
 * the client is connected and nothing of what the store held before, so no caller makes the app
 * read its history: {@link LiveFeed} reads, one reader per connection, and the reader ends when its
 * client leaves. The frames are unnamed (no {@code event:} field), so {@code EventSource.onmessage}
 * receives every one of them.
 */
@RestController
@RequestMapping("/api/events")
public class EventExplorerController implements SmartLifecycle {

  /** Maximum number of frames buffered for a slow feed client before its stream is failed. */
  static final int SLOW_CLIENT_BUFFER = 256;

  private final EventStore eventStore;
  private final ObjectMapper objectMapper;
  private final LiveFeed liveFeed = new LiveFeed();
  private volatile boolean feedOpen;

  public EventExplorerController(EventStore eventStore, ObjectMapper objectMapper) {
    this.eventStore = eventStore;
    this.objectMapper = objectMapper;
  }

  @GetMapping
  public List<Map<String, Object>> listEvents(
      @RequestParam(defaultValue = "0") long offset, @RequestParam(defaultValue = "50") int limit) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @GetMapping("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  /**
   * The live feed: a frame for every event stored while the client is connected, open to every
   * caller. {@link LiveFeed} reads; this method writes what it reads as Server-Sent Events. A
   * client that reads too slowly has up to {@link #SLOW_CLIENT_BUFFER} frames buffered; beyond that
   * the stream fails and the client reconnects.
   */
  @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> sseStream() {
    return Flux.<ServerSentEvent<String>>create(
            sink -> liveFeed.follow(eventStore, clientOf(sink)), FluxSink.OverflowStrategy.ERROR)
        .onBackpressureBuffer(SLOW_CLIENT_BUFFER);
  }

  /** The client behind one response stream: what the feed's reader sends becomes its frames. */
  private LiveFeed.Client clientOf(FluxSink<ServerSentEvent<String>> sink) {
    return new LiveFeed.Client() {
      @Override
      public boolean isGone() {
        return sink.isCancelled();
      }

      @Override
      public void comment(String text) {
        sink.next(ServerSentEvent.<String>builder().comment(text).build());
      }

      @Override
      public void event(long globalOffset, Map<String, Object> frame) throws Exception {
        sink.next(
            ServerSentEvent.<String>builder()
                .id(String.valueOf(globalOffset))
                .data(objectMapper.writeValueAsString(frame))
                .build());
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

  // --- SmartLifecycle: the open feeds end before the web server waits for requests in flight ---

  @Override
  public void start() {
    liveFeed.open();
    feedOpen = true;
  }

  /**
   * Ends the open feeds. An open feed is a request that never finishes on its own, and Spring
   * Boot's graceful shutdown waits for the requests in flight before it stops the web server. This
   * bean has the default phase, the last to start and the first to stop: the feeds are told to end
   * before that wait begins and are complete within one polling interval.
   */
  @Override
  public void stop() {
    liveFeed.close();
    feedOpen = false;
  }

  @Override
  public boolean isRunning() {
    return feedOpen;
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
}
