package org.streamrune.ecommerce.spring.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * The demo's window on the event store: the global event list, one stream's history, and a live
 * feed of new events.
 *
 * <p><b>The list and the stream history are ADMIN only.</b> They return each event's payload as the
 * event store reads it, and the store decrypts {@code @Encrypted} fields on the way out. Until a
 * customer's key is erased, these two endpoints therefore show that customer's name, e-mail and
 * address in plain text. They are gated with the same {@code requireRole("ADMIN")} check as the
 * mutating admin endpoints.
 *
 * <p><b>DEMO SHORTCUT, protect this in production.</b> The role is whatever the client sent in
 * {@code X-User-Role} (the gateway stand-in chapter 9 describes), so the check keeps the tutorial
 * honest but protects nothing against a caller who sets the header. A real deployment authenticates
 * the caller, derives the role from that identity, and does not expose raw event payloads outside
 * an operator's tooling.
 *
 * <p><b>The live feed is open and carries no payload.</b> The browser's {@code EventSource} cannot
 * send the role header, and the dashboard only needs to know that something happened to a stream. A
 * frame holds the event's global offset, its type, the stream it belongs to, its version in that
 * stream and its timestamp, never the event itself.
 *
 * <p>The feed reads the global stream from its first event and then polls once a second. Its frames
 * are unnamed (no {@code event:} field), so {@code EventSource.onmessage} receives every one of
 * them.
 */
@RestController
@RequestMapping("/api/events")
public class EventExplorerController {

  private final EventStore eventStore;
  private final ObjectMapper objectMapper;

  public EventExplorerController(EventStore eventStore, ObjectMapper objectMapper) {
    this.eventStore = eventStore;
    this.objectMapper = objectMapper;
  }

  @GetMapping
  public List<Map<String, Object>> listEvents(
      @RequestParam(defaultValue = "0") long offset, @RequestParam(defaultValue = "50") int limit) {
    AdminController.requireRole("ADMIN");
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @GetMapping("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    AdminController.requireRole("ADMIN");
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> sseStream() {
    Sinks.Many<ServerSentEvent<String>> sink = Sinks.many().unicast().onBackpressureBuffer();

    Thread.ofVirtual()
        .name("sse-event-stream")
        .start(
            () -> {
              GlobalOffset offset = GlobalOffset.initial();
              while (sink.currentSubscriberCount() > 0 || !Thread.currentThread().isInterrupted()) {
                try {
                  var events = eventStore.readGlobalStream(offset, 100);
                  for (var env : events) {
                    var data = objectMapper.writeValueAsString(toFrame(env));
                    sink.tryEmitNext(
                        ServerSentEvent.<String>builder()
                            .id(String.valueOf(env.globalOffset().value()))
                            .data(data)
                            .build());
                    offset = env.globalOffset().next();
                  }
                  if (events.isEmpty()) {
                    Thread.sleep(Duration.ofSeconds(1));
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  sink.tryEmitError(e);
                  return;
                }
              }
              sink.tryEmitComplete();
            });

    return sink.asFlux();
  }

  /**
   * What the live feed says about an event: which one, of which stream, and when. No payload — the
   * feed is open to every caller.
   */
  private static Map<String, Object> toFrame(EventEnvelope env) {
    return Map.of(
        "globalOffset", env.globalOffset().value(),
        "streamId", env.streamId().value(),
        "aggregateType", env.aggregateType().value(),
        "aggregateId", env.aggregateId().value(),
        "eventType", env.eventType().name(),
        "version", env.version().value(),
        "timestamp", env.metadata().timestamp().toString());
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
