package org.streamrune.ecommerce.micronaut.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.sse.Event;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
 * <p>Same paths, response shape and frame format as the Spring app's {@code
 * EventExplorerController}. The feed reads the global stream from its first event and then polls
 * once a second; a slow client is cut off after 256 buffered events. Its frames are unnamed (no
 * {@code event:} field), so {@code EventSource.onmessage} receives every one of them.
 */
@Controller("/api/events")
public class EventExplorerController {

  /** Maximum number of events buffered for a slow SSE client before the stream is terminated. */
  static final int SLOW_CLIENT_BUFFER = 256;

  @Inject EventStore eventStore;
  @Inject ObjectMapper objectMapper;

  @Get
  public List<Map<String, Object>> listEvents(
      @QueryValue(value = "offset", defaultValue = "0") long offset,
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    AdminController.requireRole("ADMIN");
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @Get("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    AdminController.requireRole("ADMIN");
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  /**
   * The live feed: every event of the global stream as a Server-Sent Event, open to every caller.
   *
   * <p>Frame wire format (matching Spring): {@code id} = globalOffset, no {@code event} name,
   * {@code data} = JSON map
   * (globalOffset/streamId/aggregateType/aggregateId/eventType/version/timestamp), without the
   * event's payload.
   *
   * <p>Uses a Flux with back-pressure buffering: slow clients will have up to {@link
   * #SLOW_CLIENT_BUFFER} events buffered; beyond that the stream fails and the client must
   * reconnect.
   */
  @Get("/sse")
  @Produces(MediaType.TEXT_EVENT_STREAM)
  public Flux<Event<String>> sseStream() {
    return Flux.<Event<String>>create(
            sink -> {
              Thread.ofVirtual()
                  .name("sse-global-event-stream")
                  .start(
                      () -> {
                        GlobalOffset offset = GlobalOffset.initial();
                        while (!sink.isCancelled()) {
                          try {
                            var events = eventStore.readGlobalStream(offset, 100);
                            for (var env : events) {
                              if (sink.isCancelled()) break;
                              var data = objectMapper.writeValueAsString(toFrame(env));
                              sink.next(
                                  Event.of(data).id(String.valueOf(env.globalOffset().value())));
                              offset = env.globalOffset().next();
                            }
                            if (events.isEmpty()) {
                              Thread.sleep(Duration.ofSeconds(1));
                            }
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                          } catch (Exception e) {
                            sink.error(e);
                            return;
                          }
                        }
                        if (!sink.isCancelled()) {
                          sink.complete();
                        }
                      });
            },
            FluxSink.OverflowStrategy.ERROR)
        .onBackpressureBuffer(SLOW_CLIENT_BUFFER);
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
