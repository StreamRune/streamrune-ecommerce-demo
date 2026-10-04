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
 * HTTP event explorer surface: global event list, per-aggregate history, and a global SSE tail.
 *
 * <p>Mirrors Spring's {@code EventExplorerController} exactly — same paths, same response shape
 * ({@code globalOffset}, {@code streamId}, {@code aggregateType}, {@code aggregateId}, {@code
 * eventType}, {@code timestamp}, {@code payload}), same SSE frame format (id=globalOffset,
 * event=eventType, data=JSON).
 *
 * <p>The SSE tail spins a virtual thread that polls the event store ~1s when idle, consistent with
 * the Spring implementation. Back-pressure is bounded to 256 events for slow clients.
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
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @Get("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathVariable String aggregateType, @PathVariable String aggregateId) {
    // Both path values are client input: build them through the ingress doors.
    var stream = StreamId.of(AggregateType.of(aggregateType), AggregateId.of(aggregateId));
    return eventStore.load(stream).events().stream().map(this::toMap).toList();
  }

  /**
   * Global SSE tail: emits every new event from the global stream as a Server-Sent Event.
   *
   * <p>Frame wire format (matching Spring): {@code id} = globalOffset, {@code event} = eventType,
   * {@code data} = JSON map
   * (globalOffset/streamId/aggregateType/aggregateId/eventType/timestamp/payload).
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
                              var data = objectMapper.writeValueAsString(toMap(env));
                              sink.next(
                                  Event.of(data)
                                      .id(String.valueOf(env.globalOffset().value()))
                                      .name(env.eventType().name()));
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
