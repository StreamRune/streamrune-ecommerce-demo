package org.streamrune.ecommerce.quarkus.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;

/**
 * HTTP event explorer surface: global event list, per-aggregate history, and a global SSE tail.
 *
 * <p>Mirrors Spring's {@code EventExplorerController} exactly — same paths, same response shape
 * ({@code globalOffset}, {@code streamId}, {@code aggregateType}, {@code aggregateId}, {@code
 * eventType}, {@code timestamp}, {@code payload}), same SSE frame format (id=globalOffset,
 * event=eventType, data=JSON).
 *
 * <p>The SSE tail spins a virtual thread that polls the event store ~1s when idle, consistent with
 * the Spring implementation.
 *
 * <p>Replaces the divergent {@code /api/events/orders} stub that previously lived in {@code
 * EventSseController}.
 */
@Path("/api/events")
@Produces(MediaType.APPLICATION_JSON)
public class EventExplorerController {

  @Inject EventStore eventStore;
  @Inject ObjectMapper objectMapper;

  @GET
  public List<Map<String, Object>> listEvents(
      @QueryParam("offset") @DefaultValue("0") long offset,
      @QueryParam("limit") @DefaultValue("50") int limit) {
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @GET
  @Path("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathParam("aggregateType") String aggregateType,
      @PathParam("aggregateId") String aggregateId) {
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
   * <p>Uses a bounded Mutiny emitter with back-pressure buffering: slow clients will have up to 256
   * events buffered; beyond that the stream fails and the client must reconnect.
   */
  @GET
  @Path("/sse")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  public Multi<jakarta.ws.rs.sse.OutboundSseEvent> sseStream() {
    return Multi.createFrom()
        .<jakarta.ws.rs.sse.OutboundSseEvent>emitter(
            emitter -> {
              Thread.ofVirtual()
                  .name("sse-global-event-stream")
                  .start(
                      () -> {
                        GlobalOffset offset = GlobalOffset.initial();
                        while (!emitter.isCancelled()) {
                          try {
                            var events = eventStore.readGlobalStream(offset, 100);
                            for (var env : events) {
                              if (emitter.isCancelled()) break;
                              var data = objectMapper.writeValueAsString(toMap(env));
                              emitter.emit(
                                  new SseEventFrame(
                                      String.valueOf(env.globalOffset().value()),
                                      env.eventType().name(),
                                      data));
                              offset = env.globalOffset().next();
                            }
                            if (events.isEmpty()) {
                              Thread.sleep(Duration.ofSeconds(1));
                            }
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                          } catch (Exception e) {
                            emitter.fail(e);
                            return;
                          }
                        }
                        if (!emitter.isCancelled()) {
                          emitter.complete();
                        }
                      });
            },
            BackPressureStrategy.ERROR)
        .onOverflow()
        .buffer(256);
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
   * SSE frame with explicit event type name (matches Spring's {@code
   * ServerSentEvent.event(eventType)} wire format).
   */
  private record SseEventFrame(String id, String name, String data)
      implements jakarta.ws.rs.sse.OutboundSseEvent {

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
      return name;
    }

    @Override
    public String getComment() {
      return null;
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
