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
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

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
 * once a second. Its frames are unnamed (no {@code event:} field), so {@code EventSource.onmessage}
 * receives every one of them.
 */
@Path("/api/events")
@Produces(MediaType.APPLICATION_JSON)
public class EventExplorerController {

  @Inject EventStore eventStore;
  @Inject ObjectMapper objectMapper;
  @Inject StreamRuneRequestContextHolder requestContext;

  @GET
  public List<Map<String, Object>> listEvents(
      @QueryParam("offset") @DefaultValue("0") long offset,
      @QueryParam("limit") @DefaultValue("50") int limit) {
    AdminController.requireRole(requestContext, "ADMIN");
    return eventStore.readGlobalStream(new GlobalOffset(offset), limit).stream()
        .map(this::toMap)
        .toList();
  }

  @GET
  @Path("/{aggregateType}/{aggregateId}")
  public List<Map<String, Object>> streamEvents(
      @PathParam("aggregateType") String aggregateType,
      @PathParam("aggregateId") String aggregateId) {
    AdminController.requireRole(requestContext, "ADMIN");
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
                              var data = objectMapper.writeValueAsString(toFrame(env));
                              emitter.emit(
                                  new SseEventFrame(
                                      String.valueOf(env.globalOffset().value()), data));
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

  /** An unnamed SSE frame: an id and a data line, as the Spring app sends it. */
  private record SseEventFrame(String id, String data)
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
      return null;
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
