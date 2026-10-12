package org.streamrune.ecommerce.queries.access;

import java.util.LinkedHashMap;
import java.util.Map;
import org.streamrune.core.EventEnvelope;

/**
 * What the open live feed, {@code GET /api/events/sse}, says about one event: that an event of this
 * type happened to an aggregate of this type, when, and where it stands in the global stream. The
 * three applications build every frame of the feed here.
 *
 * <p><b>A frame names no stream.</b> It carries no stream id, no aggregate id, no version within a
 * stream and no payload. A stream id holds a customer id or an order id, and the feed cannot ask
 * who is calling: a browser's {@code EventSource} sends no {@code X-User-Id} and no {@code
 * X-User-Role}, so every connection is anonymous. What an anonymous caller may not learn is
 * therefore left out for everyone, an {@code ADMIN} included. A caller of the feed learns that a
 * customer registered or that an order was placed, and when; not which customer, and not which
 * order.
 *
 * <p><b>Where the detail is.</b> A view that wants to know what changed reads it from the guarded
 * endpoints, with the caller's two headers: the customer and order reads ({@link
 * OwnerOrAdminAccess}), and for an {@code ADMIN} the event history, {@code GET /api/events}. The
 * frame is the signal to read again; the aggregate type and the event type say which read.
 */
public final class LiveFeedFrame {

  private LiveFeedFrame() {}

  /**
   * The frame of one event, with its fields in the order they are written.
   *
   * @param event an event of the global stream
   * @return {@code globalOffset}, {@code aggregateType}, {@code eventType} and {@code timestamp},
   *     and nothing else
   */
  public static Map<String, Object> of(EventEnvelope event) {
    Map<String, Object> frame = new LinkedHashMap<>();
    frame.put("globalOffset", event.globalOffset().value());
    frame.put("aggregateType", event.aggregateType().value());
    frame.put("eventType", event.eventType().name());
    frame.put("timestamp", event.metadata().timestamp().toString());
    return frame;
  }
}
