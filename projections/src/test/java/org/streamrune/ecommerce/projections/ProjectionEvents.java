package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;

/** Product and inventory events in envelopes, in global order, as a projection receives them. */
final class ProjectionEvents {

  private final AtomicLong offset = new AtomicLong();

  List<EventEnvelope> of(DomainEvent... events) {
    return Arrays.stream(events).map(this::envelope).toList();
  }

  private EventEnvelope envelope(DomainEvent event) {
    long at = offset.incrementAndGet();
    AggregateType aggregate =
        event instanceof InventoryEvent
            ? AggregateType.of("inventory")
            : AggregateType.of("product");
    return new EventEnvelope(
        GlobalOffset.of(at),
        StreamId.of(aggregate, AggregateId.of("s-" + at)),
        new Version(1),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of("c-" + at),
            null,
            UserId.of("u-1"),
            Instant.now()));
  }
}
