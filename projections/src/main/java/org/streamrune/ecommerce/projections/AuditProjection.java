package org.streamrune.ecommerce.projections;

import java.util.LinkedHashMap;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;

// AT_LEAST_ONCE_IDEMPOTENT, declared: this projection keeps its read model in memory, so its writes
// can never share the runner's checkpoint transaction, and the runner hands it no
// transaction-scoped repository. A batch can be delivered twice WITHOUT a crash — a checkpoint
// commit that fails after process() returned (a lost connection, a failed COMMIT) is retried from
// the unchanged checkpoint, and a dead-letter replay re-applies a range — so the projection must
// be idempotent itself: entries are keyed by eventId and a redelivered event is ignored. (The
// in-memory model also does not survive a restart; it is a demo of the mode, not of durability.)
//
// It keeps the newest CAPACITY events and drops the oldest, so its memory stays the same however
// long the backend that leads it runs: a model that kept every event would hold the whole event log
// on that backend's heap. A redelivered batch is one of the newest, so the ids it carries are among
// the kept ones; an event delivered again after it was dropped would be recorded again, as the
// newest.
@org.streamrune.core.ProjectionConfig(
    name = "audit",
    deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
public class AuditProjection implements Projection {

  /** How many events the projection keeps unless it is told otherwise. */
  public static final int CAPACITY = 10_000;

  private final int capacity;

  /** The kept entries by event id, the oldest first. */
  private final LinkedHashMap<String, AuditEntry> entries = new LinkedHashMap<>();

  public AuditProjection() {
    this(CAPACITY);
  }

  /** A projection that keeps the newest {@code capacity} events. */
  AuditProjection(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity must be positive, got " + capacity);
    }
    this.capacity = capacity;
  }

  @Override
  public synchronized void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      var meta = envelope.metadata();
      String eventId = meta.eventId().value();
      if (entries.containsKey(eventId)) {
        continue; // redelivered: already recorded
      }
      entries.put(
          eventId,
          new AuditEntry(
              eventId,
              envelope.aggregateType().value(),
              envelope.aggregateId().value(),
              meta.userId() != null ? meta.userId().value() : null,
              meta.commandId().value(),
              meta.correlationId().value(),
              envelope.eventType().value(),
              envelope.event(),
              meta.timestamp()));
      if (entries.size() > capacity) {
        entries.pollFirstEntry();
      }
    }
  }

  public synchronized List<AuditEntry> listByAggregateType(String aggregateType) {
    return entries.values().stream().filter(e -> aggregateType.equals(e.aggregateType())).toList();
  }
}
