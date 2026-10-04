package org.streamrune.ecommerce.projections;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;

// AT_LEAST_ONCE_IDEMPOTENT, declared: this projection keeps its read model in memory, so its writes
// can never share the runner's checkpoint transaction, and the runner hands it no
// transaction-scoped repository. A batch can be delivered twice WITHOUT a crash — a checkpoint
// commit that fails after process() returned (a lost connection, a failed COMMIT) is retried from
// the unchanged checkpoint, and a dead-letter replay re-applies a range — so the projection must
// be idempotent itself: entries are keyed by eventId and a redelivered event is ignored. (The
// in-memory list also does not survive a restart; it is a demo of the mode, not of durability.)
@org.streamrune.core.ProjectionConfig(
    name = "audit",
    deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
public class AuditProjection implements Projection {
  private final List<AuditEntry> entries = new CopyOnWriteArrayList<>();
  private final Set<String> seenEventIds = ConcurrentHashMap.newKeySet();

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      var meta = envelope.metadata();
      String eventId = meta.eventId().value();
      if (!seenEventIds.add(eventId)) {
        continue; // redelivered: already recorded
      }
      entries.add(
          new AuditEntry(
              eventId,
              envelope.aggregateType().value(),
              envelope.aggregateId().value(),
              meta.userId() != null ? meta.userId().value() : null,
              meta.commandId().value(),
              meta.correlationId().value(),
              envelope.eventType().name(),
              envelope.event(),
              meta.timestamp()));
    }
  }

  public List<AuditEntry> listByAggregateType(String aggregateType) {
    return entries.stream().filter(e -> aggregateType.equals(e.aggregateType())).toList();
  }
}
