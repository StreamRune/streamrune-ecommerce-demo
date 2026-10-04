package org.streamrune.ecommerce.projections;

import java.time.Instant;

public record AuditEntry(
    String eventId,
    String aggregateType,
    String aggregateId,
    String userId,
    String commandId,
    String correlationId,
    String eventType,
    Object eventData,
    Instant timestamp) {}
