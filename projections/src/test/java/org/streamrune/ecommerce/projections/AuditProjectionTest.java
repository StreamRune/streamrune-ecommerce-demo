package org.streamrune.ecommerce.projections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.product.ProductState;

class AuditProjectionTest {

  record Ping(int n) implements DomainEvent {}

  private static StreamId stream(AggregateType type, String id) {
    return StreamId.of(type, AggregateId.of(id));
  }

  private static EventEnvelope envelope(StreamId stream, String eventType) {
    return envelope(1, stream, eventType);
  }

  private static EventEnvelope envelope(long offset, StreamId stream) {
    return envelope(offset, stream, "Ping");
  }

  private static EventEnvelope envelope(long offset, StreamId stream, String eventType) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        stream,
        new Version(offset),
        new EventType(eventType),
        new Ping((int) offset),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("c-" + offset),
            null,
            UserId.of("u-1"),
            Instant.now()));
  }

  @Test
  void declaresAtLeastOnce() {
    var cfg = AuditProjection.class.getAnnotation(ProjectionConfig.class);
    assertEquals("audit", cfg.name());
    assertEquals(ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT, cfg.deliveryMode());
  }

  @Test
  void aRedeliveredBatchDoesNotDuplicateEntries_andOrderIsKept() {
    var projection = new AuditProjection();
    var batch =
        List.of(
            envelope(1, stream(ProductState.TYPE, "p-1")),
            envelope(2, stream(ProductState.TYPE, "p-2")),
            envelope(3, stream(InventoryState.TYPE, "p-1")));
    projection.process(batch);
    projection.process(batch); // redelivery: the checkpoint commit failed after process()
    projection.process(List.of(envelope(4, stream(ProductState.TYPE, "p-4"))));
    var products = projection.listByAggregateType("product");
    assertEquals(3, products.size());
    assertEquals(
        List.of(1L, 2L, 4L),
        products.stream().map(e -> Long.parseLong(e.correlationId().substring(2))).toList());
    assertEquals(1, projection.listByAggregateType("inventory").size());
  }

  @Test
  void recordsTheEventsAggregateTypeAndId_withoutParsingTheStreamId() {
    var projection = new AuditProjection();
    var stream = StreamId.of(ProductState.TYPE, AggregateId.of("p-1"));
    projection.process(List.of(envelope(stream, "ProductCreated")));
    var entries = projection.listByAggregateType("product");
    assertThat(entries).hasSize(1);
    assertThat(entries.getFirst().aggregateType()).isEqualTo("product");
    assertThat(entries.getFirst().aggregateId()).isEqualTo("p-1");
    assertThat(projection.listByAggregateType("inventory")).isEmpty();
  }

  @Test
  void anIdContainingAColon_isRecordedWhole() {
    var projection = new AuditProjection();
    projection.process(List.of(envelope(stream(ProductState.TYPE, "a:b"), "ProductCreated")));
    assertThat(projection.listByAggregateType("product").getFirst().aggregateId()).isEqualTo("a:b");
  }
}
