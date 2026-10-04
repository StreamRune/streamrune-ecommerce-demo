package org.streamrune.ecommerce.projections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.runtime.PollingProjectionRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * {@code CustomerProjection} registered {@code TRANSACTIONAL_LOCAL}: every write it makes inside
 * {@code process}, the forget's delete included, goes into the batch's transaction. A delete that
 * bypassed it would run outside the transaction: it would miss a row the same batch inserts, so a
 * forgotten customer's row would survive the forget, and on PostgreSQL it would wait forever on the
 * row lock of a row the same batch updates.
 */
class CustomerProjectionTransactionalTest {

  private static final ProjectionName CUSTOMERS = ProjectionName.of("customers");
  private static final StreamId STREAM = StreamId.of(CustomerState.TYPE, AggregateId.of("c-1"));

  private final InMemoryProjectionRepository processor = new InMemoryProjectionRepository();
  private final InMemoryEventStore events = new InMemoryEventStore();

  private static EventEnvelope envelope(long version, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        STREAM,
        new Version(version),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("c-" + version),
            null,
            null,
            Instant.now()));
  }

  private static CustomerEvent.CustomerRegistered registered() {
    return new CustomerEvent.CustomerRegistered(
        "c-1", "Ada", "ada@example.com", "1 Main St", "555-0100");
  }

  /** Drains the stream through a transactional runner whose processor is the projection's store. */
  private void drain() {
    new PollingProjectionRunner(events, processor, 100, 100, processor)
        .run(CUSTOMERS, new CustomerProjection(processor), TRANSACTIONAL_LOCAL);
  }

  @Test
  void registerAndForgetInOneBatch_leaveNoRow() {
    events.append(
        STREAM,
        List.of(envelope(1, registered()), envelope(2, new CustomerEvent.CustomerForgotten("c-1"))),
        new Version(0));

    drain();

    assertEquals(GlobalOffset.of(2), processor.committedOffset(CUSTOMERS), "one batch committed");
    assertTrue(
        processor.findById(CUSTOMERS, "c-1", CustomerView.class).isEmpty(),
        "the forget in the same batch deletes the row the batch inserted");
  }

  @Test
  void updateAndForgetInOneBatch_overACommittedRow_leaveNoRow() {
    events.append(STREAM, List.of(envelope(1, registered())), new Version(0));
    drain();
    assertTrue(processor.findById(CUSTOMERS, "c-1", CustomerView.class).isPresent());

    events.append(
        STREAM,
        List.of(
            envelope(
                2,
                new CustomerEvent.ProfileUpdated(
                    "c-1", "Ada L.", "ada@example.com", "2 Main St", "555-0101")),
            envelope(3, new CustomerEvent.CustomerForgotten("c-1"))),
        new Version(1));
    drain();

    assertEquals(
        GlobalOffset.of(3), processor.committedOffset(CUSTOMERS), "both batches committed");
    assertTrue(
        processor.findById(CUSTOMERS, "c-1", CustomerView.class).isEmpty(),
        "the forget in the same batch deletes the row the batch updated");
  }
}
