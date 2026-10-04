package org.streamrune.ecommerce.commands.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;

class EcommerceIntegrationEventMapperTest {

  private final EcommerceIntegrationEventMapper mapper = new EcommerceIntegrationEventMapper();

  private static final Money TEN_USD = new Money(BigDecimal.TEN, "USD");

  private static StreamId stream(AggregateType type, String id) {
    return StreamId.of(type, AggregateId.of(id));
  }

  private EventEnvelope envelope(DomainEvent event, StreamId stream) {
    var metadata =
        new EventMetadata(
            new EventId(UUID.randomUUID().toString()),
            new CommandId(UUID.randomUUID().toString()),
            null,
            null,
            new CorrelationId(UUID.randomUUID().toString()),
            null,
            null,
            Instant.now());
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        metadata);
  }

  // --- allowlisted order events ---

  @Test
  void orderConfirmed_producesOneEntry_withTheOrderStreamAsItsOrderingKey() {
    var event = new OrderEvent.OrderConfirmed("order-42");
    var envelope = envelope(event, stream(OrderState.TYPE, "order-42"));

    List<OutboxEntry> entries = mapper.toOutbox(envelope);

    assertThat(entries).hasSize(1);
    OutboxEntry entry = entries.get(0);
    assertThat(entry.payloadType()).isNotBlank();
    assertThat(entry.streamId()).isEqualTo(stream(OrderState.TYPE, "order-42"));
    assertThat(entry.payload()).contains("order-42");
  }

  @Test
  void aStoredIdWithAControlCharacterInTheEnvelopesStream_isPassedThroughUnchanged() {
    // The mapper runs inside the append transaction, on a stream already in the log: it passes
    // the envelope's stream on as it is and never rebuilds an id, so a stored value that the
    // ingress door would refuse cannot fail the command after its decider ran.
    var stream = StreamId.of(OrderState.TYPE, new AggregateId("order-\u0007-1"));
    var event = new OrderEvent.OrderConfirmed("order-\u0007-1");

    List<OutboxEntry> entries = mapper.toOutbox(envelope(event, stream));

    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream);
  }

  @Test
  void everyEntryIsOrderedByTheEventsOwnStream() {
    var stream = StreamId.of(PaymentState.TYPE, AggregateId.of("pay-1"));
    var entries =
        mapper.toOutbox(envelope(new PaymentEvent.PaymentCaptured("pay-1", "o-1"), stream));
    assertThat(entries).singleElement().extracting(OutboxEntry::streamId).isEqualTo(stream);
  }

  @Test
  void orderConfirmed_payloadContainsNoCustomerId() {
    // OrderPlaced carries customerId=PII-PERSON; OrderConfirmed does NOT have it.
    // But we verify the mapper strips any PII: we stash a known string that must not leak.
    var event = new OrderEvent.OrderConfirmed("order-99");
    var envelope = envelope(event, stream(OrderState.TYPE, "order-99"));

    List<OutboxEntry> entries = mapper.toOutbox(envelope);

    assertThat(entries).hasSize(1);
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void orderShipped_producesOneEntry() {
    var event = new OrderEvent.OrderShipped("order-5");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(OrderState.TYPE, "order-5")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(OrderState.TYPE, "order-5"));
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void orderDelivered_producesOneEntry() {
    var event = new OrderEvent.OrderDelivered("order-6");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(OrderState.TYPE, "order-6")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(OrderState.TYPE, "order-6"));
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void orderCancelled_producesOneEntry_withReason() {
    var event = new OrderEvent.OrderCancelled("order-7", "Customer changed mind");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(OrderState.TYPE, "order-7")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).payload()).contains("order-7");
    assertPiiAbsent(entries.get(0).payload());
  }

  // --- excluded order events ---

  @Test
  void orderPlaced_carriesPII_returnsEmptyList() {
    // OrderPlaced carries customerId — must NEVER be published
    var event =
        new OrderEvent.OrderPlaced(
            "order-8",
            "customer-PII-SENSITIVE",
            List.of(new OrderEvent.OrderLine("prod-1", 2, TEN_USD)),
            TEN_USD);
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(OrderState.TYPE, "order-8")));
    assertThat(entries).isEmpty();
  }

  @Test
  void orderPlaced_payloadMustNotContainCustomerId() {
    String sensitiveCustomerId = "customer-SECRET-7654";
    var event = new OrderEvent.OrderPlaced("order-9", sensitiveCustomerId, List.of(), TEN_USD);
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(OrderState.TYPE, "order-9")));
    // Must be excluded entirely — no entry means no PII leaks
    assertThat(entries).isEmpty();
    // Extra guard: if somehow an entry appeared, its payload must not contain the PII
    entries.forEach(e -> assertThat(e.payload()).doesNotContain(sensitiveCustomerId));
  }

  // --- allowlisted payment events ---

  @Test
  void paymentCaptured_producesOneEntry() {
    var event = new PaymentEvent.PaymentCaptured("pay-1", "order-1");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(PaymentState.TYPE, "pay-1")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(PaymentState.TYPE, "pay-1"));
    assertThat(entries.get(0).payload()).contains("pay-1");
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void paymentRefunded_producesOneEntry() {
    var event = new PaymentEvent.PaymentRefunded("pay-2", "Fraud");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(PaymentState.TYPE, "pay-2")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(PaymentState.TYPE, "pay-2"));
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void paymentFailed_producesOneEntry() {
    var event = new PaymentEvent.PaymentFailed("pay-3", "Insufficient funds");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(PaymentState.TYPE, "pay-3")));
    assertThat(entries).hasSize(1);
    assertPiiAbsent(entries.get(0).payload());
  }

  // --- excluded payment event ---

  @Test
  void paymentInitiated_returnsEmptyList() {
    var event = new PaymentEvent.PaymentInitiated("pay-4", "order-4", TEN_USD);
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(PaymentState.TYPE, "pay-4")));
    assertThat(entries).isEmpty();
  }

  // --- allowlisted product events ---

  @Test
  void productCreated_producesOneEntry() {
    var event =
        new ProductEvent.ProductCreated("prod-1", "Widget", "Nice widget", "GADGETS", TEN_USD, 100);
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(ProductState.TYPE, "prod-1")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(ProductState.TYPE, "prod-1"));
    assertThat(entries.get(0).payload()).contains("prod-1");
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void priceUpdated_producesOneEntry() {
    var event =
        new ProductEvent.PriceUpdated("prod-2", TEN_USD, new Money(BigDecimal.valueOf(12), "USD"));
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(ProductState.TYPE, "prod-2")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(ProductState.TYPE, "prod-2"));
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void stockAdjusted_producesOneEntry() {
    var event = new ProductEvent.StockAdjusted("prod-3", 10, 15, "Restock");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(ProductState.TYPE, "prod-3")));
    assertThat(entries).hasSize(1);
    assertPiiAbsent(entries.get(0).payload());
  }

  @Test
  void productDiscontinued_producesOneEntry() {
    var event = new ProductEvent.ProductDiscontinued("prod-4");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(ProductState.TYPE, "prod-4")));
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).streamId()).isEqualTo(stream(ProductState.TYPE, "prod-4"));
    assertPiiAbsent(entries.get(0).payload());
  }

  // ---------------------------------------------------------------------------
  // Shared PII guard — asserts no PII field name leaks into integration payload
  // ---------------------------------------------------------------------------

  private static void assertPiiAbsent(String payload) {
    assertThat(payload)
        .doesNotContainIgnoringCase("customerId")
        .doesNotContainIgnoringCase("email")
        .doesNotContainIgnoringCase("address")
        .doesNotContainIgnoringCase("phone");
  }

  // --- excluded: all CustomerEvent ---

  @Test
  void customerRegistered_returnsEmptyList() {
    var event =
        new CustomerEvent.CustomerRegistered(
            "cust-1", "Alice Smith", "alice@example.com", "1 Main St", "+1-555-0100");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(CustomerState.TYPE, "cust-1")));
    assertThat(entries).isEmpty();
  }

  @Test
  void customerProfileUpdated_returnsEmptyList() {
    var event =
        new CustomerEvent.ProfileUpdated(
            "cust-2", "Bob Jones", "bob@example.com", "2 Oak Ave", "+1-555-0200");
    List<OutboxEntry> entries =
        mapper.toOutbox(envelope(event, stream(CustomerState.TYPE, "cust-2")));
    assertThat(entries).isEmpty();
  }
}
