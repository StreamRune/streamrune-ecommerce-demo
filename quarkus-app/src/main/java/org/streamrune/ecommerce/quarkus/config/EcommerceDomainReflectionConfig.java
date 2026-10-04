package org.streamrune.ecommerce.quarkus.config;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.streamrune.ecommerce.commands.integration.EcommerceIntegrationEventMapper;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.customer.CustomerStatus;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.domain.payment.PaymentCommand;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.payment.PaymentStatus;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.domain.product.ProductStatus;
import org.streamrune.ecommerce.domain.saga.OrderFulfillmentStatus;
import org.streamrune.ecommerce.projections.AuditEntry;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.dto.ProductView;
import org.streamrune.ecommerce.queries.dto.SagaView;
import org.streamrune.ecommerce.queries.query.ComplianceReportQuery;
import org.streamrune.ecommerce.queries.query.GetOrderById;
import org.streamrune.ecommerce.queries.query.ListOrders;
import org.streamrune.ecommerce.queries.query.ListProducts;

/**
 * Registers all demo domain types for GraalVM native-image reflection.
 *
 * <p>StreamRune's Jackson serialisation and {@code CryptoShreddingModule} access event/state
 * records via {@link Class#getRecordComponents()} at runtime. In a native image every record whose
 * components are inspected this way must be explicitly registered at build time; missing even one
 * nested value object causes an {@code UnsupportedFeatureError} that only surfaces after the 10-30
 * min native compile finishes.
 *
 * <p>The framework core types are covered by {@link
 * org.streamrune.quarkus.StreamRuneReflectionConfig}. This class covers the demo's own domain
 * types: events, states, commands, the saga state, value objects (e.g. {@link Money}, {@link
 * OrderEvent.OrderLine}), enums, and query view DTOs — everything in the shared {@code domain/},
 * {@code commands/}, {@code queries/}, and {@code projections/} modules.
 *
 * <p><b>Sealed roots are registered elsewhere.</b> The five sealed command interfaces and the five
 * sealed event interfaces are listed in {@code
 * META-INF/native-image/org.streamrune.ecommerce/quarkus-app/reachability-metadata.json}, not here.
 * A native image answers {@code Class#getPermittedSubclasses()} only for a sealed type registered
 * for reflection, and StreamRune expands a sealed root through that call — the dead-letter runner's
 * {@code registerCommand(InventoryCommand.class)} (see {@link
 * StreamRuneProducer#deadLetterRetryRunner}), the {@code @Encrypted} and authorization startup
 * checks over the registered deciders' command roots — refusing to start the application when a
 * root reports none. {@code @RegisterForReflection} on a root does not make the image list its
 * permitted subclasses (measured: Quarkus writes a legacy {@code reflect-config.json} entry without
 * {@code allPermittedSubclasses}, and the binary still refused {@code PaymentCommand}); a {@code
 * {"type": ...}} entry in {@code reachability-metadata.json} does. {@code
 * EcommerceDomainReflectionConfigTest} pins that every sealed supertype of a type registered here
 * is listed there.
 */
@RegisterForReflection(
    targets = {
      // ── value objects ────────────────────────────────────────────────────────
      Money.class,

      // ── product ──────────────────────────────────────────────────────────────
      ProductEvent.ProductCreated.class,
      ProductEvent.PriceUpdated.class,
      ProductEvent.StockAdjusted.class,
      ProductEvent.ProductDiscontinued.class,
      ProductState.class,
      ProductStatus.class,
      ProductCommand.CreateProduct.class,
      ProductCommand.UpdatePrice.class,
      ProductCommand.AdjustStock.class,
      ProductCommand.DiscontinueProduct.class,

      // ── order ────────────────────────────────────────────────────────────────
      OrderEvent.OrderPlaced.class,
      OrderEvent.OrderConfirmed.class,
      OrderEvent.OrderShipped.class,
      OrderEvent.OrderDelivered.class,
      OrderEvent.OrderCancelled.class,
      OrderEvent.OrderLine.class,
      OrderState.class,
      OrderStatus.class,
      OrderCommand.PlaceOrder.class,
      OrderCommand.ConfirmOrder.class,
      OrderCommand.ShipOrder.class,
      OrderCommand.DeliverOrder.class,
      OrderCommand.CancelOrder.class,
      OrderCommand.OrderLine.class,

      // ── customer ─────────────────────────────────────────────────────────────
      CustomerEvent.CustomerRegistered.class,
      CustomerEvent.ProfileUpdated.class,
      CustomerEvent.DataExportRequested.class,
      CustomerEvent.CustomerForgotten.class,
      CustomerState.class,
      CustomerStatus.class,
      CustomerCommand.RegisterCustomer.class,
      CustomerCommand.UpdateProfile.class,
      CustomerCommand.RequestDataExport.class,
      CustomerCommand.ForgetCustomer.class,

      // ── payment ──────────────────────────────────────────────────────────────
      PaymentEvent.PaymentInitiated.class,
      PaymentEvent.PaymentCaptured.class,
      PaymentEvent.PaymentRefunded.class,
      PaymentEvent.PaymentFailed.class,
      PaymentState.class,
      PaymentStatus.class,
      PaymentCommand.InitiatePayment.class,
      PaymentCommand.CapturePayment.class,
      PaymentCommand.RefundPayment.class,
      PaymentCommand.FailPayment.class,

      // ── inventory ────────────────────────────────────────────────────────────
      InventoryEvent.StockReserved.class,
      InventoryEvent.StockReleased.class,
      InventoryEvent.ReservationConfirmed.class,
      InventoryEvent.ShipmentReceived.class,
      InventoryState.class,
      InventoryCommand.ReserveStock.class,
      InventoryCommand.ReleaseStock.class,
      InventoryCommand.ConfirmReservation.class,
      InventoryCommand.ReceiveShipment.class,

      // ── saga ─────────────────────────────────────────────────────────────────
      OrderFulfillmentState.class,
      OrderFulfillmentStatus.class,

      // ── query view DTOs ───────────────────────────────────────────────────────
      ProductView.class,
      OrderView.class,
      OrderView.OrderLineView.class,
      CustomerView.class,
      InventoryView.class,
      SagaView.class,

      // ── query records ─────────────────────────────────────────────────────────
      GetOrderById.class,
      ListOrders.class,
      ListProducts.class,
      ComplianceReportQuery.class,

      // ── projections data ──────────────────────────────────────────────────────
      AuditEntry.class,

      // ── outbox integration DTOs ───────────────────────────────────────────────
      // Jackson serialises the mapper's package-private nested DTO records when an event is
      // appended; registering the mapper registers them (ignoreNested = false).
      EcommerceIntegrationEventMapper.class,
    })
public final class EcommerceDomainReflectionConfig {}
