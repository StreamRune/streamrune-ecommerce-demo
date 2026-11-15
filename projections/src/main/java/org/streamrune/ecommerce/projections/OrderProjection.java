package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.Page;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.order.*;
import org.streamrune.ecommerce.domain.payment.PaymentEvent;
import org.streamrune.ecommerce.domain.payment.PaymentStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.paging.ListPage;

@org.streamrune.core.ProjectionConfig(
    name = "orders",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class OrderProjection extends BaseProjection {

  public OrderProjection(ProjectionRepository repository) {
    super(repository, "orders");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      Instant at = envelope.metadata().timestamp();
      switch (envelope.event()) {
        case OrderEvent evt -> applyOrderEvent(evt, at);
        case PaymentEvent.PaymentInitiated e ->
            updatePayment(e.orderId(), PaymentStatus.PENDING, at);
        case PaymentEvent.PaymentCaptured e ->
            updatePayment(e.orderId(), PaymentStatus.CAPTURED, at);
        case PaymentEvent.PaymentFailed e -> updatePayment(e.orderId(), PaymentStatus.FAILED, at);
        case PaymentEvent.PaymentVoided e -> updatePayment(e.orderId(), PaymentStatus.VOIDED, at);
        case PaymentEvent.PaymentRefunded e ->
            updatePayment(e.orderId(), PaymentStatus.REFUNDED, at);
        default -> {}
      }
    }
  }

  private void applyOrderEvent(OrderEvent evt, Instant at) {
    switch (evt) {
      case OrderEvent.OrderPlaced e ->
          save(
              e.orderId(),
              new OrderView(
                  e.orderId(),
                  e.customerId(),
                  e.lines().stream()
                      .map(
                          l ->
                              new OrderView.OrderLineView(
                                  l.productId(), l.quantity(), l.unitPrice()))
                      .toList(),
                  e.total(),
                  OrderStatus.CREATED,
                  null,
                  null,
                  at,
                  at));
      case OrderEvent.OrderConfirmed e -> updateStatus(e.orderId(), OrderStatus.CONFIRMED, at);
      case OrderEvent.OrderShipped e -> updateStatus(e.orderId(), OrderStatus.SHIPPED, at);
      case OrderEvent.OrderDelivered e -> updateStatus(e.orderId(), OrderStatus.DELIVERED, at);
      case OrderEvent.OrderCancelled e -> cancel(e.orderId(), e.reason(), at);
    }
  }

  /** The status of the order's row; its payment status and reason stay as they are. */
  private void updateStatus(String id, OrderStatus status, Instant timestamp) {
    findById(id, OrderView.class)
        .ifPresent(
            existing ->
                save(
                    id,
                    new OrderView(
                        existing.orderId(),
                        existing.customerId(),
                        existing.lines(),
                        existing.total(),
                        status,
                        existing.paymentStatus(),
                        existing.reason(),
                        existing.createdAt(),
                        timestamp)));
  }

  /**
   * The order cancelled, with the reason of its {@code OrderCancelled}. An order cancelled twice
   * (its customer, then the fulfillment saga) shows the first cancellation's reason, even when that
   * one gave none: the second cancellation changes nothing the customer sees.
   */
  private void cancel(String id, String reason, Instant timestamp) {
    findById(id, OrderView.class)
        .ifPresent(
            existing ->
                save(
                    id,
                    new OrderView(
                        existing.orderId(),
                        existing.customerId(),
                        existing.lines(),
                        existing.total(),
                        OrderStatus.CANCELLED,
                        existing.paymentStatus(),
                        existing.status() == OrderStatus.CANCELLED ? existing.reason() : reason,
                        existing.createdAt(),
                        timestamp)));
  }

  /**
   * The payment status of the order's row; a payment of an order the read model does not hold
   * changes nothing.
   */
  private void updatePayment(String orderId, PaymentStatus paymentStatus, Instant timestamp) {
    findById(orderId, OrderView.class)
        .ifPresent(
            existing ->
                save(
                    orderId,
                    new OrderView(
                        existing.orderId(),
                        existing.customerId(),
                        existing.lines(),
                        existing.total(),
                        existing.status(),
                        paymentStatus,
                        existing.reason(),
                        existing.createdAt(),
                        timestamp)));
  }

  public OrderView get(String orderId) {
    return findById(orderId, OrderView.class).orElse(null);
  }

  /**
   * The order of every list of orders: newest first, by {@code createdAt} descending, then by order
   * id ascending for orders placed at the same instant. The repository sorts on the row's JSON, so
   * {@code createdAt} is compared as the ISO-8601 text the row holds ({@link #NEWEST_FIRST_ROWS}).
   * That is time order, except between two instants of the same second whose fractions print with
   * different lengths, which it orders by their text. The in-memory read compares the same text
   * ({@link #NEWEST_FIRST}), so both reads agree on it. The id is compared by the database in its
   * collation and in memory by {@link String#compareTo}; the two differ only for ids that differ in
   * letter case or punctuation, and only between orders placed at the same instant.
   */
  public static final Sort NEWEST_FIRST_ROWS =
      new Sort(
          List.of(
              new Sort.Order("createdAt", SortDirection.DESC),
              new Sort.Order("id", SortDirection.ASC)));

  /** {@link #NEWEST_FIRST_ROWS} for orders in memory. */
  public static final Comparator<OrderView> NEWEST_FIRST =
      Comparator.comparing((OrderView order) -> order.createdAt().toString())
          .reversed()
          .thenComparing(OrderView::orderId);

  /** One page of every order, newest first; the repository pages and sorts the table. */
  public Page<OrderView> page(ListPage page) {
    return repository.findAll(projectionName(), OrderView.class, page.in(NEWEST_FIRST_ROWS));
  }

  /** Every order of one customer. */
  public List<OrderView> listByCustomer(String customerId) {
    return ordersOf(customerId);
  }

  /**
   * One page of one customer's orders, newest first. The repository has no read by a field of the
   * row, so this reads the whole order table, keeps the customer's rows in memory, sorts them and
   * cuts the page out of them: only the answer is bounded, not the read. A read model with real
   * volumes gives its repository a query by {@code customerId}.
   */
  public Page<OrderView> pageByCustomer(String customerId, ListPage page) {
    return page.slice(ordersOf(customerId).stream().sorted(NEWEST_FIRST).toList());
  }

  private List<OrderView> ordersOf(String customerId) {
    return repository.findAll(projectionName(), OrderView.class).stream()
        .filter(o -> customerId.equals(o.customerId()))
        .toList();
  }
}
