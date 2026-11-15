package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
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
   * The order of every list of orders: newest first, by {@code createdAt} descending. The
   * repository breaks a tie by the order id, ascending, so the order is total and a page never
   * overlaps the next on a list that does not change. It compares {@code createdAt} as the ISO-8601
   * text the row holds, by code point: time order, except between two instants of the same second
   * whose fractions print with different lengths.
   */
  public static final Sort NEWEST_FIRST =
      new Sort(List.of(new Sort.Order("createdAt", SortDirection.DESC)));

  /** The index that serves an {@code ADMIN}'s whole list, newest first ({@link #page}). */
  public static final ProjectionIndex NEWEST_FIRST_INDEX =
      ProjectionIndex.forSort(ProjectionName.of("orders"), NEWEST_FIRST);

  /**
   * The index that serves one customer's list, newest first ({@link #pageByCustomer}, {@link
   * #listByCustomer}): the customer id, then {@code createdAt} descending, then the id.
   */
  public static final ProjectionIndex CUSTOMER_NEWEST_FIRST_INDEX =
      ProjectionIndex.forFilter(ProjectionName.of("orders"), "customerId", NEWEST_FIRST);

  /** The most rows {@link #listByCustomer} reads in one statement. */
  private static final int LIST_PAGE_SIZE = 1000;

  /** One page of every order, newest first; the repository sorts and pages the table. */
  public Page<OrderView> page(ListPage page) {
    return repository.findAll(projectionName(), OrderView.class, page.in(NEWEST_FIRST));
  }

  /**
   * One page of one customer's orders, newest first. The repository selects the customer's rows,
   * sorts and pages them in one statement, served by {@link #CUSTOMER_NEWEST_FIRST_INDEX}.
   */
  public Page<OrderView> pageByCustomer(String customerId, ListPage page) {
    return repository.findAllBy(
        projectionName(), OrderView.class, ofCustomer(customerId), page.in(NEWEST_FIRST));
  }

  /**
   * Every order of one customer, newest first, read through the same filtered read a thousand rows
   * at a time.
   */
  public List<OrderView> listByCustomer(String customerId) {
    List<OrderView> orders = new ArrayList<>();
    for (int page = 0; ; page++) {
      Page<OrderView> rows =
          repository.findAllBy(
              projectionName(),
              OrderView.class,
              ofCustomer(customerId),
              PageRequest.of(page, LIST_PAGE_SIZE, NEWEST_FIRST));
      orders.addAll(rows.content());
      if (!rows.hasNext()) {
        return orders;
      }
    }
  }

  private static FieldEquals ofCustomer(String customerId) {
    return FieldEquals.of("customerId", customerId);
  }
}
