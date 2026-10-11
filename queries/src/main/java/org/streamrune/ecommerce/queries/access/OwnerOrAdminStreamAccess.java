package org.streamrune.ecommerce.queries.access;

import java.util.function.Function;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * Who may open the live event stream of one aggregate, {@code GET
 * /api/sse/{aggregateType}/{aggregateId}}: an authenticated caller who holds the {@code ADMIN}
 * role, or the customer the stream belongs to. Everyone else is refused, and so is every aggregate
 * type this class does not name.
 *
 * <p>A frame of that endpoint is the event as the event store reads it, with {@code @Encrypted}
 * fields decrypted. This rule is therefore what keeps one customer's name, e-mail, address, phone
 * and purchases away from every other caller.
 *
 * <p><b>The rule per aggregate type.</b> A caller without an identity is refused before the type is
 * looked at; {@code ADMIN} may open every stream of the five types below.
 *
 * <ul>
 *   <li>{@code customer}: the caller's id equals the aggregate id. The events carry the customer's
 *       personal data.
 *   <li>{@code order}: the order read model names the caller as the order's customer. {@code
 *       OrderProjection} writes that read model after the event is stored, so for a moment after
 *       {@code POST /api/orders} the order has no row. An order without a row has no known owner
 *       and is refused, exactly like an order that was never placed; the customer subscribes once
 *       {@code GET /api/orders/{id}} answers. The order's customer is the customer the order names,
 *       and {@code POST /api/orders} accepts an order only from that customer or an {@code ADMIN}.
 *   <li>{@code payment}: {@code ADMIN} only. The events carry the amount and the refund or failure
 *       reason of one customer's purchase, and no read model of the demo links a payment to its
 *       customer, so ownership cannot be shown.
 *   <li>{@code inventory}: {@code ADMIN} only. A product's stock events name the orders that
 *       reserved it, which are other customers' orders.
 *   <li>{@code product}: {@code ADMIN} only. Nothing in the events is personal, but they carry
 *       operational detail the public catalogue does not show: every stock adjustment with its
 *       free-text reason, and the price history.
 *   <li>any other type: refused, for {@code ADMIN} too. A new aggregate is closed until a line is
 *       added here.
 * </ul>
 *
 * <p><b>Who the caller is, and who owns what.</b> {@link OwnerOrAdminAccess} answers both, for this
 * endpoint as for the REST reads and writes of customers and orders: the framework's SSE endpoint
 * hands the rule the request context of the request that asks for the stream, {@code
 * caller.userId()} is the caller's id and {@code ADMIN} is the {@code role} entry of {@code
 * caller.baggage()}. The role counts only for a caller with an identity, so {@code X-User-Role:
 * ADMIN} alone opens nothing.
 *
 * <p>DEMO SHORTCUT, the gateway stand-in ch. 9 of the tutorial describes: the demo runs in the
 * trusted-gateway mode without a gateway, so the id and the role are whatever the client put in
 * {@code X-User-Id} and {@code X-User-Role}. The rule is the one a real deployment keeps; the
 * identity it is handed must then come from real authentication.
 *
 * <p><b>Shape.</b> {@link #isAuthorized} has the signature of the framework's {@code
 * SseAuthorizer}, so each application registers {@code access::isAuthorized} as its {@code
 * SseAuthorizer} bean and this module needs nothing beyond {@code streamrune-core}.
 */
public final class OwnerOrAdminStreamAccess {

  private final Function<String, OrderView> orderById;

  /**
   * @param orderById the order read model: the order with the given id, or {@code null} when the
   *     read model has no row for it ({@code OrderProjection::get})
   */
  public OwnerOrAdminStreamAccess(Function<String, OrderView> orderById) {
    if (orderById == null) {
      throw new IllegalArgumentException("orderById is required");
    }
    this.orderById = orderById;
  }

  /**
   * Whether {@code caller} may open the stream {@code streamId}.
   *
   * @param caller the request context of the request that asks for the stream; its {@code userId()}
   *     is {@code null} when the request carries no identity
   * @param streamId the requested aggregate stream
   * @return {@code true} for an {@code ADMIN} on a stream of a known type and for the customer the
   *     stream belongs to; {@code false} for everyone else
   */
  public boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId) {
    if (OwnerOrAdminAccess.identityOf(caller) == null) {
      return false;
    }
    AggregateType type = streamId.aggregateType();
    String aggregateId = streamId.aggregateId().value();
    if (CustomerState.TYPE.equals(type)) {
      return OwnerOrAdminAccess.mayAccessCustomer(caller, aggregateId);
    }
    if (OrderState.TYPE.equals(type)) {
      // An ADMIN needs no owner, so the read model is asked only for everyone else.
      return OwnerOrAdminAccess.isAdmin(caller)
          || OwnerOrAdminAccess.mayAccessOrder(caller, orderById.apply(aggregateId));
    }
    if (PaymentState.TYPE.equals(type)
        || InventoryState.TYPE.equals(type)
        || ProductState.TYPE.equals(type)) {
      return OwnerOrAdminAccess.isAdmin(caller);
    }
    return false;
  }
}
