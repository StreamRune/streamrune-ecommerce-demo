package org.streamrune.ecommerce.queries.access;

import java.util.function.Function;
import java.util.function.Supplier;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
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
 *       {@code GET /api/orders/{id}} answers. The order's customer is the customer the order names:
 *       see the second demo shortcut below.
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
 * <p><b>Where the role comes from.</b> {@code ADMIN} is the {@code role} entry of the request
 * context's baggage, the entry {@code AdminController#requireRole} and {@code
 * CustomerCommandController#requireAdminOrSelf} check. The request context is handed in as a
 * supplier because each integration keeps it somewhere else: a {@code ScopedValue} on Spring, a
 * request-scoped holder on Quarkus, a {@code ScopedValue} or a {@code ThreadLocal} on Micronaut.
 * The caller's id is the {@code principal} the framework's SSE controller resolved with the same
 * identity policy its request filter uses. The role is honoured only when the supplied context is
 * the caller's own, that is when its user id equals the {@code principal}: the two come from the
 * same request on all three integrations, and the {@code ADMIN} branch, the one that opens every
 * customer's data, does not rely on that.
 *
 * <p>DEMO SHORTCUT, the gateway stand-in ch. 9 of the tutorial describes: the demo runs in the
 * trusted-gateway mode without a gateway, so the id and the role are whatever the client put in
 * {@code X-User-Id} and {@code X-User-Role}. The rule is the one a real deployment keeps; the
 * identity it is handed must then come from real authentication.
 *
 * <p>DEMO SHORTCUT, the owner of an order: an order belongs to the customer it names, and {@code
 * POST /api/orders} takes that {@code customerId} from the request body without tying it to the
 * caller. Whoever places an order therefore chooses whose order it is. Nobody but the customer so
 * named gains access to its stream, but a real deployment places an order for the authenticated
 * caller.
 *
 * <p><b>Shape.</b> {@link #isAuthorized} has the signature of the framework's {@code
 * SseAuthorizer}, so each application registers {@code access::isAuthorized} as its {@code
 * SseAuthorizer} bean and this module needs nothing beyond {@code streamrune-core}.
 */
public final class OwnerOrAdminStreamAccess {

  private static final String ROLE_BAGGAGE_KEY = "role";
  private static final String ADMIN = "ADMIN";

  private final Supplier<StreamRuneContext.RequestContext> requestContext;
  private final Function<String, OrderView> orderById;

  /**
   * @param requestContext the request context of the HTTP request being served, or {@code null}
   *     when none is bound; read on the thread the framework's SSE controller calls the authorizer
   *     on
   * @param orderById the order read model: the order with the given id, or {@code null} when the
   *     read model has no row for it ({@code OrderProjection::get})
   */
  public OwnerOrAdminStreamAccess(
      Supplier<StreamRuneContext.RequestContext> requestContext,
      Function<String, OrderView> orderById) {
    if (requestContext == null) {
      throw new IllegalArgumentException("requestContext is required");
    }
    if (orderById == null) {
      throw new IllegalArgumentException("orderById is required");
    }
    this.requestContext = requestContext;
    this.orderById = orderById;
  }

  /**
   * Whether {@code principal} may open the stream {@code streamId}.
   *
   * @param principal the caller the framework resolved for the request; {@code null} when the
   *     request carries no identity
   * @param streamId the requested aggregate stream
   * @return {@code true} for an {@code ADMIN} on a stream of a known type and for the customer the
   *     stream belongs to; {@code false} for everyone else
   */
  public boolean isAuthorized(UserId principal, StreamId streamId) {
    if (principal == null) {
      return false;
    }
    AggregateType type = streamId.aggregateType();
    String aggregateId = streamId.aggregateId().value();
    if (CustomerState.TYPE.equals(type)) {
      return callerIsAdmin(principal) || principal.value().equals(aggregateId);
    }
    if (OrderState.TYPE.equals(type)) {
      return callerIsAdmin(principal) || ownsOrder(principal, aggregateId);
    }
    if (PaymentState.TYPE.equals(type)
        || InventoryState.TYPE.equals(type)
        || ProductState.TYPE.equals(type)) {
      return callerIsAdmin(principal);
    }
    return false;
  }

  /**
   * The current request context is the caller's own and its {@code role} baggage entry is {@code
   * ADMIN}. The caller's id and the role reach this class by two ways, the {@code principal}
   * parameter and the supplied context; the role counts only when the context names the same user,
   * so a context that belongs to another request can never make this caller an operator.
   */
  private boolean callerIsAdmin(UserId principal) {
    StreamRuneContext.RequestContext context = requestContext.get();
    return context != null
        && principal.equals(context.userId())
        && ADMIN.equals(context.baggage().get(ROLE_BAGGAGE_KEY));
  }

  /** The order read model has a row for the order and names the caller as its customer. */
  private boolean ownsOrder(UserId principal, String orderId) {
    OrderView order = orderById.apply(orderId);
    return order != null && principal.value().equals(order.customerId());
  }
}
