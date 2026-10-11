package org.streamrune.ecommerce.queries.access;

import org.streamrune.core.StreamRuneContext;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The demo's one rule for a customer's data: an authenticated caller who holds the {@code ADMIN}
 * role, or the customer the data belongs to. Everyone else is refused, a caller without an identity
 * first of all.
 *
 * <p>Every place that hands out or changes one customer's data decides here: the REST reads and
 * writes of customers and orders ({@code OwnerOrAdminQueries} for the reads, the command
 * controllers for the writes) and the per-stream SSE endpoint ({@link OwnerOrAdminStreamAccess}).
 * The {@code ADMIN}-only endpoints (the admin panel, the audit log, the event history, the saga
 * list, the product and inventory writes) ask {@link #requireAdmin}. Both go through {@link
 * #isAdmin}, the demo's one definition of an {@code ADMIN}: the role together with an identity.
 *
 * <p><b>Who the caller is.</b> The request context the framework's request filter built for the
 * request. Its {@code userId()} is the caller's identity, {@code null} for a request without one.
 * The {@code ADMIN} role is the {@code role} entry of its baggage: the entry the request filter
 * writes from {@code X-User-Role}, and the one each app's {@code HeaderUserRoleResolver} resolves
 * the caller's authority from. A role without an identity is nobody, on every endpoint.
 *
 * <p><b>Who owns what.</b>
 *
 * <ul>
 *   <li>A customer record belongs to the caller whose id equals the customer id.
 *   <li>An order belongs to the customer its read-model row names. An order without a row has no
 *       known owner: only an {@code ADMIN} gets past it, and a customer is answered as for an order
 *       of somebody else. The refusal therefore does not tell a caller whether an order exists.
 * </ul>
 *
 * <p>DEMO SHORTCUT, the gateway stand-in ch. 9 of the tutorial describes: the demo runs in the
 * trusted-gateway mode without a gateway, so the id and the role are whatever the client put in
 * {@code X-User-Id} and {@code X-User-Role}. The rule is the one a real deployment keeps; the
 * identity it is handed must then come from real authentication.
 */
public final class OwnerOrAdminAccess {

  private static final String ROLE_BAGGAGE_KEY = "role";
  private static final String ADMIN = "ADMIN";
  private static final String REFUSED = "Only an ADMIN or the customer themself may do this";
  private static final String ADMIN_REQUIRED = "Required role: ADMIN";

  private OwnerOrAdminAccess() {}

  /**
   * The caller's id.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   * @return the id, or {@code null} when the request has no request context or no identity
   */
  public static String identityOf(StreamRuneContext.RequestContext caller) {
    return caller == null || caller.userId() == null ? null : caller.userId().value();
  }

  /**
   * Whether the caller is an {@code ADMIN}: a caller with an identity whose request carries the
   * role, that is, whose {@code role} baggage entry equals {@code ADMIN} exactly. This is the
   * demo's only definition of an {@code ADMIN}, asked for a customer's data and for the {@code
   * ADMIN}-only endpoints alike. {@code X-User-Role: ADMIN} without an identity is nobody.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   */
  public static boolean isAdmin(StreamRuneContext.RequestContext caller) {
    return identityOf(caller) != null && ADMIN.equals(caller.baggage().get(ROLE_BAGGAGE_KEY));
  }

  /**
   * Refuses a caller who is not an {@code ADMIN}: the guard of the {@code ADMIN}-only endpoints.
   *
   * @throws AccessRefusedException unless {@link #isAdmin} holds
   */
  public static void requireAdmin(StreamRuneContext.RequestContext caller) {
    if (!isAdmin(caller)) {
      throw new AccessRefusedException(ADMIN_REQUIRED);
    }
  }

  /**
   * Whether the caller may read or change the customer with the given id: an authenticated {@code
   * ADMIN}, or the customer themself. The answer needs no read model, so it is the same for a
   * customer that exists and one that does not.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   * @param customerId the customer the request names; {@code null} is nobody's
   */
  public static boolean mayAccessCustomer(
      StreamRuneContext.RequestContext caller, String customerId) {
    String identity = identityOf(caller);
    return identity != null && (isAdmin(caller) || identity.equals(customerId));
  }

  /**
   * Whether the caller may read or change an order: an authenticated {@code ADMIN}, or the customer
   * the order's read-model row names.
   *
   * @param caller the request context of the request, or {@code null} when none is bound
   * @param order the order's read-model row, or {@code null} when the read model has none for the
   *     requested id; only an {@code ADMIN} may go on then
   */
  public static boolean mayAccessOrder(StreamRuneContext.RequestContext caller, OrderView order) {
    String identity = identityOf(caller);
    if (identity == null) {
      return false;
    }
    return isAdmin(caller) || (order != null && identity.equals(order.customerId()));
  }

  /**
   * Refuses a caller who may not read or change the customer with the given id.
   *
   * @throws AccessRefusedException unless {@link #mayAccessCustomer} holds
   */
  public static void requireCustomerAccess(
      StreamRuneContext.RequestContext caller, String customerId) {
    if (!mayAccessCustomer(caller, customerId)) {
      throw new AccessRefusedException(REFUSED);
    }
  }

  /**
   * Refuses a caller who may not read or change the given order.
   *
   * @throws AccessRefusedException unless {@link #mayAccessOrder} holds
   */
  public static void requireOrderAccess(StreamRuneContext.RequestContext caller, OrderView order) {
    if (!mayAccessOrder(caller, order)) {
      throw new AccessRefusedException(REFUSED);
    }
  }

  /**
   * Refuses a request without an identity; what an identified caller sees is then scoped by the
   * list it asked for.
   *
   * @return the caller's id
   * @throws AccessRefusedException when the request has no request context or no identity
   */
  public static String requireIdentity(StreamRuneContext.RequestContext caller) {
    String identity = identityOf(caller);
    if (identity == null) {
      throw new AccessRefusedException(REFUSED);
    }
    return identity;
  }
}
