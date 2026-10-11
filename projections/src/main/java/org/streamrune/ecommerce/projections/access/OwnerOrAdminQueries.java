package org.streamrune.ecommerce.projections.access;

import java.util.List;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The customer and order read models as one caller may see them: {@link OwnerOrAdminAccess} applied
 * to every read. The three applications answer {@code GET /api/customers}, {@code GET
 * /api/customers/{id}}, {@code GET /api/orders} and {@code GET /api/orders/{id}} from this class
 * and hand it the caller's request context; a refusal is an {@link AccessRefusedException}, which
 * each application answers with {@code 403}.
 *
 * <ul>
 *   <li><b>One customer, one order:</b> an authenticated {@code ADMIN} or the owner; {@code null}
 *       when the caller may read it and the read model has no row (the application answers {@code
 *       404}).
 *   <li><b>A list:</b> an {@code ADMIN} gets every row; any other authenticated caller gets the
 *       rows that are their own, selected by their id before anything is returned; a request
 *       without an identity is refused.
 * </ul>
 *
 * <p>A forgotten customer has no row: {@code CustomerProjection} deletes it. The customer and an
 * {@code ADMIN} then read {@code null} for it, and it is on no list.
 */
public final class OwnerOrAdminQueries {

  private final CustomerProjection customers;
  private final OrderProjection orders;

  public OwnerOrAdminQueries(CustomerProjection customers, OrderProjection orders) {
    if (customers == null) {
      throw new IllegalArgumentException("customers is required");
    }
    if (orders == null) {
      throw new IllegalArgumentException("orders is required");
    }
    this.customers = customers;
    this.orders = orders;
  }

  /**
   * One customer, for an {@code ADMIN} or the customer themself. The decision is made on the two
   * ids alone, before the read model is asked, so a refused caller cannot tell a customer that
   * exists from one that does not.
   *
   * @return the customer, or {@code null} when the read model has no row for the id
   * @throws AccessRefusedException for everyone else
   */
  public CustomerView customer(StreamRuneContext.RequestContext caller, String customerId) {
    OwnerOrAdminAccess.requireCustomerAccess(caller, customerId);
    return customers.get(customerId);
  }

  /**
   * The customers the caller may see: all of them for an {@code ADMIN}; for any other authenticated
   * caller their own record, read by its id, or an empty list when they have none.
   *
   * @throws AccessRefusedException for a request without an identity
   */
  public List<CustomerView> customers(StreamRuneContext.RequestContext caller) {
    String identity = OwnerOrAdminAccess.requireIdentity(caller);
    if (OwnerOrAdminAccess.isAdmin(caller)) {
      return customers.listAll();
    }
    CustomerView own = customers.get(identity);
    return own == null ? List.of() : List.of(own);
  }

  /**
   * One order, for an {@code ADMIN} or the customer the order names.
   *
   * @return the order; {@code null} only for an {@code ADMIN}, when the read model has no row for
   *     the id
   * @throws AccessRefusedException for everyone else, and for a caller who is not an {@code ADMIN}
   *     when the read model has no row for the id: an order without a row has no known owner, and
   *     the same answer as for somebody else's order keeps its existence to itself
   */
  public OrderView order(StreamRuneContext.RequestContext caller, String orderId) {
    OwnerOrAdminAccess.requireIdentity(caller);
    OrderView order = orders.get(orderId);
    OwnerOrAdminAccess.requireOrderAccess(caller, order);
    return order;
  }

  /**
   * The orders the caller may see. An {@code ADMIN} gets all of them, or those of {@code
   * customerId} when the request names one. Any other authenticated caller gets their own orders
   * and nothing else: the filter can repeat the caller's own id, and it cannot name another
   * customer.
   *
   * @param customerId the {@code customerId} filter of the request; {@code null} or empty for none
   * @throws AccessRefusedException for a request without an identity, and for a caller who is not
   *     an {@code ADMIN} and asks for another customer's orders
   */
  public List<OrderView> orders(StreamRuneContext.RequestContext caller, String customerId) {
    String identity = OwnerOrAdminAccess.requireIdentity(caller);
    boolean filtered = customerId != null && !customerId.isEmpty();
    if (OwnerOrAdminAccess.isAdmin(caller)) {
      return filtered ? orders.listByCustomer(customerId) : orders.listAll();
    }
    if (filtered) {
      OwnerOrAdminAccess.requireCustomerAccess(caller, customerId);
    }
    return orders.listByCustomer(identity);
  }

  /**
   * Refuses a caller who may not change the order with the given id: the guard of the order
   * commands that act on an existing order. The owner is the customer the order's read-model row
   * names, as for {@link #order}.
   *
   * @throws AccessRefusedException unless the caller is an {@code ADMIN} or that customer
   */
  public void requireOrderAccess(StreamRuneContext.RequestContext caller, String orderId) {
    order(caller, orderId);
  }
}
