package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.util.List;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The order read model, for an authenticated {@code ADMIN} or the customer an order names ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@Controller("/api/orders")
public class OrderQueryController {

  private final OwnerOrAdminQueries queries;

  public OrderQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /**
   * One order. {@code 404} is an {@code ADMIN}'s answer for an id without a row; a customer gets
   * {@code 403} for it, as for another customer's order.
   */
  @Get("/{id}")
  public HttpResponse<OrderView> getOrder(@PathVariable String id) {
    OrderView order = queries.order(RequestCaller.current(), id);
    return order != null ? HttpResponse.ok(order) : HttpResponse.notFound();
  }

  /**
   * Every order for an {@code ADMIN}, narrowed to one customer by {@code customerId}; the caller's
   * own orders for anyone else, who may not name another customer.
   */
  @Get
  public List<OrderView> listOrders(
      @QueryValue(value = "customerId", defaultValue = "") String customerId) {
    return queries.orders(RequestCaller.current(), customerId);
  }
}
