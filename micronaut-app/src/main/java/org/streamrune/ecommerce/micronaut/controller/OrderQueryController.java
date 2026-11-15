package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.OrderPage;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.paging.ListPage;

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
   * One page of every order for an {@code ADMIN}, narrowed to one customer by {@code customerId};
   * of the caller's own orders for anyone else, who may not name another customer. Newest first.
   * {@code page} (from 0) and {@code size} (1 to 100, 20 by default) choose the page ({@link
   * ListPage}); every value of the two is handed over, so a parameter given twice is {@code 400}
   * like a value outside the limits, whoever asks.
   */
  @Get
  public OrderPage listOrders(
      @QueryValue(value = "customerId", defaultValue = "") String customerId,
      HttpRequest<?> request) {
    ListPage requested =
        ListPage.parse(
            request.getParameters().getAll("page"), request.getParameters().getAll("size"));
    return queries.orders(RequestCaller.current(), customerId, requested);
  }
}
