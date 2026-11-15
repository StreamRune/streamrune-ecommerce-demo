package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.OrderPage;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.paging.ListPage;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/**
 * The order read model, for an authenticated {@code ADMIN} or the customer an order names ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
public class OrderQueryController {

  @Inject OwnerOrAdminQueries queries;
  @Inject StreamRuneRequestContextHolder requestContext;

  /**
   * One order. {@code 404} is an {@code ADMIN}'s answer for an id without a row; a customer gets
   * {@code 403} for it, as for another customer's order.
   */
  @GET
  @Path("/{id}")
  public Response getOrder(@PathParam("id") String id) {
    OrderView order = queries.order(RequestCaller.of(requestContext), id);
    return order != null
        ? Response.ok(order).build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }

  /**
   * One page of every order for an {@code ADMIN}, narrowed to one customer by {@code customerId};
   * of the caller's own orders for anyone else, who may not name another customer. {@code page}
   * (from 0) and {@code size} (1 to 100, 20 by default) choose the page ({@link ListPage}); a value
   * outside them is {@code 400}, whoever asks.
   */
  @GET
  public OrderPage listOrders(
      @QueryParam("customerId") String customerId,
      @QueryParam("page") String page,
      @QueryParam("size") String size) {
    ListPage requested = ListPage.parse(page, size);
    return queries.orders(RequestCaller.of(requestContext), customerId, requested);
  }
}
