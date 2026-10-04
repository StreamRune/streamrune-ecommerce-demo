package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.dto.OrderView;

@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
public class OrderQueryController {

  @Inject OrderProjection orderProjection;

  @GET
  @Path("/{id}")
  public Response getOrder(@PathParam("id") String id) {
    OrderView order = orderProjection.get(id);
    return order != null
        ? Response.ok(order).build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }

  @GET
  public List<OrderView> listOrders(@QueryParam("customerId") String customerId) {
    if (customerId != null) {
      return orderProjection.listByCustomer(customerId);
    }
    return orderProjection.listAll();
  }
}
