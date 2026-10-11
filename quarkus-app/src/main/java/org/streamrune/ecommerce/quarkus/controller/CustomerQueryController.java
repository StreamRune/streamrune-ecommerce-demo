package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/**
 * The customer read model, for an authenticated {@code ADMIN} or the customer themself ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@Path("/api/customers")
@Produces(MediaType.APPLICATION_JSON)
public class CustomerQueryController {

  // Reads decrypt the @Encrypted PII while the subject's key exists; after a forget the row is
  // gone and getCustomer returns 404 to the customer and to an ADMIN.
  @Inject OwnerOrAdminQueries queries;
  @Inject StreamRuneRequestContextHolder requestContext;

  /** One customer: {@code 404} when the caller may read it and the read model has no row. */
  @GET
  @Path("/{id}")
  public Response getCustomer(@PathParam("id") String id) {
    CustomerView customer = queries.customer(RequestCaller.of(requestContext), id);
    return customer != null
        ? Response.ok(customer).build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }

  /** Every customer for an {@code ADMIN}; the caller's own record for anyone else. */
  @GET
  public List<CustomerView> listCustomers() {
    return queries.customers(RequestCaller.of(requestContext));
  }
}
