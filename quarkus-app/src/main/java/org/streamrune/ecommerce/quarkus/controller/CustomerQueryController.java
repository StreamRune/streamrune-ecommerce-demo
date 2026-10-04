package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.queries.dto.CustomerView;

@Path("/api/customers")
@Produces(MediaType.APPLICATION_JSON)
public class CustomerQueryController {

  // Resolves the single CustomerProjection bean (the @ProjectionConfig-annotated subclass produced
  // by StreamRuneProducer extends CustomerProjection). Reads decrypt the @Encrypted PII while the
  // subject's key exists; after a forget the row is gone and getCustomer returns 404.
  @Inject CustomerProjection customerProjection;

  @GET
  @Path("/{id}")
  public Response getCustomer(@PathParam("id") String id) {
    CustomerView customer = customerProjection.get(id);
    return customer != null
        ? Response.ok(customer).build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }

  @GET
  public List<CustomerView> listCustomers() {
    return customerProjection.listAll();
  }
}
