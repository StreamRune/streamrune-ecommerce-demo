package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.streamrune.ecommerce.projections.ProductProjection;
import org.streamrune.ecommerce.queries.dto.ProductView;

@Path("/api/products")
@Produces(MediaType.APPLICATION_JSON)
public class ProductQueryController {

  @Inject ProductProjection productProjection;

  @GET
  @Path("/{id}")
  public Response getProduct(@PathParam("id") String id) {
    ProductView product = productProjection.get(id);
    return product != null
        ? Response.ok(product).build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }

  @GET
  public List<ProductView> listProducts() {
    return productProjection.listAll();
  }
}
