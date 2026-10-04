package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.quarkus.StreamRuneRequestFilter;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Path("/api/products")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ProductCommandController {

  @Inject VirtualThreadCommandBus commandBus;
  @Inject StreamRuneRequestContextHolder requestContext;

  /**
   * Executes a command with the request's {@link org.streamrune.core.StreamRuneContext} bound to
   * the {@code ScopedValue}. See {@code OrderCommandController#executeInContext} for why this
   * explicit bind is required in Quarkus (JAX-RS filters cannot wrap the resource method). Product
   * writes only gate {@code DiscontinueProduct} on {@code ADMIN} (not exposed here), but binding
   * still records the real {@code X-User-Id} on the audit trail instead of {@code GUEST}.
   */
  private void executeInContext(ProductCommand command) {
    StreamRuneRequestFilter.withContext(
        requestContext != null ? requestContext.context() : null,
        () -> commandBus.execute(command));
  }

  @POST
  public Response createProduct(CreateProductRequest req) {
    executeInContext(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD"),
            req.initialStock()));
    return Response.ok().build();
  }

  @PUT
  @Path("/{id}/stock")
  public Response adjustStock(@PathParam("id") String id, AdjustStockRequest req) {
    executeInContext(new ProductCommand.AdjustStock(id, req.quantity(), req.reason()));
    return Response.ok().build();
  }

  @PUT
  @Path("/{id}/price")
  public Response updatePrice(@PathParam("id") String id, UpdatePriceRequest req) {
    executeInContext(new ProductCommand.UpdatePrice(id, new Money(req.price(), "USD")));
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/discontinue")
  public Response discontinueProduct(@PathParam("id") String id) {
    executeInContext(new ProductCommand.DiscontinueProduct(id));
    return Response.ok().build();
  }

  public record CreateProductRequest(
      String productId,
      String name,
      String description,
      String category,
      BigDecimal price,
      int initialStock) {}

  public record AdjustStockRequest(int quantity, String reason) {}

  public record UpdatePriceRequest(BigDecimal price) {}
}
