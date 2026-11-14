package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.streamrune.ecommerce.projections.InventoryProjection;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.queries.dto.InventoryView;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/**
 * The inventory read model, for an {@code ADMIN} only: the units available, reserved and committed
 * of every product of the catalogue. The storefront shows the availability on the product instead
 * ({@code ProductQueryController}).
 */
@Path("/api/inventory")
@Produces(MediaType.APPLICATION_JSON)
public class InventoryQueryController {

  @Inject InventoryProjection inventoryProjection;
  @Inject StreamRuneRequestContextHolder requestContext;

  @GET
  public List<InventoryView> listInventory() {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.of(requestContext));
    return inventoryProjection.listAll();
  }

  @GET
  @Path("/{productId}")
  public Response getInventory(@PathParam("productId") String productId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.of(requestContext));
    InventoryView inventory = inventoryProjection.get(productId);
    return inventory != null
        ? Response.ok(inventory).build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }
}
