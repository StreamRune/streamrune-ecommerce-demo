package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.streamrune.core.Command;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.commands.product.ProductCatalog;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.quarkus.StreamRuneRequestFilter;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The inventory is the one place that holds a product's units: receiving a shipment and adjusting
 * the units on hand change the stock every order reserves from, so both are for an {@code ADMIN}
 * only. Both are refused with {@code 400} for a product the catalogue does not hold ({@link
 * ProductCatalog#requireExists}), so no inventory is opened for a product nobody created.
 */
@Path("/api/inventory")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class InventoryCommandController {

  @Inject VirtualThreadCommandBus commandBus;
  @Inject StreamRuneRequestContextHolder requestContext;
  @Inject ProductCatalog catalog;

  /**
   * Adds {@code quantity} to the inventory: applied once per {@code Idempotency-Key} the client
   * sends, and every time without one.
   */
  @POST
  @Path("/{productId}/receive")
  public Response receiveShipment(
      @PathParam("productId") String productId,
      @HeaderParam(IdempotentDispatch.HEADER) String idempotencyKey,
      ReceiveShipmentRequest req) {
    RequestChecks.present(req);
    RequestChecks.positive(req.quantity(), "quantity");
    var caller = RequestCaller.of(requestContext);
    OwnerOrAdminAccess.requireAdmin(caller);
    var key = IdempotentDispatch.keyFor(idempotencyKey, caller.userId());
    catalog.requireExists(productId);
    executeInContext(new InventoryCommand.ReceiveShipment(productId, req.quantity()), key);
    return Response.ok().build();
  }

  /**
   * Changes the units on hand by a signed {@code quantityChange}, a stocktake for example, with the
   * reason the operator gives. Like a shipment, it is applied once per {@code Idempotency-Key}, and
   * every time without one. A change to zero, without a reason, or one that would leave fewer than
   * no units available is refused with {@code 400}.
   */
  @POST
  @Path("/{productId}/adjust")
  public Response adjustInventory(
      @PathParam("productId") String productId,
      @HeaderParam(IdempotentDispatch.HEADER) String idempotencyKey,
      AdjustInventoryRequest req) {
    RequestChecks.present(req);
    var caller = RequestCaller.of(requestContext);
    OwnerOrAdminAccess.requireAdmin(caller);
    var key = IdempotentDispatch.keyFor(idempotencyKey, caller.userId());
    catalog.requireExists(productId);
    executeInContext(
        new InventoryCommand.AdjustInventory(productId, req.quantityChange(), req.reason()), key);
    return Response.ok().build();
  }

  /**
   * Bound to the request's context, as in {@code OrderCommandController#executeInContext}: the
   * audit row and the event metadata then name the {@code ADMIN} who changed the stock.
   */
  private void executeInContext(Command command, IdempotencyKey key) {
    StreamRuneRequestFilter.withContext(
        requestContext.context(), () -> IdempotentDispatch.execute(commandBus, command, key));
  }

  public record ReceiveShipmentRequest(int quantity) {}

  public record AdjustInventoryRequest(int quantityChange, String reason) {}
}
