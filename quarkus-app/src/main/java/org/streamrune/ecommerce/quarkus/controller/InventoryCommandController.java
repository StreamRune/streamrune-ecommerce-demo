package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.quarkus.StreamRuneRequestFilter;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Receiving a shipment changes the stock every order reserves from: for an {@code ADMIN} only. */
@Path("/api/inventory")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class InventoryCommandController {

  @Inject VirtualThreadCommandBus commandBus;
  @Inject StreamRuneRequestContextHolder requestContext;

  /**
   * Adds {@code quantity} to the inventory: like a stock adjustment, it is applied once per {@code
   * Idempotency-Key} the client sends, and every time without one.
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
    // Bound to the request's context, as in OrderCommandController#executeInContext: the audit row
    // and the event metadata then name the ADMIN who received the shipment.
    StreamRuneRequestFilter.withContext(
        requestContext.context(),
        () ->
            IdempotentDispatch.execute(
                commandBus, new InventoryCommand.ReceiveShipment(productId, req.quantity()), key));
    return Response.ok().build();
  }

  public record ReceiveShipmentRequest(int quantity) {}
}
