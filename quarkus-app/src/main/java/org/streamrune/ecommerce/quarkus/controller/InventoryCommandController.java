package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Path("/api/inventory")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class InventoryCommandController {

  @Inject VirtualThreadCommandBus commandBus;

  @POST
  @Path("/{productId}/receive")
  public Response receiveShipment(
      @PathParam("productId") String productId, ReceiveShipmentRequest req) {
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, req.quantity()));
    return Response.ok().build();
  }

  public record ReceiveShipmentRequest(int quantity) {}
}
