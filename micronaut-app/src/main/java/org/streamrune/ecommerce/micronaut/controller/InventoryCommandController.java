package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Receiving a shipment changes the stock every order reserves from: for an {@code ADMIN} only. */
@Controller("/api/inventory")
public class InventoryCommandController {

  private final VirtualThreadCommandBus commandBus;

  public InventoryCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  /**
   * Adds {@code quantity} to the inventory: like a stock adjustment, it is applied once per {@code
   * Idempotency-Key} the client sends, and every time without one.
   */
  @Post("/{productId}/receive")
  public HttpResponse<Void> receiveShipment(
      @PathVariable String productId,
      @Header(IdempotentDispatch.HEADER) @Nullable String idempotencyKey,
      @Body ReceiveShipmentRequest req) {
    RequestChecks.positive(req.quantity(), "quantity");
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    IdempotentDispatch.execute(
        commandBus,
        new InventoryCommand.ReceiveShipment(productId, req.quantity()),
        IdempotentDispatch.keyFor(idempotencyKey, caller.userId()));
    return HttpResponse.ok();
  }

  @io.micronaut.serde.annotation.Serdeable
  public record ReceiveShipmentRequest(int quantity) {}
}
