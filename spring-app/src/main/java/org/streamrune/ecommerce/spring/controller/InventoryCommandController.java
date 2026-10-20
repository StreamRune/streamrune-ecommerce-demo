package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Receiving a shipment changes the stock every order reserves from: for an {@code ADMIN} only. */
@RestController
@RequestMapping("/api/inventory")
public class InventoryCommandController {
  private final VirtualThreadCommandBus commandBus;

  public InventoryCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  /**
   * Adds {@code quantity} to the inventory: like a stock adjustment, it is applied once per {@code
   * Idempotency-Key} the client sends, and every time without one.
   */
  @PostMapping("/{productId}/receive")
  public ResponseEntity<Void> receiveShipment(
      @PathVariable String productId,
      @RequestHeader(name = IdempotentDispatch.HEADER, required = false) String idempotencyKey,
      @Valid @RequestBody ReceiveShipmentRequest req) {
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    IdempotentDispatch.execute(
        commandBus,
        new InventoryCommand.ReceiveShipment(productId, req.quantity()),
        IdempotentDispatch.keyFor(idempotencyKey, caller.userId()));
    return ResponseEntity.ok().build();
  }

  public record ReceiveShipmentRequest(@Positive int quantity) {}
}
