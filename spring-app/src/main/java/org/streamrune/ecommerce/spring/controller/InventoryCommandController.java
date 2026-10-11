package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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

  @PostMapping("/{productId}/receive")
  public ResponseEntity<Void> receiveShipment(
      @PathVariable String productId, @Valid @RequestBody ReceiveShipmentRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, req.quantity()));
    return ResponseEntity.ok().build();
  }

  public record ReceiveShipmentRequest(@Positive int quantity) {}
}
