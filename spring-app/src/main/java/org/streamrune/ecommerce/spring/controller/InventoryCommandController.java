package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.commands.product.ProductCatalog;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The inventory is the one place that holds a product's units: receiving a shipment and adjusting
 * the units on hand change the stock every order reserves from, so both are for an {@code ADMIN}
 * only. Both are refused with {@code 400} for a product the catalogue does not hold ({@link
 * ProductCatalog#requireExists}), so no inventory is opened for a product nobody created.
 */
@RestController
@RequestMapping("/api/inventory")
public class InventoryCommandController {
  private final VirtualThreadCommandBus commandBus;
  private final ProductCatalog catalog;

  public InventoryCommandController(VirtualThreadCommandBus commandBus, ProductCatalog catalog) {
    this.commandBus = commandBus;
    this.catalog = catalog;
  }

  /**
   * Adds {@code quantity} to the inventory: applied once per {@code Idempotency-Key} the client
   * sends, and every time without one.
   */
  @PostMapping("/{productId}/receive")
  public ResponseEntity<Void> receiveShipment(
      @PathVariable String productId,
      @RequestHeader(name = IdempotentDispatch.HEADER, required = false) String idempotencyKey,
      @Valid @RequestBody ReceiveShipmentRequest req) {
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    var key = IdempotentDispatch.keyFor(idempotencyKey, caller.userId());
    catalog.requireExists(productId);
    IdempotentDispatch.execute(
        commandBus, new InventoryCommand.ReceiveShipment(productId, req.quantity()), key);
    return ResponseEntity.ok().build();
  }

  /**
   * Changes the units on hand by a signed {@code quantityChange}, a stocktake for example, with the
   * reason the operator gives. Like a shipment, it is applied once per {@code Idempotency-Key}, and
   * every time without one. A change to zero, without a reason, or one that would leave fewer than
   * no units available is refused with {@code 400}.
   */
  @PostMapping("/{productId}/adjust")
  public ResponseEntity<Void> adjustInventory(
      @PathVariable String productId,
      @RequestHeader(name = IdempotentDispatch.HEADER, required = false) String idempotencyKey,
      @Valid @RequestBody AdjustInventoryRequest req) {
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    var key = IdempotentDispatch.keyFor(idempotencyKey, caller.userId());
    catalog.requireExists(productId);
    IdempotentDispatch.execute(
        commandBus,
        new InventoryCommand.AdjustInventory(productId, req.quantityChange(), req.reason()),
        key);
    return ResponseEntity.ok().build();
  }

  public record ReceiveShipmentRequest(@Positive int quantity) {}

  public record AdjustInventoryRequest(int quantityChange, String reason) {}
}
