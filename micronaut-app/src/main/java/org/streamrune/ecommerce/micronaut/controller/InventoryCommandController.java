package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
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
@Controller("/api/inventory")
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
  @Post("/{productId}/receive")
  public HttpResponse<Void> receiveShipment(
      @PathVariable String productId,
      @Header(IdempotentDispatch.HEADER) @Nullable String idempotencyKey,
      @Body ReceiveShipmentRequest req) {
    RequestChecks.positive(req.quantity(), "quantity");
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    var key = IdempotentDispatch.keyFor(idempotencyKey, caller.userId());
    catalog.requireExists(productId);
    IdempotentDispatch.execute(
        commandBus, new InventoryCommand.ReceiveShipment(productId, req.quantity()), key);
    return HttpResponse.ok();
  }

  /**
   * Changes the units on hand by a signed {@code quantityChange}, a stocktake for example, with the
   * reason the operator gives. Like a shipment, it is applied once per {@code Idempotency-Key}, and
   * every time without one. A change to zero, without a reason, or one that would leave fewer than
   * no units available is refused with {@code 400}.
   */
  @Post("/{productId}/adjust")
  public HttpResponse<Void> adjustInventory(
      @PathVariable String productId,
      @Header(IdempotentDispatch.HEADER) @Nullable String idempotencyKey,
      @Body AdjustInventoryRequest req) {
    if (req.quantityChange() == null) {
      throw new IllegalArgumentException("quantityChange is required");
    }
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    var key = IdempotentDispatch.keyFor(idempotencyKey, caller.userId());
    catalog.requireExists(productId);
    IdempotentDispatch.execute(
        commandBus,
        new InventoryCommand.AdjustInventory(productId, req.quantityChange(), req.reason()),
        key);
    return HttpResponse.ok();
  }

  @io.micronaut.serde.annotation.Serdeable
  public record ReceiveShipmentRequest(int quantity) {}

  @io.micronaut.serde.annotation.Serdeable
  public record AdjustInventoryRequest(@Nullable Integer quantityChange, @Nullable String reason) {}
}
