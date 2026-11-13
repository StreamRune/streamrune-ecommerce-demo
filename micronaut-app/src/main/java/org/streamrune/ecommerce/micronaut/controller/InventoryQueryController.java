package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.util.List;
import org.streamrune.ecommerce.projections.InventoryProjection;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.queries.dto.InventoryView;

/**
 * The inventory read model, for an {@code ADMIN} only: the units available, reserved and committed
 * of every product of the catalogue. The storefront shows the availability on the product instead
 * ({@code ProductQueryController}).
 */
@Controller("/api/inventory")
public class InventoryQueryController {

  private final InventoryProjection inventoryProjection;

  public InventoryQueryController(InventoryProjection inventoryProjection) {
    this.inventoryProjection = inventoryProjection;
  }

  @Get
  public List<InventoryView> listInventory() {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    return inventoryProjection.listAll();
  }

  @Get("/{productId}")
  public HttpResponse<InventoryView> getInventory(@PathVariable String productId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    InventoryView inventory = inventoryProjection.get(productId);
    return inventory != null ? HttpResponse.ok(inventory) : HttpResponse.notFound();
  }
}
