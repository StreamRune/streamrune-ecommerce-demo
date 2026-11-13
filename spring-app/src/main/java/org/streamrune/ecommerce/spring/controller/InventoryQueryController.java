package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.InventoryProjection;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.queries.dto.InventoryView;

/**
 * The inventory read model, for an {@code ADMIN} only: the units available, reserved and committed
 * of every product of the catalogue. The storefront shows the availability on the product instead
 * ({@code ProductQueryController}).
 */
@RestController
@RequestMapping("/api/inventory")
public class InventoryQueryController {

  private final InventoryProjection inventoryProjection;

  public InventoryQueryController(InventoryProjection inventoryProjection) {
    this.inventoryProjection = inventoryProjection;
  }

  @GetMapping
  public List<InventoryView> listInventory() {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    return inventoryProjection.listAll();
  }

  @GetMapping("/{productId}")
  public ResponseEntity<InventoryView> getInventory(@PathVariable String productId) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    InventoryView inventory = inventoryProjection.get(productId);
    return inventory != null ? ResponseEntity.ok(inventory) : ResponseEntity.notFound().build();
  }
}
