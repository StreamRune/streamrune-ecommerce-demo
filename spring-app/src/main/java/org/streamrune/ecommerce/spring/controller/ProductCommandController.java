package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The product commands: the catalogue is the operators', so every write is for an {@code ADMIN}
 * ({@code 403} for everyone else). Creating a product and changing its price ask {@link
 * OwnerOrAdminAccess#requireAdmin} as their first line; discontinuing is guarded in the command bus
 * by {@code @RequireRole("ADMIN")} on the command. Reading the catalogue is open ({@code
 * ProductQueryController}). The catalogue holds no stock: a product's units are received and
 * adjusted on its inventory ({@code InventoryCommandController}).
 */
@RestController
@RequestMapping("/api/products")
public class ProductCommandController {

  static final String OPENING_STOCK_REFUSED =
      "initialStock is not accepted: a product holds no stock; receive its opening stock with POST /api/inventory/{productId}/receive";

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> createProduct(@Valid @RequestBody CreateProductRequest req) {
    refuseOpeningStock(req.initialStock());
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD")));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}/price")
  public ResponseEntity<Void> updatePrice(
      @PathVariable String id, @Valid @RequestBody UpdatePriceRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new ProductCommand.UpdatePrice(id, new Money(req.price(), "USD")));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/discontinue")
  public ResponseEntity<Void> discontinueProduct(@PathVariable String id) {
    commandBus.execute(new ProductCommand.DiscontinueProduct(id));
    return ResponseEntity.ok().build();
  }

  public record CreateProductRequest(
      @NotBlank String productId,
      @NotBlank String name,
      String description,
      String category,
      @NotNull @Positive BigDecimal price,
      Integer initialStock) {}

  /**
   * The catalogue holds no stock, so a request that names an opening stock is refused with {@code
   * 400} instead of creating a product that holds nothing; the stock is received on the inventory.
   */
  private static void refuseOpeningStock(Integer initialStock) {
    if (initialStock != null) {
      throw new IllegalArgumentException(OPENING_STOCK_REFUSED);
    }
  }

  public record UpdatePriceRequest(@NotNull @Positive BigDecimal price) {}
}
