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
import org.streamrune.runtime.VirtualThreadCommandBus;

@RestController
@RequestMapping("/api/products")
public class ProductCommandController {

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> createProduct(@Valid @RequestBody CreateProductRequest req) {
    commandBus.execute(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD"),
            req.initialStock()));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}/stock")
  public ResponseEntity<Void> adjustStock(
      @PathVariable String id, @Valid @RequestBody AdjustStockRequest req) {
    commandBus.execute(new ProductCommand.AdjustStock(id, req.quantity(), req.reason()));
    return ResponseEntity.ok().build();
  }

  @PutMapping("/{id}/price")
  public ResponseEntity<Void> updatePrice(
      @PathVariable String id, @Valid @RequestBody UpdatePriceRequest req) {
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
      int initialStock) {}

  public record AdjustStockRequest(int quantity, String reason) {}

  public record UpdatePriceRequest(@NotNull @Positive BigDecimal price) {}
}
