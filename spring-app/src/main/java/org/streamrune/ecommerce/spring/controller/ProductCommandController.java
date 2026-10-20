package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.commands.IdempotentDispatch;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The product commands: the catalogue is the operators', so every write is for an {@code ADMIN}
 * ({@code 403} for everyone else). Creating a product, adjusting its stock and changing its price
 * ask {@link OwnerOrAdminAccess#requireAdmin} as their first line; discontinuing is guarded in the
 * command bus by {@code @RequireRole("ADMIN")} on the command. Reading the catalogue is open
 * ({@code ProductQueryController}).
 */
@RestController
@RequestMapping("/api/products")
public class ProductCommandController {

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @PostMapping
  public ResponseEntity<Void> createProduct(@Valid @RequestBody CreateProductRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
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

  /**
   * A change of the stock by {@code quantity}: sent twice, it is applied twice, unless the client
   * sends an {@code Idempotency-Key}. With the header the bus runs the command once per key and
   * answers a repeat with the recorded result, and a dead-lettered adjustment is replayed under the
   * same key, so the retry runner replays it on its schedule.
   */
  @PutMapping("/{id}/stock")
  public ResponseEntity<Void> adjustStock(
      @PathVariable String id,
      @RequestHeader(name = IdempotentDispatch.HEADER, required = false) String idempotencyKey,
      @Valid @RequestBody AdjustStockRequest req) {
    var caller = RequestCaller.current();
    OwnerOrAdminAccess.requireAdmin(caller);
    IdempotentDispatch.execute(
        commandBus,
        new ProductCommand.AdjustStock(id, req.quantity(), req.reason()),
        IdempotentDispatch.keyFor(idempotencyKey, caller.userId()));
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
      int initialStock) {}

  public record AdjustStockRequest(int quantity, String reason) {}

  public record UpdatePriceRequest(@NotNull @Positive BigDecimal price) {}
}
