package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.math.BigDecimal;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The product commands: the catalogue is the operators', so every write is for an {@code ADMIN}
 * ({@code 403} for everyone else). Creating a product, adjusting its stock and changing its price
 * ask {@link OwnerOrAdminAccess#requireAdmin} once the request is read; discontinuing is guarded in
 * the command bus by {@code @RequireRole("ADMIN")} on the command. Reading the catalogue is open
 * ({@code ProductQueryController}).
 */
@Controller("/api/products")
public class ProductCommandController {

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @Post
  public HttpResponse<Void> createProduct(@Body CreateProductRequest req) {
    RequestChecks.notBlank(req.productId(), "productId");
    RequestChecks.notBlank(req.name(), "name");
    RequestChecks.positive(req.price(), "price");
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD"),
            req.initialStock()));
    return HttpResponse.ok();
  }

  @Put("/{id}/stock")
  public HttpResponse<Void> adjustStock(@PathVariable String id, @Body AdjustStockRequest req) {
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new ProductCommand.AdjustStock(id, req.quantity(), req.reason()));
    return HttpResponse.ok();
  }

  @Put("/{id}/price")
  public HttpResponse<Void> updatePrice(@PathVariable String id, @Body UpdatePriceRequest req) {
    RequestChecks.positive(req.price(), "price");
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(new ProductCommand.UpdatePrice(id, new Money(req.price(), "USD")));
    return HttpResponse.ok();
  }

  @Post("/{id}/discontinue")
  public HttpResponse<Void> discontinueProduct(@PathVariable String id) {
    commandBus.execute(new ProductCommand.DiscontinueProduct(id));
    return HttpResponse.ok();
  }

  @io.micronaut.serde.annotation.Serdeable
  public record CreateProductRequest(
      String productId,
      String name,
      String description,
      String category,
      BigDecimal price,
      int initialStock) {}

  @io.micronaut.serde.annotation.Serdeable
  public record AdjustStockRequest(int quantity, String reason) {}

  @io.micronaut.serde.annotation.Serdeable
  public record UpdatePriceRequest(BigDecimal price) {}
}
