package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.math.BigDecimal;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Controller("/api/products")
public class ProductCommandController {

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @Post
  public HttpResponse<Void> createProduct(@Body CreateProductRequest req) {
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
    commandBus.execute(new ProductCommand.AdjustStock(id, req.quantity(), req.reason()));
    return HttpResponse.ok();
  }

  @Put("/{id}/price")
  public HttpResponse<Void> updatePrice(@PathVariable String id, @Body UpdatePriceRequest req) {
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
