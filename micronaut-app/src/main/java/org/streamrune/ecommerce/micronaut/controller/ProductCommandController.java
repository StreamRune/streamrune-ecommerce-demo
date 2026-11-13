package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.math.BigDecimal;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The product commands: the catalogue is the operators', so every write is for an {@code ADMIN}
 * ({@code 403} for everyone else). Creating a product and changing its price ask {@link
 * OwnerOrAdminAccess#requireAdmin} once the request is read; discontinuing is guarded in the
 * command bus by {@code @RequireRole("ADMIN")} on the command. Reading the catalogue is open
 * ({@code ProductQueryController}). The catalogue holds no stock: a product's units are received
 * and adjusted on its inventory ({@code InventoryCommandController}).
 */
@Controller("/api/products")
public class ProductCommandController {

  static final String OPENING_STOCK_REFUSED =
      "initialStock is not accepted: a product holds no stock; receive its opening stock with POST /api/inventory/{productId}/receive";

  private final VirtualThreadCommandBus commandBus;

  public ProductCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @Post
  public HttpResponse<Void> createProduct(@Body CreateProductRequest req) {
    RequestChecks.notBlank(req.productId(), "productId");
    RequestChecks.notBlank(req.name(), "name");
    RequestChecks.positive(req.price(), "price");
    refuseOpeningStock(req.initialStock());
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    commandBus.execute(
        new ProductCommand.CreateProduct(
            req.productId(),
            req.name(),
            req.description(),
            req.category(),
            new Money(req.price(), "USD")));
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
      @Nullable Integer initialStock) {}

  /**
   * The catalogue holds no stock, so a request that names an opening stock is refused with {@code
   * 400} instead of creating a product that holds nothing; the stock is received on the inventory.
   */
  private static void refuseOpeningStock(Integer initialStock) {
    if (initialStock != null) {
      throw new IllegalArgumentException(OPENING_STOCK_REFUSED);
    }
  }

  @io.micronaut.serde.annotation.Serdeable
  public record UpdatePriceRequest(BigDecimal price) {}
}
