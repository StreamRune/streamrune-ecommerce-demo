package org.streamrune.ecommerce.commands.product;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.product.*;

public class ProductDecider implements Decider<ProductCommand, ProductState, ProductEvent> {

  @Override
  public ProductState initialState() {
    return new ProductState();
  }

  @Override
  public List<ProductEvent> decide(ProductCommand cmd, ProductState state) {
    return switch (cmd) {
      case ProductCommand.CreateProduct c -> {
        // A product id is created once. Without this check a second CreateProduct would append
        // another ProductCreated, and evolve would replace the product's name and price.
        if (state.productId() != null) {
          throw new DomainException("Product already exists: " + c.productId());
        }
        yield List.of(
            new ProductEvent.ProductCreated(
                c.productId(), c.name(), c.description(), c.category(), c.price()));
      }

      case ProductCommand.UpdatePrice c -> {
        requireCreated(state, c.productId());
        yield List.of(new ProductEvent.PriceUpdated(c.productId(), state.price(), c.newPrice()));
      }

      case ProductCommand.DiscontinueProduct c -> {
        requireCreated(state, c.productId());
        if (state.status() == ProductStatus.DISCONTINUED) {
          throw new DomainException("Product already discontinued: " + c.productId());
        }
        yield List.of(new ProductEvent.ProductDiscontinued(c.productId()));
      }
    };
  }

  /**
   * A product that was never created takes no command but {@code CreateProduct}: without this check
   * a price change or a discontinuation would open a stream for an id nobody created.
   */
  private static void requireCreated(ProductState state, String productId) {
    if (state.productId() == null) {
      throw new DomainException("Product not found: " + productId);
    }
  }

  @Override
  public ProductState evolve(ProductState state, ProductEvent evt) {
    return switch (evt) {
      case ProductEvent.ProductCreated e ->
          new ProductState(
              e.productId(),
              e.name(),
              e.description(),
              e.category(),
              e.price(),
              ProductStatus.AVAILABLE);
      case ProductEvent.PriceUpdated e ->
          new ProductState(
              state.productId(),
              state.name(),
              state.description(),
              state.category(),
              e.newPrice(),
              state.status());
      case ProductEvent.ProductDiscontinued e ->
          new ProductState(
              state.productId(),
              state.name(),
              state.description(),
              state.category(),
              state.price(),
              ProductStatus.DISCONTINUED);
    };
  }
}
