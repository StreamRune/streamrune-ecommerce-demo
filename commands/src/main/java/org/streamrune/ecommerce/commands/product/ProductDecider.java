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
        // another ProductCreated, and evolve would replace the product's name, price and stock.
        if (state.productId() != null)
          throw new DomainException("Product already exists: " + c.productId());
        yield List.of(
            new ProductEvent.ProductCreated(
                c.productId(),
                c.name(),
                c.description(),
                c.category(),
                c.price(),
                c.initialStock()));
      }

      case ProductCommand.UpdatePrice c ->
          List.of(new ProductEvent.PriceUpdated(c.productId(), state.price(), c.newPrice()));

      case ProductCommand.AdjustStock c -> {
        int newStock = state.stock() + c.quantityChange();
        if (newStock < 0)
          throw new DomainException("Insufficient stock for product: " + c.productId());
        yield List.of(
            new ProductEvent.StockAdjusted(c.productId(), state.stock(), newStock, c.reason()));
      }

      case ProductCommand.DiscontinueProduct c -> {
        if (state.status() == ProductStatus.DISCONTINUED)
          throw new DomainException("Product already discontinued: " + c.productId());
        yield List.of(new ProductEvent.ProductDiscontinued(c.productId()));
      }
    };
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
              e.stock(),
              ProductState.statusForStock(e.stock()));
      case ProductEvent.PriceUpdated e ->
          new ProductState(
              state.productId(),
              state.name(),
              state.description(),
              state.category(),
              e.newPrice(),
              state.stock(),
              state.status());
      case ProductEvent.StockAdjusted e ->
          new ProductState(
              state.productId(),
              state.name(),
              state.description(),
              state.category(),
              state.price(),
              e.newStock(),
              ProductState.statusForStock(e.newStock()));
      case ProductEvent.ProductDiscontinued e ->
          new ProductState(
              state.productId(),
              state.name(),
              state.description(),
              state.category(),
              state.price(),
              state.stock(),
              ProductStatus.DISCONTINUED);
    };
  }
}
