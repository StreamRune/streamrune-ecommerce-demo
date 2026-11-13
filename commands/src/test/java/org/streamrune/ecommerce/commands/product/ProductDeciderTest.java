package org.streamrune.ecommerce.commands.product;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.*;
import org.streamrune.test.DeciderFixture;

class ProductDeciderTest {

  private final DeciderFixture<ProductCommand, ProductState, ProductEvent> fixture =
      DeciderFixture.of(new ProductDecider());

  @Test
  void createProduct_emitsProductCreated() {
    fixture
        .given()
        .when(
            new ProductCommand.CreateProduct(
                "p-1",
                "Widget",
                "A widget",
                "Electronics",
                new Money(java.math.BigDecimal.valueOf(19.99), "USD")))
        .expectEvents(
            new ProductEvent.ProductCreated(
                "p-1",
                "Widget",
                "A widget",
                "Electronics",
                new Money(java.math.BigDecimal.valueOf(19.99), "USD")))
        .expectState(s -> assertThat(s.status()).isEqualTo(ProductStatus.AVAILABLE));
  }

  @Test
  void createProduct_existingProduct_throws() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD")))
        .when(
            new ProductCommand.CreateProduct(
                "p-1", "Other", null, null, new Money(java.math.BigDecimal.ONE, "USD")))
        .expectFailedWith(DomainException.class, "Product already exists: p-1");
  }

  @Test
  void updatePrice() {
    var oldPrice = new Money(java.math.BigDecimal.TEN, "USD");
    var newPrice = new Money(java.math.BigDecimal.valueOf(25), "USD");
    fixture
        .given(new ProductEvent.ProductCreated("p-1", "Widget", null, null, oldPrice))
        .when(new ProductCommand.UpdatePrice("p-1", newPrice))
        .expectEvents(new ProductEvent.PriceUpdated("p-1", oldPrice, newPrice));
  }

  @Test
  void discontinueProduct() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD")))
        .when(new ProductCommand.DiscontinueProduct("p-1"))
        .expectEvents(new ProductEvent.ProductDiscontinued("p-1"))
        .expectState(s -> assertThat(s.status()).isEqualTo(ProductStatus.DISCONTINUED));
  }

  @Test
  void discontinueProduct_alreadyDiscontinued_throws() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD")),
            new ProductEvent.ProductDiscontinued("p-1"))
        .when(new ProductCommand.DiscontinueProduct("p-1"))
        .expectException(DomainException.class);
  }

  // A product that was never created takes no command but CreateProduct: without the check a
  // price change or a discontinuation would open a stream for an id nobody created.

  @Test
  void updatePrice_neverCreated_throws() {
    fixture
        .given()
        .when(
            new ProductCommand.UpdatePrice(
                "p-none", new Money(java.math.BigDecimal.valueOf(25), "USD")))
        .expectFailedWith(DomainException.class, "Product not found: p-none");
  }

  @Test
  void discontinueProduct_neverCreated_throws() {
    fixture
        .given()
        .when(new ProductCommand.DiscontinueProduct("p-none"))
        .expectFailedWith(DomainException.class, "Product not found: p-none");
  }
}
