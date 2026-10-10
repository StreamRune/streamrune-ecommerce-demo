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
                new Money(java.math.BigDecimal.valueOf(19.99), "USD"),
                100))
        .expectEvents(
            new ProductEvent.ProductCreated(
                "p-1",
                "Widget",
                "A widget",
                "Electronics",
                new Money(java.math.BigDecimal.valueOf(19.99), "USD"),
                100))
        .expectState(
            s -> {
              assertThat(s.status()).isEqualTo(ProductStatus.AVAILABLE);
              assertThat(s.stock()).isEqualTo(100);
            });
  }

  @Test
  void createProduct_existingProduct_throws() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 100))
        .when(
            new ProductCommand.CreateProduct(
                "p-1", "Other", null, null, new Money(java.math.BigDecimal.ONE, "USD"), 5))
        .expectFailedWith(DomainException.class, "Product already exists: p-1");
  }

  @Test
  void createProduct_lowStock() {
    fixture
        .given()
        .when(
            new ProductCommand.CreateProduct(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 5))
        .expectState(s -> assertThat(s.status()).isEqualTo(ProductStatus.LOW_STOCK));
  }

  @Test
  void createProduct_outOfStock() {
    fixture
        .given()
        .when(
            new ProductCommand.CreateProduct(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 0))
        .expectState(s -> assertThat(s.status()).isEqualTo(ProductStatus.OUT_OF_STOCK));
  }

  @Test
  void adjustStock_reducesStock() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 100))
        .when(new ProductCommand.AdjustStock("p-1", -30, "Sold"))
        .expectEvents(new ProductEvent.StockAdjusted("p-1", 100, 70, "Sold"))
        .expectState(s -> assertThat(s.stock()).isEqualTo(70));
  }

  @Test
  void adjustStock_insufficientStock_throws() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 5))
        .when(new ProductCommand.AdjustStock("p-1", -10, "Oversold"))
        .expectException(DomainException.class);
  }

  @Test
  void adjustStock_triggersLowStockStatus() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 15))
        .when(new ProductCommand.AdjustStock("p-1", -10, "Sale"))
        .expectState(s -> assertThat(s.status()).isEqualTo(ProductStatus.LOW_STOCK));
  }

  @Test
  void updatePrice() {
    var oldPrice = new Money(java.math.BigDecimal.TEN, "USD");
    var newPrice = new Money(java.math.BigDecimal.valueOf(25), "USD");
    fixture
        .given(new ProductEvent.ProductCreated("p-1", "Widget", null, null, oldPrice, 50))
        .when(new ProductCommand.UpdatePrice("p-1", newPrice))
        .expectEvents(new ProductEvent.PriceUpdated("p-1", oldPrice, newPrice));
  }

  @Test
  void discontinueProduct() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 50))
        .when(new ProductCommand.DiscontinueProduct("p-1"))
        .expectEvents(new ProductEvent.ProductDiscontinued("p-1"))
        .expectState(s -> assertThat(s.status()).isEqualTo(ProductStatus.DISCONTINUED));
  }

  @Test
  void discontinueProduct_alreadyDiscontinued_throws() {
    fixture
        .given(
            new ProductEvent.ProductCreated(
                "p-1", "Widget", null, null, new Money(java.math.BigDecimal.TEN, "USD"), 50),
            new ProductEvent.ProductDiscontinued("p-1"))
        .when(new ProductCommand.DiscontinueProduct("p-1"))
        .expectException(DomainException.class);
  }
}
