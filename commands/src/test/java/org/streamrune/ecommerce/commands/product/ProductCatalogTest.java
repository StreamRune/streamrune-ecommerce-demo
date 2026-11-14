package org.streamrune.ecommerce.commands.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.domain.product.ProductStatus;
import org.streamrune.test.InMemoryCommandBus;
import org.streamrune.test.InMemoryCommandBus.DeciderRegistration;
import org.streamrune.test.InMemoryEventStore;

/** How {@link ProductCatalog} answers from the product streams of the event store. */
class ProductCatalogTest {

  private final InMemoryEventStore eventStore = new InMemoryEventStore();
  private final InMemoryCommandBus products = productBus();
  private final ProductCatalog catalog = new ProductCatalog(eventStore);

  @Test
  void aCreatedProductExists() {
    create("p-widget");

    assertThatCode(() -> catalog.requireExists("p-widget")).doesNotThrowAnyException();
  }

  @Test
  void aProductNobodyCreatedIsNotFound() {
    assertThatThrownBy(() -> catalog.requireExists("p-none"))
        .isExactlyInstanceOf(DomainException.class)
        .hasMessage("Product not found: p-none");
    assertThat(catalog.current("p-none").productId()).isNull();
  }

  @Test
  void aDiscontinuedProductStillExists() {
    create("p-widget");
    products.execute(new ProductCommand.DiscontinueProduct("p-widget"));

    assertThatCode(() -> catalog.requireExists("p-widget")).doesNotThrowAnyException();
    assertThat(catalog.current("p-widget").status()).isEqualTo(ProductStatus.DISCONTINUED);
  }

  @Test
  void aProductWithMoreEventsThanOnePageIsFoldedWhole() {
    create("p-widget");
    for (int i = 0; i < 120; i++) {
      products.execute(new ProductCommand.UpdatePrice("p-widget", usd("10." + (i % 100))));
    }
    products.execute(new ProductCommand.UpdatePrice("p-widget", usd("15.00")));

    ProductState product = catalog.current("p-widget");

    assertThat(product.productId()).isEqualTo("p-widget");
    assertThat(product.price()).isEqualTo(usd("15.00"));
  }

  private void create(String productId) {
    products.execute(
        new ProductCommand.CreateProduct(productId, "Widget", null, "General", usd("9.99")));
  }

  private InMemoryCommandBus productBus() {
    ProductDecider decider = new ProductDecider();
    return InMemoryCommandBus.builder()
        .eventStore(eventStore)
        .register(
            registration(
                ProductCommand.CreateProduct.class,
                ProductCommand.CreateProduct::productId,
                decider))
        .register(
            registration(
                ProductCommand.UpdatePrice.class, ProductCommand.UpdatePrice::productId, decider))
        .register(
            registration(
                ProductCommand.DiscontinueProduct.class,
                ProductCommand.DiscontinueProduct::productId,
                decider))
        .build();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <C extends ProductCommand>
      DeciderRegistration<C, ProductState, ProductEvent> registration(
          Class<C> type, Function<C, String> productId, ProductDecider decider) {
    return new DeciderRegistration<>(
        ProductState.TYPE, type, c -> AggregateId.of(productId.apply(c)), (Decider) decider);
  }

  private static Money usd(String amount) {
    return new Money(new BigDecimal(amount), "USD");
  }
}
