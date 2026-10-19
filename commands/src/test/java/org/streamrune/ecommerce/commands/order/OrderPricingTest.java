package org.streamrune.ecommerce.commands.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.ecommerce.commands.product.ProductDecider;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.order.OrderEvent;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.test.InMemoryCommandBus;
import org.streamrune.test.InMemoryCommandBus.DeciderRegistration;
import org.streamrune.test.InMemoryEventStore;

/**
 * How {@link OrderPricing} prices an order: each line at the product's price as its event stream
 * holds it, a product nobody created or one that is discontinued refused.
 */
class OrderPricingTest {

  private final InMemoryEventStore eventStore = new InMemoryEventStore();
  private final InMemoryCommandBus products = productBus(eventStore);
  private final OrderPricing pricing = new OrderPricing(eventStore);

  @Test
  void eachLineIsPricedFromTheCatalogue() {
    createProduct("p-widget", "9.99");
    createProduct("p-gadget", "24.50");

    OrderCommand.PlaceOrder order =
        pricing.price(
            "o-1",
            "c-1",
            List.of(
                new OrderPricing.RequestedLine("p-widget", 2),
                new OrderPricing.RequestedLine("p-gadget", 1)));

    assertThat(order.orderId()).isEqualTo("o-1");
    assertThat(order.customerId()).isEqualTo("c-1");
    assertThat(order.lines())
        .containsExactly(
            new OrderCommand.OrderLine("p-widget", 2, usd("9.99")),
            new OrderCommand.OrderLine("p-gadget", 1, usd("24.50")));
    OrderDecider orders = new OrderDecider();
    OrderEvent.OrderPlaced placed =
        (OrderEvent.OrderPlaced) orders.decide(order, orders.initialState()).getFirst();
    assertThat(placed.total().amount()).isEqualByComparingTo("44.48");
  }

  @Test
  void aPriceUpdateIsSeenAtOnce() {
    createProduct("p-widget", "9.99");
    products.execute(new ProductCommand.UpdatePrice("p-widget", usd("12.00")));

    assertThat(priceOf("p-widget")).isEqualTo(usd("12.00"));
  }

  @Test
  void anUnknownProductIsRefused() {
    assertThatThrownBy(() -> priceOf("p-nobody"))
        .isExactlyInstanceOf(DomainException.class)
        .hasMessage("Product not found");
  }

  @Test
  void aDiscontinuedProductIsRefused() {
    createProduct("p-widget", "9.99");
    products.execute(new ProductCommand.DiscontinueProduct("p-widget"));

    assertThatThrownBy(() -> priceOf("p-widget"))
        .isExactlyInstanceOf(DomainException.class)
        .hasMessage("Product is discontinued");
  }

  @Test
  void aProductWithMoreEventsThanOnePageIsFoldedWhole() {
    createProduct("p-widget", "9.99");
    for (int i = 0; i < 120; i++) {
      products.execute(new ProductCommand.AdjustStock("p-widget", 1, "restock " + i));
    }
    products.execute(new ProductCommand.UpdatePrice("p-widget", usd("15.00")));

    assertThat(priceOf("p-widget")).isEqualTo(usd("15.00"));
  }

  @Test
  void aMalformedProductIdIsRefusedBeforeAnyRead() {
    AtomicInteger reads = new AtomicInteger();
    OrderPricing counted = new OrderPricing(counting(eventStore, reads));

    assertThatThrownBy(
            () ->
                counted.price(
                    "o-1", "c-1", List.of(new OrderPricing.RequestedLine("p-\u0007bell", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(messageOf(() -> OrderCommand.OrderLine.requireValidProductId("p-\u0007bell")));
    assertThat(reads).as("the event store is not read").hasValue(0);
  }

  @Test
  void anOrderIdTooLongForItsSagaIdIsRefusedBeforeAnyRead() {
    AtomicInteger reads = new AtomicInteger();
    OrderPricing counted = new OrderPricing(counting(eventStore, reads));
    String orderId = "o".repeat(OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH + 1);

    assertThatThrownBy(
            () -> counted.price(orderId, "c-1", List.of(new OrderPricing.RequestedLine("p-1", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "orderId must be at most "
                + OrderCommand.PlaceOrder.MAX_ORDER_ID_LENGTH
                + " characters, got "
                + orderId.length());
    assertThat(reads).as("the event store is not read").hasValue(0);
  }

  @Test
  void twoLinesOfOneProductAreRefused() {
    createProduct("p-widget", "9.99");
    AtomicInteger reads = new AtomicInteger();
    OrderPricing pricing = new OrderPricing(counting(eventStore, reads));

    assertThatThrownBy(
            () ->
                pricing.price(
                    "o-1",
                    "c-1",
                    List.of(
                        new OrderPricing.RequestedLine("p-widget", 1),
                        new OrderPricing.RequestedLine("p-widget", 2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lines must name each product once");
    assertThat(reads).as("the event store is not read").hasValue(0);
  }

  @Test
  void anEmptyOrNullLineListIsRefused() {
    assertThatThrownBy(() -> pricing.price("o-1", "c-1", List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lines must not be empty");
    assertThatThrownBy(() -> pricing.price("o-1", "c-1", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lines must not be empty");
    assertThatThrownBy(
            () ->
                pricing.price(
                    "o-1", "c-1", java.util.Arrays.asList((OrderPricing.RequestedLine) null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lines must not hold null");
  }

  private Money priceOf(String productId) {
    return pricing
        .price("o-1", "c-1", List.of(new OrderPricing.RequestedLine(productId, 1)))
        .lines()
        .getFirst()
        .unitPrice();
  }

  private void createProduct(String productId, String price) {
    products.execute(
        new ProductCommand.CreateProduct(
            productId, "Product " + productId, null, "General", usd(price), 50));
  }

  private static Money usd(String amount) {
    return new Money(new BigDecimal(amount), "USD");
  }

  private static String messageOf(Runnable check) {
    try {
      check.run();
    } catch (IllegalArgumentException e) {
      return e.getMessage();
    }
    throw new AssertionError("the check refused nothing");
  }

  private static InMemoryCommandBus productBus(EventStore eventStore) {
    ProductDecider decider = new ProductDecider();
    return InMemoryCommandBus.builder()
        .eventStore(eventStore)
        .register(registration(ProductCommand.CreateProduct.class, c -> c.productId(), decider))
        .register(registration(ProductCommand.UpdatePrice.class, c -> c.productId(), decider))
        .register(registration(ProductCommand.AdjustStock.class, c -> c.productId(), decider))
        .register(
            registration(ProductCommand.DiscontinueProduct.class, c -> c.productId(), decider))
        .build();
  }

  /** One product command type routed to the product decider, as the apps register it. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <C extends ProductCommand>
      DeciderRegistration<C, ProductState, ProductEvent> registration(
          Class<C> type, Function<C, String> productId, ProductDecider decider) {
    return new DeciderRegistration<>(
        ProductState.TYPE, type, c -> AggregateId.of(productId.apply(c)), (Decider) decider);
  }

  /** The store, with every call counted. */
  private static EventStore counting(EventStore delegate, AtomicInteger calls) {
    return (EventStore)
        Proxy.newProxyInstance(
            EventStore.class.getClassLoader(),
            new Class<?>[] {EventStore.class},
            (proxy, method, args) -> {
              calls.incrementAndGet();
              try {
                return method.invoke(delegate, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }
}
