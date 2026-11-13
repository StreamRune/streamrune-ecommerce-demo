package org.streamrune.ecommerce.commands.order;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventStore;
import org.streamrune.ecommerce.commands.product.ProductCatalog;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.domain.product.ProductStatus;

/**
 * Prices an order from the product catalogue. Each line's unit price is the product's price as its
 * event stream holds it when the order is placed; the client names products and quantities only.
 * Read from the event store through {@link ProductCatalog}, not from the product read model, so a
 * product created a moment ago is orderable and a price change applies at once. A product nobody
 * created, or one that is discontinued, is refused ({@code DomainException}, {@code 400}). An order
 * id too long for its saga id, a malformed product id and two lines of one product are refused
 * before the store is read.
 */
public final class OrderPricing {

  private final ProductCatalog catalog;

  public OrderPricing(EventStore eventStore) {
    this.catalog = new ProductCatalog(Objects.requireNonNull(eventStore, "eventStore is required"));
  }

  /** One line of an order as a client asks for it: a product and a quantity. */
  public record RequestedLine(String productId, int quantity) {}

  /** The {@code PlaceOrder} for the requested lines, each priced from the catalogue. */
  public OrderCommand.PlaceOrder price(
      String orderId, String customerId, List<RequestedLine> lines) {
    // The rules that need no read come first, so a request they refuse costs no read of the store.
    OrderCommand.PlaceOrder.requireValidOrderId(orderId);
    if (lines == null || lines.isEmpty()) {
      throw new IllegalArgumentException("lines must not be empty");
    }
    Set<String> products = new HashSet<>();
    for (RequestedLine line : lines) {
      if (line == null) {
        throw new IllegalArgumentException("lines must not hold null");
      }
      OrderCommand.OrderLine.requireValidProductId(line.productId());
      if (!products.add(line.productId())) {
        throw new IllegalArgumentException("lines must name each product once");
      }
    }
    List<OrderCommand.OrderLine> priced = new ArrayList<>(lines.size());
    for (RequestedLine line : lines) {
      ProductState product = catalog.current(line.productId());
      if (product.productId() == null) {
        throw new DomainException("Product not found");
      }
      if (product.status() == ProductStatus.DISCONTINUED) {
        throw new DomainException("Product is discontinued");
      }
      priced.add(new OrderCommand.OrderLine(line.productId(), line.quantity(), product.price()));
    }
    return new OrderCommand.PlaceOrder(orderId, customerId, priced);
  }
}
