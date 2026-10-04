package org.streamrune.ecommerce.domain.product;

import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;
import org.streamrune.ecommerce.domain.common.Money;

public record ProductState(
    String productId,
    String name,
    String description,
    String category,
    Money price,
    int stock,
    ProductStatus status)
    implements AggregateState {
  /**
   * The aggregate type the product decider is registered under: streams are product:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("product");

  private static final int LOW_STOCK_THRESHOLD = 10;

  public ProductState() {
    this(null, null, null, null, null, 0, ProductStatus.AVAILABLE);
  }

  public static ProductStatus statusForStock(int stock) {
    if (stock <= 0) return ProductStatus.OUT_OF_STOCK;
    if (stock < LOW_STOCK_THRESHOLD) return ProductStatus.LOW_STOCK;
    return ProductStatus.AVAILABLE;
  }
}
