package org.streamrune.ecommerce.domain.product;

import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;
import org.streamrune.ecommerce.domain.common.Money;

/**
 * What the catalogue knows of a product: what it is and what it costs. How many units the shop
 * holds is the inventory's ({@code InventoryState}).
 */
public record ProductState(
    String productId,
    String name,
    String description,
    String category,
    Money price,
    ProductStatus status)
    implements AggregateState {
  /**
   * The aggregate type the product decider is registered under: streams are product:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("product");

  public ProductState() {
    this(null, null, null, null, null, ProductStatus.AVAILABLE);
  }
}
