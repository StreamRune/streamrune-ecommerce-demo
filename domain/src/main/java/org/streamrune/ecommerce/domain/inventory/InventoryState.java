package org.streamrune.ecommerce.domain.inventory;

import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record InventoryState(String productId, int available, int reserved, int committed)
    implements AggregateState {
  /**
   * The aggregate type the inventory decider is registered under: streams are
   * inventory:<productId>.
   */
  public static final AggregateType TYPE = AggregateType.of("inventory");

  public InventoryState() {
    this(null, 0, 0, 0);
  }
}
