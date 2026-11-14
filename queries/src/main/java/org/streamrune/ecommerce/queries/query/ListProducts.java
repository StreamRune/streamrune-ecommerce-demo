package org.streamrune.ecommerce.queries.query;

import org.streamrune.core.Cacheable;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;

/**
 * The storefront's product list. The product view also follows the inventory, so the cached list is
 * dropped on every event that changes a product's available units as well as on the catalogue's
 * events.
 */
@Cacheable(
    ttlSeconds = 60,
    invalidateOn = {
      ProductEvent.ProductCreated.class,
      ProductEvent.PriceUpdated.class,
      ProductEvent.ProductDiscontinued.class,
      InventoryEvent.ShipmentReceived.class,
      InventoryEvent.InventoryAdjusted.class,
      InventoryEvent.StockReserved.class,
      InventoryEvent.StockReleased.class
    })
public record ListProducts()
    implements org.streamrune.core.Query<
        java.util.List<org.streamrune.ecommerce.queries.dto.ProductView>> {}
