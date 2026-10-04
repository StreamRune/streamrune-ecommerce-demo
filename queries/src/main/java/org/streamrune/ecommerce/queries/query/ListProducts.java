package org.streamrune.ecommerce.queries.query;

import org.streamrune.core.Cacheable;
import org.streamrune.ecommerce.domain.product.ProductEvent;

@Cacheable(
    ttlSeconds = 60,
    invalidateOn = {
      ProductEvent.ProductCreated.class, ProductEvent.PriceUpdated.class,
      ProductEvent.StockAdjusted.class, ProductEvent.ProductDiscontinued.class
    })
public record ListProducts()
    implements org.streamrune.core.Query<
        java.util.List<org.streamrune.ecommerce.queries.dto.ProductView>> {}
