package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductStatus;

/**
 * The storefront's product: the catalogue's fields, the units the inventory holds available, and
 * the {@link Availability} derived from both.
 */
public record ProductView(
    String productId,
    String name,
    String description,
    String category,
    Money price,
    ProductStatus status,
    int available,
    Availability availability,
    Instant createdAt,
    Instant updatedAt) {}
