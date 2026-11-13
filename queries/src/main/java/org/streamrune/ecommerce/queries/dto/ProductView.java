package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductStatus;

/**
 * The storefront's product: the catalogue's fields and the {@link Availability} the projection
 * derives from the catalogue's status and the inventory's units. The view carries no unit count:
 * the counts are the inventory's, served to an {@code ADMIN} by the inventory view.
 */
public record ProductView(
    String productId,
    String name,
    String description,
    String category,
    Money price,
    ProductStatus status,
    Availability availability,
    Instant createdAt,
    Instant updatedAt) {}
