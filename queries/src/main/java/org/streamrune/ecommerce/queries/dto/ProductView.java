package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductStatus;

public record ProductView(
    String productId,
    String name,
    String description,
    String category,
    Money price,
    int stock,
    ProductStatus status,
    Instant createdAt,
    Instant updatedAt) {}
