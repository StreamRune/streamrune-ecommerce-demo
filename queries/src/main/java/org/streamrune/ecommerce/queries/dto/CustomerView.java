package org.streamrune.ecommerce.queries.dto;

import java.time.Instant;
import org.streamrune.ecommerce.domain.customer.CustomerStatus;

public record CustomerView(
    String customerId,
    String name,
    String email,
    String address,
    String phone,
    CustomerStatus status,
    Instant createdAt,
    Instant updatedAt) {}
