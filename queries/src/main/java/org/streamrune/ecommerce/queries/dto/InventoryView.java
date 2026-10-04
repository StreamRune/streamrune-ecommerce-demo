package org.streamrune.ecommerce.queries.dto;

public record InventoryView(String productId, int available, int reserved, int committed) {}
