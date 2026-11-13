package org.streamrune.ecommerce.queries.dto;

/**
 * A product's inventory: the units available to order, reserved by orders in fulfillment, and
 * committed to shipped orders, with the product's name from the catalogue.
 */
public record InventoryView(
    String productId, String productName, int available, int reserved, int committed) {}
