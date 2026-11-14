package org.streamrune.ecommerce.queries.dto;

/**
 * How the storefront shows a product: derived by the product projection from the catalogue's status
 * and the inventory's available units. Not a state of either aggregate.
 */
public enum Availability {
  IN_STOCK,
  LOW_STOCK,
  OUT_OF_STOCK,
  DISCONTINUED
}
