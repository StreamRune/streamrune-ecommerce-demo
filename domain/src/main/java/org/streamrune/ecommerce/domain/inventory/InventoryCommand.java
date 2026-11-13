package org.streamrune.ecommerce.domain.inventory;

import jakarta.validation.constraints.NotBlank;
import org.streamrune.core.Command;

public sealed interface InventoryCommand extends Command {
  record ReserveStock(String productId, String orderId, int quantity) implements InventoryCommand {}

  record ReleaseStock(String productId, String orderId) implements InventoryCommand {}

  record ConfirmReservation(String productId, String orderId) implements InventoryCommand {}

  record ReceiveShipment(String productId, int quantity) implements InventoryCommand {}

  /**
   * A correction of the units on hand by a signed amount, a stocktake for example: a negative
   * change lowers the available units, a positive one raises them. Reserved and committed units are
   * not touched. The reason is required: the command bus's Bean Validation refuses a blank one.
   */
  record AdjustInventory(String productId, int quantityChange, @NotBlank String reason)
      implements InventoryCommand {}
}
