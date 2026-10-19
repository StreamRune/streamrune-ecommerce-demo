package org.streamrune.ecommerce.domain.order;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.streamrune.core.Command;
import org.streamrune.core.RequireRole;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.ecommerce.domain.common.Money;

public sealed interface OrderCommand extends Command {
  /**
   * Places an order. The constructor checks the order id's length, so every app that builds a
   * {@code PlaceOrder} applies the same rule: a client-supplied order id must also become a valid
   * saga id. The order stream takes any id {@code AggregateId.of} accepts, up to 255 characters,
   * but the fulfillment saga that {@code OrderPlaced} starts is identified by {@code "fulfillment-"
   * + orderId}, and a {@code SagaId} holds 255 characters at most. Refused only there, the order
   * would already be placed and the saga could never start: its first event would be quarantined as
   * poison and the order would stay {@code CREATED} for good. Refused here, the order endpoint
   * answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>A blank order id or one with a control character is refused where the command bus builds the
   * stream id with {@code AggregateId.of}, before anything is written, so the constructor adds only
   * the length rule. The payment id {@code "pay-" + orderId} is shorter by eight characters than
   * the saga id and fits whenever that does.
   *
   * <p>The lines are not empty and name each product once: stock is reserved per order and product,
   * so two lines of one product would be reserved once. The message never echoes a product id.
   */
  record PlaceOrder(String orderId, String customerId, List<OrderLine> lines)
      implements OrderCommand {

    /** The prefix of the saga id the order starts: {@code "fulfillment-" + orderId}. */
    public static final String SAGA_ID_PREFIX = "fulfillment-";

    /** The longest order id whose saga id {@code "fulfillment-" + orderId} fits 255 characters. */
    public static final int MAX_ORDER_ID_LENGTH =
        IdConstraints.MAX_LENGTH - SAGA_ID_PREFIX.length();

    public PlaceOrder {
      if (orderId != null && orderId.length() > MAX_ORDER_ID_LENGTH) {
        throw new IllegalArgumentException(
            "orderId must be at most "
                + MAX_ORDER_ID_LENGTH
                + " characters, got "
                + orderId.length());
      }
      if (lines == null || lines.isEmpty()) {
        throw new IllegalArgumentException("lines must not be empty");
      }
      Set<String> products = new HashSet<>();
      for (OrderLine line : lines) {
        if (line == null) {
          throw new IllegalArgumentException("lines must not hold null");
        }
        if (!products.add(line.productId())) {
          throw new IllegalArgumentException("lines must name each product once");
        }
      }
      lines = List.copyOf(lines);
    }
  }

  record ConfirmOrder(String orderId) implements OrderCommand {}

  @RequireRole("ADMIN")
  record ShipOrder(String orderId) implements OrderCommand {}

  @RequireRole("ADMIN")
  record DeliverOrder(String orderId) implements OrderCommand {}

  record CancelOrder(String orderId, String reason) implements OrderCommand {}

  /**
   * One line of an order to place. The constructor checks the product id, so every app that builds
   * a {@code PlaceOrder} applies the same rule: a client-supplied product id must become a valid
   * inventory id. The saga reserves stock on the inventory aggregate whose id is the product id,
   * and the inventory extractor builds that id with {@code AggregateId.of}, which refuses a control
   * character or an id longer than 255 characters. Refused only there, the order would already be
   * placed and paid for, and the saga would cancel it and the cancellation refund it. Refused here,
   * the order endpoint answers {@code 400} and nothing is written. The message never echoes the id.
   *
   * <p>{@link OrderEvent.OrderLine}, the line an {@code OrderPlaced} event records, has no such
   * rule: it is rebuilt from the event log and from saga state, where a rule would make a stored
   * order unreadable instead of stopping a write.
   */
  record OrderLine(String productId, int quantity, Money unitPrice) {

    /** The longest product id: it is the inventory aggregate id, so the id bound applies as is. */
    public static final int MAX_PRODUCT_ID_LENGTH = IdConstraints.MAX_LENGTH;

    public OrderLine {
      requireValidProductId(productId);
      if (quantity <= 0) {
        throw new IllegalArgumentException("quantity must be positive, got " + quantity);
      }
      if (unitPrice == null || unitPrice.amount() == null || unitPrice.amount().signum() <= 0) {
        throw new IllegalArgumentException("unitPrice must be positive");
      }
    }

    /**
     * The rule a product id of an order line follows: present, at most {@value
     * #MAX_PRODUCT_ID_LENGTH} characters, no control character. The message never echoes the id.
     */
    public static void requireValidProductId(String productId) {
      if (productId == null || productId.isBlank()) {
        throw new IllegalArgumentException("productId is required");
      }
      if (productId.length() > MAX_PRODUCT_ID_LENGTH) {
        throw new IllegalArgumentException(
            "productId must be at most "
                + MAX_PRODUCT_ID_LENGTH
                + " characters, got "
                + productId.length());
      }
      IdConstraints.requireNoControlCharacters(productId, "productId");
    }
  }
}
