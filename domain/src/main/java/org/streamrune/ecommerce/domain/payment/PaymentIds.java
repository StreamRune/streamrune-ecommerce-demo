package org.streamrune.ecommerce.domain.payment;

/** The payment of an order: one per order, named after it. */
public final class PaymentIds {

  /** The prefix of an order's payment id. */
  public static final String PREFIX = "pay-";

  private PaymentIds() {}

  /** The id of the payment that pays for {@code orderId}: {@code "pay-" + orderId}. */
  public static String forOrder(String orderId) {
    return PREFIX + orderId;
  }
}
