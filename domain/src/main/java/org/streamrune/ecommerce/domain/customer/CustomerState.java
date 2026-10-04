package org.streamrune.ecommerce.domain.customer;

import org.streamrune.core.AggregateState;
import org.streamrune.core.types.AggregateType;

public record CustomerState(
    String customerId,
    String name,
    String email,
    String address,
    String phone,
    CustomerStatus status)
    implements AggregateState {
  /**
   * The aggregate type the customer decider is registered under: streams are customer:<customerId>.
   */
  public static final AggregateType TYPE = AggregateType.of("customer");

  public CustomerState() {
    this(null, null, null, null, null, CustomerStatus.ACTIVE);
  }
}
