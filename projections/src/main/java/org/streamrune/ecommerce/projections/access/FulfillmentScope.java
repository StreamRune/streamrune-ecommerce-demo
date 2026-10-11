package org.streamrune.ecommerce.projections.access;

import java.util.List;

/**
 * Which fulfillment sagas one caller may list: {@link OwnerOrAdminQueries#fulfillments} decides it
 * and {@link FulfillmentSagaRows} reads exactly that.
 *
 * @param everySaga {@code true} for an {@code ADMIN}: the list is not narrowed
 * @param sagaIds for any other caller, the ids of the fulfillment sagas of the caller's own orders,
 *     and the only rows the list may hold; empty when {@code everySaga} is {@code true}
 */
public record FulfillmentScope(boolean everySaga, List<String> sagaIds) {

  public FulfillmentScope {
    if (sagaIds == null) {
      throw new IllegalArgumentException("sagaIds is required");
    }
    if (everySaga && !sagaIds.isEmpty()) {
      throw new IllegalArgumentException("a scope of every saga names no saga");
    }
    sagaIds = List.copyOf(sagaIds);
  }

  /** An {@code ADMIN}'s scope: every saga. */
  public static FulfillmentScope all() {
    return new FulfillmentScope(true, List.of());
  }

  /** Any other caller's scope: the sagas with the given ids and no other. */
  public static FulfillmentScope only(List<String> sagaIds) {
    return new FulfillmentScope(false, sagaIds);
  }
}
