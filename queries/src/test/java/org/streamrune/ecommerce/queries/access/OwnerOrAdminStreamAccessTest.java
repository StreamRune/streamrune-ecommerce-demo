package org.streamrune.ecommerce.queries.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The decision table of {@link OwnerOrAdminStreamAccess}: one row per caller, role and stream. The
 * order read model of the table holds one order, {@code o-1}, placed by customer {@code c-1}.
 */
class OwnerOrAdminStreamAccessTest {

  /** A request without {@code X-User-Role}: the context has no {@code role} baggage entry. */
  private static final String NO_ROLE = null;

  /** No request context is bound at all. */
  private static final String NO_CONTEXT = "<no request context>";

  private static final Map<String, OrderView> ORDERS =
      Map.of(
          "o-1",
          new OrderView(
              "o-1",
              "c-1",
              List.of(),
              Money.usd(new BigDecimal("10.00")),
              OrderStatus.CREATED,
              Instant.EPOCH,
              Instant.EPOCH));

  static Stream<Arguments> decisions() {
    return Stream.of(
        // caller, role, stream, allowed
        // No identity: refused whatever the role header says and whatever the stream is.
        Arguments.of(null, NO_ROLE, "customer:c-1", false),
        Arguments.of(null, "ADMIN", "customer:c-1", false),
        Arguments.of(null, "ADMIN", "order:o-1", false),
        Arguments.of(null, "ADMIN", "product:p-1", false),
        Arguments.of(null, NO_CONTEXT, "customer:c-1", false),
        // customer: the customer themself, or ADMIN.
        Arguments.of("c-1", "CUSTOMER", "customer:c-1", true),
        Arguments.of("c-1", NO_ROLE, "customer:c-1", true),
        Arguments.of("c-1", NO_CONTEXT, "customer:c-1", true),
        Arguments.of("c-2", "CUSTOMER", "customer:c-1", false),
        Arguments.of("c-2", NO_ROLE, "customer:c-1", false),
        Arguments.of("c-2", NO_CONTEXT, "customer:c-1", false),
        Arguments.of("C-1", "CUSTOMER", "customer:c-1", false),
        Arguments.of("ops-1", "ADMIN", "customer:c-1", true),
        // The role is matched exactly, as the admin endpoints match it.
        Arguments.of("ops-1", "admin", "customer:c-1", false),
        Arguments.of("ops-1", "GUEST", "customer:c-1", false),
        // order: the customer the read model names, or ADMIN.
        Arguments.of("c-1", "CUSTOMER", "order:o-1", true),
        Arguments.of("c-1", NO_CONTEXT, "order:o-1", true),
        Arguments.of("c-2", "CUSTOMER", "order:o-1", false),
        Arguments.of("ops-1", "ADMIN", "order:o-1", true),
        // An order the read model has no row for has no known owner.
        Arguments.of("c-1", "CUSTOMER", "order:o-not-projected", false),
        Arguments.of("ops-1", "ADMIN", "order:o-not-projected", true),
        // An order id equal to the caller's id is not ownership.
        Arguments.of("o-1", "CUSTOMER", "order:o-1", false),
        // payment, inventory, product: ADMIN only.
        Arguments.of("c-1", "CUSTOMER", "payment:pay-o-1", false),
        Arguments.of("ops-1", "ADMIN", "payment:pay-o-1", true),
        Arguments.of("c-1", "CUSTOMER", "inventory:p-1", false),
        Arguments.of("ops-1", "ADMIN", "inventory:p-1", true),
        Arguments.of("c-1", "CUSTOMER", "product:p-1", false),
        Arguments.of("c-1", NO_ROLE, "product:p-1", false),
        Arguments.of("ops-1", "ADMIN", "product:p-1", true),
        Arguments.of("ops-1", NO_CONTEXT, "product:p-1", false),
        // A type the rule does not name: refused, for ADMIN and for a caller whose id matches.
        Arguments.of("ops-1", "ADMIN", "shipment:s-1", false),
        Arguments.of("c-1", "CUSTOMER", "shipment:c-1", false),
        Arguments.of("c-1", "CUSTOMER", "customers:c-1", false));
  }

  @ParameterizedTest(name = "caller {0} with role {1} on {2} -> {3}")
  @MethodSource("decisions")
  void decides(String caller, String role, String stream, boolean allowed) {
    OwnerOrAdminStreamAccess access =
        new OwnerOrAdminStreamAccess(() -> requestContext(caller, role), ORDERS::get);

    assertEquals(
        allowed, access.isAuthorized(caller == null ? null : UserId.of(caller), stream(stream)));
  }

  @Test
  void adminMayOpenEveryAggregateTypeOfTheDomain() {
    OwnerOrAdminStreamAccess access =
        new OwnerOrAdminStreamAccess(() -> requestContext("ops-1", "ADMIN"), ORDERS::get);

    for (AggregateType type :
        List.of(
            CustomerState.TYPE,
            OrderState.TYPE,
            PaymentState.TYPE,
            InventoryState.TYPE,
            ProductState.TYPE)) {
      assertTrue(
          access.isAuthorized(UserId.of("ops-1"), StreamId.of(type, AggregateId.of("x-1"))),
          () -> "ADMIN on " + type.value());
    }
  }

  @Test
  void theOrderReadModelIsAskedOnlyForAnOrderStreamOfANonAdminCaller() {
    List<String> lookups = new ArrayList<>();
    OwnerOrAdminStreamAccess asCustomer =
        new OwnerOrAdminStreamAccess(
            () -> requestContext("c-1", "CUSTOMER"),
            orderId -> {
              lookups.add(orderId);
              return ORDERS.get(orderId);
            });

    assertTrue(asCustomer.isAuthorized(UserId.of("c-1"), stream("customer:c-1")));
    assertFalse(asCustomer.isAuthorized(UserId.of("c-1"), stream("product:p-1")));
    assertFalse(asCustomer.isAuthorized(null, stream("order:o-1")));
    assertEquals(List.of(), lookups);

    assertTrue(asCustomer.isAuthorized(UserId.of("c-1"), stream("order:o-1")));
    assertEquals(List.of("o-1"), lookups);
  }

  @Test
  void bothCollaboratorsAreRequired() {
    assertThrows(
        IllegalArgumentException.class, () -> new OwnerOrAdminStreamAccess(null, ORDERS::get));
    assertThrows(
        IllegalArgumentException.class, () -> new OwnerOrAdminStreamAccess(() -> null, null));
  }

  private static StreamId stream(String typeAndId) {
    int separator = typeAndId.indexOf(':');
    return StreamId.of(
        AggregateType.of(typeAndId.substring(0, separator)),
        AggregateId.of(typeAndId.substring(separator + 1)));
  }

  /**
   * The context the framework's request filter builds in the trusted-gateway mode: the caller from
   * {@code X-User-Id} and the {@code role} baggage entry from {@code X-User-Role}.
   */
  private static StreamRuneContext.RequestContext requestContext(String caller, String role) {
    if (NO_CONTEXT.equals(role)) {
      return null;
    }
    return new StreamRuneContext.RequestContext(
        null,
        caller == null ? null : UserId.of(caller),
        CorrelationId.of("corr-1"),
        Instant.EPOCH,
        role == null ? Map.of() : Map.of("role", role));
  }
}
