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
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
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

  private static final Map<String, OrderView> ORDERS =
      Map.of(
          "o-1",
          new OrderView(
              "o-1",
              "c-1",
              List.of(),
              Money.usd(new BigDecimal("10.00")),
              OrderStatus.CREATED,
              null,
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
        // customer: the customer themself, or ADMIN.
        Arguments.of("c-1", "CUSTOMER", "customer:c-1", true),
        Arguments.of("c-1", NO_ROLE, "customer:c-1", true),
        Arguments.of("c-2", "CUSTOMER", "customer:c-1", false),
        Arguments.of("c-2", NO_ROLE, "customer:c-1", false),
        Arguments.of("C-1", "CUSTOMER", "customer:c-1", false),
        Arguments.of("ops-1", "ADMIN", "customer:c-1", true),
        // The role is matched exactly, as the admin endpoints match it.
        Arguments.of("ops-1", "admin", "customer:c-1", false),
        Arguments.of("ops-1", "GUEST", "customer:c-1", false),
        // order: the customer the read model names, or ADMIN.
        Arguments.of("c-1", "CUSTOMER", "order:o-1", true),
        Arguments.of("c-1", NO_ROLE, "order:o-1", true),
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
        Arguments.of("ops-1", NO_ROLE, "product:p-1", false),
        // A type the rule does not name: refused, for ADMIN and for a caller whose id matches.
        Arguments.of("ops-1", "ADMIN", "shipment:s-1", false),
        Arguments.of("c-1", "CUSTOMER", "shipment:c-1", false),
        Arguments.of("c-1", "CUSTOMER", "customers:c-1", false));
  }

  @ParameterizedTest(name = "caller {0} with role {1} on {2} -> {3}")
  @MethodSource("decisions")
  void decides(String caller, String role, String stream, boolean allowed) {
    OwnerOrAdminStreamAccess access = new OwnerOrAdminStreamAccess(ORDERS::get);

    assertEquals(allowed, access.isAuthorized(requestContext(caller, role), stream(stream)));
  }

  @Test
  void adminMayOpenEveryAggregateTypeOfTheDomain() {
    OwnerOrAdminStreamAccess access = new OwnerOrAdminStreamAccess(ORDERS::get);
    StreamRuneContext.RequestContext operator = requestContext("ops-1", "ADMIN");

    for (AggregateType type :
        List.of(
            CustomerState.TYPE,
            OrderState.TYPE,
            PaymentState.TYPE,
            InventoryState.TYPE,
            ProductState.TYPE)) {
      assertTrue(
          access.isAuthorized(operator, StreamId.of(type, AggregateId.of("x-1"))),
          () -> "ADMIN on " + type.value());
    }
  }

  /**
   * The framework's SSE endpoint refuses a request without a request context itself and never hands
   * the rule {@code null}. The rule does not rely on that: no caller is nobody.
   */
  @Test
  void aMissingRequestContextIsRefused() {
    OwnerOrAdminStreamAccess access = new OwnerOrAdminStreamAccess(ORDERS::get);

    assertFalse(access.isAuthorized(null, stream("customer:c-1")));
    assertFalse(access.isAuthorized(null, stream("order:o-1")));
    assertFalse(access.isAuthorized(null, stream("product:p-1")));
  }

  /**
   * The caller's id and role come from the one request context the rule is handed. The authority
   * the request filter captured for the caller is a second place a role could be read from, and the
   * rule does not read it: {@code ADMIN} is the {@code role} baggage entry, here and on the admin
   * endpoints.
   */
  @Test
  void adminIsTheRoleBaggageEntryNotTheCapturedAuthority() {
    OwnerOrAdminStreamAccess access = new OwnerOrAdminStreamAccess(ORDERS::get);
    UserAuthority adminAuthority = new UserAuthority(Set.of("ADMIN"), Set.of());

    StreamRuneContext.RequestContext customerWithAdminAuthority =
        requestContext("c-2", "CUSTOMER").withAuthority(adminAuthority);
    assertFalse(access.isAuthorized(customerWithAdminAuthority, stream("customer:c-1")));
    assertFalse(access.isAuthorized(customerWithAdminAuthority, stream("product:p-1")));

    StreamRuneContext.RequestContext operatorWithoutAuthority = requestContext("ops-1", "ADMIN");
    assertTrue(access.isAuthorized(operatorWithoutAuthority, stream("customer:c-1")));
    assertTrue(access.isAuthorized(operatorWithoutAuthority, stream("product:p-1")));
  }

  @Test
  void theOrderReadModelIsAskedOnlyForAnOrderStreamOfANonAdminCaller() {
    List<String> lookups = new ArrayList<>();
    OwnerOrAdminStreamAccess access =
        new OwnerOrAdminStreamAccess(
            orderId -> {
              lookups.add(orderId);
              return ORDERS.get(orderId);
            });
    StreamRuneContext.RequestContext customer = requestContext("c-1", "CUSTOMER");

    assertTrue(access.isAuthorized(customer, stream("customer:c-1")));
    assertFalse(access.isAuthorized(customer, stream("product:p-1")));
    assertFalse(access.isAuthorized(requestContext(null, "CUSTOMER"), stream("order:o-1")));
    assertTrue(access.isAuthorized(requestContext("ops-1", "ADMIN"), stream("order:o-1")));
    assertEquals(List.of(), lookups);

    assertTrue(access.isAuthorized(customer, stream("order:o-1")));
    assertEquals(List.of("o-1"), lookups);
  }

  @Test
  void theOrderReadModelIsRequired() {
    assertThrows(IllegalArgumentException.class, () -> new OwnerOrAdminStreamAccess(null));
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
    return new StreamRuneContext.RequestContext(
        null,
        caller == null ? null : UserId.of(caller),
        CorrelationId.of("corr-1"),
        Instant.EPOCH,
        role == null ? Map.of() : Map.of("role", role));
  }
}
