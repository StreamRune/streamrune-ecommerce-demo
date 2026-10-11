package org.streamrune.ecommerce.queries.access;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
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
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The decision table of {@link OwnerOrAdminAccess}: one row per caller, role and resource. Customer
 * {@code c-1} placed order {@code o-1}.
 */
class OwnerOrAdminAccessTest {

  /** A request without {@code X-User-Role}: the context has no {@code role} baggage entry. */
  private static final String NO_ROLE = null;

  private static final OrderView ORDER_OF_C1 =
      new OrderView(
          "o-1",
          "c-1",
          List.of(),
          Money.usd(new BigDecimal("10.00")),
          OrderStatus.CREATED,
          Instant.EPOCH,
          Instant.EPOCH);

  /** No row in the order read model: an order that was never placed, or not projected yet. */
  private static final OrderView NO_ROW = null;

  static Stream<Arguments> customerDecisions() {
    return Stream.of(
        // caller, role, customer, allowed
        // No identity: refused whatever the role header says.
        Arguments.of(null, NO_ROLE, "c-1", false),
        Arguments.of(null, "CUSTOMER", "c-1", false),
        Arguments.of(null, "ADMIN", "c-1", false),
        // The customer themself, with any role or none.
        Arguments.of("c-1", "CUSTOMER", "c-1", true),
        Arguments.of("c-1", NO_ROLE, "c-1", true),
        Arguments.of("c-1", "GUEST", "c-1", true),
        // Another customer.
        Arguments.of("c-2", "CUSTOMER", "c-1", false),
        Arguments.of("c-2", NO_ROLE, "c-1", false),
        // Ids are compared exactly.
        Arguments.of("C-1", "CUSTOMER", "c-1", false),
        Arguments.of("c-1", "CUSTOMER", "c-10", false),
        // A request that names no customer is nobody's.
        Arguments.of("c-1", "CUSTOMER", null, false),
        // ADMIN, matched exactly.
        Arguments.of("ops-1", "ADMIN", "c-1", true),
        Arguments.of("ops-1", "ADMIN", null, true),
        Arguments.of("ops-1", "admin", "c-1", false),
        Arguments.of("ops-1", "ADMIN ", "c-1", false));
  }

  @ParameterizedTest(name = "caller {0} with role {1} on customer {2} -> {3}")
  @MethodSource("customerDecisions")
  void decidesForACustomer(String caller, String role, String customerId, boolean allowed) {
    StreamRuneContext.RequestContext context = requestContext(caller, role);

    assertEquals(allowed, OwnerOrAdminAccess.mayAccessCustomer(context, customerId));
    if (allowed) {
      assertDoesNotThrow(() -> OwnerOrAdminAccess.requireCustomerAccess(context, customerId));
    } else {
      assertThrows(
          AccessRefusedException.class,
          () -> OwnerOrAdminAccess.requireCustomerAccess(context, customerId));
    }
  }

  static Stream<Arguments> orderDecisions() {
    return Stream.of(
        // caller, role, order row, allowed
        Arguments.of(null, NO_ROLE, ORDER_OF_C1, false),
        Arguments.of(null, "ADMIN", ORDER_OF_C1, false),
        Arguments.of(null, "ADMIN", NO_ROW, false),
        // The customer the row names.
        Arguments.of("c-1", "CUSTOMER", ORDER_OF_C1, true),
        Arguments.of("c-1", NO_ROLE, ORDER_OF_C1, true),
        Arguments.of("c-2", "CUSTOMER", ORDER_OF_C1, false),
        // An order id equal to the caller's id is not ownership.
        Arguments.of("o-1", "CUSTOMER", ORDER_OF_C1, false),
        // An order without a row has no known owner: an ADMIN goes on, nobody else does.
        Arguments.of("c-1", "CUSTOMER", NO_ROW, false),
        Arguments.of("ops-1", "ADMIN", NO_ROW, true),
        Arguments.of("ops-1", "ADMIN", ORDER_OF_C1, true),
        Arguments.of("ops-1", "admin", ORDER_OF_C1, false));
  }

  @ParameterizedTest(name = "caller {0} with role {1} on order row {2} -> {3}")
  @MethodSource("orderDecisions")
  void decidesForAnOrder(String caller, String role, OrderView order, boolean allowed) {
    StreamRuneContext.RequestContext context = requestContext(caller, role);

    assertEquals(allowed, OwnerOrAdminAccess.mayAccessOrder(context, order));
    if (allowed) {
      assertDoesNotThrow(() -> OwnerOrAdminAccess.requireOrderAccess(context, order));
    } else {
      assertThrows(
          AccessRefusedException.class,
          () -> OwnerOrAdminAccess.requireOrderAccess(context, order));
    }
  }

  @Test
  void aMissingRequestContextIsNobody() {
    assertNull(OwnerOrAdminAccess.identityOf(null));
    assertFalse(OwnerOrAdminAccess.isAdmin(null));
    assertThrows(AccessRefusedException.class, () -> OwnerOrAdminAccess.requireAdmin(null));
    assertFalse(OwnerOrAdminAccess.mayAccessCustomer(null, "c-1"));
    assertFalse(OwnerOrAdminAccess.mayAccessOrder(null, ORDER_OF_C1));
    assertThrows(AccessRefusedException.class, () -> OwnerOrAdminAccess.requireIdentity(null));
  }

  @Test
  void theIdentityIsTheUserIdOfTheRequestContext() {
    assertEquals("c-1", OwnerOrAdminAccess.identityOf(requestContext("c-1", "CUSTOMER")));
    assertEquals("c-1", OwnerOrAdminAccess.requireIdentity(requestContext("c-1", NO_ROLE)));
    assertNull(OwnerOrAdminAccess.identityOf(requestContext(null, "ADMIN")));
    assertThrows(
        AccessRefusedException.class,
        () -> OwnerOrAdminAccess.requireIdentity(requestContext(null, "ADMIN")));
  }

  /**
   * One definition of an ADMIN, for a customer's data and for the ADMIN-only endpoints alike: the
   * role together with an identity. The role entry without an identity is nobody, and so is an
   * identity without the role.
   */
  @ParameterizedTest(name = "caller={0} role={1} -> admin={2}")
  @MethodSource("adminDecisions")
  void anAdminIsTheRoleTogetherWithAnIdentity(String caller, String role, boolean admin) {
    StreamRuneContext.RequestContext context = requestContext(caller, role);

    assertEquals(admin, OwnerOrAdminAccess.isAdmin(context));
    if (admin) {
      assertDoesNotThrow(() -> OwnerOrAdminAccess.requireAdmin(context));
    } else {
      AccessRefusedException refused =
          assertThrows(
              AccessRefusedException.class, () -> OwnerOrAdminAccess.requireAdmin(context));
      assertEquals("Required role: ADMIN", refused.getMessage());
    }
  }

  static Stream<Arguments> adminDecisions() {
    return Stream.of(
        // caller, role, admin
        Arguments.of("ops-1", "ADMIN", true),
        // The role without an identity: refused, on every endpoint.
        Arguments.of(null, "ADMIN", false),
        // An identity without the role.
        Arguments.of("c-1", "CUSTOMER", false),
        Arguments.of("c-1", NO_ROLE, false),
        Arguments.of("c-1", "admin", false),
        Arguments.of("c-1", "ADMIN,CUSTOMER", false),
        // Neither.
        Arguments.of(null, NO_ROLE, false),
        Arguments.of(null, "CUSTOMER", false));
  }

  /**
   * ADMIN is the {@code role} baggage entry. The authority the request filter captured for the
   * caller is a second place a role could be read from, and the rule does not read it.
   */
  @Test
  void adminIsTheRoleBaggageEntryNotTheCapturedAuthority() {
    UserAuthority adminAuthority = new UserAuthority(Set.of("ADMIN"), Set.of());
    StreamRuneContext.RequestContext customerWithAdminAuthority =
        requestContext("c-2", "CUSTOMER").withAuthority(adminAuthority);

    assertFalse(OwnerOrAdminAccess.isAdmin(customerWithAdminAuthority));
    assertThrows(
        AccessRefusedException.class,
        () -> OwnerOrAdminAccess.requireAdmin(customerWithAdminAuthority));
    assertFalse(OwnerOrAdminAccess.mayAccessCustomer(customerWithAdminAuthority, "c-1"));
    assertFalse(OwnerOrAdminAccess.mayAccessOrder(customerWithAdminAuthority, ORDER_OF_C1));
  }

  @Test
  void aRefusalNeverNamesTheResource() {
    AccessRefusedException refused =
        assertThrows(
            AccessRefusedException.class,
            () ->
                OwnerOrAdminAccess.requireCustomerAccess(
                    requestContext("c-2", "CUSTOMER"), "c-secret"));

    assertFalse(refused.getMessage().contains("c-secret"));
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
