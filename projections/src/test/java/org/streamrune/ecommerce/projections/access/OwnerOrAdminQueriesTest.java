package org.streamrune.ecommerce.projections.access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerStatus;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * Which rows {@link OwnerOrAdminQueries} hands to which caller. The read models hold two customers,
 * {@code alice} and {@code bob}; {@code alice} placed {@code o-a1} and {@code o-a2}, {@code bob}
 * placed {@code o-b1}; {@code carol} has an identity and no customer record.
 */
class OwnerOrAdminQueriesTest {

  private static final StreamRuneContext.RequestContext NO_CONTEXT = null;
  private static final StreamRuneContext.RequestContext ANONYMOUS = caller(null, null);
  private static final StreamRuneContext.RequestContext ROLE_WITHOUT_IDENTITY =
      caller(null, "ADMIN");
  private static final StreamRuneContext.RequestContext ALICE = caller("alice", "CUSTOMER");
  private static final StreamRuneContext.RequestContext BOB = caller("bob", "CUSTOMER");
  private static final StreamRuneContext.RequestContext CAROL = caller("carol", "CUSTOMER");
  private static final StreamRuneContext.RequestContext OPERATOR = caller("ops-1", "ADMIN");

  private final InMemoryProjectionRepository repository = new InMemoryProjectionRepository();

  /** The read-model methods the queries called, in order. */
  private final List<String> reads = new ArrayList<>();

  private final OwnerOrAdminQueries queries =
      new OwnerOrAdminQueries(
          new RecordingCustomerProjection(repository, reads),
          new RecordingOrderProjection(repository, reads));

  OwnerOrAdminQueriesTest() {
    for (String id : List.of("alice", "bob")) {
      repository.save(ProjectionName.of("customers"), id, customer(id));
    }
    repository.save(ProjectionName.of("orders"), "o-a1", order("o-a1", "alice"));
    repository.save(ProjectionName.of("orders"), "o-a2", order("o-a2", "alice"));
    repository.save(ProjectionName.of("orders"), "o-b1", order("o-b1", "bob"));
  }

  // --- one customer ---

  @Test
  void aCustomerReadsTheirOwnRecordAndAnAdminReadsAnyones() {
    assertThat(queries.customer(ALICE, "alice").customerId()).isEqualTo("alice");
    assertThat(queries.customer(OPERATOR, "alice").customerId()).isEqualTo("alice");
    assertThat(queries.customer(OPERATOR, "bob").customerId()).isEqualTo("bob");
  }

  @Test
  void anotherCustomersRecordIsRefusedBeforeTheReadModelIsAsked() {
    assertRefused(() -> queries.customer(BOB, "alice"));
    assertRefused(() -> queries.customer(BOB, "nobody"));
    assertRefused(() -> queries.customer(ANONYMOUS, "alice"));
    assertRefused(() -> queries.customer(ROLE_WITHOUT_IDENTITY, "alice"));
    assertRefused(() -> queries.customer(NO_CONTEXT, "alice"));

    assertThat(reads).as("a refused caller causes no read").isEmpty();
  }

  @Test
  void aCustomerWithoutARowIsNullForTheCustomerAndForAnAdmin() {
    // A forgotten customer has no row; so has an id nobody registered.
    assertThat(queries.customer(CAROL, "carol")).isNull();
    assertThat(queries.customer(OPERATOR, "carol")).isNull();
  }

  // --- the customer list ---

  @Test
  void anAdminListsEveryCustomer() {
    assertThat(queries.customers(OPERATOR))
        .extracting(CustomerView::customerId)
        .containsExactlyInAnyOrder("alice", "bob");
  }

  @Test
  void aCustomerListsTheirOwnRecordOnlyAndTheListIsNeverLoaded() {
    assertThat(queries.customers(ALICE))
        .extracting(CustomerView::customerId)
        .containsExactly("alice");
    assertThat(queries.customers(BOB)).extracting(CustomerView::customerId).containsExactly("bob");
    assertThat(queries.customers(CAROL)).isEmpty();

    assertThat(reads)
        .as("each list is one read by the caller's own id")
        .containsExactly("customers.get(alice)", "customers.get(bob)", "customers.get(carol)");
  }

  @Test
  void aCustomerListWithoutAnIdentityIsRefused() {
    assertRefused(() -> queries.customers(ANONYMOUS));
    assertRefused(() -> queries.customers(ROLE_WITHOUT_IDENTITY));
    assertRefused(() -> queries.customers(NO_CONTEXT));

    assertThat(reads).isEmpty();
  }

  // --- one order ---

  @Test
  void aCustomerReadsTheirOwnOrderAndAnAdminReadsAnyones() {
    assertThat(queries.order(ALICE, "o-a1").orderId()).isEqualTo("o-a1");
    assertThat(queries.order(BOB, "o-b1").orderId()).isEqualTo("o-b1");
    assertThat(queries.order(OPERATOR, "o-a1").orderId()).isEqualTo("o-a1");
  }

  @Test
  void anotherCustomersOrderAndAnOrderWithoutARowAreRefusedAlike() {
    assertRefused(() -> queries.order(BOB, "o-a1"));
    assertRefused(() -> queries.order(BOB, "o-none"));
    assertRefused(() -> queries.order(ALICE, "o-none"));
    assertRefused(() -> queries.order(CAROL, "o-a1"));
  }

  @Test
  void anOrderWithoutARowIsNullForAnAdminOnly() {
    assertThat(queries.order(OPERATOR, "o-none")).isNull();
  }

  @Test
  void anOrderIsRefusedWithoutAnIdentityBeforeTheReadModelIsAsked() {
    assertRefused(() -> queries.order(ANONYMOUS, "o-a1"));
    assertRefused(() -> queries.order(ROLE_WITHOUT_IDENTITY, "o-a1"));
    assertRefused(() -> queries.order(NO_CONTEXT, "o-a1"));

    assertThat(reads).isEmpty();
  }

  @Test
  void theOrderGuardOfTheCommandsDecidesAsTheReadDoes() {
    queries.requireOrderAccess(ALICE, "o-a1");
    queries.requireOrderAccess(OPERATOR, "o-a1");
    queries.requireOrderAccess(OPERATOR, "o-none");

    assertRefused(() -> queries.requireOrderAccess(BOB, "o-a1"));
    assertRefused(() -> queries.requireOrderAccess(ALICE, "o-none"));
    assertRefused(() -> queries.requireOrderAccess(ANONYMOUS, "o-a1"));
  }

  // --- the order list ---

  @Test
  void anAdminListsEveryOrderOrOneCustomers() {
    assertThat(queries.orders(OPERATOR, null))
        .extracting(OrderView::orderId)
        .containsExactlyInAnyOrder("o-a1", "o-a2", "o-b1");
    assertThat(queries.orders(OPERATOR, ""))
        .extracting(OrderView::orderId)
        .containsExactlyInAnyOrder("o-a1", "o-a2", "o-b1");
    assertThat(queries.orders(OPERATOR, "alice"))
        .extracting(OrderView::orderId)
        .containsExactlyInAnyOrder("o-a1", "o-a2");
    assertThat(queries.orders(OPERATOR, "carol")).isEmpty();
  }

  @Test
  void aCustomerListsTheirOwnOrdersWithOrWithoutTheFilter() {
    assertThat(queries.orders(ALICE, null))
        .extracting(OrderView::orderId)
        .containsExactlyInAnyOrder("o-a1", "o-a2");
    assertThat(queries.orders(ALICE, ""))
        .extracting(OrderView::orderId)
        .containsExactlyInAnyOrder("o-a1", "o-a2");
    assertThat(queries.orders(ALICE, "alice"))
        .extracting(OrderView::orderId)
        .containsExactlyInAnyOrder("o-a1", "o-a2");
    assertThat(queries.orders(BOB, null)).extracting(OrderView::orderId).containsExactly("o-b1");
    assertThat(queries.orders(CAROL, null)).isEmpty();

    assertThat(reads)
        .as("a customer's list is always the read by their own id, never the whole list")
        .containsExactly(
            "orders.listByCustomer(alice)",
            "orders.listByCustomer(alice)",
            "orders.listByCustomer(alice)",
            "orders.listByCustomer(bob)",
            "orders.listByCustomer(carol)");
  }

  @Test
  void theFilterCannotNameAnotherCustomer() {
    assertRefused(() -> queries.orders(BOB, "alice"));
    assertRefused(() -> queries.orders(CAROL, "alice"));
    // Not a way around it either: a filter that matches no customer.
    assertRefused(() -> queries.orders(BOB, "nobody"));

    assertThat(reads).isEmpty();
  }

  @Test
  void anOrderListWithoutAnIdentityIsRefused() {
    assertRefused(() -> queries.orders(ANONYMOUS, null));
    assertRefused(() -> queries.orders(ANONYMOUS, "alice"));
    assertRefused(() -> queries.orders(ROLE_WITHOUT_IDENTITY, null));
    assertRefused(() -> queries.orders(NO_CONTEXT, null));

    assertThat(reads).isEmpty();
  }

  // --- fulfillment sagas ---

  @Test
  void aFulfillmentIsReadByTheCustomerOfItsOrderAndByAnAdmin() {
    queries.requireFulfillmentAccess(ALICE, "fulfillment-o-a1");
    queries.requireFulfillmentAccess(ALICE, "fulfillment-o-a2");
    queries.requireFulfillmentAccess(BOB, "fulfillment-o-b1");
    queries.requireFulfillmentAccess(OPERATOR, "fulfillment-o-a1");
    queries.requireFulfillmentAccess(OPERATOR, "fulfillment-o-b1");

    assertThat(reads)
        .as("the owner is the customer the order's row names; an ADMIN needs no owner")
        .containsExactly("orders.get(o-a1)", "orders.get(o-a2)", "orders.get(o-b1)");
  }

  @Test
  void theFulfillmentOfAnotherCustomersOrderAndOfAnOrderWithoutARowAreRefusedAlike() {
    assertRefused(() -> queries.requireFulfillmentAccess(BOB, "fulfillment-o-a1"));
    assertRefused(() -> queries.requireFulfillmentAccess(CAROL, "fulfillment-o-a1"));
    // The order's read-model row does not exist (yet): no known owner, as for the order itself.
    assertRefused(() -> queries.requireFulfillmentAccess(BOB, "fulfillment-o-none"));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, "fulfillment-o-none"));
  }

  @Test
  void aSagaIdThatIsNotTheFulfillmentIdOfAnOrderHasNoOwner() {
    // The order id alone, the caller's own id, the prefix alone, another spelling, no id.
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, "o-a1"));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, "alice"));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, "fulfillment-"));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, "fulfillment- "));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, "FULFILLMENT-o-a1"));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, " fulfillment-o-a1"));
    assertRefused(() -> queries.requireFulfillmentAccess(ALICE, null));
  }

  @Test
  void anAdminGetsPastASagaIdWithoutAnOrder() {
    queries.requireFulfillmentAccess(OPERATOR, "fulfillment-o-none");
    queries.requireFulfillmentAccess(OPERATOR, "o-a1");

    assertThat(reads).isEmpty();
  }

  @Test
  void aFulfillmentIsRefusedWithoutAnIdentityBeforeTheReadModelIsAsked() {
    assertRefused(() -> queries.requireFulfillmentAccess(ANONYMOUS, "fulfillment-o-a1"));
    assertRefused(
        () -> queries.requireFulfillmentAccess(ROLE_WITHOUT_IDENTITY, "fulfillment-o-a1"));
    assertRefused(() -> queries.requireFulfillmentAccess(NO_CONTEXT, "fulfillment-o-a1"));

    assertThat(reads).isEmpty();
  }

  @Test
  void anAdminListsEverySaga() {
    FulfillmentScope scope = queries.fulfillments(OPERATOR);

    assertThat(scope.everySaga()).isTrue();
    assertThat(scope.sagaIds()).isEmpty();
    assertThat(reads).isEmpty();
  }

  @Test
  void aCustomerListsTheFulfillmentsOfTheirOwnOrdersByTheirIds() {
    assertThat(queries.fulfillments(ALICE).everySaga()).isFalse();
    assertThat(queries.fulfillments(ALICE).sagaIds())
        .containsExactlyInAnyOrder("fulfillment-o-a1", "fulfillment-o-a2");
    assertThat(queries.fulfillments(BOB).everySaga()).isFalse();
    assertThat(queries.fulfillments(BOB).sagaIds()).containsExactly("fulfillment-o-b1");
    // An identity without an order, and an identity without a role: their own, which is nothing.
    assertThat(queries.fulfillments(CAROL).everySaga()).isFalse();
    assertThat(queries.fulfillments(CAROL).sagaIds()).isEmpty();
    assertThat(queries.fulfillments(caller("alice", null)).everySaga()).isFalse();
    assertThat(queries.fulfillments(caller("alice", null)).sagaIds())
        .containsExactlyInAnyOrder("fulfillment-o-a1", "fulfillment-o-a2");

    assertThat(reads)
        .as("a customer's list comes from the read of their own orders, never from all orders")
        .doesNotContain("orders.listAll()")
        .contains("orders.listByCustomer(alice)", "orders.listByCustomer(bob)");
  }

  @Test
  void theFulfillmentListIsRefusedWithoutAnIdentity() {
    assertRefused(() -> queries.fulfillments(ANONYMOUS));
    assertRefused(() -> queries.fulfillments(ROLE_WITHOUT_IDENTITY));
    assertRefused(() -> queries.fulfillments(NO_CONTEXT));

    assertThat(reads).isEmpty();
  }

  @Test
  void aScopeThatIsNotEverySagaHoldsItsIdsAndOneThatIsHoldsNone() {
    assertThat(FulfillmentScope.all().everySaga()).isTrue();
    assertThat(FulfillmentScope.all().sagaIds()).isEmpty();
    assertThat(FulfillmentScope.only(List.of("fulfillment-o-1")).everySaga()).isFalse();
    assertThat(FulfillmentScope.only(List.of("fulfillment-o-1")).sagaIds())
        .containsExactly("fulfillment-o-1");
    assertThatThrownBy(() -> new FulfillmentScope(true, List.of("fulfillment-o-1")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FulfillmentScope(false, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void bothReadModelsAreRequired() {
    assertThatThrownBy(() -> new OwnerOrAdminQueries(null, new OrderProjection(repository)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new OwnerOrAdminQueries(new CustomerProjection(repository), null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static void assertRefused(Runnable read) {
    assertThatThrownBy(read::run).isInstanceOf(AccessRefusedException.class);
  }

  private static StreamRuneContext.RequestContext caller(String userId, String role) {
    return new StreamRuneContext.RequestContext(
        null,
        userId == null ? null : UserId.of(userId),
        CorrelationId.of("corr-1"),
        Instant.EPOCH,
        role == null ? Map.of() : Map.of("role", role));
  }

  private static CustomerView customer(String id) {
    return new CustomerView(
        id,
        "Name of " + id,
        id + "@example.com",
        "1 Test Street",
        "555-0100",
        CustomerStatus.ACTIVE,
        Instant.EPOCH,
        Instant.EPOCH);
  }

  private static OrderView order(String id, String customerId) {
    return new OrderView(
        id,
        customerId,
        List.of(),
        Money.usd(new BigDecimal("10.00")),
        OrderStatus.CREATED,
        Instant.EPOCH,
        Instant.EPOCH);
  }

  private static final class RecordingCustomerProjection extends CustomerProjection {
    private final List<String> reads;

    RecordingCustomerProjection(ProjectionRepository repository, List<String> reads) {
      super(repository);
      this.reads = reads;
    }

    @Override
    public CustomerView get(String customerId) {
      reads.add("customers.get(" + customerId + ")");
      return super.get(customerId);
    }

    @Override
    public List<CustomerView> listAll() {
      reads.add("customers.listAll()");
      return super.listAll();
    }
  }

  private static final class RecordingOrderProjection extends OrderProjection {
    private final List<String> reads;

    RecordingOrderProjection(ProjectionRepository repository, List<String> reads) {
      super(repository);
      this.reads = reads;
    }

    @Override
    public OrderView get(String orderId) {
      reads.add("orders.get(" + orderId + ")");
      return super.get(orderId);
    }

    @Override
    public List<OrderView> listAll() {
      reads.add("orders.listAll()");
      return super.listAll();
    }

    @Override
    public List<OrderView> listByCustomer(String customerId) {
      reads.add("orders.listByCustomer(" + customerId + ")");
      return super.listByCustomer(customerId);
    }
  }
}
