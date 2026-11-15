package org.streamrune.ecommerce.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;
import org.streamrune.ecommerce.queries.paging.ListPage;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The order and the selection of the order lists, on the framework's in-memory repository, which
 * sorts and filters as the PostgreSQL one does: newest first, the id breaking a tie, and a
 * customer's list holding that customer's orders only.
 */
class OrderProjectionOrderTest {

  private static final Instant NOON = Instant.parse("2026-11-15T12:00:00Z");

  private final InMemoryProjectionRepository repository = new InMemoryProjectionRepository();
  private final OrderProjection projection = new OrderProjection(repository);

  OrderProjectionOrderTest() {
    // Ids that do not follow the order of placing; o-1 and o-2 were placed at the same instant.
    save(order("o-1", "alice", NOON));
    save(order("o-3", "alice", NOON.plusSeconds(60)));
    save(order("o-2", "alice", NOON));
    save(order("o-0", "alice", NOON.minusSeconds(60)));
    save(order("o-9", "bob", NOON.plusSeconds(30)));
  }

  @Test
  void theWholeListIsNewestFirstAndTheIdBreaksATie() {
    assertThat(projection.page(new ListPage(0, 10)).content())
        .extracting(OrderView::orderId)
        .containsExactly("o-3", "o-9", "o-1", "o-2", "o-0");
  }

  @Test
  void aCustomersPagesHoldTheirOwnOrdersNewestFirst() {
    var first = projection.pageByCustomer("alice", new ListPage(0, 3));
    assertThat(first.content()).extracting(OrderView::orderId).containsExactly("o-3", "o-1", "o-2");
    assertThat(first.totalElements()).isEqualTo(4);
    assertThat(first.hasNext()).isTrue();
    assertThat(projection.pageByCustomer("alice", new ListPage(1, 3)).content())
        .extracting(OrderView::orderId)
        .containsExactly("o-0");
    assertThat(projection.pageByCustomer("bob", new ListPage(0, 3)).content())
        .extracting(OrderView::orderId)
        .containsExactly("o-9");
    assertThat(projection.pageByCustomer("carol", new ListPage(0, 3)).totalElements()).isZero();
  }

  @Test
  void aCustomersWholeListIsReadThroughTheFilter() {
    assertThat(projection.listByCustomer("alice"))
        .extracting(OrderView::orderId)
        .containsExactly("o-3", "o-1", "o-2", "o-0");
    assertThat(projection.listByCustomer("carol")).isEmpty();
  }

  @Test
  void theIndexesServeTheTwoReads() {
    ProjectionName orders = ProjectionName.of("orders");
    Sort newestFirst = new Sort(List.of(new Sort.Order("createdAt", SortDirection.DESC)));

    assertThat(OrderProjection.NEWEST_FIRST).isEqualTo(newestFirst);
    assertThat(OrderProjection.NEWEST_FIRST_INDEX)
        .isEqualTo(ProjectionIndex.forSort(orders, newestFirst));
    assertThat(OrderProjection.CUSTOMER_NEWEST_FIRST_INDEX)
        .isEqualTo(ProjectionIndex.forFilter(orders, "customerId", newestFirst));
    // The repository accepts both declarations.
    repository.declareIndex(OrderProjection.NEWEST_FIRST_INDEX);
    repository.declareIndex(OrderProjection.CUSTOMER_NEWEST_FIRST_INDEX);
  }

  private void save(OrderView order) {
    repository.save(ProjectionName.of("orders"), order.orderId(), order);
  }

  private static OrderView order(String id, String customerId, Instant placedAt) {
    return new OrderView(
        id,
        customerId,
        List.of(),
        Money.usd(new BigDecimal("10.00")),
        OrderStatus.CREATED,
        null,
        null,
        placedAt,
        placedAt);
  }
}
