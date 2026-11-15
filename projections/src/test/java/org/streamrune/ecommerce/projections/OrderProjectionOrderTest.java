package org.streamrune.ecommerce.projections;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderStatus;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The order every list of orders comes in: newest first, then by id. The repository sorts the whole
 * list on the row's JSON, a customer's list is sorted in memory; the two orders are written next to
 * each other in {@link OrderProjection} and must agree.
 */
class OrderProjectionOrderTest {

  @Test
  void theRepositorySortsByCreatedAtDescendingThenByIdAscending() {
    assertThat(OrderProjection.NEWEST_FIRST_ROWS)
        .isEqualTo(
            new Sort(
                List.of(
                    new Sort.Order("createdAt", SortDirection.DESC),
                    new Sort.Order("id", SortDirection.ASC))));
  }

  @Test
  void inMemoryTheNewestComesFirstAndTheIdBreaksATie() {
    Instant noon = Instant.parse("2026-11-15T12:00:00Z");
    List<OrderView> orders =
        List.of(
            order("o-1", noon),
            order("o-3", noon.plusSeconds(60)),
            order("o-2", noon),
            order("o-0", noon.minusSeconds(60)));

    assertThat(orders.stream().sorted(OrderProjection.NEWEST_FIRST).map(OrderView::orderId))
        .containsExactly("o-3", "o-1", "o-2", "o-0");
  }

  @Test
  void inMemoryTheInstantsAreComparedAsTheTextTheRowHolds() {
    // PostgreSQL orders the ISO-8601 text of the row: within one second, "…00.500Z" sorts before
    // "…00Z" although it is later. The in-memory order compares the same text.
    Instant whole = Instant.parse("2026-11-15T12:00:00Z");
    Instant half = Instant.parse("2026-11-15T12:00:00.500Z");

    assertThat(
            List.of(order("o-half", half), order("o-whole", whole)).stream()
                .sorted(OrderProjection.NEWEST_FIRST)
                .map(OrderView::orderId))
        .containsExactly("o-whole", "o-half");
  }

  private static OrderView order(String id, Instant placedAt) {
    return new OrderView(
        id,
        "c-1",
        List.of(),
        Money.usd(new BigDecimal("10.00")),
        OrderStatus.CREATED,
        null,
        null,
        placedAt,
        placedAt);
  }
}
