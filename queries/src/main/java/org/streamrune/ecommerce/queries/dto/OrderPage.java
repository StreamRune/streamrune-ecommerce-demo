package org.streamrune.ecommerce.queries.dto;

import java.util.List;
import org.streamrune.core.Page;

/**
 * One page of the order list, the body of {@code GET /api/orders}: the orders on the page ({@code
 * items}, newest first), the page asked for ({@code page}, counted from 0, and {@code size}), how
 * many orders the caller's list holds in all ({@code total}), and whether a page after this one
 * holds more ({@code hasNext}). A client reads on with {@code page + 1} while {@code hasNext} is
 * {@code true}.
 */
public record OrderPage(List<OrderView> items, int page, int size, long total, boolean hasNext) {

  public OrderPage {
    items = List.copyOf(items);
  }

  /** The body for a page the read model returned. */
  public static OrderPage of(Page<OrderView> rows) {
    boolean hasNext = ((long) rows.page() + 1) * rows.size() < rows.totalElements();
    return new OrderPage(rows.content(), rows.page(), rows.size(), rows.totalElements(), hasNext);
  }
}
