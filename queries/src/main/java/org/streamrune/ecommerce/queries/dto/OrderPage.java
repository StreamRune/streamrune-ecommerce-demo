package org.streamrune.ecommerce.queries.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import org.streamrune.core.Page;

/**
 * One page of the order list, the body of {@code GET /api/orders}: the orders on the page ({@code
 * items}, in the order of their ids), the page asked for ({@code page}, counted from 0, and {@code
 * size}), how many orders the caller's list holds in all ({@code total}), and whether a page after
 * this one holds more ({@code hasNext}). A client reads on with {@code page + 1} while {@code
 * hasNext} is {@code true}.
 *
 * <p>{@code items} is written even when the page is empty: Micronaut Serialization leaves an empty
 * list out unless told otherwise, and every app answers with the same five fields.
 */
public record OrderPage(
    @JsonInclude(JsonInclude.Include.ALWAYS) List<OrderView> items,
    int page,
    int size,
    long total,
    boolean hasNext) {

  public OrderPage {
    items = List.copyOf(items);
  }

  /** The body for a page the read model returned. */
  public static OrderPage of(Page<OrderView> rows) {
    return new OrderPage(
        rows.content(), rows.page(), rows.size(), rows.totalElements(), rows.hasNext());
  }
}
