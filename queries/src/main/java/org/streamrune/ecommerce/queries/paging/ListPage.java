package org.streamrune.ecommerce.queries.paging;

import java.util.List;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;

/**
 * Which page of a list a request asks for: {@code page}, counted from 0, and {@code size}, the
 * number of rows on a page. A request that names neither gets the first page of {@value
 * #DEFAULT_SIZE} rows; {@code size} goes up to {@value #MAX_SIZE}. A page past the last row is
 * empty, not an error.
 *
 * <p>The three applications bind the {@code page} and {@code size} query parameters of {@code GET
 * /api/orders} as text and hand them to {@link #parse}, so the defaults, the limits and the answer
 * to a value outside them are the same in every app. A value that is not a whole number, a negative
 * {@code page} and a {@code size} outside 1 to {@value #MAX_SIZE} are an {@link
 * IllegalArgumentException}, which each application answers with {@code 400}. The controllers parse
 * the page before they ask who the caller is, so a malformed request is {@code 400} whoever sends
 * it.
 *
 * <p>The rows of a page are in the order of their ids: the read-model table's primary key, which
 * keeps the order stable from one request to the next and lets the database page the whole list
 * without sorting it ({@link #byId}).
 */
public record ListPage(int page, int size) {

  /** The rows on a page when the request does not say. */
  public static final int DEFAULT_SIZE = 20;

  /** The most rows a page may hold. */
  public static final int MAX_SIZE = 100;

  public ListPage {
    if (page < 0) {
      throw new IllegalArgumentException("page must not be negative");
    }
    if (size < 1 || size > MAX_SIZE) {
      throw new IllegalArgumentException("size must be between 1 and " + MAX_SIZE);
    }
    if ((long) page * size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("page " + page + " of size " + size + " is out of range");
    }
  }

  /**
   * The page the two query parameters name. A parameter that is absent or empty takes its default:
   * page 0, {@value #DEFAULT_SIZE} rows.
   *
   * @throws IllegalArgumentException for a value that is not a whole number, or one outside the
   *     limits
   */
  public static ListPage parse(String page, String size) {
    return new ListPage(number("page", page, 0), number("size", size, DEFAULT_SIZE));
  }

  private static int number(String name, String value, int absent) {
    if (value == null || value.isEmpty()) {
      return absent;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a whole number");
    }
  }

  /** The framework's page request for this page, ordered by the rows' ids. */
  public PageRequest byId() {
    return PageRequest.of(page, size, Sort.by("id"));
  }

  /**
   * This page of a list that is already in order: the rows it holds, and the size of the whole list
   * as the total.
   */
  public <T> Page<T> slice(List<T> rows) {
    int from = (int) Math.min((long) page * size, rows.size());
    int to = Math.min(from + size, rows.size());
    return new Page<>(rows.subList(from, to), rows.size(), page, size);
  }
}
