package org.streamrune.ecommerce.queries.paging;

import java.util.List;
import java.util.regex.Pattern;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;

/**
 * Which page of a list a request asks for: {@code page}, counted from 0, and {@code size}, the
 * number of rows on a page. A request that names neither gets the first page of {@value
 * #DEFAULT_SIZE} rows; {@code size} goes up to {@value #MAX_SIZE}. A page past the last row is
 * empty, not an error.
 *
 * <p>The three applications bind every value of the {@code page} and {@code size} query parameters
 * of {@code GET /api/orders} as text and hand them to {@link #parse}, so the defaults, the limits
 * and the answer to a value outside them are the same in every app. A parameter given twice, a
 * value that is not a whole number, a negative {@code page}, a {@code size} outside 1 to {@value
 * #MAX_SIZE} and a page that ends past the largest row offset ({@link Integer#MAX_VALUE}) are an
 * {@link IllegalArgumentException}, which each application answers with {@code 400}. The
 * controllers parse the page before they ask who the caller is, so a malformed request is {@code
 * 400} whoever sends it.
 */
public record ListPage(int page, int size) {

  /** The rows on a page when the request does not say. */
  public static final int DEFAULT_SIZE = 20;

  /** The most rows a page may hold. */
  public static final int MAX_SIZE = 100;

  private static final Pattern WHOLE_NUMBER = Pattern.compile("[+-]?\\d+");

  public ListPage {
    if (page < 0) {
      throw new IllegalArgumentException("page must not be negative");
    }
    if (size < 1 || size > MAX_SIZE) {
      throw new IllegalArgumentException("size must be between 1 and " + MAX_SIZE);
    }
    if (page > lastPage(size)) {
      throw new IllegalArgumentException(
          "page must be at most " + lastPage(size) + " for size " + size);
    }
  }

  /** The largest page of the given size whose rows all lie within the largest row offset. */
  private static int lastPage(int size) {
    return Integer.MAX_VALUE / size - 1;
  }

  /**
   * The page the two query parameters name, each given as the list of its values in the request. A
   * parameter that is absent, or given once and empty, takes its default: page 0, {@value
   * #DEFAULT_SIZE} rows.
   *
   * @throws IllegalArgumentException for a parameter given more than once, a value that is not a
   *     whole number, or one outside the limits
   */
  public static ListPage parse(List<String> page, List<String> size) {
    return new ListPage(number("page", page, 0), number("size", size, DEFAULT_SIZE));
  }

  private static int number(String name, List<String> values, int absent) {
    if (values == null || values.isEmpty()) {
      return absent;
    }
    if (values.size() > 1) {
      throw new IllegalArgumentException(name + " must be given once");
    }
    String value = values.getFirst();
    if (value == null || value.isEmpty()) {
      return absent;
    }
    if (!WHOLE_NUMBER.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " must be a whole number");
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      // A whole number beyond an int: the range check names the limit it is past.
      return value.startsWith("-") ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
  }

  /** The framework's page request for this page, in the given order. */
  public PageRequest in(Sort order) {
    return PageRequest.of(page, size, order);
  }
}
