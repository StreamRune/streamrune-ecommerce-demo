package org.streamrune.ecommerce.quarkus.controller;

import java.math.BigDecimal;
import java.util.List;

/**
 * What a request body holds before a handler goes on: the constraints the Spring app declares on
 * its request records with Bean Validation, written out as plain checks. A failed check is an
 * {@link IllegalArgumentException}, which the app answers with {@code 400}.
 *
 * <p>A handler runs its checks first, before it asks {@code OwnerOrAdminAccess} who the caller is,
 * so the three apps answer in one order: a request that cannot be read or lacks a required field is
 * {@code 400} whoever sent it, and a well-formed request from a caller the rule refuses is {@code
 * 403}.
 */
final class RequestChecks {

  private RequestChecks() {}

  /**
   * The body itself: Quarkus REST hands a handler {@code null} for a request that carries none.
   *
   * @return the body
   */
  static <T> T present(T body) {
    if (body == null) {
      throw new IllegalArgumentException("A request body is required");
    }
    return body;
  }

  static void notBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }

  static void notEmpty(List<?> values, String field) {
    if (values == null || values.isEmpty()) {
      throw new IllegalArgumentException(field + " must not be empty");
    }
  }

  static void positive(int value, String field) {
    if (value <= 0) {
      throw new IllegalArgumentException(field + " must be greater than 0");
    }
  }

  static void positive(BigDecimal value, String field) {
    if (value == null || value.signum() <= 0) {
      throw new IllegalArgumentException(field + " must be greater than 0");
    }
  }

  /** A non-blank value with an {@code @} that has something on both sides of it. */
  static void email(String value, String field) {
    notBlank(value, field);
    int at = value.indexOf('@');
    if (at <= 0 || at == value.length() - 1) {
      throw new IllegalArgumentException(field + " must be an e-mail address");
    }
  }
}
