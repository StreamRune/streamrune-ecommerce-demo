package org.streamrune.ecommerce.queries.access;

/**
 * The caller may not read or change what the request names: thrown by {@link OwnerOrAdminAccess}
 * and by the guarded reads built on it. Each application answers it with {@code 403 Forbidden},
 * whether the request carried no identity or the wrong one.
 *
 * <p>The message never names the resource: a caller who is refused learns nothing about whether it
 * exists.
 */
public final class AccessRefusedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public AccessRefusedException(String message) {
    super(message);
  }
}
