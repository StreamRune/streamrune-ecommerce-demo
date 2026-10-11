package org.streamrune.ecommerce.micronaut.controller;

import org.streamrune.core.StreamRuneContext;
import org.streamrune.micronaut.StreamRuneContextHelper;

/**
 * The caller of the request being served: the request context the framework's {@code
 * StreamRuneContextFilter} bound. It is the {@code StreamRuneContext.CURRENT} {@code ScopedValue}
 * on the thread the filter runs on, and the {@link StreamRuneContextHelper} {@code ThreadLocal} on
 * an executor thread the {@code ScopedValue} does not reach. The controllers hand it to {@code
 * OwnerOrAdminAccess} and {@code OwnerOrAdminQueries}, which decide what the caller may do.
 */
final class RequestCaller {

  private RequestCaller() {}

  /** The request context of the current request, or {@code null} when neither place holds one. */
  static StreamRuneContext.RequestContext current() {
    StreamRuneContext.RequestContext context =
        StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
    return context != null ? context : StreamRuneContextHelper.get();
  }
}
