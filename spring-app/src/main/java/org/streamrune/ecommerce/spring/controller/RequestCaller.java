package org.streamrune.ecommerce.spring.controller;

import org.streamrune.core.StreamRuneContext;

/**
 * The caller of the request being served: the request context the framework's {@code
 * ScopedValueFilter} bound around the servlet chain. The controllers hand it to {@code
 * OwnerOrAdminAccess} and {@code OwnerOrAdminQueries}, which decide what the caller may do.
 */
final class RequestCaller {

  private RequestCaller() {}

  /** The request context of the current request, or {@code null} when none is bound. */
  static StreamRuneContext.RequestContext current() {
    return StreamRuneContext.CURRENT.isBound() ? StreamRuneContext.CURRENT.get() : null;
  }
}
