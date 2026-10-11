package org.streamrune.ecommerce.quarkus.controller;

import org.streamrune.core.StreamRuneContext;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/**
 * The caller of the request being served. A JAX-RS filter cannot bind {@code
 * StreamRuneContext.CURRENT} around the resource method, so the framework's request filter stores
 * the request context in the request-scoped {@link StreamRuneRequestContextHolder} and a resource
 * reads it from the holder it injects. The controllers hand the context to {@code
 * OwnerOrAdminAccess} and {@code OwnerOrAdminQueries}, which decide what the caller may do.
 */
final class RequestCaller {

  private RequestCaller() {}

  /**
   * The request context of the current request: the holder's first, {@code
   * StreamRuneContext.CURRENT} when that is bound, else {@code null}.
   */
  static StreamRuneContext.RequestContext of(StreamRuneRequestContextHolder holder) {
    StreamRuneContext.RequestContext context = holder != null ? holder.context() : null;
    if (context == null && StreamRuneContext.CURRENT.isBound()) {
      context = StreamRuneContext.CURRENT.get();
    }
    return context;
  }
}
