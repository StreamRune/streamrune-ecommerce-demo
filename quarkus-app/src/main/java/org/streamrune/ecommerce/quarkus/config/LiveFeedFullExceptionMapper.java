package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.ecommerce.queries.access.LiveFeed;
import org.streamrune.ecommerce.queries.access.LiveFeedFullException;

/**
 * Maps {@link LiveFeedFullException} to HTTP 503 Service Unavailable with {@code Retry-After}, as
 * the Spring {@code GlobalExceptionHandler} does: the open live feed has no place for another
 * connection ({@code LiveFeed.admit}). The limit is the application's and not the caller's, hence
 * not {@code 429}. The body is plain text: a feed client asks for an event stream, and this answer
 * is not one.
 */
@Provider
public class LiveFeedFullExceptionMapper implements ExceptionMapper<LiveFeedFullException> {

  @Override
  public Response toResponse(LiveFeedFullException e) {
    return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(LiveFeed.RETRY_AFTER_SECONDS))
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
