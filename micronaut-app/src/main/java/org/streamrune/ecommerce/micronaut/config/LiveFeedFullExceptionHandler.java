package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.streamrune.ecommerce.queries.access.LiveFeed;
import org.streamrune.ecommerce.queries.access.LiveFeedFullException;

/**
 * Maps {@link LiveFeedFullException} to HTTP 503 Service Unavailable with {@code Retry-After}, as
 * the Spring {@code GlobalExceptionHandler} does: the open live feed has no place for another
 * connection ({@code LiveFeed.admit}). The limit is the application's and not the caller's, hence
 * not {@code 429}. The body is plain text: a feed client asks for an event stream, and this answer
 * is not one.
 */
@Produces
@Singleton
@Requires(classes = {LiveFeedFullException.class, ExceptionHandler.class})
public class LiveFeedFullExceptionHandler
    implements ExceptionHandler<LiveFeedFullException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(
      io.micronaut.http.HttpRequest request, LiveFeedFullException exception) {
    return HttpResponse.<String>status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(LiveFeed.RETRY_AFTER_SECONDS))
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }
}
