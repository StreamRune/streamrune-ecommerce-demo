package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;

/**
 * Maps {@link AccessRefusedException} to HTTP 403 Forbidden, as the Spring {@code
 * GlobalExceptionHandler} does: the caller may not read or change what the request names ({@code
 * OwnerOrAdminAccess}). The answer is the same for a request without an identity and for one with
 * the wrong identity.
 */
@Produces
@Singleton
@Requires(classes = {AccessRefusedException.class, ExceptionHandler.class})
public class AccessRefusedExceptionHandler
    implements ExceptionHandler<AccessRefusedException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(
      io.micronaut.http.HttpRequest request, AccessRefusedException exception) {
    return HttpResponse.<String>status(HttpStatus.FORBIDDEN)
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }
}
