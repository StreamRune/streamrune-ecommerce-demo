package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.streamrune.core.DomainException;

/**
 * Maps domain rule violations ({@link DomainException}) to HTTP 400 Bad Request.
 *
 * <p>Mirrors the Spring {@code GlobalExceptionHandler}. The customer forget flow depends on this:
 * the {@code CustomerDecider} refuses {@code ForgetCustomer} for an id no customer registered with
 * a {@code DomainException}, which must surface as 400 (a client error) rather than an unmapped
 * 500.
 */
@Produces
@Singleton
@Requires(classes = {DomainException.class, ExceptionHandler.class})
public class DomainExceptionHandler
    implements ExceptionHandler<DomainException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(
      io.micronaut.http.HttpRequest request, DomainException exception) {
    return HttpResponse.<String>status(HttpStatus.BAD_REQUEST)
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }
}
