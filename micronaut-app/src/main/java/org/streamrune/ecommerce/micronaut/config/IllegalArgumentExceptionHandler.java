package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

/**
 * Maps {@link IllegalArgumentException} to HTTP 400 Bad Request, mirroring the Spring {@code
 * GlobalExceptionHandler} and bringing the Micronaut app to parity. The command bus rejects a blank
 * aggregate id with an {@code IllegalArgumentException} before any interceptor runs, so a malformed
 * command (e.g. placing an order with a blank {@code orderId}) surfaces as a client error rather
 * than an unmapped 500.
 */
@Produces
@Singleton
@Requires(classes = {IllegalArgumentException.class, ExceptionHandler.class})
public class IllegalArgumentExceptionHandler
    implements ExceptionHandler<IllegalArgumentException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(
      io.micronaut.http.HttpRequest request, IllegalArgumentException exception) {
    return HttpResponse.<String>status(HttpStatus.BAD_REQUEST)
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }
}
