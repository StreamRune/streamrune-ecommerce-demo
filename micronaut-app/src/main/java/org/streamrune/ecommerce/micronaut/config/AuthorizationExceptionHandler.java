package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.streamrune.core.AuthorizationException;

/**
 * Maps {@link AuthorizationException} to HTTP 403 Forbidden, as the Spring {@code
 * GlobalExceptionHandler} does: a command the caller's role does not allow ({@code @RequireRole},
 * checked in the command bus) is answered with the status {@link AccessRefusedExceptionHandler}
 * answers with. {@code AuthorizationException} is a {@code DomainException}; this handler is the
 * more specific one, so a refusal is not reported as the {@code 400} of a broken business rule.
 */
@Produces
@Singleton
@Requires(classes = {AuthorizationException.class, ExceptionHandler.class})
public class AuthorizationExceptionHandler
    implements ExceptionHandler<AuthorizationException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(
      io.micronaut.http.HttpRequest request, AuthorizationException exception) {
    return HttpResponse.<String>status(HttpStatus.FORBIDDEN)
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }
}
