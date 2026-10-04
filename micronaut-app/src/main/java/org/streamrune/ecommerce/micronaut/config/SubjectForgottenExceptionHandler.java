package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * Maps {@link SubjectForgottenException} to HTTP 410 Gone.
 *
 * <p>Terminal erasure: a command tried to encrypt PII for a crypto-shredded subject (e.g.
 * re-registering a forgotten customer id). The subject was erased under GDPR Article 17, so the
 * write is refused — 410 Gone is the honest status for a resource that was deliberately removed.
 * Mirrors the Spring {@code GlobalExceptionHandler}'s {@code SubjectForgottenException} mapping.
 *
 * <p>This handler covers the case where the exception surfaces directly. When it is raised during
 * event serialization (the common path: the {@code @Encrypted} field writer calls {@code
 * encrypt()}), the event store wraps it in an {@code EventStoreException}; that wrapped form is
 * unwrapped to 410 by {@link EventStoreExceptionHandler}, because — unlike Spring MVC — Micronaut
 * matches exception handlers by the thrown type only and does not walk the cause chain.
 */
@Produces
@Singleton
@Requires(classes = {SubjectForgottenException.class, ExceptionHandler.class})
public class SubjectForgottenExceptionHandler
    implements ExceptionHandler<SubjectForgottenException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(
      io.micronaut.http.HttpRequest request, SubjectForgottenException exception) {
    return HttpResponse.<String>status(HttpStatus.GONE)
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }
}
