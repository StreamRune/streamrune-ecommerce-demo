package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * Maps {@link EventStoreException}s whose root cause is a {@link SubjectForgottenException} to HTTP
 * 410 Gone.
 *
 * <p>When a command re-encrypts PII for a crypto-shredded subject (GDPR terminal erasure), the
 * {@code @Encrypted} field serializer throws {@link SubjectForgottenException}; the event store
 * catches the serialization failure and rethrows it wrapped in an {@link EventStoreException}
 * (cause chain: {@code EventStoreException -> JsonMappingException -> SubjectForgottenException}).
 *
 * <p>Spring MVC resolves {@code @ExceptionHandler}s by walking the cause chain, so the Spring app
 * maps this to 410 with a plain {@code SubjectForgottenException} handler. Micronaut matches
 * handlers by the thrown type only, so this handler is needed to reproduce that behaviour: it
 * unwraps the chain and returns 410 when terminal erasure is the cause, and otherwise re-surfaces a
 * genuine store failure as 500 (matching Micronaut's default for an unexpected error).
 */
@Produces
@Singleton
@Requires(classes = {EventStoreException.class, ExceptionHandler.class})
public class EventStoreExceptionHandler
    implements ExceptionHandler<EventStoreException, HttpResponse<String>> {

  @Override
  public HttpResponse<String> handle(HttpRequest request, EventStoreException exception) {
    SubjectForgottenException forgotten = findSubjectForgotten(exception);
    if (forgotten != null) {
      return HttpResponse.<String>status(HttpStatus.GONE)
          .body(forgotten.getMessage())
          .contentType(MediaType.TEXT_PLAIN);
    }
    return HttpResponse.<String>status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(exception.getMessage())
        .contentType(MediaType.TEXT_PLAIN);
  }

  private static SubjectForgottenException findSubjectForgotten(Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      if (cause instanceof SubjectForgottenException sfe) {
        return sfe;
      }
    }
    return null;
  }
}
