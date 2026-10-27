package org.streamrune.ecommerce.spring.config;

import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.DomainException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.ecommerce.commands.CommandRefusal;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;
import org.streamrune.ecommerce.queries.access.LiveFeed;
import org.streamrune.ecommerce.queries.access.LiveFeedFullException;
import org.streamrune.runtime.CommandDeadLetteredException;

@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(DomainException.class)
  public ResponseEntity<String> handleDomainException(DomainException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }

  /**
   * A command the caller's role does not allow ({@code @RequireRole}, checked in the command bus):
   * {@code 403}, the status {@link #handleAccessRefused} answers with. {@code
   * AuthorizationException} is a {@code DomainException}; this handler is the more specific one, so
   * a refusal is not reported as the {@code 400} of a broken business rule.
   */
  @ExceptionHandler(AuthorizationException.class)
  public ResponseEntity<String> handleAuthorization(AuthorizationException e) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException e) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
  }

  /**
   * Terminal erasure: a command tried to encrypt PII for a crypto-shredded subject (e.g.
   * re-registering a forgotten customer id). The subject was erased under GDPR Article 17, so the
   * write is refused — 410 Gone is the honest status for a resource that was deliberately removed.
   */
  @ExceptionHandler(SubjectForgottenException.class)
  public ResponseEntity<String> handleSubjectForgotten(SubjectForgottenException e) {
    return ResponseEntity.status(HttpStatus.GONE).body(e.getMessage());
  }

  /**
   * The caller may not read or change what the request names ({@code OwnerOrAdminAccess}): {@code
   * 403}, the same answer for a request without an identity and for one with the wrong identity.
   */
  @ExceptionHandler(AccessRefusedException.class)
  public ResponseEntity<String> handleAccessRefused(AccessRefusedException e) {
    return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.getMessage());
  }

  /**
   * The open live feed has no place for another connection ({@code LiveFeed.admit}): {@code 503}
   * with {@code Retry-After}. The limit is the application's and not the caller's, hence not {@code
   * 429}. The body is plain text whatever the request accepts: a feed client asks for an event
   * stream, and this answer is not one.
   */
  @ExceptionHandler(LiveFeedFullException.class)
  public ResponseEntity<String> handleLiveFeedFull(LiveFeedFullException e) {
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(LiveFeed.RETRY_AFTER_SECONDS))
        .contentType(MediaType.TEXT_PLAIN)
        .body(e.getMessage());
  }

  /**
   * A command the framework refused for a reason that passes ({@link CommandRefusal}): an open
   * circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock
   * that could not be taken in time, a version conflict on every attempt. {@code 503} with {@code
   * Retry-After} and a JSON body naming the reason, the answer the Quarkus and Micronaut apps give
   * too.
   *
   * <p>A command the bus dead-lettered ({@link CommandDeadLetteredException}, whatever it failed
   * on) is pending, and the body carries its entry's {@code commandId}. What happens to the entry,
   * {@link CommandDeadLetteredException#pendingOutcome()}, decides the answer: {@code 503} with
   * {@code Retry-After} and {@code QUEUED_FOR_REPLAY} while the entry is replayed under its {@code
   * Idempotency-Key}, {@code 500} with {@code AWAITING_OPERATOR} and no {@code Retry-After} when an
   * operator decides about it, for a command that ran without a key or a keyed one whose entry's
   * replays are used up or that is older than the inbox window. Spring picks this handler for the
   * dead-lettered command's own type before it looks at the cause. A genuine failure that left no
   * entry, such as a store error, is none of these and stays {@code 500}.
   */
  @ExceptionHandler({
    CircuitBreakerOpenException.class,
    CommandBusOverloadedException.class,
    CommandBusClosedException.class,
    LockException.class,
    OptimisticLockException.class,
    CommandDeadLetteredException.class
  })
  public ResponseEntity<Map<String, String>> handleCommandRefused(RuntimeException e) {
    CommandRefusal refusal =
        CommandRefusal.of(e).orElseThrow(() -> new IllegalStateException("not a refusal", e));
    ResponseEntity.BodyBuilder answer = ResponseEntity.status(refusal.status());
    if (refusal.retryLater()) {
      answer.header(HttpHeaders.RETRY_AFTER, String.valueOf(refusal.retryAfterSeconds()));
    }
    return answer.contentType(MediaType.APPLICATION_JSON).body(refusal.body());
  }
}
