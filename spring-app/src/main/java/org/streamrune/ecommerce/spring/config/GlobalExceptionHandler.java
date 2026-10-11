package org.streamrune.ecommerce.spring.config;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.DomainException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;

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
}
