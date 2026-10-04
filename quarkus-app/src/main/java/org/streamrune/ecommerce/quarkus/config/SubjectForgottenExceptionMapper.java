package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * Terminal erasure: a command tried to encrypt PII for a crypto-shredded subject (e.g.
 * re-registering a forgotten customer id). The subject was erased under GDPR Article 17, so the
 * write is refused — 410 Gone is the honest status for a resource that was deliberately removed.
 * Mirrors the Spring app's {@code GlobalExceptionHandler} mapping.
 *
 * <p>Handles the exception thrown directly. The event store wraps the encrypt-time failure (see
 * {@link CryptoShreddingFailureExceptionMapper}), so this mapper alone is not enough for the
 * register path — it covers any code path that throws {@code SubjectForgottenException} unwrapped.
 */
@Provider
public class SubjectForgottenExceptionMapper implements ExceptionMapper<SubjectForgottenException> {

  @Override
  public Response toResponse(SubjectForgottenException e) {
    return Response.status(Response.Status.GONE).entity(e.getMessage()).build();
  }
}
