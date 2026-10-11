package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.core.DomainException;

/**
 * Maps domain rule violations ({@link DomainException}) to HTTP 400 Bad Request, mirroring the
 * Spring {@code GlobalExceptionHandler} and the Micronaut {@code DomainExceptionHandler}.
 *
 * <p>{@code org.streamrune.core.AuthorizationException} extends {@link DomainException}, and the
 * command bus rethrows an interceptor's {@code before()} exception unwrapped. A failed
 * authorization (e.g. shipping an order without the {@code ADMIN} role) is answered by the more
 * specific {@link AuthorizationExceptionMapper} with 403, not by this mapper.
 *
 * <p>{@link SubjectForgottenExceptionMapper} (410) and {@link
 * CryptoShreddingFailureExceptionMapper} answer crypto-shred failures: {@code
 * SubjectForgottenException} extends {@code RuntimeException}, not {@code DomainException}, so this
 * mapper never shadows them.
 */
@Provider
public class DomainExceptionMapper implements ExceptionMapper<DomainException> {

  @Override
  public Response toResponse(DomainException e) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
