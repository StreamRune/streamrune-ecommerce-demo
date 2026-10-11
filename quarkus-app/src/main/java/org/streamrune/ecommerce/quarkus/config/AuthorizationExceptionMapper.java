package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.core.AuthorizationException;

/**
 * Maps {@link AuthorizationException} to HTTP 403 Forbidden, as the Spring {@code
 * GlobalExceptionHandler} does: a command the caller's role does not allow ({@code @RequireRole},
 * checked in the command bus) is answered with the status {@link AccessRefusedExceptionMapper}
 * answers with. {@code AuthorizationException} is a {@code DomainException}; this mapper is the
 * more specific one, so a refusal is not reported as the {@code 400} of a broken business rule.
 */
@Provider
public class AuthorizationExceptionMapper implements ExceptionMapper<AuthorizationException> {

  @Override
  public Response toResponse(AuthorizationException e) {
    return Response.status(Response.Status.FORBIDDEN)
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
