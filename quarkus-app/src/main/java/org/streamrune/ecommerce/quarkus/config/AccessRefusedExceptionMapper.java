package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.ecommerce.queries.access.AccessRefusedException;

/**
 * Maps {@link AccessRefusedException} to HTTP 403 Forbidden, as the Spring {@code
 * GlobalExceptionHandler} does: the caller may not read or change what the request names ({@code
 * OwnerOrAdminAccess}). The answer is the same for a request without an identity and for one with
 * the wrong identity.
 */
@Provider
public class AccessRefusedExceptionMapper implements ExceptionMapper<AccessRefusedException> {

  @Override
  public Response toResponse(AccessRefusedException e) {
    return Response.status(Response.Status.FORBIDDEN)
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
