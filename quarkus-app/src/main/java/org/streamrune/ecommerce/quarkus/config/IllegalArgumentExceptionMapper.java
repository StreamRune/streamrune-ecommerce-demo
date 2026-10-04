package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Maps {@link IllegalArgumentException} to HTTP 400 Bad Request, mirroring the Spring {@code
 * GlobalExceptionHandler}. Malformed input — e.g. a blank aggregate id, which the command bus
 * rejects with an {@code IllegalArgumentException} before any interceptor runs — surfaces as a
 * client error rather than an unmapped 500, bringing the Quarkus app to parity with Spring.
 */
@Provider
public class IllegalArgumentExceptionMapper implements ExceptionMapper<IllegalArgumentException> {

  @Override
  public Response toResponse(IllegalArgumentException e) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
