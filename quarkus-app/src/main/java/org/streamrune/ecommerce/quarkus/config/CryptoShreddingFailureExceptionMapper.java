package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * Maps a wrapped terminal-erasure failure to 410 Gone.
 *
 * <p>When the event store appends a {@code CustomerRegistered} event for a crypto-shredded subject,
 * the encrypt step throws {@link SubjectForgottenException}, but the store wraps it: {@code
 * EventStoreException → JsonMappingException → SubjectForgottenException}. JAX-RS {@link
 * ExceptionMapper}s match the thrown type, not the cause chain (unlike Spring's resolver, which
 * unwraps), so {@link SubjectForgottenExceptionMapper} never sees the wrapped case. This mapper
 * catches the {@link EventStoreException} wrapper and, only when a {@link
 * SubjectForgottenException} is somewhere in its cause chain, returns 410 — the same status the
 * Spring app returns. Any other {@link EventStoreException} maps to a plain 500 (this mapper does
 * not mask the failure with a misleading status; it just keeps the response shape consistent with
 * the rest of the API).
 */
@Provider
public class CryptoShreddingFailureExceptionMapper implements ExceptionMapper<EventStoreException> {

  @Override
  public Response toResponse(EventStoreException e) {
    SubjectForgottenException forgotten = findForgotten(e);
    if (forgotten != null) {
      return Response.status(Response.Status.GONE).entity(forgotten.getMessage()).build();
    }
    // Not a terminal-erasure failure — surface a generic 500 rather than masking it as 410.
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
  }

  private static SubjectForgottenException findForgotten(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur instanceof SubjectForgottenException sfe) {
        return sfe;
      }
      if (cur.getCause() == cur) {
        break;
      }
    }
    return null;
  }
}
