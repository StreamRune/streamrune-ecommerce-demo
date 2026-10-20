package org.streamrune.ecommerce.quarkus.config;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.LockException;
import org.streamrune.ecommerce.commands.CommandRefusal;

/**
 * Maps a command the framework refused for a reason that passes ({@link CommandRefusal}) to HTTP
 * 503 Service Unavailable with {@code Retry-After} and a JSON body naming the reason, as the Spring
 * {@code GlobalExceptionHandler} and the Micronaut {@code CommandRefusalExceptionHandlers} do: an
 * open circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock
 * that could not be taken in time. JAX-RS picks a mapper by the thrown type, so there is one per
 * exception type; the command bus throws each of them unwrapped. A genuine failure, such as a store
 * error, is not one of these and stays 500.
 */
public final class CommandRefusalExceptionMappers {

  private CommandRefusalExceptionMappers() {}

  /** The 503 answer to {@code refused}. */
  static Response refused(RuntimeException refused) {
    CommandRefusal refusal =
        CommandRefusal.of(refused)
            .orElseThrow(() -> new IllegalStateException("not a refusal", refused));
    return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(CommandRefusal.RETRY_AFTER_SECONDS))
        .type(MediaType.APPLICATION_JSON)
        .entity(refusal.body())
        .build();
  }

  /** The command bus's circuit breaker is open. */
  @Provider
  public static class CircuitBreakerOpen implements ExceptionMapper<CircuitBreakerOpenException> {
    @Override
    public Response toResponse(CircuitBreakerOpenException e) {
      return refused(e);
    }
  }

  /** The command bus's budget of in-flight asynchronous commands is used up. */
  @Provider
  public static class CommandBusOverloaded
      implements ExceptionMapper<CommandBusOverloadedException> {
    @Override
    public Response toResponse(CommandBusOverloadedException e) {
      return refused(e);
    }
  }

  /** The command bus is closing. */
  @Provider
  public static class CommandBusClosed implements ExceptionMapper<CommandBusClosedException> {
    @Override
    public Response toResponse(CommandBusClosedException e) {
      return refused(e);
    }
  }

  /** The command's aggregate lock could not be taken within the lock timeout. */
  @Provider
  public static class AggregateLock implements ExceptionMapper<LockException> {
    @Override
    public Response toResponse(LockException e) {
      return refused(e);
    }
  }
}
