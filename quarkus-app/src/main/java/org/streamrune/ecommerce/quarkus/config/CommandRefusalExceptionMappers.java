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
import org.streamrune.core.OptimisticLockException;
import org.streamrune.ecommerce.commands.CommandRefusal;
import org.streamrune.runtime.CommandDeadLetteredException;

/**
 * Maps a command the framework refused for a reason that passes ({@link CommandRefusal}) to HTTP
 * 503 Service Unavailable with {@code Retry-After} and a JSON body naming the reason, as the Spring
 * {@code GlobalExceptionHandler} and the Micronaut {@code CommandRefusalExceptionHandlers} do: an
 * open circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock
 * that could not be taken in time, a version conflict on every attempt. A command the bus
 * dead-lettered ({@link CommandDeadLetteredException}) is pending, answered by what happens to its
 * entry ({@code pendingOutcome()}): 503 with {@code Retry-After} and {@code QUEUED_FOR_REPLAY}
 * while the entry is replayed under its {@code Idempotency-Key}, 500 with {@code AWAITING_OPERATOR}
 * when an operator decides about it, for a command that ran without a key or a keyed one whose
 * entry's replays are used up or that is older than the inbox window, its entry's {@code commandId}
 * in the body either way. JAX-RS picks a mapper by the thrown type, so there is one per exception
 * type: a lock timeout or a version conflict that the bus dead-lettered reaches the dead-lettered
 * command's mapper, not the lock's. A genuine failure that left no entry, such as a store error, is
 * none of these and stays 500.
 */
public final class CommandRefusalExceptionMappers {

  private CommandRefusalExceptionMappers() {}

  /** The answer to {@code refused}: its status, {@code Retry-After} when it may be sent again. */
  static Response refused(RuntimeException refused) {
    CommandRefusal refusal =
        CommandRefusal.of(refused)
            .orElseThrow(() -> new IllegalStateException("not a refusal", refused));
    Response.ResponseBuilder answer = Response.status(refusal.status());
    if (refusal.retryLater()) {
      answer.header(HttpHeaders.RETRY_AFTER, String.valueOf(refusal.retryAfterSeconds()));
    }
    return answer.type(MediaType.APPLICATION_JSON).entity(refusal.body()).build();
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

  /** The aggregate's stream moved past the command's version on every attempt. */
  @Provider
  public static class AggregateVersionConflict implements ExceptionMapper<OptimisticLockException> {
    @Override
    public Response toResponse(OptimisticLockException e) {
      return refused(e);
    }
  }

  /** The command failed and its dead-letter entry is stored: its outcome is pending. */
  @Provider
  public static class CommandDeadLettered implements ExceptionMapper<CommandDeadLetteredException> {
    @Override
    public Response toResponse(CommandDeadLetteredException e) {
      return refused(e);
    }
  }
}
