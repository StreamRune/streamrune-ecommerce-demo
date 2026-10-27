package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.util.Map;
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
 * {@code GlobalExceptionHandler} and the Quarkus {@code CommandRefusalExceptionMappers} do: an open
 * circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock that
 * could not be taken in time, a version conflict on every attempt. A command the bus dead-lettered
 * ({@link CommandDeadLetteredException}) is pending: 503 with {@code Retry-After} and {@code
 * QUEUED_FOR_REPLAY} when it ran under an {@code Idempotency-Key}, 500 with {@code
 * AWAITING_OPERATOR} when it did not, its entry's {@code commandId} in the body either way.
 * Micronaut picks a handler by the thrown type, so there is one per exception type: a lock timeout
 * or a version conflict that the bus dead-lettered reaches the dead-lettered command's handler, not
 * the lock's. A genuine failure that left no entry, such as a store error, is none of these and
 * stays 500.
 */
public final class CommandRefusalExceptionHandlers {

  private CommandRefusalExceptionHandlers() {}

  /** The answer to {@code refused}: its status, {@code Retry-After} when it may be sent again. */
  static HttpResponse<Map<String, String>> refused(RuntimeException refused) {
    CommandRefusal refusal =
        CommandRefusal.of(refused)
            .orElseThrow(() -> new IllegalStateException("not a refusal", refused));
    MutableHttpResponse<Map<String, String>> answer =
        HttpResponse.<Map<String, String>>status(HttpStatus.valueOf(refusal.status()));
    if (refusal.retryLater()) {
      answer.header(HttpHeaders.RETRY_AFTER, String.valueOf(CommandRefusal.RETRY_AFTER_SECONDS));
    }
    return answer.contentType(MediaType.APPLICATION_JSON_TYPE).body(refusal.body());
  }

  /** The command bus's circuit breaker is open. */
  @Produces
  @Singleton
  @Requires(classes = {CircuitBreakerOpenException.class, ExceptionHandler.class})
  public static class CircuitBreakerOpen
      implements ExceptionHandler<CircuitBreakerOpenException, HttpResponse<Map<String, String>>> {
    @Override
    public HttpResponse<Map<String, String>> handle(
        HttpRequest request, CircuitBreakerOpenException exception) {
      return refused(exception);
    }
  }

  /** The command bus's budget of in-flight asynchronous commands is used up. */
  @Produces
  @Singleton
  @Requires(classes = {CommandBusOverloadedException.class, ExceptionHandler.class})
  public static class CommandBusOverloaded
      implements ExceptionHandler<
          CommandBusOverloadedException, HttpResponse<Map<String, String>>> {
    @Override
    public HttpResponse<Map<String, String>> handle(
        HttpRequest request, CommandBusOverloadedException exception) {
      return refused(exception);
    }
  }

  /** The command bus is closing. */
  @Produces
  @Singleton
  @Requires(classes = {CommandBusClosedException.class, ExceptionHandler.class})
  public static class CommandBusClosed
      implements ExceptionHandler<CommandBusClosedException, HttpResponse<Map<String, String>>> {
    @Override
    public HttpResponse<Map<String, String>> handle(
        HttpRequest request, CommandBusClosedException exception) {
      return refused(exception);
    }
  }

  /** The command's aggregate lock could not be taken within the lock timeout. */
  @Produces
  @Singleton
  @Requires(classes = {LockException.class, ExceptionHandler.class})
  public static class AggregateLock
      implements ExceptionHandler<LockException, HttpResponse<Map<String, String>>> {
    @Override
    public HttpResponse<Map<String, String>> handle(HttpRequest request, LockException exception) {
      return refused(exception);
    }
  }

  /** The aggregate's stream moved past the command's version on every attempt. */
  @Produces
  @Singleton
  @Requires(classes = {OptimisticLockException.class, ExceptionHandler.class})
  public static class AggregateVersionConflict
      implements ExceptionHandler<OptimisticLockException, HttpResponse<Map<String, String>>> {
    @Override
    public HttpResponse<Map<String, String>> handle(
        HttpRequest request, OptimisticLockException exception) {
      return refused(exception);
    }
  }

  /** The command failed and its dead-letter entry is stored: its outcome is pending. */
  @Produces
  @Singleton
  @Requires(classes = {CommandDeadLetteredException.class, ExceptionHandler.class})
  public static class CommandDeadLettered
      implements ExceptionHandler<CommandDeadLetteredException, HttpResponse<Map<String, String>>> {
    @Override
    public HttpResponse<Map<String, String>> handle(
        HttpRequest request, CommandDeadLetteredException exception) {
      return refused(exception);
    }
  }
}
