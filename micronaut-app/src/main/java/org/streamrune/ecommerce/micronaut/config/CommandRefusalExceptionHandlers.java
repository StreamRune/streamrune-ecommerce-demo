package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.util.Map;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.LockException;
import org.streamrune.ecommerce.commands.CommandRefusal;

/**
 * Maps a command the framework refused for a reason that passes ({@link CommandRefusal}) to HTTP
 * 503 Service Unavailable with {@code Retry-After} and a JSON body naming the reason, as the Spring
 * {@code GlobalExceptionHandler} and the Quarkus {@code CommandRefusalExceptionMappers} do: an open
 * circuit breaker, a command bus that admits no more commands or is closing, an aggregate lock that
 * could not be taken in time. Micronaut picks a handler by the thrown type, so there is one per
 * exception type; the command bus throws each of them unwrapped. A genuine failure, such as a store
 * error, is not one of these and stays 500.
 */
public final class CommandRefusalExceptionHandlers {

  private CommandRefusalExceptionHandlers() {}

  /** The 503 answer to {@code refused}. */
  static HttpResponse<Map<String, String>> refused(RuntimeException refused) {
    CommandRefusal refusal =
        CommandRefusal.of(refused)
            .orElseThrow(() -> new IllegalStateException("not a refusal", refused));
    return HttpResponse.<Map<String, String>>status(HttpStatus.SERVICE_UNAVAILABLE)
        .header(HttpHeaders.RETRY_AFTER, String.valueOf(CommandRefusal.RETRY_AFTER_SECONDS))
        .contentType(MediaType.APPLICATION_JSON_TYPE)
        .body(refusal.body());
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
}
