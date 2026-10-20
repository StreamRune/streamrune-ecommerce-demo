package org.streamrune.ecommerce.commands;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;

/**
 * A command the framework refused for a reason that passes, and how the three apps answer it:
 * {@code 503 Service Unavailable} with {@code Retry-After: 5} and a JSON body that names the reason
 * and says what happened, {@code {"reason": "CIRCUIT_BREAKER_OPEN", "message": "..."}}. Every other
 * failure keeps its own answer: a broken business rule is {@code 400}, a refused caller {@code
 * 403}, and a genuine failure, a store error for one, {@code 500}.
 *
 * <p>The five refusals, as the framework reports them:
 *
 * <ul>
 *   <li>{@link CircuitBreakerOpenException} — the command bus's circuit breaker is open (or
 *       half-open with its probe in flight) after consecutive infrastructure failures, and refuses
 *       every command until a probe succeeds. Nothing was attempted. The breaker does not say how
 *       long it stays open, so {@code Retry-After} is the same short wait as for the others.
 *   <li>{@link CommandBusOverloadedException} — the bus's budget of in-flight asynchronous commands
 *       is used up. Nothing was attempted. Only {@code executeAsync} has the budget; the demo's
 *       routes run commands synchronously, so it is mapped for completeness.
 *   <li>{@link CommandBusClosedException} — the bus is closing (the application is shutting down)
 *       and admits no new command. Nothing was attempted; another instance, or this one restarted,
 *       runs the command when it is sent again.
 *   <li>{@link LockException} — the command's aggregate lock could not be taken within the lock
 *       timeout ({@code streamrune.lock-timeout}), on every attempt of the bus's retry policy: a
 *       command on the same aggregate held it all along, or the locker could not reach its store.
 *       The decider never ran and nothing was written. A later attempt takes the lock once it is
 *       free; nothing in the request is wrong, so the answer is {@code 503} with {@code
 *       Retry-After} and not {@code 409 Conflict}, which tells a client its request conflicts with
 *       the state of the resource. Like any infrastructure failure the bus records the command in
 *       the dead-letter queue: the retry runner replays a keyed command on its schedule, and an
 *       unkeyed one waits for an operator.
 *   <li>{@link OptimisticLockException} — the aggregate's stream had moved past the version the
 *       command was decided on, on every attempt of the bus's retry policy: another writer appended
 *       to the stream between the command's load and its append each time (a second instance whose
 *       lock is its own, a writer outside the command bus). Nothing was written. The bus reloads
 *       the aggregate on each attempt, and the client sent no version that could conflict, so the
 *       answer is the lock's: {@code 503} with {@code Retry-After}, the same request succeeding
 *       later. The bus records the command in the dead-letter queue like the lock's. Only the
 *       command bus's append conflict reaches a controller; the subtypes the framework raises for
 *       projections and saga rows are thrown on the threads of subscriptions.
 * </ul>
 *
 * @param reason what refused the command
 * @param message what the body tells the client
 */
public record CommandRefusal(Reason reason, String message) {

  /** How long a client waits before it sends a refused command again, in seconds. */
  public static final int RETRY_AFTER_SECONDS = 5;

  /** What refused the command; the body's {@code reason}. */
  public enum Reason {
    CIRCUIT_BREAKER_OPEN,
    COMMAND_BUS_OVERLOADED,
    COMMAND_BUS_CLOSED,
    AGGREGATE_LOCK_UNAVAILABLE,
    AGGREGATE_VERSION_CONFLICT
  }

  /**
   * The refusal {@code failure} reports, or empty when it is none of the five: a failure the app
   * answers otherwise.
   *
   * @param failure what the command bus threw
   */
  public static Optional<CommandRefusal> of(Throwable failure) {
    return switch (failure) {
      case CircuitBreakerOpenException _ ->
          refusal(
              Reason.CIRCUIT_BREAKER_OPEN,
              "The command bus refuses commands while its circuit breaker is open.");
      case CommandBusOverloadedException _ ->
          refusal(Reason.COMMAND_BUS_OVERLOADED, "The command bus admits no more commands now.");
      case CommandBusClosedException _ ->
          refusal(
              Reason.COMMAND_BUS_CLOSED,
              "The application is shutting down and admits no more commands.");
      case LockException _ ->
          refusal(
              Reason.AGGREGATE_LOCK_UNAVAILABLE,
              "The aggregate's lock could not be taken in time.");
      case OptimisticLockException _ ->
          refusal(
              Reason.AGGREGATE_VERSION_CONFLICT,
              "The aggregate changed while the command ran; nothing was written.");
      case null, default -> Optional.empty();
    };
  }

  /** The JSON body: {@code reason}, then {@code message}. */
  public Map<String, String> body() {
    Map<String, String> body = new LinkedHashMap<>();
    body.put("reason", reason.name());
    body.put("message", message);
    return body;
  }

  private static Optional<CommandRefusal> refusal(Reason reason, String message) {
    return Optional.of(new CommandRefusal(reason, message));
  }
}
