package org.streamrune.ecommerce.commands;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.runtime.CommandDeadLetteredException;

/**
 * A command the framework did not apply, for a reason that passes or with an outcome that is still
 * pending, and how the three apps answer it: a status, {@code Retry-After: 5} when the client may
 * send the request again, and a JSON body that names the reason and says what happened, {@code
 * {"reason": "CIRCUIT_BREAKER_OPEN", "message": "..."}}. Every other failure keeps its own answer:
 * a broken business rule is {@code 400}, a refused caller {@code 403}, and a genuine failure that
 * left nothing behind, a store error with no dead-letter queue to record it, {@code 500}.
 *
 * <p>The five refusals are answered {@code 503 Service Unavailable} with {@code Retry-After}: the
 * same request succeeds later.
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
 *       the state of the resource. With a dead-letter queue the bus records the command there and
 *       throws {@link CommandDeadLetteredException} instead (below); the lock's own exception
 *       reaches a controller when there is no queue or the entry could not be stored.
 *   <li>{@link OptimisticLockException} — the aggregate's stream had moved past the version the
 *       command was decided on, on every attempt of the bus's retry policy: another writer appended
 *       to the stream between the command's load and its append each time (an instance whose lock
 *       is its own, a writer outside the command bus). Nothing was written. The bus reloads the
 *       aggregate on each attempt, and the client sent no version that could conflict, so the
 *       answer is the lock's: {@code 503} with {@code Retry-After}, the same request succeeding
 *       later, and with a dead-letter queue the bus records the command there like the lock's. Only
 *       the command bus's append conflict reaches a controller; the subtypes the framework raises
 *       for projections and saga rows are thrown on the threads of subscriptions.
 * </ul>
 *
 * <p>{@link CommandDeadLetteredException} is not a refusal: the command failed, the bus stored its
 * entry in the dead-letter queue, and a replay of the entry can still apply it. Its outcome is
 * pending, and the answer says what the client does about it, whatever the failure was, a lock
 * timeout and a version conflict included. The body carries the entry's {@code commandId}.
 *
 * <ul>
 *   <li><b>A keyed command</b> ({@code Idempotency-Key}) is queued for replay under its key: the
 *       retry runner replays it on its schedule. {@code 503} with {@code Retry-After} and {@code
 *       QUEUED_FOR_REPLAY}: the client sends the request again with the same key, or waits. The
 *       command inbox runs the command once, whichever of the two comes first. A new key is a new
 *       operation, and the replay applies the first one beside it.
 *   <li><b>An unkeyed command</b> waits for an operator, who replays or discards the entry: nothing
 *       pairs a replay with a request the client sends again, so the two would apply the command
 *       twice. {@code 500} with {@code AWAITING_OPERATOR} and no {@code Retry-After}: the client
 *       does not send it again on its own.
 * </ul>
 *
 * <p>Neither is {@code 202 Accepted}: a client takes a {@code 2xx} for done and never sends the
 * request again, while a replay can fail in its turn and be kept for an operator.
 *
 * @param reason what refused the command, or what happens to it now
 * @param message what the body tells the client
 * @param commandId the dead-letter entry's id, the {@code id} of {@code POST
 *     /api/admin/dead-letters/{id}/retry}; {@code null} for a refusal, which leaves no entry
 */
public record CommandRefusal(Reason reason, String message, String commandId) {

  /** How long a client waits before it sends a refused command again, in seconds. */
  public static final int RETRY_AFTER_SECONDS = 5;

  /** What refused the command, or what happens to it now; the body's {@code reason}. */
  public enum Reason {
    CIRCUIT_BREAKER_OPEN,
    COMMAND_BUS_OVERLOADED,
    COMMAND_BUS_CLOSED,
    AGGREGATE_LOCK_UNAVAILABLE,
    AGGREGATE_VERSION_CONFLICT,
    QUEUED_FOR_REPLAY,
    AWAITING_OPERATOR
  }

  /**
   * The answer {@code failure} gets, or empty when it is none of these: a failure the app answers
   * otherwise. A dead-lettered command is told apart before its cause is looked at: a lock timeout
   * that left an entry is pending, not refused.
   *
   * @param failure what the command bus threw
   */
  public static Optional<CommandRefusal> of(Throwable failure) {
    return switch (failure) {
      case CommandDeadLetteredException deadLettered -> Optional.of(pending(deadLettered));
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

  /**
   * The HTTP status: {@code 500 Internal Server Error} for a command that awaits an operator,
   * {@code 503 Service Unavailable} for every other answer.
   */
  public int status() {
    return reason == Reason.AWAITING_OPERATOR ? 500 : 503;
  }

  /**
   * Whether the client may send the request again: the answer then carries {@code Retry-After:}
   * {@value #RETRY_AFTER_SECONDS}. A command that awaits an operator is not sent again.
   */
  public boolean retryLater() {
    return status() == 503;
  }

  /**
   * The JSON body: {@code reason}, then {@code commandId} when there is an entry, {@code message}.
   */
  public Map<String, String> body() {
    Map<String, String> body = new LinkedHashMap<>();
    body.put("reason", reason.name());
    if (commandId != null) {
      body.put("commandId", commandId);
    }
    body.put("message", message);
    return body;
  }

  private static CommandRefusal pending(CommandDeadLetteredException deadLettered) {
    String commandId = deadLettered.commandId().value();
    return deadLettered.idempotencyKey() != null
        ? new CommandRefusal(
            Reason.QUEUED_FOR_REPLAY,
            "The command is not applied yet: it is queued for a replay under its Idempotency-Key."
                + " Send it again with the same key, or wait; a new key is a new operation.",
            commandId)
        : new CommandRefusal(
            Reason.AWAITING_OPERATOR,
            "The command failed and is recorded for an operator, who replays or discards it."
                + " Sending it again would apply it a second time if the operator replays it.",
            commandId);
  }

  private static Optional<CommandRefusal> refusal(Reason reason, String message) {
    return Optional.of(new CommandRefusal(reason, message, null));
  }
}
