package org.streamrune.ecommerce.commands;

import java.util.Objects;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.UserId;

/**
 * Dispatches a request's command under the {@code Idempotency-Key} the client sent, or without a
 * key when it sent none. The three apps call it from the endpoints whose command changes a quantity
 * by an amount ({@code PUT /api/products/{id}/stock}, {@code POST
 * /api/inventory/{productId}/receive}): applied twice, such a command is a different stock. A
 * client that sends such a request again — after a timeout, from a retry button — sends the same
 * key, and the command bus runs the command once and answers the repeat with the recorded result.
 *
 * <p>The key is also what makes the command safe to replay from the dead-letter queue: an entry
 * keeps the key, and the replay runs under it, so a replay after the caller's own successful retry
 * is answered by the command inbox instead of applying the change a second time. The retry runner
 * therefore replays keyed entries on its schedule and leaves unkeyed ones to an operator.
 *
 * <p><b>The key is the caller's.</b> The value comes from the client, so the bus would hand the
 * result recorded under it to anyone who presents it ({@link IdempotencyKey#scopedTo}). The key is
 * built inside the caller's namespace, their user id: two callers who send the same value get two
 * keys.
 */
public final class IdempotentDispatch {

  /** The request header a client puts the key in. */
  public static final String HEADER = "Idempotency-Key";

  private IdempotentDispatch() {}

  /**
   * The key the bus records the command under, or {@code null} when the request carries no {@code
   * Idempotency-Key} header.
   *
   * @param header the header's value, {@code null} when absent
   * @param caller the identity of the caller; required when a header is present
   * @throws IllegalArgumentException when the value is blank, longer than the framework accepts or
   *     holds a control character, or when the caller has no identity (the apps answer {@code 400})
   */
  public static IdempotencyKey keyFor(String header, UserId caller) {
    if (header == null) {
      return null;
    }
    if (caller == null) {
      throw new IllegalArgumentException("An Idempotency-Key needs the caller's X-User-Id");
    }
    return IdempotencyKey.scopedTo(caller.value(), header);
  }

  /** Runs {@code command} under {@code key}, or unkeyed when {@code key} is {@code null}. */
  public static CommandBus.CommandResult execute(
      CommandBus commandBus, Command command, IdempotencyKey key) {
    Objects.requireNonNull(commandBus, "commandBus is required");
    return key == null ? commandBus.execute(command) : commandBus.execute(command, key);
  }
}
