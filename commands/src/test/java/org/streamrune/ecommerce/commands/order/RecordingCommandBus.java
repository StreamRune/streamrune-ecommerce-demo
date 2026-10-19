package org.streamrune.ecommerce.commands.order;

import java.util.ArrayList;
import java.util.List;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.IdempotencyKey;

/**
 * A command bus that records every command it is handed, with its key and the context it ran under,
 * and stores nothing. Like the shipped buses it first refuses a reserved correlation id that is
 * bound without its saga origin. It can fail one command, by its position among the commands it was
 * handed.
 */
final class RecordingCommandBus implements CommandBus {

  private final List<Command> commands = new ArrayList<>();
  private final List<IdempotencyKey> keys = new ArrayList<>();
  private final List<StreamRuneContext.DispatchCause> causes = new ArrayList<>();
  private final List<StreamRuneContext.RequestContext> contexts = new ArrayList<>();
  private final List<Boolean> sagaDispatches = new ArrayList<>();
  private int failAt = -1;
  private RuntimeException failure;

  /**
   * Throws {@code failure} for the command at {@code position} (0-based) instead of recording it.
   */
  void failAt(int position, RuntimeException failure) {
    this.failAt = position;
    this.failure = failure;
  }

  List<Command> commands() {
    return commands;
  }

  List<IdempotencyKey> keys() {
    return keys;
  }

  List<StreamRuneContext.DispatchCause> causes() {
    return causes;
  }

  List<StreamRuneContext.RequestContext> contexts() {
    return contexts;
  }

  List<Boolean> sagaDispatches() {
    return sagaDispatches;
  }

  void clear() {
    commands.clear();
    keys.clear();
    causes.clear();
    contexts.clear();
    sagaDispatches.clear();
  }

  @Override
  public <C extends Command> CommandResult execute(C command) {
    return execute(command, null);
  }

  @Override
  public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
    StreamRuneContext.requireNoForeignReservedCorrelationId();
    if (commands.size() == failAt) {
      failAt = -1;
      throw failure;
    }
    commands.add(command);
    keys.add(key);
    causes.add(StreamRuneContext.cause());
    contexts.add(StreamRuneContext.capture());
    sagaDispatches.add(StreamRuneContext.isSagaDispatch());
    return new CommandResult(List.of(), null, null, List.of());
  }

  @Override
  public boolean supportsIdempotentExecution() {
    return true;
  }
}
