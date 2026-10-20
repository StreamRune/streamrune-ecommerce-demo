package org.streamrune.ecommerce.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.UserId;
import org.streamrune.ecommerce.domain.product.ProductCommand;

class IdempotentDispatchTest {

  private static final Command ADJUST = new ProductCommand.AdjustStock("p-1", 5, "restock");

  @Test
  void noHeaderIsNoKey() {
    assertThat(IdempotentDispatch.keyFor(null, UserId.of("admin-1"))).isNull();
  }

  @Test
  void theKeyIsTheHeaderInsideTheCallersNamespace() {
    assertThat(IdempotentDispatch.keyFor("k-1", UserId.of("admin-1")))
        .isEqualTo(IdempotencyKey.scopedTo("admin-1", "k-1"))
        .isNotEqualTo(IdempotentDispatch.keyFor("k-1", UserId.of("admin-2")));
  }

  @Test
  void aHeaderWithoutACallerIsRefused() {
    assertThatThrownBy(() -> IdempotentDispatch.keyFor("k-1", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("X-User-Id");
  }

  @Test
  void aBlankOrControlCharacterHeaderIsRefused() {
    assertThatThrownBy(() -> IdempotentDispatch.keyFor(" ", UserId.of("admin-1")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IdempotentDispatch.keyFor("a\nb", UserId.of("admin-1")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aKeyedCommandGoesToTheKeyedExecute_anUnkeyedOneToThePlainOne() {
    var bus = new RecordingBus();
    var key = IdempotencyKey.scopedTo("admin-1", "k-1");

    IdempotentDispatch.execute(bus, ADJUST, key);
    IdempotentDispatch.execute(bus, ADJUST, null);

    assertThat(bus.keys).containsExactly(key, null);
  }

  private static final class RecordingBus implements CommandBus {
    final List<IdempotencyKey> keys = new ArrayList<>();

    @Override
    public <C extends Command> CommandResult execute(C command) {
      keys.add(null);
      return null;
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      keys.add(key);
      return null;
    }
  }
}
