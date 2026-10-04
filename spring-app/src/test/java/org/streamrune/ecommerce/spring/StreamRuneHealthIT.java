package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.streamrune.spring.StreamRuneHealthIndicator;

/**
 * The {@code streamRune} health component as tutorial chapter 15 describes it: the head of the
 * event store, then one {@code relay.<name>} block per background relay registered for liveness. A
 * started relay whose poll thread died turns the whole component {@code DOWN}, so every relay the
 * app runs must be registered, the demo's own dead-letter retry runner included: the framework
 * registers the relays it builds, and the demo's runner replaces the framework's.
 */
class StreamRuneHealthIT extends AbstractIntegrationTest {

  @Autowired StreamRuneHealthIndicator healthIndicator;

  @Test
  void theHealthComponentReportsTheEventStoreHeadAndEveryRelayTheAppRuns() {
    Health health = healthIndicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsKey("eventStore.lastGlobalOffset");
    var relays = new TreeSet<String>();
    health.getDetails().keySet().stream().filter(k -> k.startsWith("relay.")).forEach(relays::add);
    assertThat(relays)
        .containsExactly(
            "relay.dead-letter-retention-sweeper",
            "relay.dead-letter-retry",
            "relay.inbox-retention-sweeper",
            "relay.outbox-relay",
            "relay.outbox-retention-sweeper",
            "relay.saga-compensation-retry:"
                + "org.streamrune.ecommerce.commands.saga.OrderFulfillmentState",
            "relay.saga-dead-letter-retention-sweeper");
    for (String relay : relays) {
      assertThat(block(health, relay)).as(relay).containsEntry("status", "UP");
    }
    for (String relay : new String[] {"relay.outbox-relay", "relay.dead-letter-retry"}) {
      assertThat(block(health, relay))
          .as(relay)
          .containsEntry("started", true)
          .containsEntry("alive", true)
          .containsEntry("consecutiveFailures", 0);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> block(Health health, String key) {
    return (Map<String, Object>) health.getDetails().get(key);
  }
}
