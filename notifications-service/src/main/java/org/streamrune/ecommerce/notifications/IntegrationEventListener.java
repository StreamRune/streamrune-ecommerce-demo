package org.streamrune.ecommerce.notifications;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class IntegrationEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationEventListener.class);
  private static final int CAP = 200;

  public record Received(String type, String body, String entryId, Instant receivedAt) {}

  private final Deque<Received> received = new ArrayDeque<>();
  private final Set<String> seenEntryIds = new HashSet<>();

  @RabbitListener(queues = "${app.integration.queue:streamrune.integration}")
  public void onMessage(Message msg) {
    var props = msg.getMessageProperties();

    // Publisher sets AMQP standard `type` property for the payload type.
    String type = props.getType();
    if (type == null || type.isBlank()) {
      type = "unknown";
    }

    // Publisher sets AMQP standard `messageId` property to entry.id().value()
    // and also mirrors it in header X-Outbox-Entry-Id. Prefer messageId.
    String entryId = props.getMessageId();
    if (entryId == null || entryId.isBlank()) {
      Object headerVal = props.getHeaders().get("X-Outbox-Entry-Id");
      entryId = headerVal != null ? headerVal.toString() : "unknown";
    }

    String body = new String(msg.getBody(), java.nio.charset.StandardCharsets.UTF_8);

    synchronized (this) {
      if (seenEntryIds.contains(entryId)) {
        LOG.debug("Duplicate integration event suppressed entryId={}", entryId);
        return;
      }
      seenEntryIds.add(entryId);
      received.addFirst(new Received(type, body, entryId, Instant.now()));
      while (received.size() > CAP) {
        Received oldest = received.removeLast();
        seenEntryIds.remove(oldest.entryId());
      }
    }

    LOG.info("Received integration event type={} entryId={} body={}", type, entryId, body);
  }

  public synchronized List<Received> recent() {
    return Collections.unmodifiableList(new ArrayList<>(received));
  }
}
