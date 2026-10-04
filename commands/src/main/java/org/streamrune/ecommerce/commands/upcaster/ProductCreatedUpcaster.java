package org.streamrune.ecommerce.commands.upcaster;

import java.util.Map;
import org.streamrune.core.types.EventType;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * Upcasts ProductCreated events from v1 (no category) to v2 (with category field). A v1 event
 * without a category gets "Uncategorized".
 *
 * <p>The stored schema version records the upcaster chain in effect when the event was written, not
 * the payload's shape: products created before this upcaster was registered are stamped v1 although
 * their payload already carries a category. The step therefore only fills a missing (or null)
 * category and never replaces one the payload already has.
 */
public class ProductCreatedUpcaster implements EventUpcaster {

  @Override
  public EventType eventType() {
    return new EventType("ProductCreated");
  }

  @Override
  public int currentVersion() {
    return 2;
  }

  @Override
  public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
    if (fromVersion == 1) {
      eventData.putIfAbsent("category", "Uncategorized");
    }
    return eventData;
  }
}
