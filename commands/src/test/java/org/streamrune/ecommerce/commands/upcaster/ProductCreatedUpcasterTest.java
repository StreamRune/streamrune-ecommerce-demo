package org.streamrune.ecommerce.commands.upcaster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.EventType;

class ProductCreatedUpcasterTest {

  private final ProductCreatedUpcaster upcaster = new ProductCreatedUpcaster();

  @Test
  void eventType() {
    assertThat(upcaster.eventType()).isEqualTo(new EventType("ProductCreated"));
  }

  @Test
  void currentVersion() {
    assertThat(upcaster.currentVersion()).isEqualTo(2);
  }

  @Test
  void upcast_v1_to_v2_addsCategoryField() {
    Map<String, Object> v1 = new HashMap<>();
    v1.put("productId", "p-1");
    v1.put("name", "Widget");
    v1.put("description", "A widget");
    v1.put("price", Map.of("amount", 10, "currency", "USD"));
    v1.put("stock", 50);

    Map<String, Object> v2 = upcaster.upcast(v1, 1);

    assertThat(v2).containsEntry("category", "Uncategorized");
    assertThat(v2).containsEntry("productId", "p-1");
    assertThat(v2).containsEntry("name", "Widget");
  }

  @Test
  void upcast_v1_keepsACategoryThePayloadAlreadyCarries() {
    // schema_version records the upcaster chain in effect when the event was written, not the
    // payload's shape: a product created before this upcaster was registered is stamped 1 even
    // though its payload already has a category. The step must leave that category alone.
    Map<String, Object> stampedV1 = new HashMap<>();
    stampedV1.put("productId", "p-2");
    stampedV1.put("category", "Gadgets");

    Map<String, Object> v2 = upcaster.upcast(stampedV1, 1);

    assertThat(v2).containsEntry("category", "Gadgets");
  }

  @Test
  void upcast_v1_fillsANullCategory() {
    Map<String, Object> v1 = new HashMap<>();
    v1.put("productId", "p-3");
    v1.put("category", null);

    Map<String, Object> v2 = upcaster.upcast(v1, 1);

    assertThat(v2).containsEntry("category", "Uncategorized");
  }
}
