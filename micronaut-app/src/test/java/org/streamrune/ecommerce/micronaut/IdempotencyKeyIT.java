package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.inventory.InventoryState;

/**
 * The {@code Idempotency-Key} header of the two relative writes, {@code POST
 * /api/inventory/{productId}/adjust} and {@code POST /api/inventory/{productId}/receive}, as in the
 * Spring and Quarkus apps: the command runs once per key, every time without one, and the key lives
 * in the caller's namespace.
 */
@MicronautTest(transactional = false)
class IdempotencyKeyIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  @Inject EventStore eventStore;

  @Test
  void theSameKeyTwiceAdjustsTheInventoryOnce() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    assertThat(status(adjustInventory(pid, "admin-1", key))).isEqualTo(200);
    assertThat(status(adjustInventory(pid, "admin-1", key))).isEqualTo(200);

    assertThat(inventoryAdjustments(pid)).as("the second request is the first one's retry").isOne();
  }

  @Test
  void aNewKeyIsANewAdjustment() {
    String pid = createProduct();

    assertThat(status(adjustInventory(pid, "admin-1", UUID.randomUUID().toString())))
        .isEqualTo(200);
    assertThat(status(adjustInventory(pid, "admin-1", UUID.randomUUID().toString())))
        .isEqualTo(200);

    assertThat(inventoryAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void withoutAKeyEveryRequestAdjusts() {
    String pid = createProduct();

    assertThat(status(adjustInventory(pid, "admin-1", null))).isEqualTo(200);
    assertThat(status(adjustInventory(pid, "admin-1", null))).isEqualTo(200);

    assertThat(inventoryAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void theKeyIsScopedToTheCaller() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    assertThat(status(adjustInventory(pid, "admin-1", key))).isEqualTo(200);
    assertThat(status(adjustInventory(pid, "admin-2", key))).isEqualTo(200);

    assertThat(inventoryAdjustments(pid)).isEqualTo(2);
  }

  @Test
  void aKeyPresentedForAnotherProductIsRefused() {
    String first = createProduct();
    String second = createProduct();
    String key = UUID.randomUUID().toString();

    assertThat(status(adjustInventory(first, "admin-1", key))).isEqualTo(200);
    assertThat(status(adjustInventory(second, "admin-1", key))).isEqualTo(400);

    assertThat(inventoryAdjustments(second)).isZero();
  }

  @Test
  void aKeyLongerThanTheFrameworkAcceptsIsRefused() {
    String pid = createProduct();

    assertThat(status(adjustInventory(pid, "admin-1", "k".repeat(600)))).isEqualTo(400);

    assertThat(inventoryAdjustments(pid)).isZero();
  }

  @Test
  void theSameKeyTwiceReceivesAShipmentOnce() {
    String pid = createProduct();
    String key = UUID.randomUUID().toString();

    assertThat(status(receiveShipment(pid, key))).isEqualTo(200);
    assertThat(status(receiveShipment(pid, key))).isEqualTo(200);

    assertThat(shipmentsReceived(pid)).isOne();
  }

  private String createProduct() {
    String pid = "m-idem-" + System.nanoTime();
    assertThat(
            status(
                HttpRequest.POST(
                        "/api/products",
                        """
                        {"productId": "%s", "name": "Keyed", "description": "idempotency key",
                         "category": "Test", "price": 5.00}"""
                            .formatted(pid))
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "admin-1")))
        .isEqualTo(200);
    return pid;
  }

  private static MutableHttpRequest<String> adjustInventory(String pid, String userId, String key) {
    var request =
        HttpRequest.POST(
                "/api/inventory/" + pid + "/adjust",
                "{\"quantityChange\": 5, \"reason\": \"restock\"}")
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-User-Role", "ADMIN")
            .header("X-User-Id", userId);
    return key == null ? request : request.header("Idempotency-Key", key);
  }

  private static MutableHttpRequest<String> receiveShipment(String pid, String key) {
    return HttpRequest.POST("/api/inventory/" + pid + "/receive", "{\"quantity\": 7}")
        .contentType(MediaType.APPLICATION_JSON)
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "admin-1")
        .header("Idempotency-Key", key);
  }

  private int status(MutableHttpRequest<?> request) {
    try {
      return client.toBlocking().exchange(request).getStatus().getCode();
    } catch (HttpClientResponseException e) {
      return e.getStatus().getCode();
    }
  }

  private long inventoryAdjustments(String pid) {
    return eventStore
        .readStream(
            StreamId.of(InventoryState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> e.event() instanceof InventoryEvent.InventoryAdjusted)
        .count();
  }

  private long shipmentsReceived(String pid) {
    return eventStore
        .readStream(
            StreamId.of(InventoryState.TYPE, AggregateId.of(pid)),
            Version.initial(),
            Integer.MAX_VALUE)
        .stream()
        .filter(e -> e.event() instanceof InventoryEvent.ShipmentReceived)
        .count();
  }
}
