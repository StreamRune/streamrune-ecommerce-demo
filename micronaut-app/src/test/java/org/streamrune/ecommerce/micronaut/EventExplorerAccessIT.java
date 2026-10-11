package org.streamrune.ecommerce.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Who may read the event history over HTTP. The two Event Explorer endpoints return event payloads
 * as the event store reads them, decrypted, so they answer only to the ADMIN role. What the open
 * live feed says instead is {@code LiveFeedAccessIT}'s.
 */
@MicronautTest(transactional = false)
class EventExplorerAccessIT extends AbstractIntegrationTest {

  @Inject
  @Client("/")
  HttpClient client;

  private String registerCustomer(String name, String email) {
    String cid = "m-feed-c-" + System.nanoTime();
    String body =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "1 Feed St",
         "phone": "555-0199"}"""
            .formatted(cid, name, email);
    var response =
        client
            .toBlocking()
            .exchange(
                HttpRequest.POST("/api/customers", body)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("X-User-Role", "CUSTOMER")
                    .header("X-User-Id", cid));
    assertThat(response.getStatus().getCode()).isBetween(200, 299);
    return cid;
  }

  private void assertForbidden(HttpRequest<?> request) {
    HttpClientResponseException refusal =
        assertThrows(
            HttpClientResponseException.class, () -> client.toBlocking().exchange(request));
    assertThat(refusal.getStatus().getCode()).isEqualTo(403);
  }

  @Test
  void theEventListIsForbiddenWithoutTheAdminRole() {
    assertForbidden(HttpRequest.GET("/api/events"));
    assertForbidden(
        HttpRequest.GET("/api/events?offset=0&limit=5")
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", "cust-1"));
  }

  @Test
  void aStreamIsForbiddenWithoutTheAdminRole() {
    String cid = registerCustomer("Stream Secret", "stream-secret@example.com");

    assertForbidden(HttpRequest.GET("/api/events/customer/" + cid));
    // Not even the customer whose stream it is: the explorer is an operator's tool.
    assertForbidden(
        HttpRequest.GET("/api/events/customer/" + cid)
            .header("X-User-Role", "CUSTOMER")
            .header("X-User-Id", cid));
  }

  @Test
  @SuppressWarnings("unchecked")
  void anAdminReadsTheDecryptedPayload() {
    String cid = registerCustomer("Admin Visible", "admin-visible@example.com");

    List<Map<String, Object>> events =
        client
            .toBlocking()
            .retrieve(
                HttpRequest.GET("/api/events/customer/" + cid)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin"),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));

    assertThat(events).hasSize(1);
    assertThat(events.getFirst()).containsEntry("eventType", "CustomerRegistered");
    Map<String, Object> payload = (Map<String, Object>) events.getFirst().get("payload");
    assertThat(payload)
        .containsEntry("name", "Admin Visible")
        .containsEntry("email", "admin-visible@example.com");
  }
}
