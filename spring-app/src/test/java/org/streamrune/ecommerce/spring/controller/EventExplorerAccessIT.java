package org.streamrune.ecommerce.spring.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

/**
 * Who may read the event history over HTTP. The two Event Explorer endpoints return event payloads
 * as the event store reads them, decrypted, so they answer only to the ADMIN role. What the open
 * live feed says instead is {@code LiveFeedAccessIT}'s.
 */
class EventExplorerAccessIT extends AbstractIntegrationTest {

  private String registerCustomer(String name, String email) {
    String cid = "feed-c-" + System.nanoTime();
    String body =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "1 Feed St",
         "phone": "555-0199"}"""
            .formatted(cid, name, email);
    client
        .post()
        .uri("/api/customers")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
    return cid;
  }

  @Test
  void theEventListIsForbiddenWithoutTheAdminRole() {
    client.get().uri("/api/events").exchange().expectStatus().isForbidden();
    client
        .get()
        .uri("/api/events?offset=0&limit=5")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "cust-1")
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  @Test
  void aStreamIsForbiddenWithoutTheAdminRole() {
    String cid = registerCustomer("Stream Secret", "stream-secret@example.com");

    client.get().uri("/api/events/customer/" + cid).exchange().expectStatus().isForbidden();
    // Not even the customer whose stream it is: the explorer is an operator's tool.
    client
        .get()
        .uri("/api/events/customer/" + cid)
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  @Test
  void anAdminReadsTheDecryptedPayload() {
    String cid = registerCustomer("Admin Visible", "admin-visible@example.com");

    client
        .get()
        .uri("/api/events/customer/" + cid)
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$[0].eventType")
        .isEqualTo("CustomerRegistered")
        .jsonPath("$[0].payload.name")
        .isEqualTo("Admin Visible")
        .jsonPath("$[0].payload.email")
        .isEqualTo("admin-visible@example.com");
  }
}
