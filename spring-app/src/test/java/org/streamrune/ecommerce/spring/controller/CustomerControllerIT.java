package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

class CustomerControllerIT extends AbstractIntegrationTest {

  private void registerCustomer(String id, String name, String email) {
    String body =
        """
        {
          "customerId": "%s",
          "name": "%s",
          "email": "%s",
          "address": "123 Test St",
          "phone": "555-0100"
        }"""
            .formatted(id, name, email);
    client
        .post()
        .uri("/api/customers")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", id)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  private CustomerView fetchCustomer(String id) {
    return client
        .get()
        .uri("/api/customers/" + id)
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", id)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(CustomerView.class)
        .returnResult()
        .getResponseBody();
  }

  @Test
  void registerCustomerReturns2xxAndProjectionEventuallyExposesIt() {
    String cid = "c-create-" + System.nanoTime();
    registerCustomer(cid, "Alice Test", "alice-" + System.nanoTime() + "@example.com");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              CustomerView v = fetchCustomer(cid);
              assertThat(v).isNotNull();
              assertThat(v.customerId()).isEqualTo(cid);
              assertThat(v.name()).isEqualTo("Alice Test");
              assertThat(v.status().name()).isEqualTo("ACTIVE");
            });
  }

  @Test
  void listCustomersReturnsAllRegistered() {
    String cid1 = "c-list-1-" + System.nanoTime();
    String cid2 = "c-list-2-" + System.nanoTime();
    registerCustomer(cid1, "Bob List", "bob-" + System.nanoTime() + "@example.com");
    registerCustomer(cid2, "Carol List", "carol-" + System.nanoTime() + "@example.com");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> customers =
                  client
                      .get()
                      .uri("/api/customers")
                      .header("X-User-Role", "CUSTOMER")
                      .header("X-User-Id", "test-user")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(customers).isNotNull();
              List<String> ids = customers.stream().map(c -> (String) c.get("customerId")).toList();
              assertThat(ids).contains(cid1, cid2);
            });
  }

  @Test
  void getMissingCustomerReturns404() {
    client
        .get()
        .uri("/api/customers/does-not-exist-" + System.nanoTime())
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "test-user")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  @Test
  void updateProfileReflectsInProjection() {
    String cid = "c-update-" + System.nanoTime();
    registerCustomer(cid, "Dave Original", "dave-" + System.nanoTime() + "@example.com");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchCustomer(cid)).isNotNull());

    client
        .put()
        .uri("/api/customers/" + cid)
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {
              "name": "Dave Updated",
              "email": "dave-updated@example.com",
              "address": "456 New Ave",
              "phone": "555-9999"
            }""")
        .exchange()
        .expectStatus()
        .is2xxSuccessful();

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              CustomerView v = fetchCustomer(cid);
              assertThat(v.name()).isEqualTo("Dave Updated");
            });
  }

  @Test
  void invalidRegisterPayloadReturns400() {
    client
        .post()
        .uri("/api/customers")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", "test-user")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(
            """
            {"customerId": "", "name": "", "email": "not-an-email"}""")
        .exchange()
        .expectStatus()
        .is4xxClientError();
  }

  @Test
  void exportDataReturns2xx() {
    String cid = "c-export-" + System.nanoTime();
    registerCustomer(cid, "Export User", "export-" + System.nanoTime() + "@example.com");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchCustomer(cid)).isNotNull());

    client
        .post()
        .uri("/api/customers/" + cid + "/export-data")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  @Test
  void forgetCustomerReturns2xx() {
    String cid = "c-forget-" + System.nanoTime();
    registerCustomer(cid, "Forget Me", "forget-" + System.nanoTime() + "@example.com");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(fetchCustomer(cid)).isNotNull());

    client
        .post()
        .uri("/api/customers/" + cid + "/forget")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }
}
