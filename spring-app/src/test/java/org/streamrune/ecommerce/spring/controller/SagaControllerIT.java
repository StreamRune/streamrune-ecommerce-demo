package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

class SagaControllerIT extends AbstractIntegrationTest {

  private void createProduct(String pid, int stock) {
    String body =
        """
        {
          "productId": "%s",
          "name": "Saga Widget",
          "description": "saga test product",
          "category": "Test",
          "price": 15.00,
          "initialStock": %d
        }"""
            .formatted(pid, stock);
    client
        .post()
        .uri("/api/products")
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  private void registerCustomer(String cid) {
    String body =
        """
        {
          "customerId": "%s",
          "name": "Saga Customer",
          "email": "saga-%s@example.com",
          "address": "1 Saga Lane",
          "phone": "555-0200"
        }"""
            .formatted(cid, System.nanoTime());
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
  }

  private void placeOrder(String oid, String cid, String pid) {
    String body =
        """
        {
          "orderId": "%s",
          "customerId": "%s",
          "lines": [
            {"productId": "%s", "quantity": 1, "unitPrice": 15.00}
          ]
        }"""
            .formatted(oid, cid, pid);
    client
        .post()
        .uri("/api/orders")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .is2xxSuccessful();
  }

  @Test
  void listSagasReturnsNonNullList() {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> sagas =
        client
            .get()
            .uri("/api/saga/fulfillments")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(sagas).isNotNull();
  }

  @Test
  void listSagasWithLimitQueryParamReturnsOk() {
    client.get().uri("/api/saga/fulfillments?limit=5").exchange().expectStatus().isOk();
  }

  @Test
  void getSagaForUnknownIdReturns404() {
    client
        .get()
        .uri("/api/saga/fulfillments/nonexistent-saga-" + System.nanoTime())
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  @Test
  void placingOrderEventuallyCreatesASagaEntry() {
    String cid = "saga-cust-" + System.nanoTime();
    String pid = "saga-prod-" + System.nanoTime();
    String oid = "saga-order-" + System.nanoTime();

    registerCustomer(cid);
    createProduct(pid, 50);

    // Wait for projections to catch up before placing the order
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                client
                    .get()
                    .uri("/api/products/" + pid)
                    .header("X-User-Role", "ADMIN")
                    .header("X-User-Id", "test-admin")
                    .exchange()
                    .expectStatus()
                    .isOk());

    placeOrder(oid, cid, pid);

    // A saga should eventually be created and appear in the list
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> sagas =
                  client
                      .get()
                      .uri("/api/saga/fulfillments?limit=100")
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(List.class)
                      .returnResult()
                      .getResponseBody();

              assertThat(sagas).isNotNull();
              boolean hasSagaForOrder =
                  sagas.stream()
                      .anyMatch(
                          s -> {
                            String sagaType = (String) s.get("sagaType");
                            String state = (String) s.get("state");
                            return sagaType != null
                                && sagaType.contains("Fulfillment")
                                && state != null
                                && state.contains(oid);
                          });
              assertThat(hasSagaForOrder)
                  .as("Expected a fulfillment saga entry for order %s", oid)
                  .isTrue();
            });
  }

  /** The flag both payment-failure routes flip, read through the open admin listing. */
  private boolean failureInjected() {
    @SuppressWarnings("unchecked")
    Map<String, Object> state =
        client
            .get()
            .uri("/api/admin/payment-failure")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();
    assertThat(state).isNotNull();
    return (Boolean) state.get("failureInjected");
  }

  @Test
  void injectFailureWithAdminRoleTogglesPaymentFailureState() {
    // The route flips the shared PaymentGatewaySimulator flag regardless of the saga id, so a
    // dummy id is enough; it is the admin toggle's shorthand and needs the ADMIN role like it.
    boolean initial = failureInjected();

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        client
            .post()
            .uri("/api/saga/fulfillments/any-saga-id/inject-failure")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(result).isNotNull();
    assertThat(result.get("failureInjected")).isEqualTo(!initial);
    assertThat(failureInjected()).isEqualTo(!initial);

    // restore
    client
        .post()
        .uri("/api/saga/fulfillments/any-saga-id/inject-failure")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isOk();
    assertThat(failureInjected()).isEqualTo(initial);
  }

  @Test
  void injectFailureWithoutAdminRoleIsForbiddenAndLeavesTheFlag() {
    boolean initial = failureInjected();

    client
        .post()
        .uri("/api/saga/fulfillments/any-saga-id/inject-failure")
        .exchange()
        .expectStatus()
        .isForbidden();
    assertThat(failureInjected()).isEqualTo(initial);

    client
        .post()
        .uri("/api/saga/fulfillments/any-saga-id/inject-failure")
        .header("X-User-Role", "CUSTOMER")
        .exchange()
        .expectStatus()
        .isForbidden();
    assertThat(failureInjected()).isEqualTo(initial);
  }
}
