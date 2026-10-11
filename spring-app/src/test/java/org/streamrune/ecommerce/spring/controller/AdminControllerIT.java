package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;

class AdminControllerIT extends AbstractIntegrationTest {

  @Test
  void healthReturnsUp() {
    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .get()
            .uri("/api/admin/health")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body.get("status")).isEqualTo("UP");
  }

  @Test
  void circuitBreakerStateReturnsState() {
    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .get()
            .uri("/api/admin/circuit-breaker")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body).containsKey("state");
  }

  @Test
  void deadLettersReturnsEmptyOrNonNullList() {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> body =
        client
            .get()
            .uri("/api/admin/dead-letters")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
  }

  @Test
  void deadLettersWithLimitQueryParamReturnsOk() {
    client
        .get()
        .uri("/api/admin/dead-letters?limit=10")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isOk();
  }

  @Test
  void outboxReturnsEmptyOrNonNullList() {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> body =
        client
            .get()
            .uri("/api/admin/outbox")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(List.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
  }

  @Test
  void paymentFailureStateReturnsFalseByDefault() {
    @SuppressWarnings("unchecked")
    Map<String, Object> body =
        client
            .get()
            .uri("/api/admin/payment-failure")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(body).isNotNull();
    assertThat(body).containsKey("failureInjected");
  }

  @Test
  void togglePaymentFailureTogglesState() {
    // get initial state
    @SuppressWarnings("unchecked")
    Map<String, Object> initial =
        client
            .get()
            .uri("/api/admin/payment-failure")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(initial).isNotNull();
    boolean initialState = (Boolean) initial.get("failureInjected");

    // toggle
    @SuppressWarnings("unchecked")
    Map<String, Object> toggled =
        client
            .post()
            .uri("/api/admin/payment-failure/toggle")
            .header("X-User-Role", "ADMIN")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody(Map.class)
            .returnResult()
            .getResponseBody();

    assertThat(toggled).isNotNull();
    assertThat(toggled.get("failureInjected")).isEqualTo(!initialState);

    // restore to original state
    client
        .post()
        .uri("/api/admin/payment-failure/toggle")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isOk();
  }

  @Test
  void retryDeadLetterWithUnknownIdReturns404() {
    // DeadLetterRetryRunner.retry() returns false when no entry is found, and the controller maps
    // that to 404 NOT FOUND. A valid UUID format is required because the DLQ table stores UUIDs.
    String unknownUuid = java.util.UUID.randomUUID().toString();
    client
        .post()
        .uri("/api/admin/dead-letters/" + unknownUuid + "/retry")
        .header("X-User-Role", "ADMIN")
        .exchange()
        .expectStatus()
        .isNotFound();
  }

  /**
   * The listings show operational state and, in the dead-letter and outbox lists, payloads of other
   * people's commands and events: none answers without the ADMIN role. {@code /health} is the one
   * open route.
   */
  @Test
  void adminListings_withoutAdminRole_return403() {
    for (String path :
        new String[] {
          "/api/admin/circuit-breaker",
          "/api/admin/dead-letters",
          "/api/admin/outbox",
          "/api/admin/outbox/failed",
          "/api/admin/saga-dead-letters",
          "/api/admin/sagas/faulted",
          "/api/admin/payment-failure"
        }) {
      client.get().uri(path).exchange().expectStatus().isForbidden();
      client
          .get()
          .uri(path)
          .header("X-User-Role", "CUSTOMER")
          .header("X-User-Id", "cust-alice")
          .exchange()
          .expectStatus()
          .isForbidden();
      client.get().uri(path).header("X-User-Role", "ADMIN").exchange().expectStatus().isOk();
    }
    client.get().uri("/api/admin/health").exchange().expectStatus().isOk();
  }

  @Test
  void mutatingAdminEndpoints_withoutAdminRole_return403() {
    client.post().uri("/api/admin/payment-failure/toggle").exchange().expectStatus().isForbidden();
    client
        .post()
        .uri("/api/admin/payment-failure/toggle")
        .header("X-User-Role", "CUSTOMER")
        .exchange()
        .expectStatus()
        .isForbidden();
    client
        .post()
        .uri("/api/admin/dead-letters/" + java.util.UUID.randomUUID() + "/retry")
        .exchange()
        .expectStatus()
        .isForbidden();
  }

  @Test
  void skipWithoutAReason_returns400_beforeTheReplayerIsConsulted() {
    // A missing body and a JSON null body are refused by Spring (@RequestBody is required) before
    // the controller runs; an absent or blank reason is refused by the controller. Every case is
    // decided before the replayer is consulted, so an unknown entry id is enough.
    String uri = "/api/admin/outbox/failed/no-such-entry-" + java.util.UUID.randomUUID() + "/skip";
    client
        .post()
        .uri(uri)
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .exchange()
        .expectStatus()
        .isBadRequest();
    client
        .post()
        .uri(uri)
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue("null")
        .exchange()
        .expectStatus()
        .isBadRequest();
    client
        .post()
        .uri(uri)
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of())
        .exchange()
        .expectStatus()
        .isBadRequest();
    client
        .post()
        .uri(uri)
        .header("X-User-Id", "ops-1")
        .header("X-User-Role", "ADMIN")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("reason", "   "))
        .exchange()
        .expectStatus()
        .isBadRequest();
  }
}
