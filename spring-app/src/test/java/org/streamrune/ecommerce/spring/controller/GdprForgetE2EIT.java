package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HexFormat;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.ecommerce.queries.dto.CustomerView;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.postgres.PostgresEventStore;

/**
 * End-to-end proof of the GDPR crypto-shredding + erasure flow against a real PostgreSQL
 * (Testcontainers). Each assertion is annotated with what it proves about the wired pipeline:
 * encryption at rest, decrypt-on-read, read-model purge, crypto-shred tombstone, and terminal
 * erasure.
 */
class GdprForgetE2EIT extends AbstractIntegrationTest {

  @Autowired private PostgresEventStore eventStore;
  @Autowired private PostgresCryptoEngine cryptoEngine;
  @Autowired private DataSource dataSource;

  private void registerCustomer(String id, String name, String email) {
    String body =
        """
        {
          "customerId": "%s",
          "name": "%s",
          "email": "%s",
          "address": "742 Evergreen Terrace",
          "phone": "555-0142"
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

  private String rawRegisteredPayload(String customerId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT payload::text FROM event_stream "
                    + "WHERE aggregate_type = 'customer' AND aggregate_id = ?"
                    + " AND event_type = 'CustomerRegistered'")) {
      ps.setString(1, customerId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private boolean rowExists(String table, String column, String value) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT 1 FROM " + table + " WHERE " + column + " = ?")) {
      ps.setString(1, value);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private boolean customerViewRowExists(String customerId) throws Exception {
    return rowExists("customers_view", "id", customerId);
  }

  /**
   * SHA-256 hex (lowercase) of the UTF-8 bytes of a subject id — byte-identical to what {@code
   * PostgresCryptoEngine.storedSubjectId} writes into the {@code encryption_keys} and {@code
   * forgotten_subjects} {@code subject_id} columns, so PII never persists verbatim.
   */
  private static String sha256Hex(String subjectId) throws Exception {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] digest = md.digest(subjectId.getBytes(StandardCharsets.UTF_8));
    return HexFormat.of().formatHex(digest);
  }

  /** {@code POST /forget} with the given role and user id; {@code null} leaves the header out. */
  private WebTestClient.ResponseSpec forget(String customerId, String role, String userId) {
    return post("/api/customers/" + customerId + "/forget", role, userId);
  }

  private WebTestClient.ResponseSpec exportData(String customerId, String role, String userId) {
    return post("/api/customers/" + customerId + "/export-data", role, userId);
  }

  private WebTestClient.ResponseSpec post(String uri, String role, String userId) {
    var request = client.post().uri(uri);
    if (role != null) {
      request.header("X-User-Role", role);
    }
    if (userId != null) {
      request.header("X-User-Id", userId);
    }
    return request.exchange();
  }

  private int eventCount(String customerId, String eventType) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM event_stream WHERE aggregate_type = 'customer'"
                    + " AND aggregate_id = ? AND event_type = ?")) {
      ps.setString(1, customerId);
      ps.setString(2, eventType);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private void awaitReadModelRow(String customerId, boolean present) {
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(customerViewRowExists(customerId)).isEqualTo(present));
  }

  @Test
  void forgetAndExportAreRefusedToAnyoneButAnAdminOrTheCustomerThemself() throws Exception {
    String cid = "gdpr-authz-" + System.nanoTime();
    registerCustomer(cid, "Guarded Gina", "gina-" + System.nanoTime() + "@example.com");

    forget(cid, null, null).expectStatus().isForbidden();
    forget(cid, "CUSTOMER", "someone-else").expectStatus().isForbidden();
    exportData(cid, null, null).expectStatus().isForbidden();
    exportData(cid, "CUSTOMER", "someone-else").expectStatus().isForbidden();

    // Nothing was erased or recorded for the refused requests.
    assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isTrue();
    assertThat(rowExists("forgotten_subjects", "subject_id", sha256Hex(cid))).isFalse();
    assertThat(eventCount(cid, "CustomerForgotten")).isZero();
    assertThat(eventCount(cid, "DataExportRequested")).isZero();

    // The customer themself may request the export.
    exportData(cid, "CUSTOMER", cid).expectStatus().isOk();
    assertThat(eventCount(cid, "DataExportRequested")).isEqualTo(1);
  }

  @Test
  void anAdminMayForgetAnyCustomerAndARepeatIsAnsweredTheSameWay() throws Exception {
    String cid = "gdpr-admin-" + System.nanoTime();
    registerCustomer(cid, "Admin Erased", "erased-" + System.nanoTime() + "@example.com");

    for (int attempt = 1; attempt <= 2; attempt++) {
      forget(cid, "ADMIN", "admin-1")
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.keyDeleted")
          .isEqualTo(true)
          .jsonPath("$.fullyErased")
          .isEqualTo(true);
    }

    assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isFalse();
    assertThat(rowExists("forgotten_subjects", "subject_id", sha256Hex(cid))).isTrue();
    // The repeat recorded nothing new.
    assertThat(eventCount(cid, "CustomerForgotten")).isEqualTo(1);
    // The command ran with the caller's request context bound, so the event names who sent it.
    var forgotten =
        eventStore.load(StreamId.of(CustomerState.TYPE, new AggregateId(cid))).events().stream()
            .filter(e -> e.event() instanceof CustomerEvent.CustomerForgotten)
            .findFirst()
            .orElseThrow();
    assertThat(forgotten.metadata().userId()).isEqualTo(UserId.of("admin-1"));
  }

  @Test
  void forgettingAnIdNoCustomerRegisteredIsRefusedAndLeavesTheIdFree() throws Exception {
    String cid = "gdpr-never-" + System.nanoTime();

    forget(cid, "ADMIN", "admin-1").expectStatus().isBadRequest();

    // No tombstone: a customer can still register under this id.
    assertThat(rowExists("forgotten_subjects", "subject_id", sha256Hex(cid))).isFalse();
    assertThat(eventCount(cid, "CustomerForgotten")).isZero();
    registerCustomer(cid, "Late Larry", "larry-" + System.nanoTime() + "@example.com");
    assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isTrue();
  }

  /**
   * The forget records {@code CustomerForgotten}, then shreds the key, then purges the read model.
   * Each crash point is driven here, and repeating the request finishes the erasure from each:
   *
   * <ol>
   *   <li>The shred fails after the event was recorded: 500, the key stays. The repeat records no
   *       second event and shreds the key.
   *   <li>The shred succeeds and the purge fails: 500 naming the read model, the row stays. The
   *       repeat shreds nothing new and purges the row.
   * </ol>
   *
   * <p>Neither failed attempt answers 200.
   */
  @Test
  void aForgetThatFailsPartWayIsFinishedByRepeatingIt() throws Exception {
    String cid = "gdpr-retry-" + System.nanoTime();
    String subjectHash = sha256Hex(cid);
    registerCustomer(cid, "Retry Rita", "rita-" + System.nanoTime() + "@example.com");
    awaitReadModelRow(cid, true);

    try (var failures = new ForgetFailureInjection(dataSource)) {
      failures.copyReadModelRow(cid);

      // Crash point 1: the event is recorded, the shred fails.
      failures.failKeyDeletion(subjectHash);
      forget(cid, "CUSTOMER", cid)
          .expectStatus()
          .isEqualTo(500)
          .expectBody()
          .jsonPath("$.keyDeleted")
          .isEqualTo(false)
          .jsonPath("$.fullyErased")
          .isEqualTo(false);
      assertThat(eventCount(cid, "CustomerForgotten")).isEqualTo(1);
      assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isTrue();
      assertThat(rowExists("forgotten_subjects", "subject_id", subjectHash)).isFalse();

      // The projection drops the row on CustomerForgotten. Put it back, as a read model that
      // still holds the customer's data, and make its purge fail.
      awaitReadModelRow(cid, false);
      failures.restoreReadModelRow(cid);
      failures.failReadModelPurge(cid);
      failures.allowKeyDeletion(subjectHash);

      // Crash point 2: the repeat shreds the key, the purge fails.
      forget(cid, "CUSTOMER", cid)
          .expectStatus()
          .isEqualTo(500)
          .expectBody()
          .jsonPath("$.keyDeleted")
          .isEqualTo(true)
          .jsonPath("$.fullyErased")
          .isEqualTo(false)
          .jsonPath("$.failedPurgers[0]")
          .isEqualTo("customers_view");
      assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isFalse();
      assertThat(rowExists("forgotten_subjects", "subject_id", subjectHash)).isTrue();
      assertThat(customerViewRowExists(cid)).isTrue();
      assertThat(eventCount(cid, "CustomerForgotten")).isEqualTo(1);

      // The next repeat finishes the erasure.
      failures.allowReadModelPurge(cid);
      forget(cid, "CUSTOMER", cid)
          .expectStatus()
          .isOk()
          .expectBody()
          .jsonPath("$.fullyErased")
          .isEqualTo(true);
      assertThat(customerViewRowExists(cid)).isFalse();
      assertThat(eventCount(cid, "CustomerForgotten")).isEqualTo(1);
    }
  }

  @Test
  void fullCryptoShredAndPurgeFlow() throws Exception {
    String cid = "gdpr-" + System.nanoTime();
    String name = "Erasure Eve";
    String email = "eve-" + System.nanoTime() + "@example.com";

    // (a) register a customer with PII over HTTP
    registerCustomer(cid, name, email);

    // (b) the raw event_stream payload must NOT contain the plaintext PII — proves @Encrypted
    // fields are encrypted at rest by the wired PostgresCryptoEngine (cryptoEngine was null
    // before).
    String rawPayload = rawRegisteredPayload(cid);
    assertThat(rawPayload).as("CustomerRegistered event must be persisted").isNotNull();
    assertThat(rawPayload)
        .as("raw JSONB payload must not leak plaintext PII")
        .doesNotContain(name)
        .doesNotContain(email)
        .doesNotContain("742 Evergreen Terrace")
        .doesNotContain("555-0142");
    // The encryption key for this subject was minted on first encrypt.
    assertThat(rowExists("encryption_keys", "subject_id", sha256Hex(cid)))
        .as("a per-subject encryption key must exist after registration")
        .isTrue();
    assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isTrue();

    // (c) the read model returns the decrypted PII before the forget — proves decrypt-on-read works
    // while the key exists (the projection consumed the event and the engine decrypted the fields).
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              CustomerView v =
                  client
                      .get()
                      .uri("/api/customers/" + cid)
                      .header("X-User-Role", "CUSTOMER")
                      .header("X-User-Id", cid)
                      .exchange()
                      .expectStatus()
                      .isOk()
                      .expectBody(CustomerView.class)
                      .returnResult()
                      .getResponseBody();
              assertThat(v).isNotNull();
              assertThat(v.name()).isEqualTo(name);
              assertThat(v.email()).isEqualTo(email);
            });

    // (d) POST /forget — emits ForgetCustomer AND runs ForgetSubjectService (deleteKey + purgers).
    // The customer may erase themself; the body reports a complete erasure.
    forget(cid, "CUSTOMER", cid)
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.keyDeleted")
        .isEqualTo(true)
        .jsonPath("$.fullyErased")
        .isEqualTo(true)
        .jsonPath("$.failedPurgers")
        .isEmpty();

    // (e1) crypto-shred: the key row is gone and a terminal-erasure tombstone was recorded.
    assertThat(rowExists("encryption_keys", "subject_id", sha256Hex(cid)))
        .as("forget must delete the subject's encryption key (crypto-shred)")
        .isFalse();
    assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isFalse();
    assertThat(rowExists("forgotten_subjects", "subject_id", sha256Hex(cid)))
        .as("forget must record a terminal-erasure tombstone")
        .isTrue();

    // (e2) the read-model row is GONE — proves the SubjectDataPurger (and the projection's
    // CustomerForgotten handler) removed the customers_view row. The GET now 404s.
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              assertThat(customerViewRowExists(cid))
                  .as("customers_view row must be purged after forget")
                  .isFalse();
              client
                  .get()
                  .uri("/api/customers/" + cid)
                  .header("X-User-Role", "CUSTOMER")
                  .header("X-User-Id", cid)
                  .exchange()
                  .expectStatus()
                  .isNotFound();
            });

    // (e3) crypto-shred at the event level: re-loading the aggregate decrypts the persisted
    // CustomerRegistered event to the [REDACTED] tombstone, because the key is gone. The ciphertext
    // event itself is still in the store (immutability preserved) — only the plaintext is
    // unrecoverable.
    var history = eventStore.load(StreamId.of(CustomerState.TYPE, new AggregateId(cid)));
    var registered =
        history.events().stream()
            .map(e -> e.event())
            .filter(e -> e instanceof CustomerEvent.CustomerRegistered)
            .map(e -> (CustomerEvent.CustomerRegistered) e)
            .findFirst()
            .orElseThrow();
    assertThat(registered.name()).isEqualTo(CryptoShreddingModule.REDACTED);
    assertThat(registered.email()).isEqualTo(CryptoShreddingModule.REDACTED);
    assertThat(registered.address()).isEqualTo(CryptoShreddingModule.REDACTED);
    assertThat(registered.phone()).isEqualTo(CryptoShreddingModule.REDACTED);

    // (f) terminal erasure: re-registering the SAME customer id must fail — the engine refuses to
    // mint a fresh key for a crypto-shredded subject (SubjectForgottenException -> HTTP 410 Gone).
    String body =
        """
        {"customerId": "%s", "name": "Resurrected", "email": "resurrect@example.com",
         "address": "x", "phone": "y"}"""
            .formatted(cid);
    client
        .post()
        .uri("/api/customers")
        .header("X-User-Role", "CUSTOMER")
        .header("X-User-Id", cid)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(body)
        .exchange()
        .expectStatus()
        .isEqualTo(410);
    // The encrypt attempt did not resurrect a key.
    assertThat(cryptoEngine.isKeyAvailable(SubjectId.of(cid))).isFalse();
    assertThat(rowExists("forgotten_subjects", "subject_id", sha256Hex(cid))).isTrue();
  }
}
