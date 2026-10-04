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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.customer.CustomerEvent;
import org.streamrune.ecommerce.domain.customer.CustomerState;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * A customer command that fails on the event store is dead-lettered, and the copy of the customer's
 * personal data the dead-letter queue keeps is encrypted under the customer's key: it is never
 * stored as plaintext, a replay restores the original values, and once the customer is forgotten
 * the copy can no longer be read or replayed. Mirrors the Spring app's {@code
 * CustomerCommandDeadLetterIT}.
 *
 * <p>The store failure is injected in the database ({@link AppendFailureInjection}); the commands
 * go through the real HTTP endpoints, the application's command bus and its dead-letter retry
 * runner.
 */
@MicronautTest(transactional = false)
class CustomerCommandDeadLetterIT extends AbstractIntegrationTest {

  private static final String NAME = "Dora Deadletter";
  private static final String ADDRESS = "17 Retry Lane";
  private static final String PHONE = "555-0199";

  @Inject
  @Client("/")
  HttpClient client;

  @Inject DataSource dataSource;
  @Inject DeadLetterQueue deadLetterQueue;
  @Inject DeadLetterRetryRunner deadLetterRetryRunner;
  @Inject EventStore eventStore;
  @Inject PostgresCryptoEngine cryptoEngine;

  @Test
  void aRegistrationThatFailsIsDeadLetteredEncryptedAndItsReplayRestoresThePlaintext()
      throws Exception {
    String cid = "m-dlq-pii-reg-" + System.nanoTime();
    String email = cid + "@example.com";

    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(CustomerState.TYPE, cid, "CustomerRegistered");
      assertThat(register(cid, email)).isEqualTo(500);
    }

    DeadLetterRow row = deadLetterRow(cid);
    assertThat(row).as("the failed registration is in the dead-letter queue").isNotNull();
    assertThat(row.commandType()).isEqualTo(CustomerCommand.RegisterCustomer.class.getName());
    assertThat(row.payload())
        .as("the customer id stays readable, the personal data is ciphertext")
        .contains(cid)
        .doesNotContain(NAME, email, ADDRESS, PHONE);

    assertThat(deadLetterRetryRunner.retry(row.commandId())).isTrue();

    assertThat(deadLetterQueue.find(row.commandId()))
        .as("the replay succeeded and discarded the entry")
        .isEmpty();
    var registered =
        eventStore
            .readStream(
                StreamId.of(CustomerState.TYPE, AggregateId.of(cid)),
                Version.initial(),
                Integer.MAX_VALUE)
            .stream()
            .map(e -> e.event())
            .filter(CustomerEvent.CustomerRegistered.class::isInstance)
            .map(CustomerEvent.CustomerRegistered.class::cast)
            .toList();
    assertThat(registered)
        .as("the replay registered the customer with the values the request carried")
        .containsExactly(new CustomerEvent.CustomerRegistered(cid, NAME, email, ADDRESS, PHONE));
  }

  @Test
  void aProfileUpdateDeadLetteredBeforeTheCustomerIsForgottenIsErasedWithTheCustomer()
      throws Exception {
    String cid = "m-dlq-pii-upd-" + System.nanoTime();
    assertThat(register(cid, cid + "@example.com")).isEqualTo(200);
    String newEmail = "moved-" + cid + "@example.com";

    try (var failures = new AppendFailureInjection(dataSource)) {
      failures.failAppend(CustomerState.TYPE, cid, "ProfileUpdated");
      String body =
          """
          {"name": "Dora Moved", "email": "%s", "address": "1 New Street",
           "phone": "555-0100"}"""
              .formatted(newEmail);
      assertThat(
              status(
                  HttpRequest.PUT("/api/customers/" + cid, body)
                      .contentType(MediaType.APPLICATION_JSON),
                  cid))
          .isEqualTo(500);
    }

    DeadLetterRow row = deadLetterRow(cid);
    assertThat(row).as("the failed update is in the dead-letter queue").isNotNull();
    assertThat(row.commandType()).isEqualTo(CustomerCommand.UpdateProfile.class.getName());
    assertThat(row.payload()).doesNotContain("Dora Moved", newEmail, "1 New Street", "555-0100");

    assertThat(status(HttpRequest.POST("/api/customers/" + cid + "/forget", ""), cid))
        .isEqualTo(200);

    var afterForget =
        DeadLetterRetryRunner.createObjectMapper(cryptoEngine)
            .readValue(deadLetterRow(cid).payload(), CustomerCommand.UpdateProfile.class);
    assertThat(afterForget)
        .as("the queued copy is unreadable once the customer's key is gone")
        .isEqualTo(
            new CustomerCommand.UpdateProfile(
                cid,
                CryptoShreddingModule.REDACTED,
                CryptoShreddingModule.REDACTED,
                CryptoShreddingModule.REDACTED,
                CryptoShreddingModule.REDACTED));

    assertThat(deadLetterRetryRunner.retry(row.commandId())).isTrue();
    var entry = deadLetterQueue.find(row.commandId());
    assertThat(entry).as("a redacted command is never replayed; the entry ages out").isPresent();
    assertThat(entry.get().dlqAttempts()).isPositive();
    assertThat(eventCount(cid, "ProfileUpdated")).isZero();
  }

  private int register(String cid, String email) {
    String body =
        """
        {"customerId": "%s", "name": "%s", "email": "%s", "address": "%s",
         "phone": "%s"}"""
            .formatted(cid, NAME, email, ADDRESS, PHONE);
    return status(
        HttpRequest.POST("/api/customers", body).contentType(MediaType.APPLICATION_JSON), cid);
  }

  /** Sends the request as the customer {@code cid} and answers the HTTP status code. */
  private int status(MutableHttpRequest<String> request, String cid) {
    request.header("X-User-Role", "CUSTOMER").header("X-User-Id", cid);
    try {
      return client.toBlocking().exchange(request, String.class).getStatus().getCode();
    } catch (HttpClientResponseException e) {
      return e.getStatus().getCode();
    }
  }

  private record DeadLetterRow(CommandId commandId, String commandType, String payload) {}

  private DeadLetterRow deadLetterRow(String aggregateId) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT command_id, command_type, payload::text FROM dead_letter_queue"
                    + " WHERE aggregate_id = ?")) {
      ps.setString(1, aggregateId);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        var row =
            new DeadLetterRow(CommandId.of(rs.getString(1)), rs.getString(2), rs.getString(3));
        assertThat(rs.next()).as("one dead-letter entry for " + aggregateId).isFalse();
        return row;
      }
    }
  }

  private int eventCount(String streamId, String eventType) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM event_stream WHERE aggregate_type = 'customer'"
                    + " AND aggregate_id = ? AND event_type = ?")) {
      ps.setString(1, streamId);
      ps.setString(2, eventType);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }
}
