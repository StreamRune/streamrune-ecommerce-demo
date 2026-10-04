package org.streamrune.ecommerce.spring.controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.streamrune.core.types.AggregateType;

/**
 * Makes the event store refuse one event for one stream, in the database rather than in the
 * application: the integration tests share one application context, so no bean is replaced. While a
 * flag for a stream (aggregate type and id) and an event type is set, a trigger raises on the
 * {@code event_stream} insert, so the append rolls back with a store error, the kind of failure the
 * command bus records in its dead-letter queue.
 *
 * <p>A flag holds until {@link #allowAppend} removes it. {@link #close()} drops the trigger, its
 * function and the flag table.
 */
final class AppendFailureInjection implements AutoCloseable {

  private final DataSource dataSource;

  AppendFailureInjection(DataSource dataSource) throws SQLException {
    this.dataSource = dataSource;
    execute(
        """
        CREATE TABLE IF NOT EXISTS append_test_failures (
          aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL,
          PRIMARY KEY (aggregate_type, aggregate_id, event_type));
        CREATE OR REPLACE FUNCTION append_test_fail_event() RETURNS trigger
          LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM append_test_failures
                     WHERE aggregate_type = NEW.aggregate_type
                       AND aggregate_id = NEW.aggregate_id
                       AND event_type = NEW.event_type) THEN
            RAISE EXCEPTION 'injected failure: event insert refused';
          END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS append_test_fail_event ON event_stream;
        CREATE TRIGGER append_test_fail_event BEFORE INSERT ON event_stream
          FOR EACH ROW EXECUTE FUNCTION append_test_fail_event();
        """);
  }

  /** Appending an event of this type to this stream fails until allowed again. */
  void failAppend(AggregateType type, String aggregateId, String eventType) throws SQLException {
    update(
        "INSERT INTO append_test_failures (aggregate_type, aggregate_id, event_type)"
            + " VALUES (?, ?, ?)",
        type.value(),
        aggregateId,
        eventType);
  }

  void allowAppend(AggregateType type, String aggregateId, String eventType) throws SQLException {
    update(
        "DELETE FROM append_test_failures"
            + " WHERE aggregate_type = ? AND aggregate_id = ? AND event_type = ?",
        type.value(),
        aggregateId,
        eventType);
  }

  @Override
  public void close() throws SQLException {
    execute(
        """
        DROP TRIGGER IF EXISTS append_test_fail_event ON event_stream;
        DROP FUNCTION IF EXISTS append_test_fail_event();
        DROP TABLE IF EXISTS append_test_failures;
        """);
  }

  private void execute(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
      commitUnlessAutoCommit(conn);
    }
  }

  private void update(String sql, String first, String second, String third) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, first);
      ps.setString(2, second);
      ps.setString(3, third);
      ps.executeUpdate();
      commitUnlessAutoCommit(conn);
    }
  }

  private static void commitUnlessAutoCommit(Connection conn) throws SQLException {
    if (!conn.getAutoCommit()) {
      conn.commit();
    }
  }
}
