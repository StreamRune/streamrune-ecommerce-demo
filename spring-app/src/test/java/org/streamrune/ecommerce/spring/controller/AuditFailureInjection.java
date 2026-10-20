package org.streamrune.ecommerce.spring.controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.streamrune.core.types.CommandId;

/**
 * Makes the audit store refuse the rows of one command id, in the database: while the flag is set,
 * a trigger raises on the {@code audit_log} insert of that command id, so {@code
 * PostgresAuditStore.save} throws. {@code DeadLetterReplayPolicyIT} uses it to show that an
 * operator's decision about a dead-letter entry is not acted on when it cannot be recorded. {@link
 * #close()} drops the trigger, its function and the flag table.
 */
final class AuditFailureInjection implements AutoCloseable {

  private final DataSource dataSource;

  AuditFailureInjection(DataSource dataSource, CommandId commandId) throws SQLException {
    this.dataSource = dataSource;
    execute(
        """
        CREATE TABLE IF NOT EXISTS audit_test_failures (command_id TEXT PRIMARY KEY);
        CREATE OR REPLACE FUNCTION audit_test_fail_row() RETURNS trigger
          LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM audit_test_failures WHERE command_id = NEW.command_id) THEN
            RAISE EXCEPTION 'injected failure: audit insert refused';
          END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS audit_test_fail_row ON audit_log;
        CREATE TRIGGER audit_test_fail_row BEFORE INSERT ON audit_log
          FOR EACH ROW EXECUTE FUNCTION audit_test_fail_row();
        """);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("INSERT INTO audit_test_failures (command_id) VALUES (?)")) {
      ps.setString(1, commandId.value());
      ps.executeUpdate();
      commitUnlessAutoCommit(conn);
    }
  }

  @Override
  public void close() throws SQLException {
    execute(
        """
        DROP TRIGGER IF EXISTS audit_test_fail_row ON audit_log;
        DROP FUNCTION IF EXISTS audit_test_fail_row();
        DROP TABLE IF EXISTS audit_test_failures;
        """);
  }

  private void execute(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
      commitUnlessAutoCommit(conn);
    }
  }

  private static void commitUnlessAutoCommit(Connection conn) throws SQLException {
    if (!conn.getAutoCommit()) {
      conn.commit();
    }
  }
}
