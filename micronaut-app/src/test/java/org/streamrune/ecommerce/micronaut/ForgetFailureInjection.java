package org.streamrune.ecommerce.micronaut;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * Fails one step of the GDPR forget for one customer, in the database rather than in the
 * application: no bean of the running application is replaced. Two triggers refuse a write while a
 * flag for that customer is set:
 *
 * <ul>
 *   <li>{@link #failKeyDeletion}: the key store's tombstone insert raises, so {@code deleteKey}
 *       rolls back its transaction and the subject's key stays.
 *   <li>{@link #failReadModelPurge}: deleting the customer's {@code customers_view} row raises, so
 *       the read-model purger fails while the key is already gone.
 * </ul>
 *
 * <p>A flag holds until the matching {@code allow…} call removes it. {@link #close()} drops the
 * triggers, their functions, the flag table and the copy table.
 */
final class ForgetFailureInjection implements AutoCloseable {

  private final DataSource dataSource;

  ForgetFailureInjection(DataSource dataSource) throws SQLException {
    this.dataSource = dataSource;
    execute(
        """
        CREATE TABLE IF NOT EXISTS forget_test_failures (target TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS forget_test_customers_view_copy (LIKE customers_view);
        CREATE OR REPLACE FUNCTION forget_test_fail_tombstone() RETURNS trigger
          LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM forget_test_failures
                     WHERE target = 'tombstone:' || NEW.subject_id) THEN
            RAISE EXCEPTION 'injected failure: tombstone insert refused';
          END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION forget_test_fail_view_delete() RETURNS trigger
          LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS (SELECT 1 FROM forget_test_failures
                     WHERE target = 'customers_view:' || OLD.id) THEN
            RAISE EXCEPTION 'injected failure: customers_view delete refused';
          END IF;
          RETURN OLD;
        END $$;
        DROP TRIGGER IF EXISTS forget_test_fail_tombstone ON forgotten_subjects;
        CREATE TRIGGER forget_test_fail_tombstone BEFORE INSERT ON forgotten_subjects
          FOR EACH ROW EXECUTE FUNCTION forget_test_fail_tombstone();
        DROP TRIGGER IF EXISTS forget_test_fail_view_delete ON customers_view;
        CREATE TRIGGER forget_test_fail_view_delete BEFORE DELETE ON customers_view
          FOR EACH ROW EXECUTE FUNCTION forget_test_fail_view_delete();
        """);
  }

  /** {@code deleteKey} for the subject with this SHA-256 hex id fails until allowed again. */
  void failKeyDeletion(String subjectHash) throws SQLException {
    update("INSERT INTO forget_test_failures (target) VALUES (?)", "tombstone:" + subjectHash);
  }

  void allowKeyDeletion(String subjectHash) throws SQLException {
    update("DELETE FROM forget_test_failures WHERE target = ?", "tombstone:" + subjectHash);
  }

  /** Deleting this customer's {@code customers_view} row fails until allowed again. */
  void failReadModelPurge(String customerId) throws SQLException {
    update("INSERT INTO forget_test_failures (target) VALUES (?)", "customers_view:" + customerId);
  }

  void allowReadModelPurge(String customerId) throws SQLException {
    update("DELETE FROM forget_test_failures WHERE target = ?", "customers_view:" + customerId);
  }

  /** Keeps a copy of the customer's {@code customers_view} row for {@link #restoreReadModelRow}. */
  void copyReadModelRow(String customerId) throws SQLException {
    update(
        "INSERT INTO forget_test_customers_view_copy SELECT * FROM customers_view WHERE id = ?",
        customerId);
  }

  /**
   * Puts the copied row back, as a read model still holding the customer's data after the
   * projection removed it.
   */
  void restoreReadModelRow(String customerId) throws SQLException {
    update(
        "INSERT INTO customers_view SELECT * FROM forget_test_customers_view_copy WHERE id = ?",
        customerId);
  }

  @Override
  public void close() throws SQLException {
    execute(
        """
        DROP TRIGGER IF EXISTS forget_test_fail_tombstone ON forgotten_subjects;
        DROP TRIGGER IF EXISTS forget_test_fail_view_delete ON customers_view;
        DROP FUNCTION IF EXISTS forget_test_fail_tombstone();
        DROP FUNCTION IF EXISTS forget_test_fail_view_delete();
        DROP TABLE IF EXISTS forget_test_failures;
        DROP TABLE IF EXISTS forget_test_customers_view_copy;
        """);
  }

  private void execute(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
      commitUnlessAutoCommit(conn);
    }
  }

  private void update(String sql, String value) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, value);
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
