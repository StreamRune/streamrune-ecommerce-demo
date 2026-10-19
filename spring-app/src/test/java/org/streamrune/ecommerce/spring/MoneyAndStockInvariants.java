package org.streamrune.ecommerce.spring;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;

/**
 * Runs {@code scripts/money-and-stock-invariants.sql} over a database, and waits for the database
 * to be quiet. The module directory is the test working directory, so the file is read from {@code
 * ../scripts/}.
 */
final class MoneyAndStockInvariants {

  private static final Path CHECKS = Path.of("../scripts/money-and-stock-invariants.sql");

  private static final String SAGA_TYPE =
      "org.streamrune.ecommerce.commands.saga.OrderFulfillmentState";

  /**
   * How many fulfillment sagas of the prefix's orders have not ended, plus how many of the four
   * order-flow subscriptions have not caught up with the event store: one statement, so that a
   * checkpoint saved after its page and the page's own appends cannot pass the comparison in
   * between.
   */
  private static final String NOT_YET_QUIET =
      """
      SELECT (SELECT count(*) FROM saga_state
               WHERE saga_type = ? AND saga_id LIKE ?
                 AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED'))
           + (SELECT count(*)
                FROM (VALUES ('order-fulfillment-saga'), ('payment-process-manager'),
                             ('order-cancellation'), ('order-shipment')) AS s(name)
                LEFT JOIN projection_offset p ON p.projection_name = s.name
               WHERE coalesce(p.last_offset, 0)
                   < (SELECT coalesce(max(global_offset), 0) FROM event_stream))
      """;

  /**
   * The product's stock from its stream: the available quantity its last stock event recorded, what
   * was received, the open reservations (reserved, less released, less committed) and the committed
   * quantity.
   */
  private static final String STOCK_OF_PRODUCT =
      """
      SELECT (SELECT (payload->>'availableAfter')::int FROM event_stream
               WHERE aggregate_type = 'inventory' AND aggregate_id = ?
                 AND event_type IN ('ShipmentReceived', 'StockReserved', 'StockReleased')
               ORDER BY version DESC LIMIT 1) AS available,
             coalesce(sum((payload->>'quantity')::int)
                      FILTER (WHERE event_type = 'ShipmentReceived'), 0) AS received,
             coalesce(sum((payload->>'quantity')::int)
                      FILTER (WHERE event_type = 'StockReserved'), 0) AS reserved,
             coalesce(sum((payload->>'quantity')::int)
                      FILTER (WHERE event_type = 'StockReleased'), 0) AS released,
             coalesce(sum((payload->>'quantity')::int)
                      FILTER (WHERE event_type = 'ReservationConfirmed'), 0) AS committed
        FROM event_stream
       WHERE aggregate_type = 'inventory' AND aggregate_id = ?
      """;

  private MoneyAndStockInvariants() {}

  /** Every row of the checks whose order id starts with {@code orderIdPrefix}. */
  static List<String> violations(DataSource dataSource, String orderIdPrefix) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      for (String check : checks()) {
        try (ResultSet rs = st.executeQuery(check)) {
          while (rs.next()) {
            String orderId = rs.getString("order_id");
            if (orderId != null && orderId.startsWith(orderIdPrefix)) {
              rows.add(rs.getString("violation") + ": " + orderId + " " + rs.getString("detail"));
            }
          }
        }
      }
    }
    return rows;
  }

  /** The statements of the check file: each ends with a semicolon at the end of a line. */
  static List<String> checks() throws IOException {
    return Arrays.stream(Files.readString(CHECKS).split(";\\s*\\R"))
        .map(String::strip)
        .filter(s -> s.lines().anyMatch(l -> !l.isBlank() && !l.strip().startsWith("--")))
        .toList();
  }

  /**
   * Waits until every saga of the prefix's orders has ended and the four subscriptions are at the
   * head of the event store.
   */
  static void awaitQuiet(DataSource dataSource, String orderIdPrefix, Duration atMost) {
    await()
        .atMost(atMost)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> notYetQuiet(dataSource, orderIdPrefix) == 0);
  }

  private static int notYetQuiet(DataSource dataSource, String orderIdPrefix) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(NOT_YET_QUIET)) {
      ps.setString(1, SAGA_TYPE);
      ps.setString(2, "fulfillment-" + orderIdPrefix + "%");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /**
   * Checks the stock of one product against its stream: the available quantity the last stock event
   * recorded equals what was received, less the open reservations, less the committed quantity.
   * Returns a description of the mismatch, or {@code null} when the stock is conserved.
   */
  static String stockMismatch(DataSource dataSource, String productId) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(STOCK_OF_PRODUCT)) {
      ps.setString(1, productId);
      ps.setString(2, productId);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        int available = rs.getInt("available");
        int received = rs.getInt("received");
        int open = rs.getInt("reserved") - rs.getInt("released") - rs.getInt("committed");
        int committed = rs.getInt("committed");
        if (available == received - open - committed && open >= 0) {
          return null;
        }
        return "product %s: available %d, received %d, open reservations %d, committed %d"
            .formatted(productId, available, received, open, committed);
      }
    }
  }
}
