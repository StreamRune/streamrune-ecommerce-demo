package org.streamrune.ecommerce.projections.access;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * The saga list of {@code GET /api/saga/fulfillments}, read from the framework's {@code saga_state}
 * table for one {@link FulfillmentScope}. The three applications answer the list from here, so a
 * caller gets the same rows from each of them.
 *
 * <p>The scope is applied in the statement: a caller who is not an {@code ADMIN} gets the rows
 * whose id is one of the scope's saga ids, selected by the database, and no other row is read.
 *
 * <p>A row is the saga's id, its type, the framework's {@code SagaStatus} and the saga's state as
 * the JSON the store holds. For a fulfillment saga that state names the order, its customer, the
 * payment id derived from the order id, the order's lines and total and the fulfillment status:
 * everything in it describes one order of one customer, the customer who may read the row.
 */
public final class FulfillmentSagaRows {

  private static final String EVERY_SAGA =
      "SELECT saga_id, saga_type, status, state_json FROM saga_state"
          + " ORDER BY saga_id DESC LIMIT ?";

  private static final String SAGAS_WITH_IDS =
      "SELECT saga_id, saga_type, status, state_json FROM saga_state"
          + " WHERE saga_id = ANY (?) ORDER BY saga_id DESC LIMIT ?";

  private FulfillmentSagaRows() {}

  /**
   * The rows of the scope, the highest saga id first.
   *
   * @param limit the most rows to return
   * @throws IllegalArgumentException for a negative {@code limit}
   */
  public static List<Map<String, Object>> read(
      DataSource dataSource, FulfillmentScope scope, int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    List<Map<String, Object>> rows = new ArrayList<>();
    try (var connection = dataSource.getConnection();
        var statement =
            connection.prepareStatement(scope.everySaga() ? EVERY_SAGA : SAGAS_WITH_IDS)) {
      if (scope.everySaga()) {
        statement.setInt(1, limit);
      } else {
        statement.setArray(1, connection.createArrayOf("varchar", scope.sagaIds().toArray()));
        statement.setInt(2, limit);
      }
      try (var result = statement.executeQuery()) {
        while (result.next()) {
          String state = result.getString("state_json");
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("sagaId", result.getString("saga_id"));
          row.put("sagaType", result.getString("saga_type"));
          row.put("status", result.getString("status"));
          row.put("state", state != null ? state : "");
          rows.add(row);
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to query saga state", e);
    }
    return rows;
  }
}
