package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

@Controller("/api/audit")
public class AuditController {

  @Inject DataSource dataSource;

  @Get("/commands")
  public List<Map<String, Object>> commandAuditLog(
      @QueryValue(value = "limit", defaultValue = "100") int limit) {
    var results = new ArrayList<Map<String, Object>>();
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT command_id, command_type, aggregate_id, user_id, occurred_at, outcome, "
                    + "error_message, event_count FROM audit_log ORDER BY occurred_at DESC LIMIT ?")) {
      ps.setInt(1, limit);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          results.add(
              Map.of(
                  "commandId", rs.getString("command_id"),
                  "commandType", rs.getString("command_type"),
                  "aggregateId",
                      rs.getString("aggregate_id") != null ? rs.getString("aggregate_id") : "",
                  "userId", rs.getString("user_id") != null ? rs.getString("user_id") : "",
                  "occurredAt", rs.getTimestamp("occurred_at").toInstant().toString(),
                  "outcome", rs.getString("outcome"),
                  "eventCount", rs.getInt("event_count")));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to query audit log", e);
    }
    return results;
  }
}
