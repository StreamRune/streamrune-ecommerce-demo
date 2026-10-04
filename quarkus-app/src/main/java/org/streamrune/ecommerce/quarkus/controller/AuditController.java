package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

@Path("/api/audit")
@Produces(MediaType.APPLICATION_JSON)
public class AuditController {

  @Inject DataSource dataSource;

  @GET
  @Path("/commands")
  public List<Map<String, Object>> commandAuditLog(
      @QueryParam("limit") @DefaultValue("100") int limit) {
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
