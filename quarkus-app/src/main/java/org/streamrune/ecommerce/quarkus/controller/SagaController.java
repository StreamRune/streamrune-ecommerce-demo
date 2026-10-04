package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.quarkus.config.PaymentGatewaySimulator;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

@Path("/api/saga/fulfillments")
@Produces(MediaType.APPLICATION_JSON)
public class SagaController {

  @Inject SagaStore sagaStore;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject DataSource dataSource;

  /** The request context the StreamRune filter parsed from the headers (see AdminController). */
  @Inject StreamRuneRequestContextHolder requestContext;

  @GET
  public List<Map<String, Object>> listSagas(@QueryParam("limit") @DefaultValue("50") int limit) {
    var results = new ArrayList<Map<String, Object>>();
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT saga_id, saga_type, status, state_json FROM saga_state ORDER BY saga_id DESC LIMIT ?")) {
      ps.setInt(1, limit);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          results.add(
              Map.of(
                  "sagaId", rs.getString("saga_id"),
                  "sagaType", rs.getString("saga_type"),
                  "status", rs.getString("status"),
                  "state", rs.getString("state_json") != null ? rs.getString("state_json") : ""));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to query saga state", e);
    }
    return results;
  }

  @GET
  @Path("/{id}")
  public Response getSaga(@PathParam("id") String id) {
    return sagaStore
        .load(
            new SagaId(id),
            SagaType.fromClass(OrderFulfillmentState.class),
            OrderFulfillmentState.class)
        .map(LoadedSaga::state)
        .map(state -> Response.ok(state).build())
        .orElse(Response.status(Response.Status.NOT_FOUND).build());
  }

  /**
   * Shorthand for {@code POST /api/admin/payment-failure/toggle}: the saga id is ignored and the
   * single shared {@link PaymentGatewaySimulator} flag is flipped, so the route applies the admin
   * toggle's role check and answers 403 without the ADMIN role.
   */
  @POST
  @Path("/{id}/inject-failure")
  public Map<String, Boolean> injectFailure(@PathParam("id") String id) {
    AdminController.requireRole(requestContext, "ADMIN");
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }
}
