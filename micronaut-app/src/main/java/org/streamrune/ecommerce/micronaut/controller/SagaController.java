package org.streamrune.ecommerce.micronaut.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.micronaut.config.PaymentGatewaySimulator;

@Controller("/api/saga/fulfillments")
public class SagaController {

  @Inject SagaStore sagaStore;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject DataSource dataSource;
  @Inject ObjectMapper objectMapper;

  @Get
  public List<Map<String, Object>> listSagas(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
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

  @Get("/{id}")
  public HttpResponse<Map<String, Object>> getSaga(@PathVariable String id) {
    TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
    return sagaStore
        .load(
            new SagaId(id),
            SagaType.fromClass(OrderFulfillmentState.class),
            OrderFulfillmentState.class)
        .map(LoadedSaga::state)
        .map(
            state ->
                HttpResponse.<Map<String, Object>>ok(objectMapper.convertValue(state, typeRef)))
        .orElse(HttpResponse.notFound());
  }

  /**
   * Shorthand for {@code POST /api/admin/payment-failure/toggle}: the saga id is ignored and the
   * single shared {@link PaymentGatewaySimulator} flag is flipped, so the route applies the admin
   * toggle's role check and answers 403 without the ADMIN role.
   */
  @Post("/{id}/inject-failure")
  public Map<String, Boolean> injectFailure(@PathVariable String id) {
    AdminController.requireRole("ADMIN");
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }
}
