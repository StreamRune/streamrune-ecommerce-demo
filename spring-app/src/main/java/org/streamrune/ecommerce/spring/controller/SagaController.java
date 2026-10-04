package org.streamrune.ecommerce.spring.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;

@RestController
@RequestMapping("/api/saga/fulfillments")
public class SagaController {

  private final SagaStore sagaStore;
  private final PaymentGatewaySimulator paymentGateway;
  private final DataSource dataSource;

  public SagaController(
      SagaStore sagaStore, PaymentGatewaySimulator paymentGateway, DataSource dataSource) {
    this.sagaStore = sagaStore;
    this.paymentGateway = paymentGateway;
    this.dataSource = dataSource;
  }

  @GetMapping
  public List<Map<String, Object>> listSagas(@RequestParam(defaultValue = "50") int limit) {
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

  @GetMapping("/{id}")
  public ResponseEntity<OrderFulfillmentState> getSaga(@PathVariable String id) {
    return sagaStore
        .load(
            new SagaId(id),
            SagaType.fromClass(OrderFulfillmentState.class),
            OrderFulfillmentState.class)
        .map(LoadedSaga::state)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  /**
   * Shorthand for {@code POST /api/admin/payment-failure/toggle}: the saga id is ignored and the
   * single shared {@link PaymentGatewaySimulator} flag is flipped, so the route applies the admin
   * toggle's role check and answers 403 without the ADMIN role.
   */
  @PostMapping("/{id}/inject-failure")
  public Map<String, Boolean> injectFailure(@PathVariable String id) {
    AdminController.requireRole("ADMIN");
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }
}
