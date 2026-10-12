package org.streamrune.ecommerce.spring.controller;

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
import org.streamrune.ecommerce.projections.access.FulfillmentSagaRows;
import org.streamrune.ecommerce.projections.access.FulfillmentScope;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;

/**
 * The order-fulfillment sagas, for an authenticated {@code ADMIN} or the customer of the order a
 * saga fulfils ({@link OwnerOrAdminQueries}); {@code 403} for everyone else. A saga's state names
 * the order, its customer, the payment and the total of one purchase: an {@code ADMIN} reads every
 * saga, a customer the sagas of their own orders. The payment-failure switch is an {@code ADMIN}'s.
 */
@RestController
@RequestMapping("/api/saga/fulfillments")
public class SagaController {

  /** The type the saga store records a fulfillment saga under: the list and the single read. */
  private static final SagaType FULFILLMENT = SagaType.fromClass(OrderFulfillmentState.class);

  private final SagaStore sagaStore;
  private final PaymentGatewaySimulator paymentGateway;
  private final DataSource dataSource;
  private final OwnerOrAdminQueries queries;

  public SagaController(
      SagaStore sagaStore,
      PaymentGatewaySimulator paymentGateway,
      DataSource dataSource,
      OwnerOrAdminQueries queries) {
    this.sagaStore = sagaStore;
    this.paymentGateway = paymentGateway;
    this.dataSource = dataSource;
    this.queries = queries;
  }

  /**
   * Every fulfillment saga for an {@code ADMIN}; for anyone else the sagas of the caller's own
   * orders, selected by their ids where the rows are read. A request without an identity is
   * refused.
   */
  @GetMapping
  public List<Map<String, Object>> listSagas(@RequestParam(defaultValue = "50") int limit) {
    FulfillmentScope scope = queries.fulfillments(RequestCaller.current());
    return FulfillmentSagaRows.read(dataSource, FULFILLMENT, scope, limit);
  }

  /**
   * One saga. {@code 404} is the answer for a saga that does not exist to a caller who may read it:
   * an {@code ADMIN}, or the customer of the order the id names. A customer gets {@code 403} for
   * the saga of an order that is not theirs or has no read-model row, as for the order itself.
   */
  @GetMapping("/{id}")
  public ResponseEntity<OrderFulfillmentState> getSaga(@PathVariable String id) {
    queries.requireFulfillmentAccess(RequestCaller.current(), id);
    return sagaStore
        .load(new SagaId(id), FULFILLMENT, OrderFulfillmentState.class)
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
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }
}
