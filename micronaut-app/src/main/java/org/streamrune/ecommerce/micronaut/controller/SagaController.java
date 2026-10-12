package org.streamrune.ecommerce.micronaut.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.micronaut.config.PaymentGatewaySimulator;
import org.streamrune.ecommerce.projections.access.FulfillmentSagaRows;
import org.streamrune.ecommerce.projections.access.FulfillmentScope;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;

/**
 * The order-fulfillment sagas, for an authenticated {@code ADMIN} or the customer of the order a
 * saga fulfils ({@link OwnerOrAdminQueries}); {@code 403} for everyone else. A saga's state names
 * the order, its customer, the payment and the total of one purchase: an {@code ADMIN} reads every
 * saga, a customer the sagas of their own orders. The payment-failure switch is an {@code ADMIN}'s.
 */
@Controller("/api/saga/fulfillments")
public class SagaController {

  /** The type the saga store records a fulfillment saga under: the list and the single read. */
  private static final SagaType FULFILLMENT = SagaType.fromClass(OrderFulfillmentState.class);

  @Inject SagaStore sagaStore;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject DataSource dataSource;
  @Inject OwnerOrAdminQueries queries;
  @Inject ObjectMapper objectMapper;

  /**
   * Every fulfillment saga for an {@code ADMIN}; for anyone else the sagas of the caller's own
   * orders, selected by their ids where the rows are read. A request without an identity is
   * refused.
   */
  @Get
  public List<Map<String, Object>> listSagas(
      @QueryValue(value = "limit", defaultValue = "50") int limit) {
    FulfillmentScope scope = queries.fulfillments(RequestCaller.current());
    return FulfillmentSagaRows.read(dataSource, FULFILLMENT, scope, limit);
  }

  /**
   * One saga. {@code 404} is the answer for a saga that does not exist to a caller who may read it:
   * an {@code ADMIN}, or the customer of the order the id names. A customer gets {@code 403} for
   * the saga of an order that is not theirs or has no read-model row, as for the order itself.
   */
  @Get("/{id}")
  public HttpResponse<Map<String, Object>> getSaga(@PathVariable String id) {
    queries.requireFulfillmentAccess(RequestCaller.current(), id);
    TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
    return sagaStore
        .load(new SagaId(id), FULFILLMENT, OrderFulfillmentState.class)
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
    OwnerOrAdminAccess.requireAdmin(RequestCaller.current());
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }
}
