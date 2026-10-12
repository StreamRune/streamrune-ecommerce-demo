package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.projections.access.FulfillmentSagaRows;
import org.streamrune.ecommerce.projections.access.FulfillmentScope;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.quarkus.config.PaymentGatewaySimulator;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;

/**
 * The order-fulfillment sagas, for an authenticated {@code ADMIN} or the customer of the order a
 * saga fulfils ({@link OwnerOrAdminQueries}); {@code 403} for everyone else. A saga's state names
 * the order, its customer, the payment and the total of one purchase: an {@code ADMIN} reads every
 * saga, a customer the sagas of their own orders. The payment-failure switch is an {@code ADMIN}'s.
 */
@Path("/api/saga/fulfillments")
@Produces(MediaType.APPLICATION_JSON)
public class SagaController {

  /** The type the saga store records a fulfillment saga under: the list and the single read. */
  private static final SagaType FULFILLMENT = SagaType.fromClass(OrderFulfillmentState.class);

  @Inject SagaStore sagaStore;
  @Inject PaymentGatewaySimulator paymentGateway;
  @Inject DataSource dataSource;
  @Inject OwnerOrAdminQueries queries;

  /** The request context the StreamRune filter parsed from the headers (see RequestCaller). */
  @Inject StreamRuneRequestContextHolder requestContext;

  /**
   * Every fulfillment saga for an {@code ADMIN}; for anyone else the sagas of the caller's own
   * orders, selected by their ids where the rows are read. A request without an identity is
   * refused.
   */
  @GET
  public List<Map<String, Object>> listSagas(@QueryParam("limit") String limit) {
    int rows = RequestChecks.wholeNumber(limit, "limit", 50);
    FulfillmentScope scope = queries.fulfillments(RequestCaller.of(requestContext));
    return FulfillmentSagaRows.read(dataSource, FULFILLMENT, scope, rows);
  }

  /**
   * One saga. {@code 404} is the answer for a saga that does not exist to a caller who may read it:
   * an {@code ADMIN}, or the customer of the order the id names. A customer gets {@code 403} for
   * the saga of an order that is not theirs or has no read-model row, as for the order itself.
   */
  @GET
  @Path("/{id}")
  public Response getSaga(@PathParam("id") String id) {
    queries.requireFulfillmentAccess(RequestCaller.of(requestContext), id);
    return sagaStore
        .load(new SagaId(id), FULFILLMENT, OrderFulfillmentState.class)
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
    OwnerOrAdminAccess.requireAdmin(RequestCaller.of(requestContext));
    boolean current = paymentGateway.isFailureInjected();
    paymentGateway.setFailureInjected(!current);
    return Map.of("failureInjected", !current);
  }
}
