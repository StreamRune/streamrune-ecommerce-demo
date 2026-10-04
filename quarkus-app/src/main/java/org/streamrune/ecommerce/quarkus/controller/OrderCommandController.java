package org.streamrune.ecommerce.quarkus.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.util.List;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.quarkus.StreamRuneRequestContextHolder;
import org.streamrune.quarkus.StreamRuneRequestFilter;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderCommandController {

  @Inject VirtualThreadCommandBus commandBus;
  @Inject StreamRuneRequestContextHolder requestContext;

  /**
   * Executes a command with the request's {@link org.streamrune.core.StreamRuneContext} bound to
   * the {@code ScopedValue}. JAX-RS filters cannot wrap the resource-method invocation, so the
   * {@code StreamRuneRequestFilter} only stashes the parsed context in a request-scoped holder —
   * without this explicit bind, the authorization interceptor sees no user/role and an
   * {@code @RequireRole} command (ship/deliver need {@code ADMIN}) fails closed regardless of the
   * {@code X-User-Role} header. Binding here also lets the audit trail record the real {@code
   * X-User-Id} instead of {@code GUEST}. The Micronaut and Spring integrations bind the context in
   * their server filters, so this mirrors their behavior.
   */
  private void executeInContext(OrderCommand command) {
    StreamRuneRequestFilter.withContext(
        requestContext != null ? requestContext.context() : null,
        () -> commandBus.execute(command));
  }

  @POST
  public Response createOrder(CreateOrderRequest req) {
    List<OrderCommand.OrderLine> lines =
        req.lines().stream()
            .map(
                l ->
                    new OrderCommand.OrderLine(
                        l.productId(), l.quantity(), new Money(l.unitPrice(), "USD")))
            .toList();
    executeInContext(new OrderCommand.PlaceOrder(req.orderId(), req.customerId(), lines));
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/confirm")
  public Response confirmOrder(@PathParam("id") String id) {
    executeInContext(new OrderCommand.ConfirmOrder(id));
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/ship")
  public Response shipOrder(@PathParam("id") String id) {
    executeInContext(new OrderCommand.ShipOrder(id));
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/deliver")
  public Response deliverOrder(@PathParam("id") String id) {
    executeInContext(new OrderCommand.DeliverOrder(id));
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/cancel")
  public Response cancelOrder(@PathParam("id") String id, CancelOrderRequest req) {
    executeInContext(new OrderCommand.CancelOrder(id, req.reason()));
    return Response.ok().build();
  }

  public record CreateOrderRequest(
      String orderId, String customerId, List<OrderLineRequest> lines) {}

  public record OrderLineRequest(String productId, int quantity, BigDecimal unitPrice) {}

  public record CancelOrderRequest(String reason) {}
}
