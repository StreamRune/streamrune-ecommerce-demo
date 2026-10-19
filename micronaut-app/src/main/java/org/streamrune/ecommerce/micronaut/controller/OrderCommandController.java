package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.util.List;
import org.streamrune.ecommerce.commands.order.OrderPricing;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The order commands. An order is placed and cancelled by an authenticated {@code ADMIN} or by the
 * customer the order names ({@link OwnerOrAdminAccess}); {@code 403} for everyone else. The price
 * of each line comes from the product catalogue ({@link OrderPricing}). The fulfillment saga
 * confirms an order; there is no endpoint for it. Shipping and delivering are for the {@code ADMIN}
 * role, which {@code @RequireRole} on the two commands enforces in the command bus. A shipped or
 * delivered order is not cancelled ({@code 400}).
 */
@Controller("/api/orders")
public class OrderCommandController {

  private final VirtualThreadCommandBus commandBus;
  private final OwnerOrAdminQueries queries;
  private final OrderPricing orderPricing;

  public OrderCommandController(
      VirtualThreadCommandBus commandBus, OwnerOrAdminQueries queries, OrderPricing orderPricing) {
    this.commandBus = commandBus;
    this.queries = queries;
    this.orderPricing = orderPricing;
  }

  /**
   * The order's customer is the caller, unless the caller is an {@code ADMIN}. A product nobody
   * created, or one that is discontinued, answers {@code 400}.
   */
  @Post
  public HttpResponse<Void> createOrder(@Body CreateOrderRequest req) {
    requireWellFormed(req);
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    commandBus.execute(
        orderPricing.price(
            req.orderId(),
            req.customerId(),
            req.lines().stream()
                .map(l -> new OrderPricing.RequestedLine(l.productId(), l.quantity()))
                .toList()));
    return HttpResponse.ok();
  }

  @Post("/{id}/ship")
  public HttpResponse<Void> shipOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.ShipOrder(id));
    return HttpResponse.ok();
  }

  @Post("/{id}/deliver")
  public HttpResponse<Void> deliverOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.DeliverOrder(id));
    return HttpResponse.ok();
  }

  @Post("/{id}/cancel")
  public HttpResponse<Void> cancelOrder(@PathVariable String id, @Body CancelOrderRequest req) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.CancelOrder(id, req.reason()));
    return HttpResponse.ok();
  }

  /** The constraints of the Spring app's {@code PlaceOrderRequest}; {@code 400} when one fails. */
  private static void requireWellFormed(CreateOrderRequest req) {
    RequestChecks.notBlank(req.orderId(), "orderId");
    RequestChecks.notBlank(req.customerId(), "customerId");
    RequestChecks.notEmpty(req.lines(), "lines");
    for (OrderLineRequest line : req.lines()) {
      if (line == null) {
        throw new IllegalArgumentException("lines must not hold null");
      }
      RequestChecks.notBlank(line.productId(), "lines.productId");
      RequestChecks.positive(line.quantity(), "lines.quantity");
    }
  }

  @io.micronaut.serde.annotation.Serdeable
  public record CreateOrderRequest(
      String orderId, String customerId, List<OrderLineRequest> lines) {}

  /** One line as a client asks for it: a product and a quantity; the catalogue sets the price. */
  @io.micronaut.serde.annotation.Serdeable
  public record OrderLineRequest(String productId, int quantity) {}

  @io.micronaut.serde.annotation.Serdeable
  public record CancelOrderRequest(String reason) {}
}
