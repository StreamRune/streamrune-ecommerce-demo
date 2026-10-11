package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.math.BigDecimal;
import java.util.List;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.access.OwnerOrAdminAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The order commands. An order is placed, confirmed and cancelled by an authenticated {@code ADMIN}
 * or by the customer the order names ({@link OwnerOrAdminAccess}); {@code 403} for everyone else.
 * Shipping and delivering are for the {@code ADMIN} role, which {@code @RequireRole} on the two
 * commands enforces in the command bus.
 */
@Controller("/api/orders")
public class OrderCommandController {

  private final VirtualThreadCommandBus commandBus;
  private final OwnerOrAdminQueries queries;

  public OrderCommandController(VirtualThreadCommandBus commandBus, OwnerOrAdminQueries queries) {
    this.commandBus = commandBus;
    this.queries = queries;
  }

  /** The order's customer is the caller, unless the caller is an {@code ADMIN}. */
  @Post
  public HttpResponse<Void> createOrder(@Body CreateOrderRequest req) {
    requireWellFormed(req);
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    List<OrderCommand.OrderLine> lines =
        req.lines().stream()
            .map(
                l ->
                    new OrderCommand.OrderLine(
                        l.productId(), l.quantity(), new Money(l.unitPrice(), "USD")))
            .toList();
    commandBus.execute(new OrderCommand.PlaceOrder(req.orderId(), req.customerId(), lines));
    return HttpResponse.ok();
  }

  @Post("/{id}/confirm")
  public HttpResponse<Void> confirmOrder(@PathVariable String id) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.ConfirmOrder(id));
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
      RequestChecks.positive(line.unitPrice(), "lines.unitPrice");
    }
  }

  @io.micronaut.serde.annotation.Serdeable
  public record CreateOrderRequest(
      String orderId, String customerId, List<OrderLineRequest> lines) {}

  @io.micronaut.serde.annotation.Serdeable
  public record OrderLineRequest(String productId, int quantity, BigDecimal unitPrice) {}

  @io.micronaut.serde.annotation.Serdeable
  public record CancelOrderRequest(String reason) {}
}
