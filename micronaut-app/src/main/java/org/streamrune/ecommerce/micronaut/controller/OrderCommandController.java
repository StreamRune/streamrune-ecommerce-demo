package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.math.BigDecimal;
import java.util.List;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Controller("/api/orders")
public class OrderCommandController {

  private final VirtualThreadCommandBus commandBus;

  public OrderCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @Post
  public HttpResponse<Void> createOrder(@Body CreateOrderRequest req) {
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
    commandBus.execute(new OrderCommand.CancelOrder(id, req.reason()));
    return HttpResponse.ok();
  }

  @io.micronaut.serde.annotation.Serdeable
  public record CreateOrderRequest(
      String orderId, String customerId, List<OrderLineRequest> lines) {}

  @io.micronaut.serde.annotation.Serdeable
  public record OrderLineRequest(String productId, int quantity, BigDecimal unitPrice) {}

  @io.micronaut.serde.annotation.Serdeable
  public record CancelOrderRequest(String reason) {}
}
