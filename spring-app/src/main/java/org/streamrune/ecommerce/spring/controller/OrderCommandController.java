package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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
@RestController
@RequestMapping("/api/orders")
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
  @PostMapping
  public ResponseEntity<Void> placeOrder(@Valid @RequestBody PlaceOrderRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    commandBus.execute(
        orderPricing.price(
            req.orderId(),
            req.customerId(),
            req.lines().stream()
                .map(l -> new OrderPricing.RequestedLine(l.productId(), l.quantity()))
                .toList()));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/ship")
  public ResponseEntity<Void> shipOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.ShipOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/deliver")
  public ResponseEntity<Void> deliverOrder(@PathVariable String id) {
    commandBus.execute(new OrderCommand.DeliverOrder(id));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/cancel")
  public ResponseEntity<Void> cancelOrder(
      @PathVariable String id, @Valid @RequestBody CancelOrderRequest req) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.CancelOrder(id, req.reason()));
    return ResponseEntity.ok().build();
  }

  public record PlaceOrderRequest(
      @NotBlank String orderId,
      @NotBlank String customerId,
      @NotEmpty List<@Valid OrderLineRequest> lines) {}

  /** One line as a client asks for it: a product and a quantity; the catalogue sets the price. */
  public record OrderLineRequest(@NotBlank String productId, @Positive int quantity) {}

  public record CancelOrderRequest(String reason) {}
}
