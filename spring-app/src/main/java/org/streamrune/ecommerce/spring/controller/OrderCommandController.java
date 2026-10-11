package org.streamrune.ecommerce.spring.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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
@RestController
@RequestMapping("/api/orders")
public class OrderCommandController {

  private final VirtualThreadCommandBus commandBus;
  private final OwnerOrAdminQueries queries;

  public OrderCommandController(VirtualThreadCommandBus commandBus, OwnerOrAdminQueries queries) {
    this.commandBus = commandBus;
    this.queries = queries;
  }

  /** The order's customer is the caller, unless the caller is an {@code ADMIN}. */
  @PostMapping
  public ResponseEntity<Void> placeOrder(@Valid @RequestBody PlaceOrderRequest req) {
    OwnerOrAdminAccess.requireCustomerAccess(RequestCaller.current(), req.customerId());
    List<OrderCommand.OrderLine> lines =
        req.lines().stream()
            .map(
                l ->
                    new OrderCommand.OrderLine(
                        l.productId(), l.quantity(), new Money(l.unitPrice(), "USD")))
            .toList();
    commandBus.execute(new OrderCommand.PlaceOrder(req.orderId(), req.customerId(), lines));
    return ResponseEntity.ok().build();
  }

  @PostMapping("/{id}/confirm")
  public ResponseEntity<Void> confirmOrder(@PathVariable String id) {
    queries.requireOrderAccess(RequestCaller.current(), id);
    commandBus.execute(new OrderCommand.ConfirmOrder(id));
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

  public record OrderLineRequest(
      @NotBlank String productId,
      @NotNull @Positive int quantity,
      @NotNull @Positive BigDecimal unitPrice) {}

  public record CancelOrderRequest(String reason) {}
}
