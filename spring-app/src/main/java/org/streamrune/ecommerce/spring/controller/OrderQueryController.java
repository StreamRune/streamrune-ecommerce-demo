package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.OrderView;

/**
 * The order read model, for an authenticated {@code ADMIN} or the customer an order names ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {

  private final OwnerOrAdminQueries queries;

  public OrderQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /**
   * One order. {@code 404} is an {@code ADMIN}'s answer for an id without a row; a customer gets
   * {@code 403} for it, as for another customer's order.
   */
  @GetMapping("/{id}")
  public ResponseEntity<OrderView> getOrder(@PathVariable String id) {
    OrderView order = queries.order(RequestCaller.current(), id);
    return order != null ? ResponseEntity.ok(order) : ResponseEntity.notFound().build();
  }

  /**
   * Every order for an {@code ADMIN}, narrowed to one customer by {@code customerId}; the caller's
   * own orders for anyone else, who may not name another customer.
   */
  @GetMapping
  public List<OrderView> listOrders(@RequestParam(required = false) String customerId) {
    return queries.orders(RequestCaller.current(), customerId);
  }
}
