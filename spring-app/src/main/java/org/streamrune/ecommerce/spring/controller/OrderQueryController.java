package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.dto.OrderView;

@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {

  private final OrderProjection orderProjection;

  public OrderQueryController(OrderProjection orderProjection) {
    this.orderProjection = orderProjection;
  }

  @GetMapping("/{id}")
  public ResponseEntity<OrderView> getOrder(@PathVariable String id) {
    OrderView order = orderProjection.get(id);
    return order != null ? ResponseEntity.ok(order) : ResponseEntity.notFound().build();
  }

  @GetMapping
  public List<OrderView> listOrders(@RequestParam(required = false) String customerId) {
    if (customerId != null) {
      return orderProjection.listByCustomer(customerId);
    }
    return orderProjection.listAll();
  }
}
