package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.util.List;
import org.streamrune.ecommerce.projections.OrderProjection;
import org.streamrune.ecommerce.queries.dto.OrderView;

@Controller("/api/orders")
public class OrderQueryController {

  private final OrderProjection orderProjection;

  public OrderQueryController(OrderProjection orderProjection) {
    this.orderProjection = orderProjection;
  }

  @Get("/{id}")
  public HttpResponse<OrderView> getOrder(@PathVariable String id) {
    OrderView order = orderProjection.get(id);
    return order != null ? HttpResponse.ok(order) : HttpResponse.notFound();
  }

  @Get
  public List<OrderView> listOrders(
      @QueryValue(value = "customerId", defaultValue = "") String customerId) {
    if (!customerId.isEmpty()) {
      return orderProjection.listByCustomer(customerId);
    }
    return orderProjection.listAll();
  }
}
