package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.CustomerProjection;
import org.streamrune.ecommerce.queries.dto.CustomerView;

@RestController
@RequestMapping("/api/customers")
public class CustomerQueryController {

  private final CustomerProjection customerProjection;

  public CustomerQueryController(CustomerProjection customerProjection) {
    this.customerProjection = customerProjection;
  }

  @GetMapping("/{id}")
  public ResponseEntity<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = customerProjection.get(id);
    return customer != null ? ResponseEntity.ok(customer) : ResponseEntity.notFound().build();
  }

  @GetMapping
  public List<CustomerView> listCustomers() {
    return customerProjection.listAll();
  }
}
