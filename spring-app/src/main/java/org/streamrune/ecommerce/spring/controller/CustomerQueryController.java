package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.CustomerView;

/**
 * The customer read model, for an authenticated {@code ADMIN} or the customer themself ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@RestController
@RequestMapping("/api/customers")
public class CustomerQueryController {

  private final OwnerOrAdminQueries queries;

  public CustomerQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /** One customer: {@code 404} when the caller may read it and the read model has no row. */
  @GetMapping("/{id}")
  public ResponseEntity<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = queries.customer(RequestCaller.current(), id);
    return customer != null ? ResponseEntity.ok(customer) : ResponseEntity.notFound().build();
  }

  /** Every customer for an {@code ADMIN}; the caller's own record for anyone else. */
  @GetMapping
  public List<CustomerView> listCustomers() {
    return queries.customers(RequestCaller.current());
  }
}
