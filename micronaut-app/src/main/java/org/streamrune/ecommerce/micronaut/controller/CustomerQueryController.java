package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import java.util.List;
import org.streamrune.ecommerce.projections.access.OwnerOrAdminQueries;
import org.streamrune.ecommerce.queries.dto.CustomerView;

/**
 * The customer read model, for an authenticated {@code ADMIN} or the customer themself ({@link
 * OwnerOrAdminQueries}); {@code 403} for everyone else.
 */
@Controller("/api/customers")
public class CustomerQueryController {

  private final OwnerOrAdminQueries queries;

  public CustomerQueryController(OwnerOrAdminQueries queries) {
    this.queries = queries;
  }

  /** One customer: {@code 404} when the caller may read it and the read model has no row. */
  @Get("/{id}")
  public HttpResponse<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = queries.customer(RequestCaller.current(), id);
    return customer != null ? HttpResponse.ok(customer) : HttpResponse.notFound();
  }

  /** Every customer for an {@code ADMIN}; the caller's own record for anyone else. */
  @Get
  public List<CustomerView> listCustomers() {
    return queries.customers(RequestCaller.current());
  }
}
