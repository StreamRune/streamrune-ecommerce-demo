package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import java.util.List;
import org.streamrune.ecommerce.micronaut.config.CustomerViewProjection;
import org.streamrune.ecommerce.queries.dto.CustomerView;

@Controller("/api/customers")
public class CustomerQueryController {

  private final CustomerViewProjection customerProjection;

  public CustomerQueryController(CustomerViewProjection customerProjection) {
    this.customerProjection = customerProjection;
  }

  @Get("/{id}")
  public HttpResponse<CustomerView> getCustomer(@PathVariable String id) {
    CustomerView customer = customerProjection.get(id);
    return customer != null ? HttpResponse.ok(customer) : HttpResponse.notFound();
  }

  @Get
  public List<CustomerView> listCustomers() {
    return customerProjection.listAll();
  }
}
