package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import java.util.List;
import org.streamrune.ecommerce.projections.ProductProjection;
import org.streamrune.ecommerce.queries.dto.ProductView;

@Controller("/api/products")
public class ProductQueryController {

  private final ProductProjection productProjection;

  public ProductQueryController(ProductProjection productProjection) {
    this.productProjection = productProjection;
  }

  @Get("/{id}")
  public HttpResponse<ProductView> getProduct(@PathVariable String id) {
    ProductView product = productProjection.get(id);
    return product != null ? HttpResponse.ok(product) : HttpResponse.notFound();
  }

  @Get
  public List<ProductView> listProducts() {
    return productProjection.listAll();
  }
}
