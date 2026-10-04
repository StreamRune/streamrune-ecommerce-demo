package org.streamrune.ecommerce.spring.controller;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.streamrune.core.QueryBus;
import org.streamrune.ecommerce.projections.ProductProjection;
import org.streamrune.ecommerce.queries.dto.ProductView;
import org.streamrune.ecommerce.queries.query.ListProducts;

@RestController
@RequestMapping("/api/products")
public class ProductQueryController {

  private final ProductProjection productProjection;
  private final QueryBus queryBus;

  public ProductQueryController(ProductProjection productProjection, QueryBus queryBus) {
    this.productProjection = productProjection;
    this.queryBus = queryBus;
  }

  @GetMapping("/{id}")
  public ResponseEntity<ProductView> getProduct(@PathVariable String id) {
    ProductView product = productProjection.get(id);
    return product != null ? ResponseEntity.ok(product) : ResponseEntity.notFound().build();
  }

  @GetMapping
  public List<ProductView> listProducts() {
    return queryBus.dispatch(new ListProducts());
  }
}
