package org.streamrune.ecommerce.micronaut;

import io.micronaut.runtime.Micronaut;
import io.micronaut.serde.annotation.SerdeImport;

/**
 * Micronaut entry point. The read-model DTOs live in the framework-agnostic queries module and
 * cannot carry Micronaut annotations, so micronaut-serde learns them via {@link SerdeImport}.
 */
@SerdeImport(org.streamrune.ecommerce.domain.common.Money.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.OrderView.OrderLineView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.ProductView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.OrderView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.CustomerView.class)
@SerdeImport(org.streamrune.ecommerce.queries.dto.InventoryView.class)
public class MicronautEcommerceApplication {
  public static void main(String[] args) {
    Micronaut.run(MicronautEcommerceApplication.class, args);
  }
}
