package org.streamrune.ecommerce.projections;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.product.*;
import org.streamrune.ecommerce.queries.dto.ProductView;

@org.streamrune.core.ProjectionConfig(
    name = "products",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class ProductProjection extends BaseProjection {

  public ProductProjection(ProjectionRepository repository) {
    super(repository, "products");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      if (envelope.event() instanceof ProductEvent evt) {
        switch (evt) {
          case ProductEvent.ProductCreated e ->
              save(
                  e.productId(),
                  new ProductView(
                      e.productId(),
                      e.name(),
                      e.description(),
                      e.category(),
                      e.price(),
                      e.stock(),
                      ProductState.statusForStock(e.stock()),
                      envelope.metadata().timestamp(),
                      envelope.metadata().timestamp()));
          case ProductEvent.PriceUpdated e ->
              findById(e.productId(), ProductView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new ProductView(
                                  existing.productId(),
                                  existing.name(),
                                  existing.description(),
                                  existing.category(),
                                  e.newPrice(),
                                  existing.stock(),
                                  existing.status(),
                                  existing.createdAt(),
                                  envelope.metadata().timestamp())));
          case ProductEvent.StockAdjusted e ->
              findById(e.productId(), ProductView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new ProductView(
                                  existing.productId(),
                                  existing.name(),
                                  existing.description(),
                                  existing.category(),
                                  existing.price(),
                                  e.newStock(),
                                  ProductState.statusForStock(e.newStock()),
                                  existing.createdAt(),
                                  envelope.metadata().timestamp())));
          case ProductEvent.ProductDiscontinued e ->
              findById(e.productId(), ProductView.class)
                  .ifPresent(
                      existing ->
                          save(
                              e.productId(),
                              new ProductView(
                                  existing.productId(),
                                  existing.name(),
                                  existing.description(),
                                  existing.category(),
                                  existing.price(),
                                  existing.stock(),
                                  ProductStatus.DISCONTINUED,
                                  existing.createdAt(),
                                  envelope.metadata().timestamp())));
        }
      }
    }
  }

  public ProductView get(String productId) {
    return findById(productId, ProductView.class).orElse(null);
  }

  public List<ProductView> listAll() {
    return repository.findAll(projectionName(), ProductView.class);
  }
}
