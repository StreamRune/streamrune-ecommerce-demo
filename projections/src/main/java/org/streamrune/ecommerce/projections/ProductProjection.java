package org.streamrune.ecommerce.projections;

import java.time.Instant;
import java.util.List;
import java.util.function.UnaryOperator;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.ecommerce.domain.inventory.InventoryEvent;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductStatus;
import org.streamrune.ecommerce.queries.dto.Availability;
import org.streamrune.ecommerce.queries.dto.ProductView;

/**
 * The storefront's product view, built from two streams of each product: the catalogue's {@code
 * product:<id>} gives the name, the price and the status, the inventory's {@code inventory:<id>}
 * the units available to order. A read model may combine as many streams as its question needs; the
 * aggregates stay apart.
 *
 * <p>Every inventory event that changes the available units carries the count after it ({@code
 * availableAfter}), so the view derives the availability from that count as it is and keeps the
 * availability alone: the storefront shows no unit count. A product is {@link
 * Availability#LOW_STOCK} below {@link #LOW_STOCK_THRESHOLD} units, {@link
 * Availability#OUT_OF_STOCK} at none, and {@link Availability#DISCONTINUED} once the catalogue
 * discontinues it, whatever it holds. A refused reservation changes nothing.
 */
@org.streamrune.core.ProjectionConfig(
    name = "products",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class ProductProjection extends BaseProjection {

  /** Below this many available units the storefront shows a product as low in stock. */
  public static final int LOW_STOCK_THRESHOLD = 10;

  public ProductProjection(ProjectionRepository repository) {
    super(repository, "products");
  }

  @Override
  public void process(List<EventEnvelope> events) {
    for (var envelope : events) {
      Instant at = envelope.metadata().timestamp();
      switch (envelope.event()) {
        case ProductEvent.ProductCreated e ->
            save(
                e.productId(),
                new ProductView(
                    e.productId(),
                    e.name(),
                    e.description(),
                    e.category(),
                    e.price(),
                    ProductStatus.AVAILABLE,
                    availability(ProductStatus.AVAILABLE, 0),
                    at,
                    at));
        case ProductEvent.PriceUpdated e ->
            update(
                e.productId(),
                p ->
                    new ProductView(
                        p.productId(),
                        p.name(),
                        p.description(),
                        p.category(),
                        e.newPrice(),
                        p.status(),
                        p.availability(),
                        p.createdAt(),
                        at));
        case ProductEvent.ProductDiscontinued e ->
            update(
                e.productId(),
                p ->
                    new ProductView(
                        p.productId(),
                        p.name(),
                        p.description(),
                        p.category(),
                        p.price(),
                        ProductStatus.DISCONTINUED,
                        Availability.DISCONTINUED,
                        p.createdAt(),
                        at));
        case InventoryEvent.ShipmentReceived e -> available(e.productId(), e.availableAfter(), at);
        case InventoryEvent.InventoryAdjusted e -> available(e.productId(), e.availableAfter(), at);
        case InventoryEvent.StockReserved e -> available(e.productId(), e.availableAfter(), at);
        case InventoryEvent.StockReleased e -> available(e.productId(), e.availableAfter(), at);
        default -> {}
      }
    }
  }

  /** The storefront's availability of a product with this status and this many units. */
  static Availability availability(ProductStatus status, int available) {
    if (status == ProductStatus.DISCONTINUED) {
      return Availability.DISCONTINUED;
    }
    if (available <= 0) {
      return Availability.OUT_OF_STOCK;
    }
    if (available < LOW_STOCK_THRESHOLD) {
      return Availability.LOW_STOCK;
    }
    return Availability.IN_STOCK;
  }

  private void available(String productId, int available, Instant at) {
    update(
        productId,
        p ->
            new ProductView(
                p.productId(),
                p.name(),
                p.description(),
                p.category(),
                p.price(),
                p.status(),
                availability(p.status(), available),
                p.createdAt(),
                at));
  }

  /**
   * Rewrites the view of a product the projection has seen created, as {@code InventoryProjection}
   * does. The inventory endpoints refuse a product the catalogue does not hold, so its {@code
   * ProductCreated} comes first in the global stream.
   */
  private void update(String productId, UnaryOperator<ProductView> change) {
    findById(productId, ProductView.class).ifPresent(p -> save(productId, change.apply(p)));
  }

  public ProductView get(String productId) {
    return findById(productId, ProductView.class).orElse(null);
  }

  public List<ProductView> listAll() {
    return repository.findAll(projectionName(), ProductView.class);
  }
}
