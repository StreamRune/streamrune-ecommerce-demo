package org.streamrune.ecommerce.commands.product;

import java.util.List;
import java.util.Objects;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;

/**
 * The catalogue as the product streams hold it. Another aggregate's command that needs to know a
 * product, the price of an order line or whether an inventory may be touched, asks here before it
 * is sent: the product is read from the event store, not from the product read model, so a product
 * created a moment ago is known at once. The read and the command that follows are two steps, not
 * one transaction: the catalogue has no command that deletes a product, so a product found here
 * still exists when the command runs.
 */
public final class ProductCatalog {

  private static final int PAGE_SIZE = 100;

  private final EventStore eventStore;
  private final ProductDecider products = new ProductDecider();

  public ProductCatalog(EventStore eventStore) {
    this.eventStore = Objects.requireNonNull(eventStore, "eventStore is required");
  }

  /**
   * The product as its stream holds it, folded from its first event; the decider's initial state,
   * whose {@code productId} is {@code null}, for a product nobody created. This reads and folds the
   * whole stream on each call, so its cost grows with the product's history. A product's stream is
   * small in the demo (its creation, a few price changes, perhaps its discontinuation), so the read
   * is cheap here; a catalogue whose products have long histories would read a snapshot or a read
   * model instead, and accept the lag of the read model.
   */
  public ProductState current(String productId) {
    StreamId stream = StreamId.of(ProductState.TYPE, AggregateId.of(productId));
    ProductState state = products.initialState();
    Version after = Version.initial();
    while (true) {
      List<EventEnvelope> page = eventStore.readStream(stream, after, PAGE_SIZE);
      for (EventEnvelope envelope : page) {
        state = products.evolve(state, (ProductEvent) envelope.event());
        after = envelope.version();
      }
      if (page.size() < PAGE_SIZE) {
        return state;
      }
    }
  }

  /**
   * Refuses a product id nobody created ({@code DomainException}, {@code 400} in the three apps).
   * The inventory endpoints ask this before they send a command, so no inventory stream is opened
   * for a product the catalogue does not hold.
   */
  public void requireExists(String productId) {
    if (current(productId).productId() == null) {
      throw new DomainException("Product not found: " + productId);
    }
  }
}
