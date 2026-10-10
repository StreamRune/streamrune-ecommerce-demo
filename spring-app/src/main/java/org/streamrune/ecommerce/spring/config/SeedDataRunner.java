package org.streamrune.ecommerce.spring.config;

import java.math.BigDecimal;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.streamrune.core.Command;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Seeds the demo data at startup: three products with their opening stock, two customers and one
 * order.
 *
 * <p>Seeding is idempotent, one command at a time. A product, a customer and an order are each
 * created by a command its decider refuses when the aggregate already exists, so on a later start
 * those commands are skipped. The opening stock is received only while the product's inventory
 * stream is empty. A start that was interrupted halfway is therefore completed by the next one, and
 * a complete seed is never applied twice.
 */
@Component
public class SeedDataRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SeedDataRunner.class);

  private final VirtualThreadCommandBus commandBus;
  private final EventStore eventStore;

  public SeedDataRunner(VirtualThreadCommandBus commandBus, EventStore eventStore) {
    this.commandBus = commandBus;
    this.eventStore = eventStore;
  }

  @Override
  public void run(ApplicationArguments args) {
    int applied = seed();
    if (applied == 0) {
      LOG.info("Seed data skipped (already exists)");
    } else {
      LOG.info("Demo data seeded ({} commands)", applied);
    }
  }

  /**
   * Dispatches every seed command that has not been applied yet.
   *
   * @return the number of commands applied; 0 when the data was already there
   */
  public int seed() {
    int applied = 0;

    // Products
    applied +=
        dispatch(
            new ProductCommand.CreateProduct(
                "prod-widget",
                "Widget",
                "A versatile widget for all occasions",
                "Electronics",
                new Money(BigDecimal.valueOf(19.99), "USD"),
                100));
    applied +=
        dispatch(
            new ProductCommand.CreateProduct(
                "prod-gadget",
                "Gadget",
                "The latest and greatest gadget",
                "Electronics",
                new Money(BigDecimal.valueOf(49.99), "USD"),
                50));
    applied +=
        dispatch(
            new ProductCommand.CreateProduct(
                "prod-doohickey",
                "Doohickey",
                "Nobody knows what it does, but everybody wants one",
                "Accessories",
                new Money(BigDecimal.valueOf(9.99), "USD"),
                200));

    // Inventory (opening stock, before the seed order so the saga's ReserveStock succeeds)
    applied += receiveOpeningStock("prod-widget", 100);
    applied += receiveOpeningStock("prod-gadget", 50);
    applied += receiveOpeningStock("prod-doohickey", 200);

    // Customers (encrypted PII)
    applied +=
        dispatch(
            new CustomerCommand.RegisterCustomer(
                "cust-alice", "Alice Johnson", "alice@example.com", "123 Main St", "+1-555-0101"));
    applied +=
        dispatch(
            new CustomerCommand.RegisterCustomer(
                "cust-bob", "Bob Smith", "bob@example.com", "456 Oak Ave", "+1-555-0102"));

    // One seed order; the fulfillment saga confirms it
    applied +=
        dispatch(
            new OrderCommand.PlaceOrder(
                "order-seed-1",
                "cust-alice",
                List.of(
                    new OrderCommand.OrderLine(
                        "prod-widget", 2, new Money(BigDecimal.valueOf(19.99), "USD")),
                    new OrderCommand.OrderLine(
                        "prod-gadget", 1, new Money(BigDecimal.valueOf(49.99), "USD")))));

    return applied;
  }

  /**
   * Dispatches one creation command. The decider refuses it with a {@link DomainException} when the
   * aggregate already exists, which is how a later start finds the data seeded.
   */
  private int dispatch(Command command) {
    try {
      commandBus.execute(command);
      return 1;
    } catch (DomainException e) {
      LOG.info("Seed data skipped: {}", e.getMessage());
      return 0;
    } catch (RuntimeException e) {
      LOG.warn("Seed command {} failed: {}", command.getClass().getSimpleName(), e.toString());
      return 0;
    }
  }

  /**
   * Receives a product's opening stock once. {@code ReceiveShipment} adds to the stock every time
   * it runs and no decider rule can tell the opening shipment from a later one, so the runner
   * dispatches it only while the product's inventory stream is still empty.
   */
  private int receiveOpeningStock(String productId, int quantity) {
    try {
      StreamId stream = StreamId.of(InventoryState.TYPE, AggregateId.of(productId));
      if (!eventStore.load(stream).events().isEmpty()) {
        LOG.info("Seed data skipped: inventory already stocked: {}", productId);
        return 0;
      }
      commandBus.execute(new InventoryCommand.ReceiveShipment(productId, quantity));
      return 1;
    } catch (RuntimeException e) {
      LOG.warn("Seed shipment for {} failed: {}", productId, e.toString());
      return 0;
    }
  }
}
