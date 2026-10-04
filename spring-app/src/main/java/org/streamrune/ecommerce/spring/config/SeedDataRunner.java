package org.streamrune.ecommerce.spring.config;

import java.math.BigDecimal;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Component
public class SeedDataRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SeedDataRunner.class);

  private final VirtualThreadCommandBus commandBus;

  public SeedDataRunner(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @Override
  public void run(ApplicationArguments args) {
    try {
      LOG.info("Seeding demo data...");

      // Products
      commandBus.execute(
          new ProductCommand.CreateProduct(
              "prod-widget",
              "Widget",
              "A versatile widget for all occasions",
              "Electronics",
              new Money(BigDecimal.valueOf(19.99), "USD"),
              100));
      commandBus.execute(
          new ProductCommand.CreateProduct(
              "prod-gadget",
              "Gadget",
              "The latest and greatest gadget",
              "Electronics",
              new Money(BigDecimal.valueOf(49.99), "USD"),
              50));
      commandBus.execute(
          new ProductCommand.CreateProduct(
              "prod-doohickey",
              "Doohickey",
              "Nobody knows what it does, but everybody wants one",
              "Accessories",
              new Money(BigDecimal.valueOf(9.99), "USD"),
              200));

      // Inventory (initial stock)
      commandBus.execute(new InventoryCommand.ReceiveShipment("prod-widget", 100));
      commandBus.execute(new InventoryCommand.ReceiveShipment("prod-gadget", 50));
      commandBus.execute(new InventoryCommand.ReceiveShipment("prod-doohickey", 200));

      // Customers (encrypted PII)
      commandBus.execute(
          new CustomerCommand.RegisterCustomer(
              "cust-alice", "Alice Johnson", "alice@example.com", "123 Main St", "+1-555-0101"));
      commandBus.execute(
          new CustomerCommand.RegisterCustomer(
              "cust-bob", "Bob Smith", "bob@example.com", "456 Oak Ave", "+1-555-0102"));

      // One completed order with full event history (for snapshot demo)
      commandBus.execute(
          new OrderCommand.PlaceOrder(
              "order-seed-1",
              "cust-alice",
              List.of(
                  new OrderCommand.OrderLine(
                      "prod-widget", 2, new Money(BigDecimal.valueOf(19.99), "USD")),
                  new OrderCommand.OrderLine(
                      "prod-gadget", 1, new Money(BigDecimal.valueOf(49.99), "USD")))));

      LOG.info("Demo data seeded successfully");
    } catch (Exception e) {
      LOG.warn("Seed data skipped (likely already exists): {}", e.getMessage());
    }
  }
}
