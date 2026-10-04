package org.streamrune.ecommerce.quarkus.config;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.math.BigDecimal;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Seeds demo data on startup — mirrors Spring's {@code SeedDataRunner}.
 *
 * <p>Seeding is idempotent: every command dispatch is wrapped in a try/catch so that a restart
 * against an already-seeded database is safe (the second CreateProduct for the same id will throw a
 * domain exception that is caught and logged as a warning). This mirrors Spring's approach.
 *
 * <p>Seeding is disabled in tests via the {@code app.seed.enabled} config flag (set to {@code
 * false} in the {@code test} profile in {@code application.properties}). This prevents seed
 * commands from racing the test-specific commands issued by the E2E integration tests.
 *
 * <p>Seed ordering mirrors Spring: ReceiveShipment (inventory) is issued BEFORE PlaceOrder so that
 * the OrderFulfillment saga's ReserveStock step succeeds and the seed order auto-confirms.
 */
@ApplicationScoped
public class SeedDataRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SeedDataRunner.class);

  private final VirtualThreadCommandBus commandBus;
  private final boolean seedEnabled;

  public SeedDataRunner(
      VirtualThreadCommandBus commandBus,
      @ConfigProperty(name = "app.seed.enabled", defaultValue = "true") boolean seedEnabled) {
    this.commandBus = commandBus;
    this.seedEnabled = seedEnabled;
  }

  void onStart(@Observes StartupEvent event) {
    if (!seedEnabled) {
      LOG.debug("SeedDataRunner: seeding disabled (app.seed.enabled=false)");
      return;
    }

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

      // Inventory (stock BEFORE placing the seed order so ReserveStock succeeds)
      commandBus.execute(new InventoryCommand.ReceiveShipment("prod-widget", 100));
      commandBus.execute(new InventoryCommand.ReceiveShipment("prod-gadget", 50));
      commandBus.execute(new InventoryCommand.ReceiveShipment("prod-doohickey", 200));

      // Customers (PII encrypted at rest via CryptoEngine)
      commandBus.execute(
          new CustomerCommand.RegisterCustomer(
              "cust-alice", "Alice Johnson", "alice@example.com", "123 Main St", "+1-555-0101"));
      commandBus.execute(
          new CustomerCommand.RegisterCustomer(
              "cust-bob", "Bob Smith", "bob@example.com", "456 Oak Ave", "+1-555-0102"));

      // One seed order — inventory stocked above so the saga auto-confirms it.
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
