package org.streamrune.ecommerce.micronaut.config;

import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.math.BigDecimal;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.customer.CustomerCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.order.OrderCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Seeds demo data on startup — mirrors Spring's and Quarkus's {@code SeedDataRunner}.
 *
 * <p>Seeding is idempotent: every command dispatch is wrapped in a try/catch so that a restart
 * against an already-seeded database is safe (a second CreateProduct for the same id will throw a
 * domain exception that is caught and logged as a warning).
 *
 * <p>Seeding is disabled in tests via the {@code app.seed.enabled} property (set to {@code false}
 * in {@code application-test.yml}). This prevents seed commands from racing the test-specific
 * commands issued by the E2E integration tests.
 *
 * <p>Seed ordering mirrors Spring: ReceiveShipment (inventory) is issued BEFORE PlaceOrder so that
 * the OrderFulfillment saga's ReserveStock step succeeds and the seed order auto-confirms.
 */
@Singleton
public class SeedDataRunner implements ApplicationEventListener<StartupEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SeedDataRunner.class);

  private final VirtualThreadCommandBus commandBus;
  private final boolean seedEnabled;

  public SeedDataRunner(
      VirtualThreadCommandBus commandBus, @Value("${app.seed.enabled:true}") boolean seedEnabled) {
    this.commandBus = commandBus;
    this.seedEnabled = seedEnabled;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (!seedEnabled) {
      LOG.debug("SeedDataRunner: seeding disabled (app.seed.enabled=false)");
      return;
    }

    try {
      LOG.info("Seeding demo data...");

      // Products
      dispatch(
          new ProductCommand.CreateProduct(
              "prod-widget",
              "Widget",
              "A versatile widget for all occasions",
              "Electronics",
              new Money(BigDecimal.valueOf(19.99), "USD"),
              100));
      dispatch(
          new ProductCommand.CreateProduct(
              "prod-gadget",
              "Gadget",
              "The latest and greatest gadget",
              "Electronics",
              new Money(BigDecimal.valueOf(49.99), "USD"),
              50));
      dispatch(
          new ProductCommand.CreateProduct(
              "prod-doohickey",
              "Doohickey",
              "Nobody knows what it does, but everybody wants one",
              "Accessories",
              new Money(BigDecimal.valueOf(9.99), "USD"),
              200));

      // Inventory (stock BEFORE placing the seed order so ReserveStock succeeds)
      dispatch(new InventoryCommand.ReceiveShipment("prod-widget", 100));
      dispatch(new InventoryCommand.ReceiveShipment("prod-gadget", 50));
      dispatch(new InventoryCommand.ReceiveShipment("prod-doohickey", 200));

      // Customers (PII encrypted at rest via CryptoEngine)
      dispatch(
          new CustomerCommand.RegisterCustomer(
              "cust-alice", "Alice Johnson", "alice@example.com", "123 Main St", "+1-555-0101"));
      dispatch(
          new CustomerCommand.RegisterCustomer(
              "cust-bob", "Bob Smith", "bob@example.com", "456 Oak Ave", "+1-555-0102"));

      // One seed order — inventory stocked above so the saga auto-confirms it.
      dispatch(
          new OrderCommand.PlaceOrder(
              "order-seed-1",
              "cust-alice",
              List.of(
                  new OrderCommand.OrderLine(
                      "prod-widget", 2, new Money(BigDecimal.valueOf(19.99), "USD")))));

      LOG.info("Demo data seeded successfully.");
    } catch (Exception e) {
      LOG.warn("SeedDataRunner: seeding failed (possibly already seeded): {}", e.getMessage());
    }
  }

  private void dispatch(org.streamrune.core.Command command) {
    try {
      commandBus.execute(command);
    } catch (Exception e) {
      LOG.warn("Seed command skipped ({}): {}", command.getClass().getSimpleName(), e.getMessage());
    }
  }
}
