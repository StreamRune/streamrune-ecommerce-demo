package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.product.ProductCommand;
import org.streamrune.ecommerce.quarkus.config.PaymentGatewaySimulator;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Whatever happens to an order, the money and the stock end consistent with it. A mix of orders —
 * two lines, cancelled at once, cancelled once confirmed, shipped and then refused a cancellation,
 * one whose payment the gateway fails — runs against the application; once every fulfillment saga
 * of the test's orders has ended and the order-flow subscriptions have caught up with the event
 * store, {@code scripts/money-and-stock-invariants.sql} returns no row for them, and each product's
 * stock equals what was received, less the open reservations, less the committed quantity.
 *
 * <p>The checks run on the shared database, which holds other classes' orders, some left
 * inconsistent on purpose: only the rows of this test's orders, whose ids start with its prefix,
 * count.
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class MoneyAndStockInvariantsIT {

  private static final Logger LOG = LoggerFactory.getLogger(MoneyAndStockInvariantsIT.class);

  private static final int ORDERS = 24;

  @Inject VirtualThreadCommandBus commandBus;
  @Inject DataSource dataSource;
  @Inject PaymentGatewaySimulator paymentGateway;

  @Test
  void aMixOfCancelsAndShipmentsLeavesMoneyAndStockConsistent() throws Exception {
    String prefix = "inv-" + System.nanoTime() + "-";
    List<String> products = new ArrayList<>();
    for (int p = 1; p <= 3; p++) {
      String productId = prefix + "product-" + p;
      commandBus.execute(
          new ProductCommand.CreateProduct(
              productId, "Product", null, "General", new Money(new BigDecimal("9.99"), "USD"), 0));
      commandBus.execute(new InventoryCommand.ReceiveShipment(productId, 100));
      products.add(productId);
    }
    OrderMix.Api api =
        (path, role, userId, body) -> {
          var request =
              given()
                  .header("X-User-Role", role)
                  .header("X-User-Id", userId)
                  .contentType(ContentType.JSON);
          return (body == null ? request : request.body(body)).when().post(path).statusCode();
        };

    OrderMix.placeOrderWithFailingGateway(
        api, dataSource, prefix, products.getFirst(), paymentGateway::setFailureInjected);
    OrderMix.run(api, dataSource, prefix, products, ORDERS);

    MoneyAndStockInvariants.awaitQuiet(dataSource, prefix, Duration.ofMinutes(3));

    assertThat(MoneyAndStockInvariants.violations(dataSource, prefix))
        .as("rows of the money and stock checks for the orders of this test")
        .isEmpty();
    for (String productId : products) {
      assertThat(MoneyAndStockInvariants.stockMismatch(dataSource, productId))
          .as("stock of %s", productId)
          .isNull();
    }

    // Non-vacuity: the mix reached the undo of a captured payment and of one before its order was
    // confirmed, a shipment and a failed payment.
    List<String> refunded = OrderMix.paymentsWith(dataSource, prefix, "PaymentRefunded");
    assertThat(refunded).as("payments refunded").isNotEmpty();
    assertThat(OrderMix.paymentsCancelledBeforeConfirmation(dataSource, prefix))
        .as("payments cancelled before their order was confirmed")
        .isNotEmpty();
    assertThat(OrderMix.paymentsWith(dataSource, prefix, "PaymentFailed"))
        .as("payments the gateway failed")
        .isNotEmpty();
    assertThat(OrderMix.stockEvents(dataSource, prefix, "ReservationConfirmed"))
        .as("reservations committed by a shipment")
        .isPositive();
    LOG.info(
        "Money and stock mix {}: payments refunded {}, voided {}, failed {}; reservations"
            + " committed {}",
        prefix,
        refunded.size(),
        OrderMix.paymentsWith(dataSource, prefix, "PaymentVoided").size(),
        OrderMix.paymentsWith(dataSource, prefix, "PaymentFailed").size(),
        OrderMix.stockEvents(dataSource, prefix, "ReservationConfirmed"));
    // The refunds reached the gateway the payments were captured at.
    for (String paymentId : refunded) {
      assertThat(paymentGateway.hasRefunded(paymentId))
          .as("the gateway refunded %s", paymentId)
          .isTrue();
    }
  }
}
