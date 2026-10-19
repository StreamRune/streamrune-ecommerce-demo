package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.sql.DataSource;

/**
 * A mix of orders placed over HTTP, for the checks of money and stock: every third order has two
 * lines; every fourth is cancelled by its customer as soon as the order answers; every fifth is
 * cancelled once it is confirmed; every seventh is shipped by an {@code ADMIN} once it is
 * confirmed, and its customer's later cancellation is refused. One more order is placed while the
 * payment gateway fails.
 */
final class OrderMix {

  /** One POST over HTTP: the status the application answered. */
  interface Api {
    int post(String path, String role, String userId, String body) throws Exception;
  }

  private static final int CLIENTS = 8;

  private static final Duration SETTLE = Duration.ofSeconds(60);

  private OrderMix() {}

  /**
   * Places an order while the gateway fails, and waits until its payment has failed. The gateway is
   * back to succeeding when this returns.
   */
  static void placeOrderWithFailingGateway(
      Api api,
      DataSource dataSource,
      String prefix,
      String productId,
      Consumer<Boolean> injectGatewayFailure)
      throws Exception {
    String orderId = prefix + "gateway-failure";
    injectGatewayFailure.accept(true);
    try {
      assertThat(
              api.post(
                  "/api/orders",
                  "CUSTOMER",
                  prefix + "cust-0",
                  order(orderId, prefix + "cust-0", List.of(productId))))
          .isEqualTo(200);
      await()
          .atMost(SETTLE)
          .pollInterval(Duration.ofMillis(200))
          .until(() -> events(dataSource, "payment", "pay-" + orderId, "PaymentFailed") > 0);
    } finally {
      injectGatewayFailure.accept(false);
    }
  }

  /**
   * Places {@code orders} orders of the products from {@link #CLIENTS} threads, each with its
   * follow-up, and returns when every follow-up has finished. Each follow-up asserts the status its
   * request is answered with.
   */
  static void run(Api api, DataSource dataSource, String prefix, List<String> products, int orders)
      throws Exception {
    ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
    try {
      List<Future<?>> work = new ArrayList<>();
      for (int i = 1; i <= orders; i++) {
        int n = i;
        work.add(clients.submit(() -> one(api, dataSource, prefix, products, n)));
      }
      for (Future<?> f : work) {
        f.get(3, TimeUnit.MINUTES);
      }
    } finally {
      clients.shutdownNow();
    }
  }

  private static Void one(
      Api api, DataSource dataSource, String prefix, List<String> products, int i)
      throws Exception {
    String orderId = prefix + i;
    String customer = prefix + "cust-" + (i % 5);
    List<String> lines =
        i % 3 == 0
            ? List.of(products.get(i % products.size()), products.get((i + 1) % products.size()))
            : List.of(products.get(i % products.size()));
    assertThat(api.post("/api/orders", "CUSTOMER", customer, order(orderId, customer, lines)))
        .as("placing %s", orderId)
        .isEqualTo(200);
    if (i % 4 == 0) {
      assertThat(cancel(api, orderId, customer))
          .as("cancel of %s right away", orderId)
          .isEqualTo(200);
    } else if (i % 5 == 0) {
      awaitConfirmed(dataSource, orderId);
      assertThat(cancel(api, orderId, customer))
          .as("cancel of confirmed %s", orderId)
          .isEqualTo(200);
    } else if (i % 7 == 0) {
      awaitConfirmed(dataSource, orderId);
      assertThat(api.post("/api/orders/" + orderId + "/ship", "ADMIN", "admin-1", null))
          .as("ship of %s", orderId)
          .isEqualTo(200);
      assertThat(cancel(api, orderId, customer)).as("cancel of shipped %s", orderId).isEqualTo(400);
    }
    return null;
  }

  /**
   * The customer's cancellation. The access check reads the order's read model, which has no row
   * for a moment after the order was placed, so a {@code 403} or {@code 404} is asked again for up
   * to ten seconds.
   */
  private static int cancel(Api api, String orderId, String customer) throws Exception {
    long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (true) {
      int status =
          api.post(
              "/api/orders/" + orderId + "/cancel",
              "CUSTOMER",
              customer,
              "{\"reason\":\"changed my mind\"}");
      if ((status != 403 && status != 404) || System.nanoTime() > until) {
        return status;
      }
      Thread.sleep(200);
    }
  }

  private static void awaitConfirmed(DataSource dataSource, String orderId) {
    await()
        .atMost(SETTLE)
        .pollInterval(Duration.ofMillis(200))
        .until(() -> events(dataSource, "order", orderId, "OrderConfirmed") > 0);
  }

  private static String order(String orderId, String customer, List<String> productIds) {
    StringBuilder lines = new StringBuilder();
    for (String productId : productIds) {
      lines.append(lines.isEmpty() ? "" : ",");
      lines.append("{\"productId\":\"%s\",\"quantity\":1}".formatted(productId));
    }
    return "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"lines\":[%s]}"
        .formatted(orderId, customer, lines);
  }

  /** How many events of the type the stream holds. */
  static int events(DataSource dataSource, String aggregateType, String aggregateId, String type)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM event_stream"
                    + " WHERE aggregate_type = ? AND aggregate_id = ? AND event_type = ?")) {
      ps.setString(1, aggregateType);
      ps.setString(2, aggregateId);
      ps.setString(3, type);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /** The ids of the payments of the prefix's orders that hold an event of the type. */
  static List<String> paymentsWith(DataSource dataSource, String prefix, String type)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT DISTINCT aggregate_id FROM event_stream"
                    + " WHERE aggregate_type = 'payment' AND aggregate_id LIKE ? AND event_type = ?"
                    + " ORDER BY aggregate_id")) {
      ps.setString(1, "pay-" + prefix + "%");
      ps.setString(2, type);
      try (ResultSet rs = ps.executeQuery()) {
        List<String> ids = new ArrayList<>();
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
        return ids;
      }
    }
  }

  /**
   * The ids of the payments of the prefix's orders that were cancelled before their order was
   * confirmed: voided, or refunded with no confirmation of the order.
   */
  static List<String> paymentsCancelledBeforeConfirmation(DataSource dataSource, String prefix)
      throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT DISTINCT p.aggregate_id FROM event_stream p"
                    + " WHERE p.aggregate_type = 'payment' AND p.aggregate_id LIKE ?"
                    + " AND (p.event_type = 'PaymentVoided'"
                    + " OR (p.event_type = 'PaymentRefunded' AND NOT EXISTS"
                    + " (SELECT 1 FROM event_stream o WHERE o.aggregate_type = 'order'"
                    + " AND o.aggregate_id = substr(p.aggregate_id, 5)"
                    + " AND o.event_type = 'OrderConfirmed')))"
                    + " ORDER BY p.aggregate_id")) {
      ps.setString(1, "pay-" + prefix + "%");
      try (ResultSet rs = ps.executeQuery()) {
        List<String> ids = new ArrayList<>();
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
        return ids;
      }
    }
  }

  /** How many events of the type the prefix's orders' inventory streams hold. */
  static int stockEvents(DataSource dataSource, String prefix, String type) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM event_stream"
                    + " WHERE aggregate_type = 'inventory' AND event_type = ?"
                    + " AND payload->>'orderId' LIKE ?")) {
      ps.setString(1, type);
      ps.setString(2, prefix + "%");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }
}
