package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Each check of {@code scripts/money-and-stock-invariants.sql} reports the inconsistency it names,
 * and none reports a consistent history. The events are inserted with plain JDBC into a database of
 * its own, {@code invariant_checks}, whose schema the application creates when one context starts
 * and closes on it; a check that returned nothing for its case would let the integration tests and
 * the two-backend run pass over the inconsistency it exists to catch.
 *
 * <p>The check for a forward command stored after its saga's compensation is proven on the row
 * shape only. A saga whose compensation episode has begun is resumed, never advanced, so a forward
 * command can reach the log after the compensation only when it was held at its commit while the
 * claim landed; the framework's own fence tests hold a command there deterministically, and the
 * demo has no seam that does.
 */
class MoneyAndStockInvariantsQueriesIT {

  private static final String DATABASE = "invariant_checks";

  private static DataSource dataSource;

  private long offset;

  private final Map<String, Long> versions = new HashMap<>();

  @BeforeAll
  static void createDatabaseAndSchema() throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + DATABASE + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + DATABASE);
    }
    var rabbit = AbstractIntegrationTest.RABBIT;
    new SpringApplicationBuilder(SpringEcommerceApplication.class)
        .run(
            "--server.port=0",
            "--spring.datasource.url=" + databaseUrl(),
            "--spring.datasource.username=" + postgres.getUsername(),
            "--spring.datasource.password=" + postgres.getPassword(),
            "--spring.rabbitmq.host=" + rabbit.getHost(),
            "--spring.rabbitmq.port=" + rabbit.getAmqpPort(),
            "--spring.rabbitmq.username=" + rabbit.getAdminUsername(),
            "--spring.rabbitmq.password=" + rabbit.getAdminPassword(),
            "--streamrune.projections.auto-discovery.enabled=false",
            "--streamrune.sse.keep-alive-interval=1h")
        .close();
    var source = new PGSimpleDataSource();
    source.setUrl(databaseUrl());
    source.setUser(postgres.getUsername());
    source.setPassword(postgres.getPassword());
    dataSource = source;
  }

  private static String databaseUrl() {
    var postgres = AbstractIntegrationTest.POSTGRES;
    return "jdbc:postgresql://"
        + postgres.getHost()
        + ":"
        + postgres.getMappedPort(5432)
        + "/"
        + DATABASE;
  }

  @BeforeEach
  void emptyTheStore() throws Exception {
    offset = 0;
    versions.clear();
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DELETE FROM event_stream");
      st.execute("DELETE FROM command_inbox");
    }
  }

  /** One event of a stream: the aggregate type and id, the event type and its payload. */
  private record Event(
      String aggregateType, String aggregateId, String eventType, String payload) {}

  private static Event order(String orderId, String eventType, String payload) {
    return new Event("order", orderId, eventType, payload);
  }

  private static Event payment(String orderId, String eventType) {
    return new Event("payment", "pay-" + orderId, eventType, "{}");
  }

  private static Event stock(String productId, String eventType, String orderId, int quantity) {
    return new Event(
        "inventory",
        productId,
        eventType,
        "{\"productId\":\"%s\",\"orderId\":\"%s\",\"quantity\":%d}"
            .formatted(productId, orderId, quantity));
  }

  private static String placed(String productId) {
    return placed(productId, 1);
  }

  private static String placed(String productId, int quantity) {
    return "{\"lines\":[{\"productId\":\"%s\",\"quantity\":%d}]}".formatted(productId, quantity);
  }

  private static String lines(String productId) {
    return placed(productId);
  }

  private static final String NO_LINES = "{\"lines\":[]}";

  /** A command of the inbox: its idempotency key and the offsets of the events it appended. */
  private record Command(String idempotencyKey, long offset) {}

  private record Case(String name, String violation, List<Event> events, List<Command> commands) {}

  static Stream<Arguments> cases() {
    return Stream.of(
            new Case(
                "a cancelled order whose payment was captured and never refunded",
                "cancelled order with its payment captured and not refunded",
                List.of(
                    order("q-1", "OrderCancelled", NO_LINES), payment("q-1", "PaymentCaptured")),
                List.of()),
            new Case(
                "a cancelled order that still holds its reservation",
                "cancelled order still holding stock",
                List.of(
                    order("q-2", "OrderCancelled", NO_LINES),
                    stock("p-2", "StockReserved", "q-2", 1)),
                List.of()),
            new Case(
                "stock reserved twice for one order at one product",
                "stock reserved twice, or released or committed beyond its reservation",
                List.of(
                    stock("p-3", "StockReserved", "q-3", 1),
                    stock("p-3", "StockReserved", "q-3", 1)),
                List.of()),
            new Case(
                "stock released twice after one reservation",
                "stock reserved twice, or released or committed beyond its reservation",
                List.of(
                    stock("p-3b", "StockReserved", "q-3b", 1),
                    stock("p-3b", "StockReleased", "q-3b", 1),
                    stock("p-3b", "StockReleased", "q-3b", 1)),
                List.of()),
            new Case(
                "a payment captured twice",
                "payment captured twice, or refunded beyond its capture",
                List.of(payment("q-4", "PaymentCaptured"), payment("q-4", "PaymentCaptured")),
                List.of()),
            new Case(
                "a payment refunded without a capture",
                "payment captured twice, or refunded beyond its capture",
                List.of(payment("q-4b", "PaymentRefunded")),
                List.of()),
            new Case(
                "an order both shipped and cancelled",
                "order shipped and cancelled",
                List.of(
                    order("q-5", "OrderShipped", NO_LINES),
                    order("q-5", "OrderCancelled", NO_LINES)),
                List.of()),
            new Case(
                "a confirmed order whose line was never reserved",
                "confirmed order with a line never reserved in its quantity",
                List.of(
                    order("q-6", "OrderPlaced", placed("p-6")),
                    order("q-6", "OrderConfirmed", "{}"),
                    payment("q-6", "PaymentCaptured")),
                List.of()),
            new Case(
                "a shipped order whose line was never committed",
                "shipped order with a line never committed in its quantity",
                List.of(
                    order("q-7", "OrderPlaced", placed("p-7")),
                    order("q-7", "OrderShipped", lines("p-7"))),
                List.of()),
            new Case(
                "a confirmed order whose line was reserved short",
                "confirmed order with a line never reserved in its quantity",
                List.of(
                    order("q-6b", "OrderPlaced", placed("p-6b", 2)),
                    order("q-6b", "OrderConfirmed", "{}"),
                    payment("q-6b", "PaymentCaptured"),
                    stock("p-6b", "StockReserved", "q-6b", 1)),
                List.of()),
            new Case(
                "a shipped order whose line was committed short",
                "shipped order with a line never committed in its quantity",
                List.of(
                    order("q-7b", "OrderPlaced", placed("p-7b", 2)),
                    order("q-7b", "OrderShipped", placed("p-7b", 2)),
                    stock("p-7b", "StockReserved", "q-7b", 2),
                    stock("p-7b", "ReservationConfirmed", "q-7b", 1)),
                List.of()),
            new Case(
                "a confirmed order without a captured payment",
                "order confirmed without a captured payment",
                List.of(
                    order("q-8", "OrderPlaced", NO_LINES), order("q-8", "OrderConfirmed", "{}")),
                List.of()),
            new Case(
                "a confirmed order whose payment was refunded",
                "order confirmed without a captured payment",
                List.of(
                    order("q-8b", "OrderPlaced", NO_LINES),
                    order("q-8b", "OrderConfirmed", "{}"),
                    payment("q-8b", "PaymentCaptured"),
                    payment("q-8b", "PaymentRefunded")),
                List.of()),
            new Case(
                "a forward command stored after the saga's compensation",
                "forward command stored after its saga's compensation",
                List.of(),
                List.of(
                    new Command("saga:fulfillment-q-9:500:0", 10),
                    new Command("saga:fulfillment-q-9:episode:1:comp:0", 5))))
        .map(c -> Arguments.of(c.name(), c));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void eachCheckReportsTheInconsistencyItNames(String name, Case c) throws Exception {
    for (Event e : c.events()) {
      insert(e);
    }
    for (Command command : c.commands()) {
      insert(command);
    }

    List<String> rows = MoneyAndStockInvariants.violations(dataSource, "q-");

    assertThat(rows).as("rows of the checks for %s", name).hasSize(1);
    assertThat(rows.getFirst()).startsWith(c.violation() + ": q-");
  }

  @Test
  void aConsistentHistoryReportsNothing() throws Exception {
    // An order that was confirmed, then cancelled: reserved, released and refunded.
    insert(order("q-ok-1", "OrderPlaced", placed("p-ok-1")));
    insert(payment("q-ok-1", "PaymentInitiated"));
    insert(payment("q-ok-1", "PaymentCaptured"));
    insert(stock("p-ok-1", "StockReserved", "q-ok-1", 1));
    insert(order("q-ok-1", "OrderConfirmed", "{}"));
    insert(order("q-ok-1", "OrderCancelled", lines("p-ok-1")));
    insert(stock("p-ok-1", "StockReleased", "q-ok-1", 1));
    insert(payment("q-ok-1", "PaymentRefunded"));
    // An order that was shipped: reserved and committed.
    insert(order("q-ok-2", "OrderPlaced", placed("p-ok-2")));
    insert(payment("q-ok-2", "PaymentCaptured"));
    insert(stock("p-ok-2", "StockReserved", "q-ok-2", 1));
    insert(order("q-ok-2", "OrderConfirmed", "{}"));
    insert(order("q-ok-2", "OrderShipped", lines("p-ok-2")));
    insert(stock("p-ok-2", "ReservationConfirmed", "q-ok-2", 1));
    // A fulfillment saga whose forward commands all came before its compensation.
    insert(new Command("saga:fulfillment-q-ok-3:3:0", 3));
    insert(new Command("saga:fulfillment-q-ok-3:episode:1:comp:0", 9));

    assertThat(MoneyAndStockInvariants.violations(dataSource, "q-")).isEmpty();
    assertThat(MoneyAndStockInvariants.checks()).hasSize(8);
  }

  /**
   * Histories that look odd and are right: none of them is reported. A capture recorded after a
   * void is refunded in the same append; a second cancellation of an order finds nothing left to
   * undo; a reservation stored after the first cancellation's undo is released by the second; a
   * payment voided before any stock was reserved; an order shipped and paid for whose saga ended
   * {@code FAILED} because the compensation's cancellation was refused.
   */
  @Test
  void historiesThatLookOddAndAreRightReportNothing() throws Exception {
    insert(order("q-n1", "OrderPlaced", placed("p-n1")));
    insert(payment("q-n1", "PaymentInitiated"));
    insert(order("q-n1", "OrderCancelled", lines("p-n1")));
    insert(payment("q-n1", "PaymentVoided"));
    insert(payment("q-n1", "PaymentCaptured"));
    insert(payment("q-n1", "PaymentRefunded"));

    insert(order("q-n2", "OrderPlaced", placed("p-n2")));
    insert(payment("q-n2", "PaymentCaptured"));
    insert(stock("p-n2", "StockReserved", "q-n2", 1));
    insert(order("q-n2", "OrderCancelled", lines("p-n2")));
    insert(order("q-n2", "OrderCancelled", lines("p-n2")));
    insert(stock("p-n2", "StockReleased", "q-n2", 1));
    insert(payment("q-n2", "PaymentRefunded"));

    insert(order("q-n3", "OrderPlaced", placed("p-n3")));
    insert(order("q-n3", "OrderCancelled", lines("p-n3")));
    insert(stock("p-n3", "StockReserved", "q-n3", 1));
    insert(order("q-n3", "OrderCancelled", lines("p-n3")));
    insert(stock("p-n3", "StockReleased", "q-n3", 1));

    insert(order("q-n4", "OrderPlaced", placed("p-n4")));
    insert(payment("q-n4", "PaymentInitiated"));
    insert(order("q-n4", "OrderCancelled", lines("p-n4")));
    insert(payment("q-n4", "PaymentVoided"));

    insert(order("q-n5", "OrderPlaced", placed("p-n5", 3)));
    insert(payment("q-n5", "PaymentCaptured"));
    insert(stock("p-n5", "StockReserved", "q-n5", 3));
    insert(order("q-n5", "OrderConfirmed", "{}"));
    insert(order("q-n5", "OrderShipped", placed("p-n5", 3)));
    insert(stock("p-n5", "ReservationConfirmed", "q-n5", 3));
    insert(new Command("saga:fulfillment-q-n5:2:0", 3));
    insert(new Command("saga:fulfillment-q-n5:episode:1:comp:0", 20));

    assertThat(MoneyAndStockInvariants.violations(dataSource, "q-")).isEmpty();
  }

  private void insert(Event e) throws Exception {
    long version = versions.merge(e.aggregateType() + ":" + e.aggregateId(), 1L, Long::sum);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                    + " event_type, payload, metadata)"
                    + " VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST('{}' AS jsonb))")) {
      ps.setLong(1, ++offset);
      ps.setString(2, e.aggregateType());
      ps.setString(3, e.aggregateId());
      ps.setLong(4, version);
      ps.setString(5, e.eventType());
      ps.setString(6, e.payload());
      ps.executeUpdate();
    }
  }

  private void insert(Command c) throws Exception {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type,"
                    + " aggregate_id, final_version, global_offsets)"
                    + " VALUES (?, 'Command', 'order', 'q-x', 1, ARRAY[CAST(? AS bigint)])")) {
      ps.setString(1, c.idempotencyKey());
      ps.setLong(2, c.offset());
      ps.executeUpdate();
    }
  }
}
