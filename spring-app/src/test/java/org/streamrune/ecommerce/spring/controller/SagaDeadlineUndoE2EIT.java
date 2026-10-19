package org.streamrune.ecommerce.spring.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentSaga;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.ecommerce.domain.inventory.InventoryState;
import org.streamrune.ecommerce.domain.order.OrderState;
import org.streamrune.ecommerce.domain.payment.PaymentIds;
import org.streamrune.ecommerce.domain.payment.PaymentState;
import org.streamrune.ecommerce.spring.AbstractIntegrationTest;
import org.streamrune.ecommerce.spring.SpringEcommerceApplication;
import org.streamrune.ecommerce.spring.config.PaymentGatewaySimulator;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.SagaTimeoutRunnerTestAccess;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * A fulfillment's deadline claims the saga at a step the saga row does not record yet, and the
 * order's cancellation undoes whatever the streams hold: a capture recorded after the claim is
 * refunded, and a reservation stored before it is released.
 *
 * <p>The app runs in a context of its own, on a database of its own ({@code deadline_undo}): a
 * timeout sweep with a clock past the deadline claims every running fulfillment saga of its
 * database, and the shared test database holds other classes' sagas. The test drives a timeout
 * runner of its own, bound to the app's saga runner and its command bus, with a 30-minute deadline
 * and a clock 31 minutes ahead, through {@code pollOnce}. It follows the app's saga lease, so each
 * test waits for the app's saga subscription to lead first.
 *
 * <p>Each case holds one step with {@link AppendFailureInjection}: the event store refuses one
 * event of one stream until the test allows it. A refused {@code PaymentCaptured} holds the capture
 * while the gateway has taken the money; a refused {@code StockReserved} holds the saga's
 * reservation step, which the saga runner delivers again (a store failure is no answer of the
 * domain) while its row still reads that the saga awaits its payment.
 */
class SagaDeadlineUndoE2EIT {

  private static final String DATABASE = "deadline_undo";

  private static final Duration TIMEOUT = Duration.ofMinutes(30);

  private static ConfigurableApplicationContext context;

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @BeforeAll
  static void start() throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    var rabbit = AbstractIntegrationTest.RABBIT;
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + DATABASE + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + DATABASE);
    }
    context =
        new SpringApplicationBuilder(SpringEcommerceApplication.class)
            .run(
                "--server.port=0",
                "--spring.datasource.url=jdbc:postgresql://"
                    + postgres.getHost()
                    + ":"
                    + postgres.getMappedPort(5432)
                    + "/"
                    + DATABASE,
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.rabbitmq.host=" + rabbit.getHost(),
                "--spring.rabbitmq.port=" + rabbit.getAmqpPort(),
                "--spring.rabbitmq.username=" + rabbit.getAdminUsername(),
                "--spring.rabbitmq.password=" + rabbit.getAdminPassword(),
                // No read model is needed: the cancellations come from the test's timeout runner,
                // not over HTTP, and every assertion reads the event store and the saga table.
                "--streamrune.projections.auto-discovery.enabled=false",
                "--streamrune.polling-interval-ms=200",
                "--streamrune.sse.keep-alive-interval=1h");
  }

  @AfterAll
  static void stop() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  void aDeadlineBeforeTheCaptureIsRecorded_refundsTheLateCapture() throws Exception {
    String order = "dl-late-capture-" + System.nanoTime();
    String product = "dl-prod-" + System.nanoTime();
    String payment = PaymentIds.forOrder(order);
    stock(product, 5);

    try (var failures = new AppendFailureInjection(dataSource())) {
      failures.failAppend(PaymentState.TYPE, payment, "PaymentCaptured");
      place(order, Map.of(product, 1));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(failures.refusals()).isPositive();
                assertThat(gateway().hasCaptured(payment)).isTrue();
              });

      claimEveryDeadline();

      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(() -> assertThat(probe().paymentEvents(order)).contains("PaymentVoided"));
      failures.allowAppend(PaymentState.TYPE, payment, "PaymentCaptured");
    }

    // The payment process manager delivers PaymentInitiated again after its failure backoff,
    // which grows to 60 seconds at most.
    await()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> {
              assertThat(probe().paymentEvents(order))
                  .containsExactly(
                      "PaymentInitiated", "PaymentVoided", "PaymentCaptured", "PaymentRefunded");
              assertThat(gateway().hasRefunded(payment)).isTrue();
            });
    assertThat(probe().eventTypes(OrderState.TYPE, order)).last().isEqualTo("OrderCancelled");
    assertThat(probe().sagaStatus(order)).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(probe().stockEventsOf(product, order)).isEmpty();
  }

  @Test
  void aDeadlineWhileTheCaptureIsNotYetApplied_refundsIt() throws Exception {
    String order = "dl-unapplied-" + System.nanoTime();
    String product = "dl-prod-" + System.nanoTime();
    stock(product, 5);

    try (var failures = new AppendFailureInjection(dataSource())) {
      failures.failAppend(InventoryState.TYPE, product, "StockReserved");
      place(order, Map.of(product, 1));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(probe().paymentEvents(order)).contains("PaymentCaptured");
                assertThat(failures.refusals()).isPositive();
              });
      // The refused StockReserved is a store failure, so the saga runner delivers the capture
      // again; it does not compensate the step, and its row still reads that it awaits payment.
      assertThat(probe().eventTypes(OrderState.TYPE, order))
          .as("the refused reservation is retried, not compensated")
          .doesNotContain("OrderCancelled");
      assertThat(probe().saga(order).orElseThrow().state().fulfillmentStatus().name())
          .isEqualTo("AWAITING_PAYMENT");

      claimEveryDeadline();

      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> assertThat(probe().paymentEvents(order)).contains("PaymentRefunded"));
      failures.allowAppend(InventoryState.TYPE, product, "StockReserved");
    }

    await()
        .during(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(probe().stockEventsOf(product, order))
                    .as(
                        "the reservation is never dispatched: the redelivered step finds the saga"
                            + " ended")
                    .isEmpty());
    assertThat(probe().paymentEvents(order))
        .containsExactly("PaymentInitiated", "PaymentCaptured", "PaymentRefunded");
    assertThat(probe().sagaStatus(order)).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(gateway().hasRefunded(PaymentIds.forOrder(order))).isTrue();
  }

  @Test
  void aDeadlineAfterOneOfTwoLinesWasReserved_releasesItAndRefunds() throws Exception {
    String order = "dl-half-" + System.nanoTime();
    String first = "dl-prod-a-" + System.nanoTime();
    String second = "dl-prod-b-" + System.nanoTime();
    stock(first, 5);
    stock(second, 5);
    var lines = new LinkedHashMap<String, Integer>();
    lines.put(first, 2);
    lines.put(second, 1);

    try (var failures = new AppendFailureInjection(dataSource())) {
      failures.failAppend(InventoryState.TYPE, second, "StockReserved");
      place(order, lines);
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertThat(probe().stockEventsOf(first, order)).containsExactly("StockReserved:2");
                assertThat(failures.refusals()).isPositive();
              });

      claimEveryDeadline();

      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(probe().stockEventsOf(first, order))
                      .containsExactly("StockReserved:2", "StockReleased:2"));
      failures.allowAppend(InventoryState.TYPE, second, "StockReserved");
    }

    await()
        .during(Duration.ofSeconds(10))
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(() -> assertThat(probe().stockEventsOf(second, order)).isEmpty());
    assertThat(probe().stockEventsOf(first, order))
        .containsExactly("StockReserved:2", "StockReleased:2");
    assertThat(probe().inventory(first).available()).isEqualTo(5);
    assertThat(probe().inventory(second).available()).isEqualTo(5);
    assertThat(probe().paymentEvents(order))
        .containsExactly("PaymentInitiated", "PaymentCaptured", "PaymentRefunded");
    assertThat(probe().sagaStatus(order)).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * One sweep of a timeout runner whose clock is past the deadline of every saga of the database,
   * after the app's saga subscription leads (the runner follows the app's saga lease).
   */
  private void claimEveryDeadline() {
    PollingEventSubscription sagaSubscription =
        context.getBean("orderFulfillmentSagaSubscription", PollingEventSubscription.class);
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> sagaSubscription.leaseRole() == LeaseRole.LEADING);
    SagaTimeoutRunner<OrderFulfillmentState> runner =
        SagaTimeoutRunner.<OrderFulfillmentState>builder()
            .decider(new OrderFulfillmentSaga(TIMEOUT))
            .sagaStore(context.getBean(SagaStore.class))
            .sagaType(SagaType.fromClass(OrderFulfillmentState.class))
            .commandBus(context.getBean(VirtualThreadCommandBus.class))
            .clock(Clock.fixed(Instant.now().plus(TIMEOUT).plusSeconds(60), ZoneOffset.UTC))
            .sagaRunner(context.getBean("orderFulfillmentSagaRunner", SagaRunner.class))
            .build();
    SagaTimeoutRunnerTestAccess.pollOnce(runner);
  }

  private void stock(String product, int quantity) {
    context
        .getBean(VirtualThreadCommandBus.class)
        .execute(new InventoryCommand.ReceiveShipment(product, quantity));
  }

  private void place(String order, Map<String, Integer> lines) throws Exception {
    String customer = "dl-cust-" + System.nanoTime();
    String body =
        """
        {"orderId": "%s", "customerId": "%s", "lines": [%s]}"""
            .formatted(
                order,
                customer,
                lines.entrySet().stream()
                    .map(
                        l ->
                            """
                            {"productId": "%s", "quantity": %d, "unitPrice": 9.99}"""
                                .formatted(l.getKey(), l.getValue()))
                    .collect(Collectors.joining(", ")));
    int port = Integer.parseInt(context.getEnvironment().getRequiredProperty("local.server.port"));
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/orders"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .header("X-User-Role", "CUSTOMER")
                .header("X-User-Id", customer)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isBetween(200, 299);
  }

  private static DataSource dataSource() {
    return context.getBean(DataSource.class);
  }

  private static PaymentGatewaySimulator gateway() {
    return context.getBean(PaymentGatewaySimulator.class);
  }

  private static OrderUndoProbe probe() {
    return new OrderUndoProbe(
        context.getBean(EventStore.class), context.getBean(SagaStore.class), dataSource());
  }
}
