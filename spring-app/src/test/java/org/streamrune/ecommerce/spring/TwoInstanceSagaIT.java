package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;
import org.streamrune.runtime.PollingEventSubscription;

/**
 * Two backends on one database: one leads the fulfillment saga, the other stands by and takes over
 * when the leader stops, no forward command of a fulfillment saga is stored after that saga's
 * compensation, whichever backend runs the step and whichever claims the deadline, and the money
 * and the stock end consistent with every order.
 *
 * <p>Two Spring contexts of the app run in this JVM against a database of their own, {@code
 * two_instances}, on the shared PostgreSQL container. They are built with {@link
 * SpringApplicationBuilder}, not {@code @SpringBootTest}, so that neither is the cached test
 * context of the other integration tests. Each has its own connection pool, its own saga runner and
 * its own timeout runner; they share the database and nothing else. The saga runner carries the
 * lease {@code "saga:" + saga type} over the single-active-consumer leadership, and the saga
 * subscription built with {@code sagaRunner(...)} takes it: one backend delivers the saga's events
 * and claims its deadlines, the other reads nothing and reports {@link LeaseRole#STANDBY}. The
 * payment process manager and the order cancellation and order shipment process managers each hold
 * a lease of their own, on whichever backend takes it. The lease lives 3 s here ({@code
 * streamrune.subscription.single-active-consumer.lease-ttl}) instead of 15 s.
 *
 * <p>The deadline is short ({@code app.saga.fulfillment-timeout=2s}), the timeout runners poll
 * every 200 ms, and every saga command waits a random 0 to 300 ms first ({@code
 * app.chaos.saga-command-delay-max}): a fulfillment takes about as long as its deadline, so some
 * complete, some are compensated at their deadline, and some deadlines pass while a forward command
 * is in flight. Eight client threads place 100 orders, alternating the two backends; after 50
 * orders the test closes the backend whose saga subscription leads (read in this process from the
 * subscription bean's {@code leaseRole()}), waits for the other to lead, and starts a new backend
 * in its place. Closing a context gives its leases back, so the survivor takes over within one
 * polling interval. Exactly one backend leads before the restart, the survivor leads after it, and
 * exactly one leads once the new backend is up.
 *
 * <p>A forward command refused at its commit is counted and logged, not asserted: the leader runs
 * every step and claims every deadline, and a refusal needs a claim that lands underneath a step in
 * flight — a claim after the bounded wait, or the overlap of a handover. The framework's own
 * two-node tests hold a command at its commit on purpose and prove the refusal deterministically;
 * this test checks the outcome under load.
 *
 * <p>Besides placing, the clients' orders get follow-ups, sent by a second set of threads to
 * whichever backend answers: every third order has two lines; every fourth is cancelled by its
 * customer as soon as the read model knows it (a {@code 403} is asked again for up to ten seconds);
 * every fifth is cancelled once it reads {@code CONFIRMED}; every seventh is shipped by an {@code
 * ADMIN} once it reads {@code CONFIRMED}, and its customer's cancellation then answers {@code 400}.
 * Each follow-up request holds the read lock, like a placing request.
 *
 * <p>Then every fulfillment saga ends ({@code COMPLETED}, {@code COMPENSATED} or {@code FAILED};
 * none {@code FAULTED}), the order-flow subscriptions catch up with the event store, and {@code
 * scripts/money-and-stock-invariants.sql} returns no row for the test's orders: no forward command
 * of a saga stored after the first compensation command of that saga, no cancelled order with its
 * payment captured and not refunded or with stock held, no stock or payment settled twice, no
 * shipped order cancelled, no confirmed order without its stock and its captured payment. The run
 * must have exercised both outcomes of the deadline (at least one saga compensated by a timeout
 * claim, whose episode has no cause event, and at least one completed), a refund of a confirmed
 * order and a shipment.
 */
class TwoInstanceSagaIT {

  private static final Logger LOG = LoggerFactory.getLogger(TwoInstanceSagaIT.class);

  private static final String DATABASE = "two_instances";

  private static final int ORDERS = 100;

  private static final int CLIENTS = 8;

  private static final int RESTART_AFTER = 50;

  private static final int PRODUCTS = 4;

  /** Every product has stock for the whole run, with two-line orders in it. */
  private static final int STOCK = 2000;

  /** How long a follow-up waits for its order to read {@code CONFIRMED}. */
  private static final Duration CONFIRMATION_WAIT = Duration.ofSeconds(60);

  /** How long a customer's request is asked again while the read model has no row for the order. */
  private static final Duration READ_MODEL_WAIT = Duration.ofSeconds(10);

  private static final String ORDER_PREFIX = "two-order-";

  /** Each client pauses a random time between these two between two orders. */
  private static final Duration CLIENT_PAUSE_MIN = Duration.ofSeconds(4);

  private static final Duration CLIENT_PAUSE_MAX = Duration.ofSeconds(12);

  /** Every saga command waits a random time up to this first. */
  private static final String SAGA_COMMAND_DELAY_MAX = "300ms";

  private static final String SAGA_TYPE = SagaType.fromClass(OrderFulfillmentState.class).value();

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

  @AfterEach
  void closeContexts() {
    contexts.forEach(ConfigurableApplicationContext::close);
    contexts.clear();
  }

  @Test
  void twoInstancesUnderLoad_keepMoneyAndStockConsistentWithEveryOrder() throws Exception {
    createDatabase();
    ConfigurableApplicationContext[] slots = {start(), start()};
    int[] ports = {port(slots[0]), port(slots[1])};

    for (int p = 0; p < PRODUCTS; p++) {
      post(
          ports[0],
          "/api/products",
          "ADMIN",
          "admin-1",
          """
          {"productId":"two-prod-%d","name":"Two %d","description":"Two-instance product",
           "category":"General","price":10.00,"initialStock":0}
          """
              .formatted(p, p));
      post(
          ports[0],
          "/api/inventory/two-prod-" + p + "/receive",
          "ADMIN",
          "admin-1",
          "{\"quantity\":" + STOCK + "}");
    }

    // Every client holds the read lock around its request; the restart holds the write lock, so
    // it waits for the requests in flight and no request reaches a closed context.
    var backends = new ReentrantReadWriteLock();
    var next = new AtomicInteger();
    var placed = new AtomicInteger();
    var restarted = new AtomicInteger();
    int[] closedSlot = {-1};
    long[] fencedOnClosed = {0};
    // How long closing the leader took, and how long the survivor took to lead after that.
    Duration[] closing = {null};
    Duration[] takeover = {null};
    ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
    ExecutorService followUps = Executors.newFixedThreadPool(CLIENTS);
    List<Future<?>> followUpWork = new CopyOnWriteArrayList<>();
    // A client that fails stops the others, so that none posts to a context being closed.
    try {
      List<Future<?>> work = new ArrayList<>();
      for (int c = 0; c < CLIENTS; c++) {
        work.add(
            clients.submit(
                () -> {
                  for (int i = next.getAndIncrement(); i < ORDERS; i = next.getAndIncrement()) {
                    String customer = "two-cust-" + (i % 10);
                    String body = orderBody(i, customer);
                    backends.readLock().lock();
                    try {
                      post(ports[i % 2], "/api/orders", "CUSTOMER", customer, body);
                    } finally {
                      backends.readLock().unlock();
                    }
                    if (i % 4 == 0 || i % 5 == 0 || i % 7 == 0) {
                      int placedAt = i;
                      followUpWork.add(
                          followUps.submit(
                              () -> {
                                followUp(placedAt, customer, backends, ports);
                                return null;
                              }));
                    }
                    // Each client pauses CLIENT_PAUSE_MIN to CLIENT_PAUSE_MAX between two orders,
                    // about one order a second over the eight. The leading backend's saga
                    // subscription runs every step of both backends' orders, one at a time, and
                    // each order's three forward commands wait 450 ms on average there: at this
                    // pace the subscription keeps up, so a fulfillment takes about as long as its
                    // deadline instead of waiting behind a backlog of every order placed at once.
                    Thread.sleep(
                        ThreadLocalRandom.current()
                            .nextLong(CLIENT_PAUSE_MIN.toMillis(), CLIENT_PAUSE_MAX.toMillis()));
                    if (placed.incrementAndGet() == RESTART_AFTER) {
                      backends.writeLock().lock();
                      try {
                        int leader = awaitOneLeader(slots, "before the restart");
                        int survivor = 1 - leader;
                        fencedOnClosed[0] = fenced(slots[leader]);
                        long closingAt = System.nanoTime();
                        slots[leader].close();
                        contexts.remove(slots[leader]);
                        long closedAt = System.nanoTime();
                        closing[0] = Duration.ofNanos(closedAt - closingAt);
                        await()
                            .atMost(Duration.ofSeconds(30))
                            .pollInterval(Duration.ofMillis(100))
                            .untilAsserted(
                                () ->
                                    assertThat(role(slots[survivor]))
                                        .as("the surviving backend's saga subscription")
                                        .isEqualTo(LeaseRole.LEADING));
                        takeover[0] = Duration.ofNanos(System.nanoTime() - closedAt);
                        closedSlot[0] = leader;
                        slots[leader] = start();
                        ports[leader] = port(slots[leader]);
                        assertThat(awaitOneLeader(slots, "after the restart"))
                            .as("the backend that leads after the restart")
                            .isEqualTo(survivor);
                        restarted.incrementAndGet();
                      } finally {
                        backends.writeLock().unlock();
                      }
                    }
                  }
                  return null;
                }));
      }
      for (Future<?> f : work) {
        f.get(5, TimeUnit.MINUTES);
      }
      for (Future<?> f : followUpWork) {
        f.get(5, TimeUnit.MINUTES);
      }
    } finally {
      clients.shutdownNow();
      followUps.shutdownNow();
    }
    assertThat(placed).hasValue(ORDERS);
    assertThat(restarted).hasValue(1);

    await()
        .atMost(Duration.ofSeconds(90))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              Map<String, Integer> byStatus = countByStatus();
              assertThat(byStatus.keySet())
                  .as("statuses of the fulfillment sagas: %s", byStatus)
                  .isSubsetOf("COMPLETED", "COMPENSATED", "FAILED");
              assertThat(
                      queryInt(
                          "SELECT count(*) FROM saga_state WHERE saga_type = '"
                              + SAGA_TYPE
                              + "' AND saga_id LIKE 'fulfillment-"
                              + ORDER_PREFIX
                              + "%'"))
                  .as("one fulfillment saga per order")
                  .isEqualTo(ORDERS);
            });

    Map<String, Integer> byStatus = countByStatus();
    int timeoutCompensations =
        queryInt(
            "SELECT count(*) FROM saga_state WHERE saga_type = '"
                + SAGA_TYPE
                + "' AND saga_id LIKE 'fulfillment-"
                + ORDER_PREFIX
                + "%'"
                + " AND status = 'COMPENSATED' AND episode_cause_event_id IS NULL");
    int completed =
        queryInt(
            "SELECT count(*) FROM saga_state WHERE saga_type = '"
                + SAGA_TYPE
                + "' AND saga_id LIKE 'fulfillment-"
                + ORDER_PREFIX
                + "%' AND status = 'COMPLETED'");
    long fencedSurvivor = fenced(slots[1 - closedSlot[0]]);
    long fencedRestarted = fenced(slots[closedSlot[0]]);
    LOG.info(
        "Two-instance run: fulfillment sagas by status {}; of the test's orders, completed {} and"
            + " compensated at their deadline {}; slot {} led and was closed in {} ms, slot {} led"
            + " {} ms after that; forward commands fenced {} (closed leader {}, survivor {},"
            + " restarted backend {})",
        byStatus,
        completed,
        timeoutCompensations,
        closedSlot[0],
        closing[0].toMillis(),
        1 - closedSlot[0],
        takeover[0].toMillis(),
        fencedOnClosed[0] + fencedSurvivor + fencedRestarted,
        fencedOnClosed[0],
        fencedSurvivor,
        fencedRestarted);

    // The undo of every cancellation is the order cancellation process manager's, the commit of a
    // shipment the order shipment process manager's: wait until they and the other order-flow
    // subscriptions have caught up with the event store, then look.
    MoneyAndStockInvariants.awaitQuiet(dataSource(), ORDER_PREFIX, Duration.ofMinutes(3));
    int refundsOfConfirmedOrders =
        queryInt(
            """
            SELECT count(*) FROM event_stream p
             WHERE p.aggregate_type = 'payment' AND p.event_type = 'PaymentRefunded'
               AND p.aggregate_id LIKE 'pay-two-order-%'
               AND EXISTS (SELECT 1 FROM event_stream c
                            WHERE c.aggregate_type = 'order'
                              AND c.aggregate_id = substr(p.aggregate_id, 5)
                              AND c.event_type = 'OrderConfirmed')
            """);
    int shipments =
        queryInt(
            "SELECT count(*) FROM event_stream WHERE aggregate_type = 'order'"
                + " AND event_type = 'OrderShipped' AND aggregate_id LIKE 'two-order-%'");
    LOG.info(
        "Two-instance run: refunds of confirmed orders {}, shipments {}",
        refundsOfConfirmedOrders,
        shipments);

    assertThat(MoneyAndStockInvariants.violations(dataSource(), ORDER_PREFIX))
        .as("rows of the money and stock checks for the orders of this test")
        .isEmpty();
    assertThat(timeoutCompensations)
        .as("sagas compensated by a timeout claim (non-vacuity)")
        .isPositive();
    assertThat(completed).as("sagas completed (non-vacuity)").isPositive();
    assertThat(refundsOfConfirmedOrders)
        .as("refunds of confirmed orders (non-vacuity)")
        .isPositive();
    assertThat(shipments).as("shipments (non-vacuity)").isPositive();
  }

  /**
   * The body of order {@code i}: one line, or two when {@code i} is a multiple of three, at the
   * products {@code i} and {@code i + 1} modulo the number of products.
   */
  private static String orderBody(int i, String customer) {
    String line = "{\"productId\":\"two-prod-%d\",\"quantity\":1}";
    String lines =
        i % 3 == 0
            ? line.formatted(i % PRODUCTS) + "," + line.formatted((i + 1) % PRODUCTS)
            : line.formatted(i % PRODUCTS);
    return "{\"orderId\":\"%s%d\",\"customerId\":\"%s\",\"lines\":[%s]}"
        .formatted(ORDER_PREFIX, i, customer, lines);
  }

  /**
   * What happens to order {@code i} after it was placed: a cancellation by its customer at once
   * (every fourth), once confirmed (every fifth), or a shipment by an {@code ADMIN} once confirmed
   * and the refusal of the customer's cancellation after it (every seventh). Each request goes to
   * whichever backend answers and holds the read lock, so that the restart waits for it. An order
   * that its deadline cancelled before it read {@code CONFIRMED} gets no follow-up.
   */
  private void followUp(int i, String customer, ReentrantReadWriteLock backends, int[] ports)
      throws Exception {
    String orderId = ORDER_PREFIX + i;
    if (i % 4 == 0) {
      assertThat(cancelAsCustomer(orderId, customer, backends, ports))
          .as("cancel of %s right after placing it", orderId)
          .isEqualTo(200);
      return;
    }
    if (!awaitConfirmed(orderId, customer, backends, ports)) {
      return;
    }
    if (i % 5 == 0) {
      assertThat(cancelAsCustomer(orderId, customer, backends, ports))
          .as("cancel of the confirmed %s", orderId)
          .isEqualTo(200);
    } else {
      int shipped =
          send(
                  backends,
                  ports,
                  "POST",
                  "/api/orders/" + orderId + "/ship",
                  "ADMIN",
                  "admin-1",
                  null)
              .statusCode();
      // A 400 is the order's deadline cancelling it between the read and the shipment.
      assertThat(shipped).as("shipment of %s", orderId).isIn(200, 400);
      if (shipped == 200) {
        assertThat(cancelAsCustomer(orderId, customer, backends, ports))
            .as("cancel of the shipped %s", orderId)
            .isEqualTo(400);
      }
    }
  }

  /**
   * The customer's cancellation. Its access check reads the order's read model, which has no row
   * for a moment after the order was placed, so a {@code 403} or {@code 404} is asked again for up
   * to ten seconds.
   */
  private int cancelAsCustomer(
      String orderId, String customer, ReentrantReadWriteLock backends, int[] ports)
      throws InterruptedException {
    long until = System.nanoTime() + READ_MODEL_WAIT.toNanos();
    while (true) {
      int status =
          send(
                  backends,
                  ports,
                  "POST",
                  "/api/orders/" + orderId + "/cancel",
                  "CUSTOMER",
                  customer,
                  "{\"reason\":\"changed my mind\"}")
              .statusCode();
      if ((status != 403 && status != 404) || System.nanoTime() > until) {
        return status;
      }
      Thread.sleep(200);
    }
  }

  /**
   * Waits until the order reads {@code CONFIRMED}; returns {@code false} when it reads {@code
   * CANCELLED} first (its deadline cancelled it).
   */
  private boolean awaitConfirmed(
      String orderId, String customer, ReentrantReadWriteLock backends, int[] ports)
      throws InterruptedException {
    long until = System.nanoTime() + CONFIRMATION_WAIT.toNanos();
    while (System.nanoTime() < until) {
      HttpResponse<String> response =
          send(backends, ports, "GET", "/api/orders/" + orderId, "CUSTOMER", customer, null);
      String body = response.statusCode() == 200 ? response.body() : "";
      if (body.contains("\"status\":\"CONFIRMED\"")) {
        return true;
      }
      if (body.contains("\"status\":\"CANCELLED\"")) {
        return false;
      }
      Thread.sleep(300);
    }
    throw new AssertionError(orderId + " read neither CONFIRMED nor CANCELLED within the wait");
  }

  /**
   * One request to a backend, under the read lock; the backend is the one the request's turn picks
   * and, when it does not answer, the other.
   */
  private HttpResponse<String> send(
      ReentrantReadWriteLock backends,
      int[] ports,
      String method,
      String path,
      String role,
      String userId,
      String json) {
    backends.readLock().lock();
    try {
      int first = ThreadLocalRandom.current().nextInt(2);
      try {
        return request(ports[first], method, path, role, userId, json);
      } catch (RuntimeException unreachable) {
        return request(ports[1 - first], method, path, role, userId, json);
      }
    } finally {
      backends.readLock().unlock();
    }
  }

  private HttpResponse<String> request(
      int port, String method, String path, String role, String userId, String json) {
    try {
      return http.send(
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
              .timeout(Duration.ofSeconds(30))
              .header("Content-Type", "application/json")
              .header("X-User-Role", role)
              .header("X-User-Id", userId)
              .method(
                  method,
                  json == null
                      ? HttpRequest.BodyPublishers.noBody()
                      : HttpRequest.BodyPublishers.ofString(json))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException(method + " " + path + " on port " + port + " failed", e);
    }
  }

  /**
   * Waits until exactly one of the two backends' saga subscriptions reports {@link
   * LeaseRole#LEADING} and the other {@link LeaseRole#STANDBY}, and returns the leading slot.
   */
  private static int awaitOneLeader(ConfigurableApplicationContext[] slots, String when) {
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () ->
                assertThat(Arrays.stream(slots).map(TwoInstanceSagaIT::role).toList())
                    .as("the saga subscriptions' roles %s", when)
                    .containsExactlyInAnyOrder(LeaseRole.LEADING, LeaseRole.STANDBY));
    return IntStream.range(0, slots.length)
        .filter(i -> role(slots[i]) == LeaseRole.LEADING)
        .findFirst()
        .orElseThrow();
  }

  /** The role of the backend's saga subscription under the saga's lease, read in this process. */
  private static LeaseRole role(ConfigurableApplicationContext context) {
    return context
        .getBean("orderFulfillmentSagaSubscription", PollingEventSubscription.class)
        .leaseRole();
  }

  /**
   * Starts one backend on the two-instance database, on a free port. The settings are command-line
   * arguments, which take precedence over {@code application.yml}; default properties would not.
   */
  private ConfigurableApplicationContext start() {
    var postgres = AbstractIntegrationTest.POSTGRES;
    var rabbit = AbstractIntegrationTest.RABBIT;
    ConfigurableApplicationContext context =
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
                "--streamrune.polling-interval-ms=200",
                "--streamrune.sse.keep-alive-interval=1h",
                "--app.saga.fulfillment-timeout=2s",
                "--app.saga.timeout-poll-interval=200ms",
                "--app.chaos.saga-command-delay-max=" + SAGA_COMMAND_DELAY_MAX,
                "--streamrune.subscription.single-active-consumer.lease-ttl=3s");
    contexts.add(context);
    return context;
  }

  private static int port(ConfigurableApplicationContext context) {
    return Integer.parseInt(context.getEnvironment().getRequiredProperty("local.server.port"));
  }

  /** The forward commands the backend's saga runner refused at their commit so far. */
  private static long fenced(ConfigurableApplicationContext context) {
    return Math.round(
        context
            .getBean(MeterRegistry.class)
            .find(MetricNames.SAGA_FORWARD_COMMANDS_FENCED)
            .counters()
            .stream()
            .mapToDouble(Counter::count)
            .sum());
  }

  private void post(int port, String path, String role, String userId, String json) {
    HttpResponse<String> response = request(port, "POST", path, role, userId, json);
    assertThat(response.statusCode())
        .as("POST %s on port %d: %s", path, port, response.body())
        .isBetween(200, 299);
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

  private static void createDatabase() throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    try (Connection conn =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = conn.createStatement()) {
      st.execute("DROP DATABASE IF EXISTS " + DATABASE + " WITH (FORCE)");
      st.execute("CREATE DATABASE " + DATABASE);
    }
  }

  private static Connection connect() throws Exception {
    var postgres = AbstractIntegrationTest.POSTGRES;
    return DriverManager.getConnection(
        databaseUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static Map<String, Integer> countByStatus() throws Exception {
    Map<String, Integer> byStatus = new TreeMap<>();
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT status, count(*) FROM saga_state WHERE saga_type = '"
                    + SAGA_TYPE
                    + "' GROUP BY status")) {
      while (rs.next()) {
        byStatus.put(rs.getString(1), rs.getInt(2));
      }
    }
    return byStatus;
  }

  private static int queryInt(String sql) throws Exception {
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /** A data source on the two-instance database. */
  private static DataSource dataSource() {
    var postgres = AbstractIntegrationTest.POSTGRES;
    var source = new PGSimpleDataSource();
    source.setUrl(databaseUrl());
    source.setUser(postgres.getUsername());
    source.setPassword(postgres.getPassword());
    return source;
  }
}
