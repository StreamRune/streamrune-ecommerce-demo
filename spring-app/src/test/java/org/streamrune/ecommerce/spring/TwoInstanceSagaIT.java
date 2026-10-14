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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.types.SagaType;
import org.streamrune.ecommerce.commands.saga.OrderFulfillmentState;

/**
 * Two backends on one database: no forward command of a fulfillment saga is stored after that
 * saga's compensation, whichever backend runs the step and whichever claims the deadline.
 *
 * <p>Two Spring contexts of the app run in this JVM against a database of their own, {@code
 * two_instances}, on the shared PostgreSQL container. They are built with {@link
 * SpringApplicationBuilder}, not {@code @SpringBootTest}, so that neither is the cached test
 * context of the other integration tests. Each has its own connection pool, its own saga runner and
 * its own timeout runner; they share the database and nothing else. A step lock is memory: a
 * timeout runner on one backend does not wait for a step in flight on the other. What keeps a
 * forward command and its saga's compensation in order across the two is the saga runner's forward
 * fence: every forward command takes a lock that every write stamping a compensation episode or
 * ending the saga also takes, held to the command's commit, so the command is stored wholly before
 * the claim or refused at its commit.
 *
 * <p>The deadline is short ({@code app.saga.fulfillment-timeout=2s}), the timeout runners poll
 * every 200 ms, and every saga command waits a random 0 to 300 ms first ({@code
 * app.chaos.saga-command-delay-max}): a fulfillment takes about as long as its deadline, so some
 * complete, some are compensated at their deadline, and some deadlines pass while a forward command
 * is in flight. Eight client threads place 100 orders, alternating the two backends; after 50
 * orders the second backend is closed and a new one is started on the same database.
 *
 * <p>Then every fulfillment saga ends ({@code COMPLETED}, {@code COMPENSATED} or {@code FAILED};
 * none {@code FAULTED}), and the command inbox holds no forward command of a saga stored after the
 * first compensation command of that saga. The run must have exercised both outcomes: at least one
 * saga compensated by a timeout claim (its episode has no cause event) and at least one completed.
 */
class TwoInstanceSagaIT {

  private static final Logger LOG = LoggerFactory.getLogger(TwoInstanceSagaIT.class);

  private static final String DATABASE = "two_instances";

  private static final int ORDERS = 100;

  private static final int CLIENTS = 8;

  private static final int RESTART_AFTER = 50;

  private static final int PRODUCTS = 4;

  private static final String SAGA_TYPE = SagaType.fromClass(OrderFulfillmentState.class).value();

  /** A forward command stored after the first compensation command of its saga. */
  private static final String INVARIANT_QUERY =
      """
      SELECT f.idempotency_key, c.idempotency_key
        FROM command_inbox f
        JOIN command_inbox c
          ON split_part(c.idempotency_key, ':', 2) = split_part(f.idempotency_key, ':', 2)
       WHERE c.idempotency_key LIKE 'saga:%:episode:%:comp:%'
         AND f.idempotency_key LIKE 'saga:%'
         AND f.idempotency_key NOT LIKE 'saga:%:episode:%'
         AND (SELECT max(o) FROM unnest(f.global_offsets) o)
           > (SELECT min(o) FROM unnest(c.global_offsets) o)
      """;

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

  @AfterEach
  void closeContexts() {
    contexts.forEach(ConfigurableApplicationContext::close);
    contexts.clear();
  }

  @Test
  void twoInstancesUnderLoad_storeNoForwardCommandAfterItsSagasCompensation() throws Exception {
    createDatabase();
    ConfigurableApplicationContext a = start();
    ConfigurableApplicationContext[] b = {start()};
    int portA = port(a);
    int[] portB = {port(b[0])};

    for (int p = 0; p < PRODUCTS; p++) {
      post(
          portA,
          "/api/products",
          "ADMIN",
          "admin-1",
          """
          {"productId":"two-prod-%d","name":"Two %d","description":"Two-instance product",
           "category":"General","price":10.00,"initialStock":0}
          """
              .formatted(p, p));
      post(
          portA,
          "/api/inventory/two-prod-" + p + "/receive",
          "ADMIN",
          "admin-1",
          "{\"quantity\":1000}");
    }

    // Clients that send to the second backend hold the read lock; the restart holds the write
    // lock, so it waits for the requests in flight and no request reaches a closed context.
    var backendB = new ReentrantReadWriteLock();
    var next = new AtomicInteger();
    var placed = new AtomicInteger();
    var restarted = new AtomicInteger();
    long[] fencedOnClosedB = {0};
    ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
    List<Future<?>> work = new ArrayList<>();
    for (int c = 0; c < CLIENTS; c++) {
      work.add(
          clients.submit(
              () -> {
                for (int i = next.getAndIncrement(); i < ORDERS; i = next.getAndIncrement()) {
                  String customer = "two-cust-" + (i % 10);
                  String body =
                      """
                      {"orderId":"two-order-%d","customerId":"%s",
                       "lines":[{"productId":"two-prod-%d","quantity":1,"unitPrice":10.00}]}
                      """
                          .formatted(i, customer, i % PRODUCTS);
                  if (i % 2 == 0) {
                    post(portA, "/api/orders", "CUSTOMER", customer, body);
                  } else {
                    backendB.readLock().lock();
                    try {
                      post(portB[0], "/api/orders", "CUSTOMER", customer, body);
                    } finally {
                      backendB.readLock().unlock();
                    }
                  }
                  // Each client pauses 4 to 12 seconds between two orders, about one order a second
                  // over the eight. A backend's saga subscription runs one step at a time, and each
                  // order's three forward commands wait 450 ms on average there: at this pace the
                  // subscription keeps up, so a fulfillment takes about as long as its deadline
                  // instead of waiting behind a backlog of every order placed at once.
                  Thread.sleep(ThreadLocalRandom.current().nextLong(4_000, 12_000));
                  if (placed.incrementAndGet() == RESTART_AFTER) {
                    backendB.writeLock().lock();
                    try {
                      fencedOnClosedB[0] = fenced(b[0]);
                      b[0].close();
                      contexts.remove(b[0]);
                      b[0] = start();
                      portB[0] = port(b[0]);
                      restarted.incrementAndGet();
                    } finally {
                      backendB.writeLock().unlock();
                    }
                  }
                }
                return null;
              }));
    }
    for (Future<?> f : work) {
      f.get(5, TimeUnit.MINUTES);
    }
    clients.shutdown();
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
                              + "' AND saga_id LIKE 'fulfillment-two-order-%'"))
                  .as("one fulfillment saga per order")
                  .isEqualTo(ORDERS);
            });

    Map<String, Integer> byStatus = countByStatus();
    int timeoutCompensations =
        queryInt(
            "SELECT count(*) FROM saga_state WHERE saga_type = '"
                + SAGA_TYPE
                + "' AND saga_id LIKE 'fulfillment-two-order-%'"
                + " AND status = 'COMPENSATED' AND episode_cause_event_id IS NULL");
    int completed =
        queryInt(
            "SELECT count(*) FROM saga_state WHERE saga_type = '"
                + SAGA_TYPE
                + "' AND saga_id LIKE 'fulfillment-two-order-%' AND status = 'COMPLETED'");
    long fencedA = fenced(a);
    long fencedB = fenced(b[0]);
    LOG.info(
        "Two-instance run: fulfillment sagas by status {}; of the test's orders, completed {} and"
            + " compensated at their deadline {}; forward commands fenced {} (first backend {},"
            + " closed second backend {}, restarted second backend {})",
        byStatus,
        completed,
        timeoutCompensations,
        fencedA + fencedOnClosedB[0] + fencedB,
        fencedA,
        fencedOnClosedB[0],
        fencedB);

    assertThat(invariantViolations())
        .as("forward commands stored after the first compensation command of their saga")
        .isEmpty();
    assertThat(timeoutCompensations)
        .as("sagas compensated by a timeout claim (non-vacuity)")
        .isPositive();
    assertThat(completed).as("sagas completed (non-vacuity)").isPositive();
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
                "--app.chaos.saga-command-delay-max=300ms");
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
    HttpResponse<String> response;
    try {
      response =
          http.send(
              HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                  .timeout(Duration.ofSeconds(30))
                  .header("Content-Type", "application/json")
                  .header("X-User-Role", role)
                  .header("X-User-Id", userId)
                  .POST(HttpRequest.BodyPublishers.ofString(json))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException("POST " + path + " on port " + port + " failed", e);
    }
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

  private static List<String> invariantViolations() throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(INVARIANT_QUERY)) {
      while (rs.next()) {
        rows.add(rs.getString(1) + " after " + rs.getString(2));
      }
    }
    return rows;
  }
}
