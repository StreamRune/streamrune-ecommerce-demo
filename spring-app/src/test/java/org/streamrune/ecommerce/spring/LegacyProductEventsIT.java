package org.streamrune.ecommerce.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.ecommerce.domain.common.Money;
import org.streamrune.ecommerce.domain.product.ProductEvent;
import org.streamrune.ecommerce.domain.product.ProductState;
import org.streamrune.ecommerce.spring.config.StreamRuneConfig;
import org.streamrune.postgres.PostgresEventStore;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * Tutorial chapter 4 against a real database and the demo's own event type registry and {@code
 * ProductCreatedUpcaster}: a product written before the upcaster was registered keeps its category,
 * and the chapter's hand-written {@code INSERT} produces a row every reader accepts. The SQL is
 * read from the chapter itself, so the published instructions are what is tested.
 */
@Testcontainers
class LegacyProductEventsIT {

  private static final Path CHAPTER = Path.of("../docs/tutorial/04-event-upcasting.md");

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("streamrune_ecommerce")
          .withUsername("postgres")
          .withPassword("postgres")
          .withCopyFileToContainer(
              MountableFile.forHostPath("../scripts/init-db.sql"),
              "/docker-entrypoint-initdb.d/init-db.sql");

  private static PGSimpleDataSource dataSource;
  private static EventTypeRegistry registry;
  private static PostgresCryptoEngine cryptoEngine;

  @BeforeAll
  static void connect() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    registry = new StreamRuneConfig().eventTypeRegistry();
    // The registry holds the customer events, whose @Encrypted fields a store refuses to write
    // without a crypto engine.
    cryptoEngine = new StreamRuneConfig().cryptoEngine(dataSource);
  }

  /** The store as chapters 2 and 3 have it: no upcaster registered yet. */
  private static EventStore storeWithoutUpcaster() {
    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(registry)
        .cryptoEngine(cryptoEngine)
        .build();
  }

  /** The store from chapter 4 on: the demo's upcaster bean is part of the chain. */
  private static EventStore storeWithUpcaster() {
    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(registry)
        .cryptoEngine(cryptoEngine)
        .upcasters(List.of(new StreamRuneConfig().productCreatedUpcaster()))
        .build();
  }

  @Test
  void aProductCreatedBeforeTheUpcasterWasRegisteredKeepsItsCategory() throws SQLException {
    StreamId streamId = StreamId.of(ProductState.TYPE, AggregateId.of("p-gadget"));
    var created =
        new ProductEvent.ProductCreated(
            "p-gadget",
            "Gadget",
            "Created in chapter 2",
            "Gadgets",
            new Money(new BigDecimal("19.99"), "USD"),
            5);
    storeWithoutUpcaster()
        .append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    new Version(1),
                    new EventType("ProductCreated"),
                    created,
                    new EventMetadata(
                        IdGenerator.generateEventId(),
                        IdGenerator.generateCommandId(),
                        null,
                        null,
                        IdGenerator.generateCorrelationId(),
                        null,
                        null,
                        Instant.now()))),
            Version.initial());
    // Stamped with the chain in effect at write time (none, so 1), although the payload is v2.
    assertThat(storedSchemaVersion("p-gadget")).isEqualTo(1);

    AggregateHistory history = storeWithUpcaster().load(streamId);

    var loaded = (ProductEvent.ProductCreated) history.events().getFirst().event();
    assertThat(loaded.category()).isEqualTo("Gadgets");
  }

  @Test
  void theChaptersHandInsertedLegacyRowIsReadByTheAggregateAndTheGlobalStream()
      throws IOException, SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement statement = conn.createStatement()) {
      statement.execute(chapterInsertSql());
    }
    EventStore store = storeWithUpcaster();

    AggregateHistory history =
        store.load(StreamId.of(ProductState.TYPE, AggregateId.of("p-legacy")));

    assertThat(history.events()).hasSize(1);
    var loaded = (ProductEvent.ProductCreated) history.events().getFirst().event();
    assertThat(loaded.category()).isEqualTo("Uncategorized");
    assertThat(loaded.name()).isEqualTo("Old Widget");
    assertThat(storedSchemaVersion("p-legacy")).isEqualTo(1);

    // Every row is also part of the global stream that projections and sagas read.
    List<EventEnvelope> global = store.readGlobalStream(GlobalOffset.initial(), 100);
    assertThat(global)
        .filteredOn(e -> e.streamId().value().equals("product:p-legacy"))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.metadata().eventId()).isNotNull();
              assertThat(e.metadata().commandId()).isNotNull();
              assertThat(e.metadata().correlationId()).isNotNull();
              assertThat(e.metadata().timestamp()).isNotNull();
            });
  }

  private static int storedSchemaVersion(String productId) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT schema_version FROM event_stream"
                    + " WHERE aggregate_type = 'product' AND aggregate_id = ?")) {
      ps.setString(1, productId);
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        return rs.getInt(1);
      }
    }
  }

  /**
   * The body of the chapter's {@code psql <<'SQL' ... SQL} heredoc that inserts into {@code
   * event_stream}. Exactly one such block must exist.
   */
  private static String chapterInsertSql() throws IOException {
    List<String> lines = Files.readAllLines(CHAPTER, StandardCharsets.UTF_8);
    List<String> blocks = new ArrayList<>();
    StringBuilder current = null;
    for (String line : lines) {
      if (current == null) {
        if (line.stripTrailing().endsWith("<<'SQL'")) {
          current = new StringBuilder();
        }
      } else if (line.strip().equals("SQL")) {
        blocks.add(current.toString());
        current = null;
      } else {
        current.append(line).append('\n');
      }
    }
    List<String> inserts =
        blocks.stream().filter(b -> b.contains("INSERT INTO event_stream")).toList();
    assertThat(inserts).as("heredoc INSERT INTO event_stream blocks in %s", CHAPTER).hasSize(1);
    return inserts.getFirst();
  }
}
