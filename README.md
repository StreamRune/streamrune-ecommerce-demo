# StreamRune E-Commerce Demo

A complete CQRS/ES e-commerce application built with StreamRune, demonstrating the framework across three runtimes: Spring Boot, Micronaut, and Quarkus. It implements five aggregates (Product, Order, Customer, Payment, Inventory) with full command handling, event sourcing, projections, sagas, and 17+ infrastructure features.

## Architecture

The demo follows a strict CQRS/ES (Command Query Responsibility Segregation / Event Sourcing) architecture. Write operations flow through Deciders that validate commands against aggregate state and emit domain events. Events are persisted to a PostgreSQL-backed event store and projected into read-model views for queries.

```
Command ──► Decider ──► Events ──► Event Store
                                       │
                                       ▼
                                  Projection ──► Read Model (Views)
                                       │
                                       ▼
                                  Query API
```

All domain logic is framework-agnostic. The three application modules (Spring Boot, Micronaut, Quarkus) provide only the HTTP transport and dependency wiring, sharing the same domain, commands, queries, and projections code.

## Modules

| Module | Description |
|--------|-------------|
| `domain/` | Events, commands, aggregate state, value types |
| `commands/` | Decider implementations -- pure domain logic, no I/O |
| `queries/` | Read-model DTOs (`ProductView`, `OrderView`, `CustomerView`, `InventoryView`) |
| `projections/` | Event-to-view projectors (`ProductProjection`, `OrderProjection`, `CustomerProjection`, `InventoryProjection`) |
| `spring-app/` | Spring Boot 4 REST application (port 8080) |
| `micronaut-app/` | Micronaut REST application (port 8081) |
| `quarkus-app/` | Quarkus REST application (port 8082) |
| `notifications-service/` | Separate Spring Boot service that consumes the integration events the outbox publishes to RabbitMQ (port 8090) |
| `frontend/` | Next.js frontend (port 3000) |

## Prerequisites

- Java 25 (with `--enable-preview`)
- Docker (for PostgreSQL, RabbitMQ and the full-stack demo). The demo runs PostgreSQL 17 — the oldest version StreamRune supports (it requires 17 or newer and refuses an older server at startup)
- GraalVM CE 25.0.1 (for native builds only)

## Quick Start (Docker Compose)

The fastest way to run the full stack:

```bash
./gradlew :spring-app:bootJar :notifications-service:bootJar
docker compose up
```

The two images copy the JARs the first command builds, so run it again before `docker compose up --build` whenever the code changes. Compose starts five services:

| Service | Port | What it is |
|---------|------|------------|
| `postgres` | 5432 | PostgreSQL 17 with an empty `streamrune_ecommerce` database |
| `rabbitmq` | 5672, 15672 | RabbitMQ broker for the transactional outbox; management UI on 15672 (`guest`/`guest`) |
| `backend` | 8080 | The Spring Boot application |
| `notifications-service` | 8090 | Consumer of the integration events the backend publishes |
| `frontend` | 3000 | The Next.js frontend |

Open http://localhost:3000 in your browser.

The database needs no schema script. The backend creates the framework's tables itself when it starts: `streamrune.event-store.schema.auto-initialize=true` has StreamRune apply the Flyway baselines it ships (event store and crypto), each recorded in its own history table, and the read-model tables are created by the projections on first use. The Micronaut and Quarkus apps do the same.

## Running Individually

### 1. Start PostgreSQL and RabbitMQ

All three apps need both: the database for the event store, and the broker because the outbox publisher opens its AMQP connection at startup.

```bash
docker compose up -d postgres rabbitmq
```

### 2. Run the application

**Spring Boot:**
```bash
./gradlew :spring-app:bootRun
```

**Micronaut:**
```bash
./gradlew :micronaut-app:run
```

**Quarkus:**
```bash
./gradlew :quarkus-app:quarkusDev
```

## API

All three apps expose the same REST API. The `X-User-Id` and `X-User-Role` headers control identity and authorization. `X-Trace-Id` and `X-Correlation-Id` are optional (auto-generated if missing).

> **Demo shortcuts, not a pattern.** The apps run in StreamRune's trusted-gateway mode (`streamrune.security.trust-user-id-header=true`): `X-User-Id` is the identity because the demo stands in for a gateway that would authenticate every caller, set the header itself and strip any value a client sent. Without the flag the framework ignores the header. The role comes straight from `X-User-Role` through the demo's `HeaderUserRoleResolver`, which anyone can set to `ADMIN`; a real deployment derives roles from the authenticated identity. See [Chapter 9 of the tutorial](docs/tutorial/09-authorization.md).

### Products

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/products` | Create product |
| `PUT` | `/api/products/{id}/stock` | Adjust stock |
| `PUT` | `/api/products/{id}/price` | Update price |
| `GET` | `/api/products/{id}` | Get product |
| `GET` | `/api/products` | List products |

### Orders

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/orders` | Place order |
| `PUT` | `/api/orders/{id}/confirm` | Confirm order |
| `PUT` | `/api/orders/{id}/ship` | Ship order |
| `PUT` | `/api/orders/{id}/cancel` | Cancel order |
| `GET` | `/api/orders/{id}` | Get order |
| `GET` | `/api/orders` | List orders |

### Customers

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/customers` | Register customer |
| `GET` | `/api/customers/{id}` | Get customer |
| `GET` | `/api/customers` | List customers |
| `POST` | `/api/customers/{id}/forget` | GDPR forget (ADMIN or the customer themself; `200` only when fully erased, else `500` — send it again) |

### SSE & Events

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/events/sse` | Live event feed (SSE), open to every caller. A frame names the event (global offset, type, stream type and id, version, timestamp) and carries no payload |
| `GET` | `/api/sse/{aggregateType}/{aggregateId}` | Live events of one stream (SSE). The framework's endpoint (`streamrune.sse.enabled`), fed by the framework from the head of the global stream: live, at-most-once, no replay. A frame carries the event **with its payload**, so the demo's `SseAuthorizer` lets in only an authenticated `ADMIN` or the customer the stream belongs to (their own `customer` stream, the streams of their own orders); `403` for everyone else |
| `GET` | `/api/events?offset=0&limit=10` | Paginated event history with payloads. ADMIN only, `403` otherwise |
| `GET` | `/api/events/{aggregateType}/{aggregateId}` | One stream's events with payloads. ADMIN only, `403` otherwise |

> **Demo shortcut, protect this in production.** The two history endpoints return each event as the event store reads it, which means `@Encrypted` fields come back **decrypted** until the subject's key is erased. They check the ADMIN role, but that role is the client-supplied `X-User-Role` header (see above), so the check documents the intent and protects nothing. Put an endpoint like this behind real authentication, or do not ship it. The per-stream SSE endpoint carries decrypted payloads too. Its `SseAuthorizer` applies the rule a real deployment keeps — an `ADMIN`, or the stream's owner — but to the same client-supplied `X-User-Id` and `X-User-Role` headers, so it is only as strong as whatever sets them. See [Chapter 15 of the tutorial](docs/tutorial/15-observability.md).

### Example

```bash
# Create a product
curl -X POST http://localhost:8080/api/products \
  -H "Content-Type: application/json" \
  -H "X-User-Id: admin-1" \
  -H "X-User-Role: ADMIN" \
  -d '{"productId":"p1","name":"Widget","description":"A widget","category":"General","price":9.99,"initialStock":100}'

# Place an order
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-User-Id: cust-1" \
  -H "X-User-Role: CUSTOMER" \
  -d '{"orderId":"o1","customerId":"cust-1","lines":[{"productId":"p1","quantity":2,"unitPrice":9.99}]}'
```

## Projection Naming

All projections use explicit lowercase names to keep naming consistent and avoid SQL reserved-word conflicts:

| Projection | Name | DB Table |
|------------|------|----------|
| `ProductProjection` | `products` | `products_view` |
| `OrderProjection` | `orders` | `orders_view` |
| `CustomerProjection` | `customers` | `customers_view` |
| `InventoryProjection` | `inventory` | `inventory_view` |

## Tutorial

A step-by-step tutorial (17 chapters) is in `docs/tutorial/`. It builds this application from scratch, covering every StreamRune feature.

## GraalVM Native Image

All three apps support GraalVM native compilation for sub-second startup (~0.2s).

**GraalVM installation.** The Spring and Micronaut builds use native-build-tools, which refuses its
default reachability-metadata repository when the GraalVM installation has no
`lib/svm/schemas/reachability-metadata-schema.json` (*"…provides a reachability-metadata schema,
but your GraalVM installation … does not"*). GraalVM CE 25.0.1 has none. Use a GraalVM that ships
the schema, or point the build at a copy of the repository without its schema file — the failed
build has already downloaded it to the directory the message names:

```bash
cp -R ~/.gradle/native-build-tools/repositories/<hash>/exploded /tmp/metadata-repo
rm /tmp/metadata-repo/schemas/reachability-metadata-schema-v*.json
export ORG_GRADLE_PROJECT_graalvmMetadataRepository=/tmp/metadata-repo   # or -PgraalvmMetadataRepository=…
```

The smoke script's builds pick the variable up. The Quarkus build does not use native-build-tools
and needs none of this.

```bash
# Requires running Postgres (AOT processing needs a live DataSource)
./gradlew :spring-app:nativeCompile --no-configuration-cache
```

The binary is produced at `spring-app/build/native/nativeCompile/spring-app`.

A smoke test script verifies the native binary end-to-end:

```bash
bash scripts/native-image-smoke-test.sh spring
```

It starts PostgreSQL (port 5433, an empty database) and RabbitMQ (port 5672) in Docker, runs the
binary, and checks health, that the binary created the schema at startup (the framework registers
its migration scripts as native-image resources), a command-to-read-model round trip, the replay of a
dead-lettered `InventoryCommand$ReceiveShipment` through `POST /api/admin/dead-letters/{id}/retry`,
and a product with a 1.5 MiB description written and read back. The replay check proves that the
dead-letter runner's `registerCommand(InventoryCommand.class)` expanded the sealed root in the
native image: GraalVM lists a sealed type's permitted subclasses only when the sealed type itself
is registered for reflection, which `EcommerceNativeHints` does through
`StreamRuneRuntimeHints.registerDomainPackages`. After the checks the script stops the binary with
SIGTERM, waits for it to exit and prints its output (kept in
`build/native-smoke-<framework>-app.log`). The run fails if that output holds a GraalVM
`UnsupportedFeatureError`, shutdown included.

The Quarkus app builds and runs the same way:

```bash
./gradlew :quarkus-app:build -Dquarkus.native.enabled=true -Dquarkus.package.jar.enabled=false
bash scripts/native-image-smoke-test.sh quarkus
```

The binary is produced at `quarkus-app/build/quarkus-app-0.1.0-SNAPSHOT-runner`, and the smoke test
runs the same checks against it on port 8082. Its domain records are registered with
`@RegisterForReflection` in `EcommerceDomainReflectionConfig`, but the ten sealed command and event
interfaces are listed in
`quarkus-app/src/main/resources/META-INF/native-image/org.streamrune.ecommerce/quarkus-app/reachability-metadata.json`
instead: `@RegisterForReflection` on a sealed interface does not make the image list its permitted
subclasses (Quarkus writes a legacy `reflect-config.json` entry without them), so the framework's
startup walks would refuse the application with *"Sealed type … reports no permitted subclasses"*.
`EcommerceDomainReflectionConfigTest` checks on the JVM that every sealed supertype of a registered
record is listed there.

Creating the schema at startup takes two more settings in a Quarkus image, because Flyway finds
parts of itself at run time and a Quarkus build does not apply the GraalVM reachability-metadata
repository the other two builds get Flyway's metadata from.
`quarkus.native.auto-service-loader-registration=true` in `application.properties` registers
Flyway's plugins, which it loads with `ServiceLoader` (without it the binary fails at startup with a
`NullPointerException` from Flyway's configuration), and `FlywayNativeImageConfig` registers the
log creator Flyway instantiates by class name. The smoke test starts the binary on an empty
database and requires the schema to exist afterwards, so it fails if either is missing.

The Micronaut app too:

```bash
./gradlew :micronaut-app:nativeCompile
bash scripts/native-image-smoke-test.sh micronaut
```

The binary is produced at `micronaut-app/build/native/nativeCompile/micronaut-app`, and the smoke
test runs the same checks against it on port 8081. Its records and the outbox mapper's DTOs
are listed in `micronaut-app/src/main/resources/META-INF/native-image/org.streamrune.ecommerce/micronaut-app/reflect-config.json`,
and the ten sealed command and event interfaces in the `reachability-metadata.json` next to it.
Micronaut's `@TypeHint` (or `@ReflectiveAccess`) on the sealed interfaces is not enough: it
registers the type without its permitted subclasses, and the binary refused to start with *"Sealed
type … reports no permitted subclasses"*. `NativeImageMetadataTest` checks on the JVM that every
sealed supertype of a registered record is listed. A second file,
`micronaut-app-flyway/reachability-metadata.json` in the same `native-image` directory, registers
the fields of Flyway's configuration extensions: Flyway copies them reflectively when the schema is
created at startup, and without the registration the binary stopped there with a
`MissingReflectionRegistrationError`. The build sets
`graalvmNative.binaries.main.sharedLibrary = false` (otherwise native-build-tools produced a
`micronaut-app.dylib`), and `logback.xml` keeps the app at INFO: at DEBUG, HikariCP reads every
pool setting reflectively and the image's DataSource fails to start. The build also passes
`-H:+SharedArenaSupport`, between `-H:+UnlockExperimentalVMOptions` and
`-H:-UnlockExperimentalVMOptions` because the option is experimental. Micronaut's Netty 4.2 frees a
direct buffer above 1 MiB, and every pooled chunk when an event loop exits, through
`Arena.ofShared().close()`, and GraalVM 25 supports that call only with this option. Without it the
binary answered the 1.5 MiB request with 500, threw `UnsupportedFeatureError: Support for
Arena.ofShared is not active` from six event-loop threads at shutdown, and never freed those buffers.
Your own Micronaut native build needs the option too (see the GraalVM Native Image section of StreamRune's Quickstart).

## License

The demo is licensed under the [Apache License, Version 2.0](LICENSE); see also [NOTICE](NOTICE). You may use, copy, modify and redistribute the code in this repository and the listings in the tutorial under those terms.

The demo is built on StreamRune, which is a separate work with its own license: the Business Source License 1.1, with a commercial license for organizations above its revenue threshold. The Apache License here covers the files of this repository only and does not change the terms under which StreamRune itself is licensed. Read StreamRune's own `LICENSE` and `NOTICE` before you build a product on it.
