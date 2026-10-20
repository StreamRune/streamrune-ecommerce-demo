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
| `queries/` | Read-model DTOs (`ProductView`, `OrderView`, `CustomerView`, `InventoryView`) and the access rule for a customer's data (`OwnerOrAdminAccess`, `OwnerOrAdminStreamAccess`) |
| `projections/` | Event-to-view projectors (`ProductProjection`, `OrderProjection`, `CustomerProjection`, `InventoryProjection`) and the customer and order reads as one caller may see them (`OwnerOrAdminQueries`) |
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
| `notifications-service` | 8090 | Consumer of the integration events the backend publishes. `GET /received` shows them to an `ADMIN`; `GET /health` is open and carries no body |
| `frontend` | 3000 | The Next.js frontend |

Open http://localhost:3000 in your browser.

The profile `two-backends` (`docker compose --profile two-backends up -d --build`) adds `backend-2`, a second backend on the same database on port 8091. Both serve every request; one of them leads the order-fulfillment saga, one the payment process manager, one the order cancellation process manager and one the order shipment process manager (the same backend or different ones), and the other stands by and takes over when a leader stops or dies. With `MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS=always`, `/actuator/health` shows each backend's role (`LEADING` or `STANDBY`). `scripts/two-backends-invariant.sh` places orders on both backends, cancels some at once or once confirmed (the cancellation releases the stock and voids or refunds the payment), ships others, kills the leader halfway through, waits for the other backend to take over and for every saga to end, and then runs `scripts/money-and-stock-invariants.sql`: no saga's forward command was stored after its compensation, and the money and the stock of every order are consistent with it ([Chapter 17](docs/tutorial/17-full-demo.md#two-backends-on-one-database)).

The database needs no schema script. The backend creates the framework's tables itself when it starts: `streamrune.event-store.schema.auto-initialize=true` has StreamRune apply the Flyway baselines it ships (event store and crypto), each recorded in its own history table, and the read-model tables are created by the projections on first use. The Micronaut and Quarkus apps do the same.

## Running Individually

### 1. Start PostgreSQL and RabbitMQ

All three apps need the database for the event store. The broker receives the transactional outbox's integration events: an app starts and serves commands without it, its outbox entries wait in the database, and the relay delivers them once the broker answers ([Chapter 13](docs/tutorial/13-inventory-outbox.md#step-5b-wire-the-rabbitmq-publisher)).

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

All three apps expose the same REST API. The `X-User-Id` and `X-User-Role` headers control identity and authorization: `X-User-Id` says who is calling, `X-User-Role` which role they hold (`ADMIN`, `CUSTOMER`; anything else is a guest). `X-Trace-Id` and `X-Correlation-Id` are optional (auto-generated if missing). Both are tracing metadata a client chooses: a correlation id addresses nothing. The reply to a fulfillment saga's own command reaches that saga through the origin the command bus stores with the event, and a confirmation or cancellation a request causes reaches the saga of the order the event names. One form of correlation id is reserved: a request whose `X-Correlation-Id` starts with `streamrune:` (the framework binds `streamrune:saga:<sagaId>` around a saga's commands) is answered `400` with a plain-text body.

> **Demo shortcuts, not a pattern.** The apps run in StreamRune's trusted-gateway mode (`streamrune.security.trust-user-id-header=true`): `X-User-Id` is the identity because the demo stands in for a gateway that would authenticate every caller, set the header itself and strip any value a client sent. Without the flag the framework ignores the header. The role comes straight from `X-User-Role` through the demo's `HeaderUserRoleResolver`, which anyone can set to `ADMIN`; a real deployment derives roles from the authenticated identity. See [Chapter 9 of the tutorial](docs/tutorial/09-authorization.md).

### Who may call what

One rule guards a customer's data, in all three apps: **an authenticated caller who holds the `ADMIN` role, or the customer the data belongs to** (`OwnerOrAdminAccess` in the `queries` module). A customer record belongs to the caller whose `X-User-Id` equals the customer id; an order belongs to the customer it names, and so does the order's fulfillment saga. Everyone else gets `403` — a caller without `X-User-Id` first of all, whatever `X-User-Role` says. The per-stream SSE endpoint applies the same rule.

- **An `ADMIN` is the role together with an identity**, on every endpoint: `X-User-Role: ADMIN` on a request without `X-User-Id` is nobody (`OwnerOrAdminAccess.isAdmin`, the demo's one definition).
- **A refusal is always `403`, and the refusal says nothing about existence.** Another customer's record and a record that does not exist are refused alike, and so are another customer's order and an order id without a row. `404` is an answer for the two callers who may know: the customer for their own record, and an `ADMIN`. The read model follows the event store by up to the projection's polling interval (five seconds by default), so for that moment after `POST /api/orders` the order has no row and its own customer gets `403` for it as well. A command refused by `@RequireRole` in the command bus (ship, deliver, discontinue) is a `403` too.
- **The open feed names no stream.** The live event feed (`GET /api/events/sse`) is open to every caller and cannot ask who is calling: a browser's `EventSource` sends no headers. A frame therefore holds the global offset, the aggregate type, the event type and the timestamp of an event and nothing else: no stream id, no aggregate id, no payload, for an `ADMIN` as for everyone else (`LiveFeedFrame` in the `queries` module). A caller of the feed learns that a customer registered or an order was placed, and when, not which one; no caller learns a customer id or an order id from it. A view that wants the detail reads it from the guarded endpoints with the caller's headers.
- **The open feed is live.** A connection starts at the head of the global stream: it is sent a frame for every event stored while it is connected and nothing the store held before, whatever the request carries, so no caller makes an app read its history (`LiveFeed` in the `queries` module). Events stored while a client is away are not sent after it reconnects; the client reads its lists again. A stream opens with a `: connected` comment and stays alive with a `: keepalive` comment every five quiet seconds.
- **The open feed costs an app the same whoever connects.** One reader per app polls the global stream once a second and hands each frame to every connected client, so the event store is asked once per second however many connections are open, and an event is read and decrypted once per app. The reader runs while a client is connected. The feed serves 100 connections at once (`LiveFeed.MAX_CONNECTIONS`); the connection that would be one more is answered `503 Service Unavailable` with `Retry-After: 5`, a client that does not read is dropped once 256 frames are buffered for it, and a place whose request was given up before its stream started is free again after five seconds.
- **What a caller without headers reaches**: the public catalogue (`GET /api/products`, `GET /api/products/{id}`), the open feed, `GET /api/admin/health`, each framework's own health and metrics endpoints (`/actuator/health` and `/actuator/metrics` on Spring, `/q/health` and `/q/metrics` on Quarkus, `/health` and `/metrics` on Micronaut), which hold no id, and `GET /health` of the notifications service on port 8090, a `200` without a body. Every other route answers `403` without the two headers. `GET /received` on the notifications service returns the bodies of the integration events it consumed, which name orders, payments and products and carry a customer's cancel reason: it answers an `ADMIN` and is `403` for everyone else.
- **One answer tells a caller that an id is taken.** `POST /api/orders` answers `400 Order already exists` to a caller who places an order of their own under an id in use. What the rule keeps from a refused caller is the data behind an id: the record, the order, the payloads.
- **A list holds what the caller may see.** An `ADMIN` gets every row; any other authenticated caller gets their own record from `GET /api/customers`, their own orders from `GET /api/orders` and the sagas of their own orders from `GET /api/saga/fulfillments`. The rows are chosen inside `OwnerOrAdminQueries` and the read models, before a controller sees one; the saga list selects its rows by saga type and id in the statement that reads them (`FulfillmentSagaRows`). No query parameter widens that: `?customerId=` may repeat the caller's own id, and naming another customer is a `403`.
- **A fulfillment saga is read by whoever may read its order.** The saga of an order has the id `fulfillment-<orderId>`, so the order is known from the saga id and its owner is the customer the order's read-model row names. `GET /api/saga/fulfillments/{id}` answers an `ADMIN` and that customer; everyone else gets the `403` an order of somebody else gets, also for an id that names no order and for an order without a row yet. The state a customer reads is the state an `ADMIN` reads: the order, the customer, the payment id derived from the order id, the lines, the products reserved so far, the total and the fulfillment status, all of it the customer's own purchase.
- **Operations are for an `ADMIN`**: the audit log, the event history, the payment-failure switch and everything under `/api/admin` except `/api/admin/health`, reads and writes alike.
- **The catalogue is read by everyone and written by an `ADMIN`**: `GET /api/products` is open; creating a product, changing its price or stock, discontinuing it and receiving a shipment into the inventory are for an `ADMIN`.
- **A request is read before the caller is decided.** A request without a body, with malformed JSON or without a required field is `400` for every caller, in all three apps, and so is a query parameter that cannot be read: a `limit`, `offset` or `eventOffset` that is not a whole number, or a required `sagaId` or `eventOffset` that is missing. A well-formed request from a caller the rule refuses is `403`.
- **A command refused for a reason that passes is `503`**, in all three apps: an open circuit breaker on the command bus, a bus that is shutting down or admits no more commands, an aggregate lock that could not be taken within the lock timeout, or a version conflict on every attempt of the bus's retries. The answer carries `Retry-After: 5` and a JSON body naming the reason, e.g. `{"reason": "CIRCUIT_BREAKER_OPEN", "message": "..."}` (`CIRCUIT_BREAKER_OPEN`, `COMMAND_BUS_OVERLOADED`, `COMMAND_BUS_CLOSED`, `AGGREGATE_LOCK_UNAVAILABLE`, `AGGREGATE_VERSION_CONFLICT`); the same request succeeds later. A genuine failure, a store error say, is `500`.
- **A command for an id nothing was created under is `400` and writes nothing**: a profile update, an export or a forget for a customer who never registered (`Customer not found`), a ship or deliver for an order nobody placed, a price, stock or discontinue for a product nobody created. Cancelling is the exception: `CancelOrder` is the fulfillment saga's compensation and is accepted for any order that is not shipped or delivered. Every cancellation releases the order's stock and voids or refunds its payment.

### Products

| Method | Path | Description | Who |
|--------|------|-------------|-----|
| `POST` | `/api/products` | Create product | `ADMIN` |
| `PUT` | `/api/products/{id}/stock` | Adjust stock by `quantity`. With an `Idempotency-Key` header the change is applied once per key (a repeat answers the first request's result), and a failed adjustment is replayed by the dead-letter runner on its schedule | `ADMIN` |
| `PUT` | `/api/products/{id}/price` | Update price | `ADMIN` |
| `POST` | `/api/products/{id}/discontinue` | Discontinue product | `ADMIN` (`@RequireRole`) |
| `POST` | `/api/inventory/{productId}/receive` | Receive a shipment into the inventory the fulfillment saga reserves from; takes an `Idempotency-Key` like the stock adjustment | `ADMIN` |
| `GET` | `/api/products/{id}` | Get product | anyone: the public catalogue |
| `GET` | `/api/products` | List products | anyone: the public catalogue |

### Orders

| Method | Path | Description | Who |
|--------|------|-------------|-----|
| `POST` | `/api/orders` | Place order. Each line names a product and a quantity; the price is the product's in the catalogue (its event stream), and a product nobody created or one that is discontinued answers `400`. One line per product. The fulfillment saga confirms the order: there is no endpoint for it, and a `ConfirmOrder` sent by anybody but the saga is refused | `ADMIN`, or the customer the order names in `customerId` |
| `POST` | `/api/orders/{id}/cancel` | Cancel order. Accepted for any order that is not shipped or delivered (a shipped order answers `400`); every cancellation releases the order's stock and voids its payment, or refunds it in full once captured | `ADMIN` or the order's customer |
| `POST` | `/api/orders/{id}/ship` | Ship order | `ADMIN` (`@RequireRole`) |
| `POST` | `/api/orders/{id}/deliver` | Deliver order | `ADMIN` (`@RequireRole`) |
| `GET` | `/api/orders/{id}` | Get order, with its payment status (`paymentStatus`: `PENDING`, `CAPTURED`, `REFUNDED`, `VOIDED` or `FAILED`; `null` until the payment is initiated) | `ADMIN` or the order's customer; `403` otherwise, also for an id without a row |
| `GET` | `/api/orders` | List orders; `?customerId=` narrows the list | `ADMIN`: every order. Any other authenticated caller: their own orders. `403` without an identity |
| `GET` | `/api/saga/fulfillments/{id}` | The fulfillment saga of an order, id `fulfillment-<orderId>`: order, customer, payment id, lines, total and fulfillment status | `ADMIN` or the order's customer; `403` otherwise, also for an order without a row |
| `GET` | `/api/saga/fulfillments?limit=50` | List the fulfillment sagas with the framework's saga status | `ADMIN`: every fulfillment saga. Any other authenticated caller: the sagas of their own orders. `403` without an identity |

### Customers

| Method | Path | Description | Who |
|--------|------|-------------|-----|
| `POST` | `/api/customers` | Register customer | `ADMIN`, or the customer registering themself (`customerId` equal to `X-User-Id`) |
| `PUT` | `/api/customers/{id}` | Update profile | `ADMIN` or the customer themself |
| `GET` | `/api/customers/{id}` | Get customer; `404` once the customer is forgotten | `ADMIN` or the customer themself |
| `GET` | `/api/customers` | List customers | `ADMIN`: every customer. Any other authenticated caller: their own record. `403` without an identity |
| `POST` | `/api/customers/{id}/export-data` | GDPR data export request | `ADMIN` or the customer themself |
| `POST` | `/api/customers/{id}/forget` | GDPR forget (`200` only when fully erased, else `500` — send it again) | `ADMIN` or the customer themself |

### Operations

All of these answer `403` to everyone but an `ADMIN` (the role together with `X-User-Id`), except the health answer.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/audit/commands?limit=100` | The command audit log: who sent which command to which aggregate |
| `GET` | `/api/admin/circuit-breaker`, `/api/admin/payment-failure` | Breaker states and the payment-failure flag |
| `GET` | `/api/admin/dead-letters`, `/api/admin/outbox`, `/api/admin/outbox/failed`, `/api/admin/saga-dead-letters`, `/api/admin/sagas/faulted` | Dead-lettered commands, outbox entries, quarantined saga events, faulted sagas |
| `POST` | `/api/admin/dead-letters/{id}/retry` | Replays one dead-lettered command: `200` `REPLAYED`. A command that ran without an idempotency key (or whose key may be gone from the inbox) answers `409` with the reason until it is sent with `?force=true`, which the audit log records under the caller (`DLQ_FORCED_REPLAY`) before the replay runs; `404` without an entry, `503` with `Retry-After` while the bus admits nothing more. The dead-letter runner replays keyed entries on its own schedule and leaves unkeyed ones to this endpoint |
| `DELETE` | `/api/admin/dead-letters/{id}` | Discards a dead-lettered command whose effect is present (its caller already retried it), so that nothing replays it: `200` `DISCARDED`, `404` without an entry. Recorded in the audit log under the caller (`DLQ_DISCARDED`) before the entry is removed |
| `POST`, `DELETE` | `/api/admin/…`, `/api/saga/fulfillments/{id}/inject-failure` | The other recovery operations and the payment-failure toggle (tutorial chapters 11 to 14) |
| `GET` | `/api/admin/health` | `{"status":"UP"}`, open to every caller |

### SSE & Events

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/events/sse` | Live event feed (SSE), open to every caller, from the head of the global stream: a frame for every event stored while the client is connected, none for the events before. At most 100 connections at once; one more is answered `503` with `Retry-After`; a `HEAD` gets the headers and takes none. A frame holds the event's global offset, the aggregate type, the event type and the timestamp. It names no stream and carries no payload: no caller learns a customer id or an order id from it |
| `GET` | `/api/sse/{aggregateType}/{aggregateId}` | Live events of one stream (SSE). The framework's endpoint (`streamrune.sse.enabled`), fed by the framework from the head of the global stream: live, at-most-once, no replay. A frame carries the event **with its payload**, so the demo's `SseAuthorizer` lets in only an authenticated `ADMIN` or the customer the stream belongs to (their own `customer` stream, the streams of their own orders); `403` for everyone else. An order belongs to the customer it names, and only that customer or an `ADMIN` may place it |
| `GET` | `/api/events?offset=0&limit=10` | Paginated event history with payloads. ADMIN only, `403` otherwise |
| `GET` | `/api/events/{aggregateType}/{aggregateId}` | One stream's events with payloads. ADMIN only, `403` otherwise |

> **Demo shortcut, protect this in production.** The two history endpoints return each event as the event store reads it, which means `@Encrypted` fields come back **decrypted** until the subject's key is erased. They check the ADMIN role, but that role is the client-supplied `X-User-Role` header (see above), so the check documents the intent and protects nothing. Put an endpoint like this behind real authentication, or do not ship it. The per-stream SSE endpoint carries decrypted payloads too. Its `SseAuthorizer` applies the rule a real deployment keeps — an `ADMIN`, or the stream's owner — but to the same client-supplied `X-User-Id` and `X-User-Role` headers, so it is only as strong as whatever sets them. See [Chapter 15 of the tutorial](docs/tutorial/15-observability.md).

When an app stops, the open streams end with it. The open feed's readers end within a second of the stop in all three apps, so a connected browser does not hold a shutdown; in the Spring app the controller ends them before the web server waits for the requests in flight. The streams of the per-stream endpoint end as follows. The Spring app completes them before its web server stops, and so does the Quarkus app, because it is built with `quarkus.shutdown.delay-enabled=true` (a build-time property in `quarkus-app/src/main/resources/application.properties`; without it Quarkus stops its HTTP server first and a client sees its connection cut). On the Micronaut app the server closes the connections as it stops. An `EventSource` reconnects in every case.

### Example

```bash
# Create a product
curl -X POST http://localhost:8080/api/products \
  -H "Content-Type: application/json" \
  -H "X-User-Id: admin-1" \
  -H "X-User-Role: ADMIN" \
  -d '{"productId":"p1","name":"Widget","description":"A widget","category":"General","price":9.99,"initialStock":100}'

# Place an order as the customer it names
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-User-Id: cust-1" \
  -H "X-User-Role: CUSTOMER" \
  -d '{"orderId":"o1","customerId":"cust-1","lines":[{"productId":"p1","quantity":2}]}'

# Read it as that customer; without the two headers, or as another customer, the answer is 403
curl http://localhost:8080/api/orders/o1 -H "X-User-Id: cust-1" -H "X-User-Role: CUSTOMER"

# The customer's list holds their own orders; an ADMIN's holds everyone's
curl http://localhost:8080/api/orders -H "X-User-Id: cust-1" -H "X-User-Role: CUSTOMER"
curl http://localhost:8080/api/orders -H "X-User-Id: admin-1" -H "X-User-Role: ADMIN"
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
a product with a 1.5 MiB description written and read back, and an order its fulfillment saga takes
to `COMPLETED`. The replay check proves that the
dead-letter runner's `registerCommand(InventoryCommand.class)` expanded the sealed root in the
native image: GraalVM lists a sealed type's permitted subclasses only when the sealed type itself
is registered for reflection, which `EcommerceNativeHints` does through
`StreamRuneRuntimeHints.registerDomainPackages`. The saga check proves that the binary writes and
reads the saga origin in an event's metadata: without it the replies to the saga's commands would
reach no saga. After the checks the script stops the binary with
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

Creating the schema at startup takes no setting of the app's own. Flyway finds parts of itself at
run time (its plugins with `ServiceLoader`, its log creator by class name, the fields of its
configuration extensions), and `streamrune-quarkus` ships the native-image metadata for them, along
with the build argument that initializes Flyway's `InsertRowLock` at run time. The smoke test starts
the binary on an empty database and requires the schema to exist afterwards, so it fails if any of
that is missing.

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
sealed supertype of a registered record is listed. Creating the schema at startup takes no
metadata of the app's own: `streamrune-micronaut` registers the Flyway configuration extensions
that Flyway copies reflectively, and its log creator. The build sets
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
