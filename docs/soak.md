# Soak test

`scripts/soak/soak.py` runs the demo for hours under a steady mix of commands, queries and live
streams, breaks its infrastructure on a schedule, and checks at the end that nothing was lost,
applied twice, left stuck or leaked. The integration tests prove each behaviour for a minute; the
soak test shows that the behaviours hold together over hours, across three backends of two
frameworks on one database, while the database, the broker and the backends fail.

It is not part of CI: a meaningful run takes two hours or more. A five-minute smoke run
(`--duration 5m`) exercises every part of the harness on a compressed failure schedule.

## Running it

From the repository root, with Docker running, a JDK 25 (`JAVA_HOME` or `java` on the `PATH`) and
Python 3.10 or later (the script uses the standard library only):

```bash
./gradlew :spring-app:bootJar :quarkus-app:quarkusBuild
python3 scripts/soak/soak.py --duration 5m      # smoke run, about 12 minutes in all
python3 scripts/soak/soak.py --duration 2h      # soak run
```

The script needs ports 5432, 5672, 15672, 8080, 8082 and 8091 free. It starts PostgreSQL and
RabbitMQ from `docker-compose.yml` under the Compose project `streamrune-soak` on an empty database,
starts the backends, runs the load and the failure schedule, waits for the system to settle, checks
the invariants and stops everything again (`--keep-stack` leaves PostgreSQL and RabbitMQ running,
so `python3 scripts/soak/soak.py check build/soak/run-<id>` can run the checks again). It prints one
line per invariant and exits with 0 when all of them hold, 1 when one fails and 2 when the run could
not start.

| Option | Default | Effect |
|---|---|---|
| `--duration` | `5m` | How long the load runs. Below 30 minutes the failures come on the compressed schedule. |
| `--rate` | `30` | Commands per second. |
| `--query-rate` | `10` | Queries per second beside the commands. |
| `--sse-readers` | `3` | Live streams (`GET /api/sse/order/{id}`) held open at a time. |
| `--drain-timeout` | `10m` | How long the script waits after the load for the system to settle. |
| `--heap` | `768m` | `-Xmx` of each backend. |
| `--quarkus-pool-size` | `20` | `quarkus.datasource.jdbc.max-size` of the Quarkus backend; `0` keeps the Quarkus default. |
| `--no-chaos` | off | Load only, no failures. |
| `--reuse-db`, `--keep-stack`, `--out` | off | Keep an earlier database, leave the stack running, write the files elsewhere than `build/soak`. |

## Topology

One PostgreSQL 17 and one RabbitMQ, and on the one database three backends, each a JVM process the
script starts from the built JARs:

| Backend | Port | Built from |
|---|---|---|
| `spring-1` | 8080 | `spring-app/build/libs/spring-app-0.1.0-SNAPSHOT.jar` |
| `spring-2` | 8091 | the same JAR, `--server.port=8091` |
| `quarkus` | 8082 | `quarkus-app/build/quarkus-app/quarkus-run.jar` |

Each backend starts with `MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS=always`, so its health shows
which subscription it leads (Chapter 17). The three take the aggregate lock in PostgreSQL
(`PgAdvisoryLocker`), so a command on one backend waits for a command on the same aggregate on
another; one of them leads the order-fulfillment saga and each process manager, and the others
stand by.

**The connection budget.** PostgreSQL allows 100 connections. A Spring backend opens two pools of
ten (the application's and the lock's) and a few listener connections. The Quarkus backend sizes
its lock pool after its application pool, `quarkus.datasource.jdbc.max-size`, and Quarkus' own
default for that key is 50: one Quarkus backend with its default holds 50 lock connections from
the start and up to 50 more under load, which together with two Spring backends is more than 100.
The script therefore starts the Quarkus backend with `QUARKUS_DATASOURCE_JDBC_MAX_SIZE=20`, so the
three backends hold at most about 90 connections (`--quarkus-pool-size 0` keeps the default).

The notifications service is not started. The script binds a queue of its own, `soak.audit`, to the
`streamrune.events` exchange (`integration.#`) before the backends start, reads it every 20 seconds
through RabbitMQ's management API and records the message id (the outbox entry id) of every
message, so it counts how often each outbox entry reached the broker. It empties the demo's own
queue, `streamrune.integration`, which nothing consumes in a soak run.

## The load

A generator sends `--rate` commands per second to the three backends in turn; a backend that refuses
the connection (it is down) passes the command to the next one, as a load balancer would. The mix:

| Share | Command | Notes |
|---|---|---|
| 45 % | `POST /api/orders` as the customer | one or two lines; see the order intents below |
| 12 % | `POST /api/customers` as the new customer | |
| 10 % | `POST /api/inventory/{id}/receive` as an `ADMIN` | keyed: the bus runs it once per `Idempotency-Key` |
| 6 % | `PUT /api/products/{id}/stock` as an `ADMIN` | keyed |
| 4 % | `PUT /api/customers/{id}` | |
| 3 % | `PUT /api/products/{id}/price` | |
| 2 % | `POST /api/products`, then a keyed receipt of its first stock | the receipt is sent again under its key until it is applied; orders use the product only then |

Every command carries an `Idempotency-Key` (a new UUID per operation). One in seven keyed commands is
sent a second time with the same key and body, 0.05 to 5 seconds later, and one in twenty
creations (customer, product, order) is sent again with the same id: the keyed endpoints answer the
repeat with the recorded result, and a creation answers `400` because the id exists.

Each order gets an intent: 70 % run to the end untouched (*healthy*), 8 % are cancelled by their
customer one to three seconds after they are placed, 8 % are cancelled once they are confirmed
(the cancellation refunds the payment and releases the stock), 12 % are shipped and then
delivered by an `ADMIN` once confirmed, and 2 % ask for more than any product holds, so their saga
compensates. Beside the commands, two readers query orders, products and customers (`--query-rate`),
and `--sse-readers` threads each hold a live stream of a recent order for a minute at a time.

Every command is recorded in `requests.jsonl`: time, operation, method, path, the backend that
answered, status, latency, `Idempotency-Key`, entity id, intent, whether it is a re-send, and for
a refusal its `reason` and `commandId`.

## The failure schedule

Every failure is recorded with its start and end in `chaos.jsonl`.

| Failure | Soak schedule | Smoke schedule (share of the duration) |
|---|---|---|
| The backend that leads the order-fulfillment saga killed with `SIGKILL`, started again 30 to 60 s later | every 15 min from 7.5 min | at 25 % |
| RabbitMQ stopped (`docker compose stop`) for 2 min | every 30 min from 12 min | at 40 %, 8 % long |
| PostgreSQL paused (`docker pause`) for 20 s | every 40 min from 20 min | at 55 %, 10 s |
| The payment gateway's outage switched on in every backend for 3 min | every 25 min from 5 min | at 12 %, 10 % long |
| The payment gateway declining every payment in every backend for 20 s | every 4 min from 2 min | at 8 %, 15 s |
| Every backend stopped with `SIGTERM` and started again, one after the other | once, at half time | at 65 % |

A backend that starts again gets the gateway switches that are on at that moment. Failures that
would end later than three minutes before the load stops are left out.

## The invariants

After the load the script waits, up to `--drain-timeout`, until every saga of the run has ended,
every subscription is at the head of the event store, the outbox holds nothing pending and every
`503 QUEUED_FOR_REPLAY` command is in the inbox. It then checks:

1. **Creations applied exactly once.** Every customer, product and order answered with `2xx` has
   exactly one creation event; none has two, whatever the answers were.
2. **Keyed commands applied once per key.** Every key answered with `2xx` is in `command_inbox`,
   a key sent twice is answered the same way, and the number of `ShipmentReceived` and
   `StockAdjusted` events of every product equals the number of its inbox entries.
3. **Every `503 QUEUED_FOR_REPLAY` applied or awaiting an operator.** Its key is in the inbox, or
   its entry is still in `dead_letter_queue`.
4. **No `500` without a dead-letter entry.** Every `500` carries `AWAITING_OPERATOR` and the
   `commandId` of an entry in `dead_letter_queue`.
5. **Every saga terminal.** Every order-fulfillment saga of the run is `COMPLETED`, `COMPENSATED` or
   `FAILED`.
6. **No healthy flow compensated.** Every healthy order is `COMPLETED`, unless its payment failed
   inside a decline window; every order beyond the stock is compensated.
7. **Money and stock consistent**, by `scripts/money-and-stock-invariants.sql`, for the orders of
   the run.
8. **Outbox drained, every entry published once.** No outbox entry is pending, in progress or
   failed; every delivered entry reached `soak.audit`; an entry that reached it twice did so
   within five minutes of a failure (the relay publishes at least once: a backend killed between
   the broker's confirmation and the entry's update publishes it again).
9. **Projections equal to a rebuild.** Every order's status and payment status, every product's
   stock and price, every inventory's available, reserved and committed counts and every customer
   in the read model equal what the event store says, and every subscription is at its head.
10. **Every dead letter explained.** Every entry of `dead_letter_queue`, `saga_dead_letters` and
    `projection_dead_letters` was written during a failure or within a minute after it.
11. **No steady growth of a resource.** Every minute the script samples each backend's heap, live
    data after GC, resident memory, threads, the active and pending connections of both pools, the
    outbox and dead-letter gauges, and PostgreSQL's connections. A series whose every third is more
    than 20 % above the one before, with the last third's minimum above the first third's maximum,
    fails the check (per backend process, so a restart starts a new series). The samples of the first ten minutes and those within three minutes of a failure other than a decline window are left out.
12. **Every `ERROR` line inside a failure window.** An `ERROR` line of a backend's log during a
    failure, or within two minutes after it or after a restart, is expected; any other fails the
    check, and the summary groups them by message.
13. **Periodic checks.** Every ten minutes during the load (every two in a smoke run): no aggregate
    created twice and no payment captured twice.

## The files of a run

`build/soak/run-<id>/` holds `run.json` (parameters and timings), `requests.jsonl`,
`queries.jsonl`, `sse.jsonl`, `chaos.jsonl`, `schedule.json`, `lifecycle.jsonl` (every start, stop
and kill of a backend, and the drain), `metrics.jsonl`, `received.jsonl` (the messages read from
`soak.audit`), `checks.jsonl`, `logs/<backend>.<n>.log` (one file per process) and
`summary.json`, the result of every check with its evidence.
