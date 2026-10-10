#!/usr/bin/env bash
# Native image smoke test for StreamRune e-commerce demo.
# Builds a native binary for the chosen framework, starts it against an empty Postgres database and
# RabbitMQ, and probes it: health, the schema the binary created at startup, a command-to-read-model
# round trip, a dead-lettered command replayed through its sealed root, and a read model larger than
# 1 MiB written and read back.
# It then stops the binary with SIGTERM, waits for it to exit, prints its output, and fails the run
# when that output (startup, requests and shutdown) holds a GraalVM UnsupportedFeatureError.
#
# Usage:
#   ./scripts/native-image-smoke-test.sh [spring|quarkus|micronaut|all]
#
# Requires: Docker (for Postgres), GraalVM JDK 25 on PATH, curl. For spring and micronaut on a
# GraalVM without lib/svm/schemas/reachability-metadata-schema.json (e.g. GraalVM CE 25.0.1), export
# ORG_GRADLE_PROJECT_graalvmMetadataRepository first (README, "GraalVM Native Image").
# The binary's output goes to build/native-smoke-<framework>-app.log and is printed after it stops.
#
# SMOKE_PG_CONTAINER=<name-or-id> reuses an already-running Postgres container published on
# localhost:5433 (CI passes its `postgres` service container) instead of starting and removing
# its own. Its streamrune_ecommerce database must be empty: every binary has to create the schema
# itself, so a reused container supports one framework per run (`all` is refused).
# SMOKE_RABBIT_CONTAINER=<name-or-id> does the same for the RabbitMQ broker on localhost:5672
# that all three binaries need.
# SMOKE_SKIP_BUILD=1 smoke-tests the binary an earlier step already built instead of building the
# native image again.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS=()
FAILED=0
PG_CONTAINER="${SMOKE_PG_CONTAINER:-sr-smoke-pg}"
PG_EXTERNAL="${SMOKE_PG_CONTAINER:+1}"  # set when the caller owns the container (never removed)
PG_PORT=5433  # non-standard to avoid clashing with local Postgres
RABBIT_CONTAINER="${SMOKE_RABBIT_CONTAINER:-sr-smoke-rabbit}"
RABBIT_EXTERNAL="${SMOKE_RABBIT_CONTAINER:+1}"
RABBIT_STARTED=""

# ── helpers ──────────────────────────────────────────────────────────────────

cleanup() {
  local pid_file="$ROOT/build/native-smoke-pid"
  if [ -f "$pid_file" ]; then
    local pid
    pid=$(<"$pid_file")
    kill "$pid" 2>/dev/null || true
    rm -f "$pid_file"
  fi
  if [ -z "$PG_EXTERNAL" ]; then
    docker rm -f "$PG_CONTAINER" 2>/dev/null || true
  fi
  if [ -n "$RABBIT_STARTED" ]; then
    docker rm -f "$RABBIT_CONTAINER" 2>/dev/null || true
    RABBIT_STARTED=""
  fi
}
trap cleanup EXIT

start_rabbitmq() {
  # The Spring app's outbox publisher (RabbitMqConfig#outboxPublisher) opens its AMQP channel while
  # the context starts, and AOT fixes that bean into the native image at build time, so the binary
  # does not start without a broker on localhost:5672. The Quarkus app's publisher
  # (RabbitMqProducer#outboxPublisher) connects at startup too, when the framework creates the
  # outbox poller, and so does the Micronaut app's (RabbitMqFactory#rabbitMqChannel, behind the
  # framework's outbox poller).
  if [ -n "$RABBIT_EXTERNAL" ]; then
    echo "▶ Using the running RabbitMQ container $RABBIT_CONTAINER (port 5672) …"
  else
    echo "▶ Starting RabbitMQ on port 5672 …"
    docker rm -f "$RABBIT_CONTAINER" 2>/dev/null || true
    docker run -d --name "$RABBIT_CONTAINER" -p 5672:5672 rabbitmq:4-management-alpine >/dev/null
    RABBIT_STARTED=1
  fi
  # Readiness from the broker's own log, not `docker exec rabbitmq-diagnostics`: run as root before
  # the broker has written its Erlang cookie, the CLI creates a root-owned cookie the broker can
  # then not read (eacces), and the broker crashes on start.
  for i in $(seq 1 90); do
    if docker logs "$RABBIT_CONTAINER" 2>&1 | grep -q 'Server startup complete'; then
      echo "  RabbitMQ ready."
      return 0
    fi
    sleep 1
  done
  echo "  ✗ RabbitMQ failed to start."
  return 1
}

start_postgres() {
  if [ -n "$PG_EXTERNAL" ]; then
    echo "▶ Using the running Postgres container $PG_CONTAINER (port $PG_PORT) …"
  else
    echo "▶ Starting Postgres on port $PG_PORT …"
    docker rm -f "$PG_CONTAINER" 2>/dev/null || true
    docker run -d --name "$PG_CONTAINER" \
      -e POSTGRES_DB=streamrune_ecommerce \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=postgres \
      -p "$PG_PORT":5432 \
      postgres:17 >/dev/null
  fi

  # Wait for the server on TCP. The postgres image first runs a temporary server for its init
  # phase, on the Unix socket only, and then restarts; a readiness check over the socket could
  # answer during that phase and let the binary connect into the restart.
  for i in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
      echo "  Postgres ready."
      # The binary must find an empty database: it creates the schema itself at startup, and
      # schema_smoke below requires that it did.
      local tables
      if ! tables=$(pg_sql "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'"); then
        echo "  ✗ Could not query the streamrune_ecommerce database."
        return 1
      fi
      if [ "$tables" != "0" ]; then
        echo "  ✗ The streamrune_ecommerce database already holds $tables table(s); it must be empty."
        return 1
      fi
      return 0
    fi
    sleep 1
  done
  echo "  ✗ Postgres failed to start."
  return 1
}

wait_for_health() {
  local url="$1"
  local timeout="${2:-60}"
  echo "  Waiting for $url (${timeout}s timeout) …"
  for i in $(seq 1 "$timeout"); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "  ✓ Health check passed (${i}s)."
      return 0
    fi
    sleep 1
  done
  echo "  ✗ Health check timed out after ${timeout}s."
  return 1
}

schema_smoke() {
  # The database was empty when the binary started, so every table it now holds was created by the
  # binary: the framework's event store factory applies its event-store and crypto migration series
  # at startup (streamrune.event-store.schema.auto-initialize=true). In a native image that only
  # works when the migration scripts were registered as resources at build time. Each series
  # records its baseline in its own Flyway history table; one table of each series is checked too.
  echo "  Checking the schema the binary created …"
  local applied
  for history in flyway_schema_history_streamrune flyway_schema_history_crypto; do
    if ! applied=$(pg_sql "SELECT count(*) FROM $history WHERE success AND script LIKE 'V001%'" 2>/dev/null) \
        || [ "$applied" != "1" ]; then
      echo "  ✗ $history does not record the applied baseline (got: ${applied:-no such table})."
      return 1
    fi
  done
  local present
  present=$(pg_sql "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'
      AND tablename IN ('event_stream', 'encryption_keys')")
  if [ "$present" != "2" ]; then
    echo "  ✗ event_stream and encryption_keys are not both present (found $present of 2)."
    return 1
  fi
  echo "  ✓ The binary created the event-store and crypto schema on the empty database."
  return 0
}

functional_smoke() {
  # Full CQRS round-trip against the running native binary: POST a product command,
  # then poll the read model until the projection exposes it. Proves the whole path
  # works in the native image — HTTP dispatch, JSON codec, event-store append,
  # projection runner, query read, response serialization — not just that the
  # process is alive (which is all the health check proves).
  local base="$1"
  local pid="smoke-$$-$(date +%s)"
  echo "  Running CQRS round-trip (product $pid) …"

  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/api/products" \
    -H 'Content-Type: application/json' \
    -H 'X-User-Role: ADMIN' -H 'X-User-Id: smoke' \
    -d "{\"productId\":\"$pid\",\"name\":\"SmokeWidget\",\"description\":\"smoke\",\"category\":\"Smoke\",\"price\":9.99,\"initialStock\":10}")
  if [[ ! "$code" =~ ^2 ]]; then
    echo "  ✗ POST /api/products returned $code"
    return 1
  fi

  for i in $(seq 1 20); do
    if curl -s "$base/api/products/$pid" | grep -q "SmokeWidget"; then
      echo "  ✓ Read model exposed the product after POST (${i}s)."
      return 0
    fi
    sleep 1
  done
  echo "  ✗ Read model never exposed product $pid (projection path broken)."
  return 1
}

pg_sql() {
  # One SQL statement against the smoke database; prints unaligned, tuples-only output.
  docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d streamrune_ecommerce \
    -tAq -c "$1"
}

dlq_sealed_root_replay_smoke() {
  # Sealed command-root probe. The dead-letter retry runner is given sealed ROOTS
  # (registerCommand(InventoryCommand.class), ...) and expands each into the commands it permits
  # with Class#getPermittedSubclasses(), because a dead-letter entry names the CONCRETE command
  # class. A native image answers that call only for a root registered for reflection; for any
  # other sealed root it reports isSealed() == true with NO permitted subclasses, and the framework
  # refuses such a root at startup. This proves the expansion in the binary: dead-letter a
  # ReceiveShipment (registered only through its root, InventoryCommand), replay it through the
  # admin endpoint, and require the replay to have run — the entry is discarded and the inventory
  # stream holds the ShipmentReceived event. An unresolved type would answer 200 too, but leave the
  # entry in place with a failed attempt and no event.
  local base="$1"
  local pid="dlq-smoke-$$-$(date +%s)"
  local cid="$pid-cmd"
  echo "  Running sealed-root DLQ replay probe (command $cid) …"

  if ! pg_sql "INSERT INTO dead_letter_queue (command_id, command_type, payload, aggregate_type,
        aggregate_id, error_type, error_message, attempts, first_attempt_at, user_id)
      VALUES ('$cid',
        'org.streamrune.ecommerce.domain.inventory.InventoryCommand\$ReceiveShipment',
        '{\"productId\":\"$pid\",\"quantity\":7}', 'inventory', '$pid',
        'java.lang.IllegalStateException', 'native smoke probe', 3, NOW(), 'smoke')" >/dev/null; then
    echo "  ✗ Could not insert the dead-letter entry."
    return 1
  fi

  # The retry endpoint is a mutating admin endpoint: it answers 403 without the ADMIN role, which
  # the demo reads from X-User-Role (its trusted-gateway stand-in, see AdminController).
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'X-User-Role: ADMIN' \
    "$base/api/admin/dead-letters/$cid/retry")
  if [ "$code" != "200" ]; then
    echo "  ✗ POST /api/admin/dead-letters/$cid/retry returned $code"
    return 1
  fi

  local left events
  left=$(pg_sql "SELECT count(*) FROM dead_letter_queue WHERE command_id = '$cid'")
  events=$(pg_sql "SELECT count(*) FROM event_stream
      WHERE aggregate_type = 'inventory' AND aggregate_id = '$pid'
        AND event_type = 'ShipmentReceived'")
  if [ "$left" != "0" ] || [ "$events" != "1" ]; then
    echo "  ✗ Replay did not run: entry left=$left, ShipmentReceived events=$events."
    pg_sql "SELECT 'dlq_attempts=' || dlq_attempts FROM dead_letter_queue
        WHERE command_id = '$cid'" || true
    return 1
  fi

  # The event explorer reads the same stream by its two key parts. It returns event payloads, so
  # it answers only to the ADMIN role.
  local body
  body=$(curl -s -H 'X-User-Role: ADMIN' "$base/api/events/inventory/$pid")
  if ! printf '%s' "$body" | grep -q '"aggregateType":"inventory"'; then
    echo "  ✗ GET /api/events/inventory/$pid did not return the typed stream: $body"
    return 1
  fi
  echo "  ✓ Dead-lettered InventoryCommand\$ReceiveShipment resolved through its sealed root and replayed."
  return 0
}

large_read_model_smoke() {
  # A read model larger than 1 MiB, written over HTTP and read back. Netty 4.2 (the Micronaut
  # server) does not pool a buffer above 1 MiB: it allocates it on its own and frees it as soon as
  # the request or response is done with it, and on Java 25 it frees it through
  # Arena.ofShared().close(). A GraalVM native image supports that call only when built with
  # -H:+SharedArenaSupport; without it the free throws UnsupportedFeatureError on the event loop and
  # the memory is never returned. Without the option the Micronaut binary answered this POST with
  # 500 (Micronaut's request-body composition hid the arena error behind an
  # IllegalReferenceCountException). Where such an exchange still succeeds, the output scan after
  # the binary stops catches the error. Either way the path runs while the application serves
  # requests, not only when its event loops free their buffers at shutdown.
  local base="$1"
  local pid="big-smoke-$$-$(date +%s)"
  local body="$ROOT/build/native-smoke-big-body.json"
  local min_bytes=1572864  # 1.5 MiB of description
  echo "  Running large read-model probe (product $pid, 1.5 MiB description) …"
  {
    printf '{"productId":"%s","name":"BigWidget","description":"' "$pid"
    head -c "$min_bytes" /dev/zero | tr '\0' 'x'
    printf '","category":"Smoke","price":1.00,"initialStock":1}'
  } > "$body"

  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/api/products" \
    -H 'Content-Type: application/json' \
    -H 'X-User-Role: ADMIN' -H 'X-User-Id: smoke' \
    --data-binary "@$body")
  rm -f "$body"
  if [[ ! "$code" =~ ^2 ]]; then
    echo "  ✗ POST /api/products (1.5 MiB body) returned $code"
    return 1
  fi

  local size
  for i in $(seq 1 20); do
    size=$(curl -s -o /dev/null -w '%{size_download}' "$base/api/products/$pid")
    if [ "${size%.*}" -gt "$min_bytes" ]; then
      echo "  ✓ Read model returned the product, ${size%.*} bytes (${i}s)."
      return 0
    fi
    sleep 1
  done
  echo "  ✗ Read model never returned the 1.5 MiB product $pid (last size: ${size:-none})."
  return 1
}

stop_binary() {
  # SIGTERM, then wait for the process to exit, so its shutdown runs and logs before the output is
  # scanned (the Micronaut binary's Netty event loops free their pooled direct buffers there); after
  # 30 s, SIGKILL.
  local pid_file="$ROOT/build/native-smoke-pid"
  [ -f "$pid_file" ] || return 0
  local pid
  pid=$(<"$pid_file")
  kill "$pid" 2>/dev/null || true
  for i in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "  ✗ The binary did not exit 30s after SIGTERM; killing it."
    kill -9 "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
  rm -f "$pid_file"
}

probe_and_stop() {
  # Runs every probe against the started binary, stops it, prints and scans its output, and
  # records the framework's result.
  local fw="$1" health_url="$2" base="$3" app_log="$4"
  local outcome="PASS"
  local detail="health + schema created at startup + CQRS round-trip + sealed-root DLQ replay + 1.5 MiB read model + clean output"
  if ! wait_for_health "$health_url"; then
    outcome="FAIL"; detail="health check timeout"
  elif ! schema_smoke; then
    outcome="FAIL"; detail="the binary did not create the schema"
  elif ! functional_smoke "$base"; then
    outcome="FAIL"; detail="CQRS round-trip failed"
  elif ! dlq_sealed_root_replay_smoke "$base"; then
    outcome="FAIL"; detail="sealed-root DLQ replay failed"
  elif ! large_read_model_smoke "$base"; then
    outcome="FAIL"; detail="large read-model probe failed"
  fi

  stop_binary
  echo "  ── $fw binary output ($app_log) ──"
  sed 's/^/  | /' "$app_log"
  echo "  ── end of $fw binary output ──"

  # A GraalVM UnsupportedFeatureError is a native-image gap a request or the shutdown ran into, even
  # when every probe above answered as expected.
  if grep -q 'UnsupportedFeatureError' "$app_log"; then
    echo "  ✗ The binary's output holds an UnsupportedFeatureError:"
    grep -m 3 'UnsupportedFeatureError' "$app_log" | sed 's/^/    /'
    if [ "$outcome" = "PASS" ]; then
      detail="UnsupportedFeatureError in the binary's output"
    else
      detail="$detail; UnsupportedFeatureError in the binary's output"
    fi
    outcome="FAIL"
  else
    echo "  ✓ No UnsupportedFeatureError in the binary's output (startup, requests, shutdown)."
  fi
  record "$fw" "$outcome" "$detail"
}

record() {
  local fw="$1" status="$2" detail="${3:-}"
  if [ "$status" = "PASS" ]; then
    RESULTS+=("✓ $fw: PASS${detail:+ ($detail)}")
  else
    RESULTS+=("✗ $fw: FAIL${detail:+ — $detail}")
    FAILED=$((FAILED + 1))
  fi
}

# ── framework runners ────────────────────────────────────────────────────────

smoke_spring() {
  echo ""
  echo "═══════════════════════════════════════════════"
  echo " Spring Boot native image"
  echo "═══════════════════════════════════════════════"

  start_postgres
  if ! start_rabbitmq; then
    record "spring" "FAIL" "RabbitMQ did not start"
    cleanup
    return
  fi

  if [ "${SMOKE_SKIP_BUILD:-}" = "1" ]; then
    echo "▶ Reusing the native image built by an earlier step (SMOKE_SKIP_BUILD=1) …"
  else
    echo "▶ Building native image (this takes a few minutes) …"
    if ! "$ROOT/gradlew" -p "$ROOT" \
        :spring-app:nativeCompile \
        -x test -x spotlessCheck --no-daemon; then
      record "spring" "FAIL" "nativeCompile failed"
      return
    fi
  fi

  local binary="$ROOT/spring-app/build/native/nativeCompile/spring-app"
  if [ ! -f "$binary" ]; then
    record "spring" "FAIL" "binary not found at $binary"
    return
  fi

  local app_log="$ROOT/build/native-smoke-spring-app.log"
  echo "▶ Starting native binary (output: $app_log) …"
  SPRING_DATASOURCE_URL="jdbc:postgresql://localhost:$PG_PORT/streamrune_ecommerce" \
  SPRING_DATASOURCE_USERNAME=postgres \
  SPRING_DATASOURCE_PASSWORD=postgres \
    "$binary" > "$app_log" 2>&1 &
  echo $! > "$ROOT/build/native-smoke-pid"

  probe_and_stop "spring" "http://localhost:8080/actuator/health" "http://localhost:8080" "$app_log"
  cleanup
}

smoke_quarkus() {
  echo ""
  echo "═══════════════════════════════════════════════"
  echo " Quarkus native image"
  echo "═══════════════════════════════════════════════"

  start_postgres
  if ! start_rabbitmq; then
    record "quarkus" "FAIL" "RabbitMQ did not start"
    cleanup
    return
  fi

  if [ "${SMOKE_SKIP_BUILD:-}" = "1" ]; then
    echo "▶ Reusing the native image built by an earlier step (SMOKE_SKIP_BUILD=1) …"
  else
    echo "▶ Building native image (this takes a few minutes) …"
    if ! "$ROOT/gradlew" -p "$ROOT" \
        :quarkus-app:build \
        -Dquarkus.native.enabled=true \
        -Dquarkus.package.jar.enabled=false \
        -x test -x spotlessCheck --no-daemon; then
      record "quarkus" "FAIL" "native build failed"
      return
    fi
  fi

  local binary
  binary=$(find "$ROOT/quarkus-app/build" -name "*-runner" -type f 2>/dev/null | head -1)
  if [ -z "$binary" ]; then
    record "quarkus" "FAIL" "native runner binary not found"
    return
  fi

  local app_log="$ROOT/build/native-smoke-quarkus-app.log"
  echo "▶ Starting native binary (output: $app_log) …"
  QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://localhost:$PG_PORT/streamrune_ecommerce" \
  QUARKUS_DATASOURCE_USERNAME=postgres \
  QUARKUS_DATASOURCE_PASSWORD=postgres \
    "$binary" > "$app_log" 2>&1 &
  echo $! > "$ROOT/build/native-smoke-pid"

  probe_and_stop "quarkus" "http://localhost:8082/q/health" "http://localhost:8082" "$app_log"
  cleanup
}

smoke_micronaut() {
  echo ""
  echo "═══════════════════════════════════════════════"
  echo " Micronaut native image"
  echo "═══════════════════════════════════════════════"

  start_postgres
  if ! start_rabbitmq; then
    record "micronaut" "FAIL" "RabbitMQ did not start"
    cleanup
    return
  fi

  if [ "${SMOKE_SKIP_BUILD:-}" = "1" ]; then
    echo "▶ Reusing the native image built by an earlier step (SMOKE_SKIP_BUILD=1) …"
  else
    echo "▶ Building native image (this takes a few minutes) …"
    if ! "$ROOT/gradlew" -p "$ROOT" \
        :micronaut-app:nativeCompile \
        -x test -x spotlessCheck --no-daemon; then
      record "micronaut" "FAIL" "nativeCompile failed"
      return
    fi
  fi

  local binary="$ROOT/micronaut-app/build/native/nativeCompile/micronaut-app"
  if [ ! -f "$binary" ]; then
    record "micronaut" "FAIL" "binary not found at $binary"
    return
  fi

  local app_log="$ROOT/build/native-smoke-micronaut-app.log"
  echo "▶ Starting native binary (output: $app_log) …"
  DATASOURCES_DEFAULT_URL="jdbc:postgresql://localhost:$PG_PORT/streamrune_ecommerce" \
  DATASOURCES_DEFAULT_USERNAME=postgres \
  DATASOURCES_DEFAULT_PASSWORD=postgres \
    "$binary" > "$app_log" 2>&1 &
  echo $! > "$ROOT/build/native-smoke-pid"

  probe_and_stop "micronaut" "http://localhost:8081/health" "http://localhost:8081" "$app_log"
  cleanup
}

# ── main ─────────────────────────────────────────────────────────────────────

TARGET="${1:-all}"

mkdir -p "$ROOT/build"

case "$TARGET" in
  spring)    smoke_spring ;;
  quarkus)   smoke_quarkus ;;
  micronaut) smoke_micronaut ;;
  all)
    if [ -n "$PG_EXTERNAL" ]; then
      echo "SMOKE_PG_CONTAINER supports one framework per run (each binary needs an empty database); pass spring, quarkus or micronaut."
      exit 1
    fi
    smoke_spring
    smoke_quarkus
    smoke_micronaut
    ;;
  *)
    echo "Usage: $0 [spring|quarkus|micronaut|all]"
    exit 1
    ;;
esac

echo ""
echo "═══════════════════════════════════════════════"
echo " Results"
echo "═══════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do
  echo "  $r"
done

if [ "$FAILED" -gt 0 ]; then
  echo ""
  echo "  $FAILED framework(s) failed."
  exit 1
else
  echo ""
  echo "  All passed."
  exit 0
fi
