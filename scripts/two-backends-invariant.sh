#!/usr/bin/env bash
# Two backends on one database: one leads the order-fulfillment saga, the other stands by. Places
# orders on both — one in three with two lines — cancels one in four right after placing it and one
# in five once it is confirmed, ships one in seven once it is confirmed and checks that its
# customer's cancellation is then refused, kills the leader with SIGKILL halfway, waits until the
# other backend leads and restarts the killed one. Then it waits until every order-fulfillment saga
# has ended and the four order-flow subscriptions have caught up with the event store, and runs
# scripts/money-and-stock-invariants.sql: no forward command of a saga stored after that saga's
# first compensation command, and the money and the stock of every order consistent with it.
#
# Usage (from the repository root, with the JARs built as the README's Quick Start shows):
#   MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS=always APP_SAGA_FULFILLMENT_TIMEOUT=1500ms \
#     APP_SAGA_TIMEOUT_POLL_INTERVAL=200ms APP_CHAOS_SAGA_COMMAND_DELAY_MAX=300ms \
#     docker compose --profile two-backends up -d --build
#   scripts/two-backends-invariant.sh
#   docker compose --profile two-backends down -v
#
# The script reads which backend leads from /actuator/health, whose details list the saga
# subscription with its role (LEADING or STANDBY) only with MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS
# set to always; without it the script stops at the start and says so.
#
# With the defaults (a 15-minute deadline, no delay) no saga reaches its deadline and the checks
# hold without a deadline in the run; the three variables above make some deadlines pass while a
# forward command is in flight. Every compensation cancels the order, and the cancellation's undo
# releases the order's stock and voids or refunds its payment whatever the saga row recorded.
# ORDERS (default 60) sets the number of orders, PAUSE_SECONDS (default 1) the pause
# between two of them. Exit status: 0 when the checks hold, 1 when one does not, a request the
# script sends is answered with an unexpected status, or the sagas and subscriptions did not
# settle within five minutes.

set -euo pipefail

cd "$(dirname "$0")/.."

ORDERS="${ORDERS:-60}"
PAUSE_SECONDS="${PAUSE_SECONDS:-1}"
FIRST=http://localhost:8080
SECOND=http://localhost:8091
SAGA_TYPE=org.streamrune.ecommerce.commands.saga.OrderFulfillmentState
RUN="$(date +%s)"
ADMIN=(-H "X-User-Id: admin-1" -H "X-User-Role: ADMIN")
FAILURES="$(mktemp)"
trap 'rm -f "$FAILURES"' EXIT

# Fulfillment sagas of this run that have not ended, plus the order-flow subscriptions whose
# checkpoint is behind the last event: one statement, so that a checkpoint saved after its page and
# the page's own appends cannot pass the comparison in between.
NOT_YET_QUIET="
SELECT (SELECT count(*) FROM saga_state
         WHERE saga_type = '$SAGA_TYPE' AND saga_id LIKE 'fulfillment-two-backends-$RUN-%'
           AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED'))
     + (SELECT count(*)
          FROM (VALUES ('order-fulfillment-saga'), ('payment-process-manager'),
                       ('order-cancellation'), ('order-shipment')) AS s(name)
          LEFT JOIN projection_offset p ON p.projection_name = s.name
         WHERE coalesce(p.last_offset, 0)
             < (SELECT coalesce(max(global_offset), 0) FROM event_stream))"

# Runs one statement; a failing psql ends the script with status 1, like a failed check.
sql() {
  if ! docker compose exec -T postgres psql -U postgres -d streamrune_ecommerce -v ON_ERROR_STOP=1 -tA -c "$1"; then
    echo "✗ psql failed: $1" >&2
    exit 1
  fi
}

post() {
  local url="$1" body="$2"
  shift 2
  curl -sf -m 30 -X POST "$url" -H "Content-Type: application/json" "$@" -d "$body" > /dev/null
}

# The HTTP status of a request, from whichever backend answers (the leader may be down).
request_any() {
  local method="$1" path="$2" user="$3" role="$4" body="${5:-}" code backend
  for backend in "$FIRST" "$SECOND"; do
    code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' -X "$method" "$backend$path" \
      -H "Content-Type: application/json" -H "X-User-Id: $user" -H "X-User-Role: $role" \
      ${body:+-d "$body"}) || code=000
    [ "$code" != 000 ] && { echo "$code"; return; }
  done
  echo 000
}

# The order's status as its customer reads it, from whichever backend answers.
status_of() {
  local oid="$1" customer="$2" backend
  for backend in "$FIRST" "$SECOND"; do
    curl -sf -m 5 "$backend/api/orders/$oid" -H "X-User-Id: $customer" -H "X-User-Role: CUSTOMER" \
      | sed -n 's/.*"status":"\([A-Z]*\)".*/\1/p' && return
  done
}

# Waits (at most 60 s) for CONFIRMED or CANCELLED; prints the last status seen.
await_confirmed() {
  local oid="$1" customer="$2" status="" waited=0
  while [ "$waited" -lt 60 ]; do
    status=$(status_of "$oid" "$customer")
    case "$status" in CONFIRMED|CANCELLED) break ;; esac
    sleep 1; waited=$((waited + 1))
  done
  echo "${status:-unknown}"
}

# The customer cancels as soon as the read model knows the order (it answers 403 until then).
cancel_soon() {
  local oid="$1" customer="$2" code waited=0
  while :; do
    code=$(request_any POST "/api/orders/$oid/cancel" "$customer" CUSTOMER '{"reason":"changed my mind"}')
    [ "$code" = 200 ] && return
    if [ "$code" != 403 ] || [ "$waited" -ge 10 ]; then
      echo "cancel of $oid right after placing answered $code" >> "$FAILURES"; return
    fi
    sleep 1; waited=$((waited + 1))
  done
}

cancel_when_confirmed() {
  local oid="$1" customer="$2" status code
  status=$(await_confirmed "$oid" "$customer")
  [ "$status" = CONFIRMED ] || return 0      # its deadline cancelled it first
  code=$(request_any POST "/api/orders/$oid/cancel" "$customer" CUSTOMER '{"reason":"changed my mind"}')
  [ "$code" = 200 ] || echo "cancel of confirmed $oid answered $code" >> "$FAILURES"
}

ship_then_try_cancel() {
  local oid="$1" customer="$2" status code
  status=$(await_confirmed "$oid" "$customer")
  [ "$status" = CONFIRMED ] || return 0
  code=$(request_any POST "/api/orders/$oid/ship" admin-1 ADMIN)
  case "$code" in
    200) code=$(request_any POST "/api/orders/$oid/cancel" "$customer" CUSTOMER '{"reason":"too late"}')
         [ "$code" = 400 ] || echo "cancel of shipped $oid answered $code, not 400" >> "$FAILURES" ;;
    400) ;;                                   # its deadline cancelled it between the read and the ship
    *)   echo "ship of $oid answered $code" >> "$FAILURES" ;;
  esac
}

# The order-fulfillment saga subscription's lease role on a backend, from its health details.
role_of() {
  curl -s -m 5 "$1/actuator/health" \
    | sed -n 's/.*"subscription\.order-fulfillment-saga":{[^}]*"role":"\([A-Z_]*\)".*/\1/p' \
    || true # a backend that does not answer has no role
}

# The compose service of a backend URL.
service_of() {
  if [ "$1" = "$FIRST" ]; then echo backend; else echo backend-2; fi
}

WAIT_SECONDS=180 scripts/wait-for-http.sh \
  "$FIRST/actuator/health" '"status":"UP"' "$SECOND/actuator/health" '"status":"UP"'

if [ -z "$(role_of "$FIRST")" ] || [ -z "$(role_of "$SECOND")" ]; then
  echo "✗ The health details do not show the saga subscription's role; start the backends with" \
    "MANAGEMENT_ENDPOINT_HEALTH_SHOW_DETAILS=always (see the usage above)." >&2
  exit 1
fi

# Two products of this run, each with stock for every order (two-line orders take one of each).
for p in 1 2; do
  product="two-backends-$RUN-$p"
  post "$FIRST/api/products" \
    "{\"productId\":\"$product\",\"name\":\"Two backends $p\",\"description\":\"Two-backend run\",\"category\":\"General\",\"price\":10.00,\"initialStock\":0}" \
    "${ADMIN[@]}"
  post "$FIRST/api/inventory/$product/receive" "{\"quantity\":$((2 * ORDERS))}" "${ADMIN[@]}"
done

echo "Placing $ORDERS orders, alternating $FIRST and $SECOND …"
for i in $(seq 1 "$ORDERS"); do
  if [ $((i % 2)) -eq 1 ]; then backend="$FIRST"; else backend="$SECOND"; fi
  customer="two-backends-cust-$((i % 5))"
  oid="two-backends-$RUN-$i"
  lines="{\"productId\":\"two-backends-$RUN-$((i % 2 + 1))\",\"quantity\":1}"
  if [ $((i % 3)) -eq 0 ]; then
    lines="$lines,{\"productId\":\"two-backends-$RUN-$(((i + 1) % 2 + 1))\",\"quantity\":1}"
  fi
  post "$backend/api/orders" \
    "{\"orderId\":\"$oid\",\"customerId\":\"$customer\",\"lines\":[$lines]}" \
    -H "X-User-Id: $customer" -H "X-User-Role: CUSTOMER"
  if [ $((i % 4)) -eq 0 ]; then
    cancel_soon "$oid" "$customer" &
  elif [ $((i % 5)) -eq 0 ]; then
    cancel_when_confirmed "$oid" "$customer" &
  elif [ $((i % 7)) -eq 0 ]; then
    ship_then_try_cancel "$oid" "$customer" &
  fi
  if [ "$i" -eq $((ORDERS / 2)) ]; then
    first_role=$(role_of "$FIRST")
    second_role=$(role_of "$SECOND")
    if [ "$first_role" = LEADING ] && [ "$second_role" != LEADING ]; then
      leader="$FIRST" survivor="$SECOND"
    elif [ "$second_role" = LEADING ] && [ "$first_role" != LEADING ]; then
      leader="$SECOND" survivor="$FIRST"
    else
      echo "✗ Expected exactly one backend to lead the saga: $FIRST is ${first_role:-unknown}," \
        "$SECOND is ${second_role:-unknown}." >&2
      exit 1
    fi
    killed=$(service_of "$leader")
    echo "Killing the leader, $killed ($leader), with SIGKILL after $i orders …"
    docker compose kill -s SIGKILL "$killed"
    killed_at=$SECONDS
    # A killed backend does not give its lease back: the other takes over once the lease has
    # expired (15 s) and it asks again (every 500 ms).
    until [ "$(role_of "$survivor")" = LEADING ]; do
      if [ $((SECONDS - killed_at)) -ge 60 ]; then
        echo "✗ $survivor did not take the saga over within 60 s of the kill" \
          "(its role: $(role_of "$survivor"))." >&2
        exit 1
      fi
      sleep 1
    done
    echo "  ✓ $survivor leads the saga $((SECONDS - killed_at)) s after the kill."
    # start, not up: the same container with the environment it was created with.
    docker compose --profile two-backends start "$killed"
    restarted="$leader"
    WAIT_SECONDS=180 scripts/wait-for-http.sh "$restarted/actuator/health" '"status":"UP"'
    echo "  ✓ $killed is back, as standby: $(role_of "$restarted")."
  fi
  sleep "$PAUSE_SECONDS"
done

wait
if [ -s "$FAILURES" ]; then
  sed 's/^/✗ /' "$FAILURES" >&2
  exit 1
fi

echo "Waiting until every order-fulfillment saga has ended and the order-flow subscriptions are at the head of the event store (at most 5 minutes) …"
deadline=$((SECONDS + 300))
while :; do
  behind=$(sql "$NOT_YET_QUIET")
  [ "$behind" -eq 0 ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "✗ $behind order-fulfillment saga(s) and subscription(s) have not settled (FAULTED counts as not ended):" >&2
    sql "SELECT saga_id, status FROM saga_state WHERE saga_type = '$SAGA_TYPE' AND saga_id LIKE 'fulfillment-two-backends-$RUN-%' AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')" >&2
    sql "SELECT s.name || ' at ' || coalesce(p.last_offset, 0) || ' of ' || (SELECT coalesce(max(global_offset), 0) FROM event_stream)
           FROM (VALUES ('order-fulfillment-saga'), ('payment-process-manager'), ('order-cancellation'), ('order-shipment')) AS s(name)
           LEFT JOIN projection_offset p ON p.projection_name = s.name" >&2
    exit 1
  fi
  sleep 2
done

echo "Order-fulfillment sagas of this run by status:"
sql "SELECT status, count(*) FROM saga_state WHERE saga_type = '$SAGA_TYPE' AND saga_id LIKE 'fulfillment-two-backends-$RUN-%' GROUP BY status ORDER BY status"
echo "Compensated at their deadline (an episode with no cause event):"
sql "SELECT count(*) FROM saga_state WHERE saga_type = '$SAGA_TYPE' AND saga_id LIKE 'fulfillment-two-backends-$RUN-%' AND status = 'COMPENSATED' AND episode_cause_event_id IS NULL"
echo "Orders of this run cancelled, shipped, with their payment refunded and with their payment voided:"
sql "SELECT (SELECT count(DISTINCT aggregate_id) FROM event_stream WHERE aggregate_type = 'order' AND event_type = 'OrderCancelled' AND aggregate_id LIKE 'two-backends-$RUN-%') || ' | ' ||
            (SELECT count(DISTINCT aggregate_id) FROM event_stream WHERE aggregate_type = 'order' AND event_type = 'OrderShipped' AND aggregate_id LIKE 'two-backends-$RUN-%') || ' | ' ||
            (SELECT count(DISTINCT aggregate_id) FROM event_stream WHERE aggregate_type = 'payment' AND event_type = 'PaymentRefunded' AND aggregate_id LIKE 'pay-two-backends-$RUN-%') || ' | ' ||
            (SELECT count(DISTINCT aggregate_id) FROM event_stream WHERE aggregate_type = 'payment' AND event_type = 'PaymentVoided' AND aggregate_id LIKE 'pay-two-backends-$RUN-%')"
for backend in "$FIRST" "$SECOND"; do
  # The meter exists once the first forward command was refused; until then the endpoint is 404.
  fenced=$(curl -sf -m 5 "$backend/actuator/metrics/streamrune.saga.forward_commands_fenced" \
    | sed -n 's/.*"value":\([0-9]*\).*/\1/p' || true)
  echo "Forward commands refused at their commit on $backend since it started: ${fenced:-0}"
done

echo "Checking money and stock (scripts/money-and-stock-invariants.sql) …"
if ! violations=$(docker compose exec -T postgres psql -U postgres -d streamrune_ecommerce \
    -v ON_ERROR_STOP=1 -tA -F ' | ' < scripts/money-and-stock-invariants.sql); then
  echo "✗ psql failed on the checks" >&2
  exit 1
fi
violations=$(printf '%s\n' "$violations" | grep -F "two-backends-$RUN-" || true)
if [ -n "$violations" ]; then
  printf '%s\n' "$violations" | sed 's/^/✗ /' >&2
  exit 1
fi
echo "✓ Money and stock end consistent with every order, and no forward command came after its saga's compensation."
