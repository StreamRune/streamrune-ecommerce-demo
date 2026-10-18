#!/usr/bin/env bash
# Two backends on one database: one leads the order-fulfillment saga, the other stands by. Places
# orders on both, kills the leader with SIGKILL halfway, waits until the other backend leads,
# restarts the killed one, waits until every order-fulfillment saga has ended and checks that no
# forward command of a saga was stored after that saga's first compensation command.
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
# With the defaults (a 15-minute deadline, no delay) no saga reaches its deadline and the check
# holds trivially; the three variables above make some deadlines pass while a forward command is in
# flight. Such a run can leave cancelled orders whose stock stays reserved or whose payment stays
# captured: the demo's compensation works from the saga row (tutorial chapter 17 lists them with
# two queries); this script checks the order of commands, not that every compensation undid it all.
# ORDERS (default 60) sets the number of orders, PAUSE_SECONDS (default 1) the pause
# between two of them. Exit status: 0 when the check holds, 1 when it does not or a saga did not end
# within five minutes.

set -euo pipefail

cd "$(dirname "$0")/.."

ORDERS="${ORDERS:-60}"
PAUSE_SECONDS="${PAUSE_SECONDS:-1}"
FIRST=http://localhost:8080
SECOND=http://localhost:8091
SAGA_TYPE=org.streamrune.ecommerce.commands.saga.OrderFulfillmentState
RUN="$(date +%s)"
ADMIN=(-H "X-User-Id: admin-1" -H "X-User-Role: ADMIN")

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

# Two products of this run, each with stock for every order.
for p in 1 2; do
  product="two-backends-$RUN-$p"
  post "$FIRST/api/products" \
    "{\"productId\":\"$product\",\"name\":\"Two backends $p\",\"description\":\"Two-backend run\",\"category\":\"General\",\"price\":10.00,\"initialStock\":0}" \
    "${ADMIN[@]}"
  post "$FIRST/api/inventory/$product/receive" "{\"quantity\":$ORDERS}" "${ADMIN[@]}"
done

echo "Placing $ORDERS orders, alternating $FIRST and $SECOND …"
for i in $(seq 1 "$ORDERS"); do
  if [ $((i % 2)) -eq 1 ]; then backend="$FIRST"; else backend="$SECOND"; fi
  customer="two-backends-cust-$((i % 5))"
  post "$backend/api/orders" \
    "{\"orderId\":\"two-backends-$RUN-$i\",\"customerId\":\"$customer\",\"lines\":[{\"productId\":\"two-backends-$RUN-$((i % 2 + 1))\",\"quantity\":1,\"unitPrice\":10.00}]}" \
    -H "X-User-Id: $customer" -H "X-User-Role: CUSTOMER"
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

echo "Waiting until every order-fulfillment saga has ended (at most 5 minutes) …"
deadline=$((SECONDS + 300))
while :; do
  open=$(sql "SELECT count(*) FROM saga_state WHERE saga_type = '$SAGA_TYPE' AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')")
  [ "$open" -eq 0 ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "✗ $open order-fulfillment saga(s) have not ended (FAULTED counts as not ended):" >&2
    sql "SELECT saga_id, status FROM saga_state WHERE saga_type = '$SAGA_TYPE' AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')" >&2
    exit 1
  fi
  sleep 2
done

echo "Order-fulfillment sagas by status:"
sql "SELECT status, count(*) FROM saga_state WHERE saga_type = '$SAGA_TYPE' GROUP BY status ORDER BY status"
echo "Compensated at their deadline (an episode with no cause event):"
sql "SELECT count(*) FROM saga_state WHERE saga_type = '$SAGA_TYPE' AND status = 'COMPENSATED' AND episode_cause_event_id IS NULL"
for backend in "$FIRST" "$SECOND"; do
  # The meter exists once the first forward command was refused; until then the endpoint is 404.
  fenced=$(curl -sf -m 5 "$backend/actuator/metrics/streamrune.saga.forward_commands_fenced" \
    | sed -n 's/.*"value":\([0-9]*\).*/\1/p' || true)
  echo "Forward commands refused at their commit on $backend since it started: ${fenced:-0}"
done

# A forward command (key saga:<sagaId>:<offset>:<index>) stored after the first compensation command
# (key saga:<sagaId>:episode:<version>:comp:<index>) of the same order-fulfillment saga (its ids
# start with fulfillment- and hold no ':').
violations=$(sql "
SELECT f.idempotency_key || ' after ' || c.idempotency_key
  FROM command_inbox f
  JOIN command_inbox c
    ON split_part(c.idempotency_key, ':', 2) = split_part(f.idempotency_key, ':', 2)
 WHERE c.idempotency_key LIKE 'saga:fulfillment-%:episode:%:comp:%'
   AND f.idempotency_key LIKE 'saga:fulfillment-%'
   AND f.idempotency_key NOT LIKE 'saga:%:episode:%'
   AND (SELECT max(o) FROM unnest(f.global_offsets) o)
     > (SELECT min(o) FROM unnest(c.global_offsets) o)")

if [ -n "$violations" ]; then
  echo "✗ Forward commands stored after their saga's compensation:" >&2
  echo "$violations" >&2
  exit 1
fi
echo "✓ No forward command was stored after its saga's compensation."
