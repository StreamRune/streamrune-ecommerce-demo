#!/usr/bin/env bash
# Two backends on one database: places orders on both, kills the second with SIGKILL halfway,
# restarts it, waits until every order-fulfillment saga has ended and checks that no forward command
# of a saga was stored after that saga's first compensation command.
#
# Usage (from the repository root, with the JARs built as the README's Quick Start shows):
#   APP_SAGA_FULFILLMENT_TIMEOUT=1500ms APP_SAGA_TIMEOUT_POLL_INTERVAL=200ms \
#     APP_CHAOS_SAGA_COMMAND_DELAY_MAX=300ms docker compose --profile two-backends up -d --build
#   scripts/two-backends-invariant.sh
#   docker compose --profile two-backends down -v
#
# With the defaults (a 15-minute deadline, no delay) no saga reaches its deadline and the check
# holds trivially; the three variables above make some deadlines pass while a forward command is in
# flight. ORDERS (default 60) sets the number of orders, PAUSE_SECONDS (default 1) the pause
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

sql() {
  docker compose exec -T postgres psql -U postgres -d streamrune_ecommerce -v ON_ERROR_STOP=1 -tA -c "$1"
}

post() {
  local url="$1" body="$2"
  shift 2
  curl -sf -m 30 -X POST "$url" -H "Content-Type: application/json" "$@" -d "$body" > /dev/null
}

WAIT_SECONDS=180 scripts/wait-for-http.sh \
  "$FIRST/actuator/health" '"status":"UP"' "$SECOND/actuator/health" '"status":"UP"'

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
    echo "Killing backend-2 with SIGKILL after $i orders …"
    docker compose kill -s SIGKILL backend-2
    # start, not up: the same container with the environment it was created with.
    docker compose --profile two-backends start backend-2
    WAIT_SECONDS=180 scripts/wait-for-http.sh "$SECOND/actuator/health" '"status":"UP"'
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
    | sed -n 's/.*"value":\([0-9.]*\).*/\1/p' || true)
  echo "Forward commands refused at their commit on $backend since it started: ${fenced:-0}"
done

# A forward command (key saga:<sagaId>:<offset>:<index>) stored after the first compensation command
# (key saga:<sagaId>:episode:<version>:comp:<index>) of the same saga. Saga ids hold no ':'.
violations=$(sql "
SELECT f.idempotency_key || ' after ' || c.idempotency_key
  FROM command_inbox f
  JOIN command_inbox c
    ON split_part(c.idempotency_key, ':', 2) = split_part(f.idempotency_key, ':', 2)
 WHERE c.idempotency_key LIKE 'saga:%:episode:%:comp:%'
   AND f.idempotency_key LIKE 'saga:%'
   AND f.idempotency_key NOT LIKE 'saga:%:episode:%'
   AND (SELECT max(o) FROM unnest(f.global_offsets) o)
     > (SELECT min(o) FROM unnest(c.global_offsets) o)")

if [ -n "$violations" ]; then
  echo "✗ Forward commands stored after their saga's compensation:" >&2
  echo "$violations" >&2
  exit 1
fi
echo "✓ No forward command was stored after its saga's compensation."
