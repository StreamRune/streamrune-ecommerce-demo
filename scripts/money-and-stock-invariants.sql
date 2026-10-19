-- Money and stock checks of the order flow. Each query returns one row per inconsistency, and
-- nothing once every fulfillment saga has ended and the subscriptions have caught up with the
-- event store. Run by the three apps' integration tests and by scripts/two-backends-invariant.sh.

-- A cancelled order whose payment was captured more often than refunded.
SELECT 'cancelled order with its payment captured and not refunded' AS violation,
       o.aggregate_id AS order_id, '' AS detail
  FROM event_stream o
 WHERE o.aggregate_type = 'order' AND o.event_type = 'OrderCancelled'
 GROUP BY o.aggregate_id
HAVING (SELECT count(*) FROM event_stream p
         WHERE p.aggregate_type = 'payment' AND p.aggregate_id = 'pay-' || o.aggregate_id
           AND p.event_type = 'PaymentCaptured')
     > (SELECT count(*) FROM event_stream p
         WHERE p.aggregate_type = 'payment' AND p.aggregate_id = 'pay-' || o.aggregate_id
           AND p.event_type = 'PaymentRefunded');

-- A cancelled order that still holds stock at a product.
SELECT 'cancelled order still holding stock' AS violation,
       s.payload->>'orderId' AS order_id, 'product ' || s.aggregate_id AS detail
  FROM event_stream s
 WHERE s.aggregate_type = 'inventory'
   AND s.event_type IN ('StockReserved', 'StockReleased', 'ReservationConfirmed')
   AND EXISTS (SELECT 1 FROM event_stream o
                WHERE o.aggregate_type = 'order' AND o.aggregate_id = s.payload->>'orderId'
                  AND o.event_type = 'OrderCancelled')
 GROUP BY s.payload->>'orderId', s.aggregate_id
HAVING sum(CASE s.event_type WHEN 'StockReserved' THEN (s.payload->>'quantity')::int
                             ELSE -(s.payload->>'quantity')::int END) <> 0;

-- Stock reserved twice for one order at one product, or released or committed beyond its
-- reservation (released twice, or released after it was shipped).
SELECT 'stock reserved twice, or released or committed beyond its reservation' AS violation,
       s.payload->>'orderId' AS order_id, 'product ' || s.aggregate_id AS detail
  FROM event_stream s
 WHERE s.aggregate_type = 'inventory'
   AND s.event_type IN ('StockReserved', 'StockReleased', 'ReservationConfirmed')
 GROUP BY s.payload->>'orderId', s.aggregate_id
HAVING count(*) FILTER (WHERE s.event_type = 'StockReserved') > 1
    OR count(*) FILTER (WHERE s.event_type <> 'StockReserved')
     > count(*) FILTER (WHERE s.event_type = 'StockReserved');

-- A payment captured twice, or refunded more often than captured.
SELECT 'payment captured twice, or refunded beyond its capture' AS violation,
       substr(aggregate_id, 5) AS order_id, aggregate_id AS detail
  FROM event_stream
 WHERE aggregate_type = 'payment'
 GROUP BY aggregate_id
HAVING count(*) FILTER (WHERE event_type = 'PaymentRefunded')
     > count(*) FILTER (WHERE event_type = 'PaymentCaptured')
    OR count(*) FILTER (WHERE event_type = 'PaymentCaptured') > 1;

-- An order both shipped and cancelled.
SELECT 'order shipped and cancelled' AS violation, aggregate_id AS order_id, '' AS detail
  FROM event_stream
 WHERE aggregate_type = 'order'
 GROUP BY aggregate_id
HAVING bool_or(event_type = 'OrderShipped') AND bool_or(event_type = 'OrderCancelled');

-- A confirmed order with a line that was never reserved in the line's quantity, or a shipped order
-- with a line whose reservation was never committed in the line's quantity.
SELECT 'confirmed order with a line never reserved in its quantity' AS violation,
       o.aggregate_id AS order_id, 'product ' || (l->>'productId') AS detail
  FROM event_stream o, jsonb_array_elements(o.payload->'lines') l
 WHERE o.aggregate_type = 'order' AND o.event_type = 'OrderPlaced'
   AND EXISTS (SELECT 1 FROM event_stream c
                WHERE c.aggregate_type = 'order' AND c.aggregate_id = o.aggregate_id
                  AND c.event_type = 'OrderConfirmed')
   AND NOT EXISTS (SELECT 1 FROM event_stream s
                    WHERE s.aggregate_type = 'inventory' AND s.aggregate_id = l->>'productId'
                      AND s.event_type = 'StockReserved'
                      AND s.payload->>'orderId' = o.aggregate_id
                      AND (s.payload->>'quantity')::int = (l->>'quantity')::int)
UNION ALL
SELECT 'shipped order with a line never committed in its quantity',
       o.aggregate_id, 'product ' || (l->>'productId')
  FROM event_stream o, jsonb_array_elements(o.payload->'lines') l
 WHERE o.aggregate_type = 'order' AND o.event_type = 'OrderShipped'
   AND NOT EXISTS (SELECT 1 FROM event_stream s
                    WHERE s.aggregate_type = 'inventory' AND s.aggregate_id = l->>'productId'
                      AND s.event_type = 'ReservationConfirmed'
                      AND s.payload->>'orderId' = o.aggregate_id
                      AND (s.payload->>'quantity')::int = (l->>'quantity')::int);

-- A confirmed order that is not cancelled and whose payment is not captured, or is refunded or
-- voided.
SELECT 'order confirmed without a captured payment' AS violation,
       c.aggregate_id AS order_id, '' AS detail
  FROM event_stream c
 WHERE c.aggregate_type = 'order' AND c.event_type = 'OrderConfirmed'
   AND NOT EXISTS (SELECT 1 FROM event_stream x
                    WHERE x.aggregate_type = 'order' AND x.aggregate_id = c.aggregate_id
                      AND x.event_type = 'OrderCancelled')
   AND (NOT EXISTS (SELECT 1 FROM event_stream p
                     WHERE p.aggregate_type = 'payment' AND p.aggregate_id = 'pay-' || c.aggregate_id
                       AND p.event_type = 'PaymentCaptured')
        OR EXISTS (SELECT 1 FROM event_stream p
                    WHERE p.aggregate_type = 'payment' AND p.aggregate_id = 'pay-' || c.aggregate_id
                      AND p.event_type IN ('PaymentRefunded', 'PaymentVoided')));

-- A forward command of a fulfillment saga (key saga:<sagaId>:<offset>:<index>) stored after the
-- first compensation command of the same saga (key saga:<sagaId>:episode:<version>:comp:<index>).
SELECT 'forward command stored after its saga''s compensation' AS violation,
       substr(split_part(f.idempotency_key, ':', 2), 13) AS order_id,
       f.idempotency_key || ' after ' || c.idempotency_key AS detail
  FROM command_inbox f
  JOIN command_inbox c
    ON split_part(c.idempotency_key, ':', 2) = split_part(f.idempotency_key, ':', 2)
 WHERE c.idempotency_key LIKE 'saga:fulfillment-%:episode:%:comp:%'
   AND f.idempotency_key LIKE 'saga:fulfillment-%'
   AND f.idempotency_key NOT LIKE 'saga:%:episode:%'
   AND (SELECT max(o) FROM unnest(f.global_offsets) o)
     > (SELECT min(o) FROM unnest(c.global_offsets) o);
