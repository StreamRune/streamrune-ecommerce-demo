/**
 * Ids for the aggregates the page creates.
 *
 * The backend creates nothing under an id of its own: `POST /api/orders`,
 * `POST /api/customers` and `POST /api/products` each require the id in the
 * request body, and refuse the request with `400` without one. The page
 * therefore picks the id, in the form the seed data uses (`order-seed-1`,
 * `cust-alice`, `prod-widget`): the kind, a dash, and eight hex digits of a
 * random UUID. An id that is taken is the backend's to refuse ("already
 * exists", `400`), and the page shows that message.
 */
export type IdKind = "order" | "cust" | "prod";

export function newId(kind: IdKind): string {
  return `${kind}-${crypto.randomUUID().slice(0, 8)}`;
}

/**
 * A fresh `Idempotency-Key` for one user action: a stock adjustment or a
 * received shipment. The page's own retry of that action sends it again, so the
 * backend applies the change once.
 */
export function newIdempotencyKey(): string {
  return crypto.randomUUID();
}
