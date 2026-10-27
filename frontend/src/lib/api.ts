import type {
  ProductView,
  OrderView,
  CustomerView,
  EventEntry,
  AuditEntry,
  CircuitBreakerState,
  DeadLetterEntry,
  DeadLetterRetryAnswer,
  ForgetResult,
  HealthState,
  OutboxEntry,
  PaymentFailureState,
  SagaListEntry,
  OrderFulfillmentState,
} from "./types";

export const API_URL =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8080";

let currentRole = "GUEST";
let currentUserId: string | null = null;

export function setApiAuth(role: string, userId: string | null) {
  currentRole = role;
  currentUserId = userId;
}

function buildHeaders(): HeadersInit {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-User-Role": currentRole,
  };
  if (currentUserId) {
    headers["X-User-Id"] = currentUserId;
  }
  return headers;
}

/**
 * A request the backend refused. `status` is the HTTP status and `message`
 * what a toast shows: for `403` that the selected role may not do this, for
 * `503` that the backend is busy and the request may be sent again in a few
 * seconds, or that the command is queued for a replay whose outcome shows in
 * the admin dead-letter list, for a `500` that waits for an operator that an
 * operator decides, for every other status the backend's own words (a `400`
 * carries the decider's or the validation's message, e.g. "Insufficient stock
 * for product: prod-1"). The backend's words follow in every case.
 */
export class ApiError extends Error {
  readonly status: number;
  /** The backend's own words, without the status the message starts with. */
  readonly detail: string;
  /**
   * The `reason` of a JSON answer, e.g. `CIRCUIT_BREAKER_OPEN`, or
   * `QUEUED_FOR_REPLAY` for a command the backend recorded for a replay under
   * its `Idempotency-Key`, or the `outcome` of a dead-letter retry the backend
   * did not carry out, e.g. `READER_BEHIND`; `undefined` for an answer without
   * one.
   */
  readonly reason?: string;
  /** The answer's `Retry-After`, in seconds, when it has one the page can read. */
  readonly retryAfterSeconds?: number;

  constructor(status: number, detail: string, reason?: string, retryAfterSeconds?: number) {
    super(describeRefusal(status, detail, reason));
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
    this.reason = reason;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

/**
 * Whether the answer says the command is not applied yet and may still be: the
 * backend stored it for a replay under its `Idempotency-Key` (`503` with
 * `QUEUED_FOR_REPLAY`). The replay can fail in its turn and wait for an
 * operator, so its outcome shows in the admin dead-letter list. Sending it
 * again with the same key is safe.
 */
export function isQueuedForReplay(error: unknown): boolean {
  return error instanceof ApiError && error.reason === "QUEUED_FOR_REPLAY";
}

/** The reasons of a `503` the backend gives a command it refused before writing anything. */
const REFUSALS = new Set([
  "CIRCUIT_BREAKER_OPEN",
  "COMMAND_BUS_OVERLOADED",
  "COMMAND_BUS_CLOSED",
  "AGGREGATE_LOCK_UNAVAILABLE",
  "AGGREGATE_VERSION_CONFLICT",
]);

/**
 * Whether the backend refused the command for a reason that passes and wrote
 * nothing: a `503` naming one of the five refusals. The same request succeeds
 * later.
 */
export function isRefusal(error: unknown): boolean {
  return (
    error instanceof ApiError && error.status === 503 && REFUSALS.has(error.reason ?? "")
  );
}

/** `Retry-After` in seconds; a date or a value the page cannot read counts as none. */
function retryAfterOf(res: Response): number | undefined {
  const value = Number(res.headers.get("retry-after") ?? Number.NaN);
  return Number.isFinite(value) && value >= 0 ? value : undefined;
}

function describeRefusal(status: number, detail: string, reason?: string): string {
  if (status === 403) {
    return `Not allowed for the selected role (403): ${detail || "Forbidden"}`;
  }
  if (status === 400) {
    return `The backend refused the request (400): ${detail || "Bad Request"}`;
  }
  if (status === 503 && reason === "QUEUED_FOR_REPLAY") {
    // The command failed and the backend recorded it with its key for a replay, which can fail
    // in its turn: the admin dead-letter list shows what became of it.
    return `Queued for replay; the outcome shows in the admin dead-letter list (503): ${detail}`;
  }
  if (status === 500 && reason === "AWAITING_OPERATOR") {
    // The command failed and waits for an operator, who replays or discards it.
    return `Not applied: an operator decides whether it is (500): ${detail}`;
  }
  if (status === 503) {
    // A command the backend refused for a reason that passes (an open circuit breaker, a busy
    // aggregate, a shutdown): the same request succeeds later. The body names the reason.
    return `The backend is busy, try again in a few seconds (503): ${detail || "Service Unavailable"}`;
  }
  return `API error ${status}: ${detail}`;
}

/**
 * What an error answer says, and its `reason` when it has one. The apps answer
 * a refusal with the message as plain text, a refused or pending command with a
 * JSON body that names its `reason`, and a request Spring could not read with
 * its JSON error object.
 */
async function refusalOf(res: Response): Promise<{ detail: string; reason?: string }> {
  const text = await res.text().catch(() => "");
  if ((res.headers.get("content-type") ?? "").includes("json")) {
    try {
      const body = JSON.parse(text) as Record<string, unknown>;
      if (body.fullyErased === false) return { detail: describeUnfinishedErasure(body) };
      const detail = body.message ?? body.detail ?? body.error ?? body.outcome;
      // A refused or pending command names its `reason`, a dead-letter retry its `outcome`.
      const named = body.reason ?? body.outcome;
      const reason = typeof named === "string" ? named : undefined;
      if (typeof detail === "string" && detail) return { detail, reason };
    } catch {
      // not JSON after all: the text is the detail
    }
  }
  return { detail: text || res.statusText };
}

/**
 * What the answer of a forget that stopped part-way says (`500` with the
 * `ForgetResult`): whether the customer's key is gone, and which read models
 * still hold a copy. Sending the request again finishes the erasure.
 */
function describeUnfinishedErasure(result: Record<string, unknown>): string {
  const key = result.keyDeleted === true ? "the key is deleted" : "the key is not deleted";
  const left = Array.isArray(result.failedPurgers) ? result.failedPurgers.join(", ") : "";
  const copies = left ? `, a copy is left in: ${left}` : "";
  return `The erasure is not complete: ${key}${copies}. Send Forget again.`;
}

/** One segment of a request path: an id as the backend's path variable reads it. */
function segment(id: string): string {
  return encodeURIComponent(id);
}

async function apiFetch<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      ...buildHeaders(),
      ...(options?.headers ?? {}),
    },
  });
  if (!res.ok) {
    const { detail, reason } = await refusalOf(res);
    throw new ApiError(res.status, detail, reason, retryAfterOf(res));
  }
  const contentType = res.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return res.json() as Promise<T>;
  }
  return res.text() as unknown as Promise<T>;
}

/**
 * A command: the backend answers `200` with an empty body once the events are
 * stored. What changed is read from the read models, which follow.
 */
async function apiCommand(
  path: string,
  method: "POST" | "PUT",
  body?: unknown,
  headers?: Record<string, string>
): Promise<void> {
  await apiFetch<string>(path, {
    method,
    body: body === undefined ? undefined : JSON.stringify(body),
    headers,
  });
}

/**
 * The header of a command that changes a quantity by an amount. The backend
 * runs the command once per key: a request sent again with the same key (the
 * page's own retry after a lost answer, the same change sent from the dialog
 * again) is answered with the first one's result, and a dead-lettered command
 * keeps its key, so the backend's retry runner replays it on its schedule. One
 * intended change, one key (`pending-operations.ts`).
 */
function idempotencyKeyHeader(idempotencyKey: string): Record<string, string> {
  return { "Idempotency-Key": idempotencyKey };
}

// ─── Products ────────────────────────────────────────────────────────────────

export function fetchProducts(): Promise<ProductView[]> {
  return apiFetch("/api/products");
}

export function fetchProduct(productId: string): Promise<ProductView> {
  return apiFetch(`/api/products/${segment(productId)}`);
}

/** `POST /api/products`: the id is the client's to pick, the price a plain USD amount. */
export function createProduct(body: {
  productId: string;
  name: string;
  description: string;
  category: string;
  price: number;
  initialStock: number;
}): Promise<void> {
  return apiCommand("/api/products", "POST", body);
}

/**
 * `PUT /api/products/{id}/stock`: `quantity` is the change, positive to add,
 * negative to remove, applied once per `idempotencyKey`.
 */
export function adjustStock(
  productId: string,
  body: { quantity: number; reason?: string },
  idempotencyKey: string
): Promise<void> {
  return apiCommand(
    `/api/products/${segment(productId)}/stock`,
    "PUT",
    body,
    idempotencyKeyHeader(idempotencyKey)
  );
}

/** `PUT /api/products/{id}/price`: the new price as a plain USD amount. */
export function updatePrice(productId: string, body: { price: number }): Promise<void> {
  return apiCommand(`/api/products/${segment(productId)}/price`, "PUT", body);
}

export function discontinueProduct(productId: string): Promise<void> {
  return apiCommand(`/api/products/${segment(productId)}/discontinue`, "POST");
}

// ─── Inventory ───────────────────────────────────────────────────────────────

/**
 * `POST /api/inventory/{productId}/receive`: adds to the inventory the
 * fulfillment saga reserves from. The inventory is its own aggregate; the
 * stock column of the catalogue is the product's and does not move with it.
 * Applied once per `idempotencyKey`, like a stock adjustment.
 */
export function receiveShipment(
  productId: string,
  body: { quantity: number },
  idempotencyKey: string
): Promise<void> {
  return apiCommand(
    `/api/inventory/${segment(productId)}/receive`,
    "POST",
    body,
    idempotencyKeyHeader(idempotencyKey)
  );
}

// ─── Orders ──────────────────────────────────────────────────────────────────

export function fetchOrders(customerId?: string): Promise<OrderView[]> {
  const qs = customerId ? `?customerId=${encodeURIComponent(customerId)}` : "";
  return apiFetch(`/api/orders${qs}`);
}

export function fetchOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${segment(orderId)}`);
}

/**
 * `POST /api/orders`: the id is the client's to pick, and each line names a
 * product and a quantity; the catalogue sets the price. The backend takes one
 * line per product.
 */
export function placeOrder(body: {
  orderId: string;
  customerId: string;
  lines: { productId: string; quantity: number }[];
}): Promise<void> {
  return apiCommand("/api/orders", "POST", body);
}

export function shipOrder(orderId: string): Promise<void> {
  return apiCommand(`/api/orders/${segment(orderId)}/ship`, "POST");
}

export function deliverOrder(orderId: string): Promise<void> {
  return apiCommand(`/api/orders/${segment(orderId)}/deliver`, "POST");
}

/** `POST /api/orders/{id}/cancel`: the body is required and holds the reason. */
export function cancelOrder(orderId: string, reason: string): Promise<void> {
  return apiCommand(`/api/orders/${segment(orderId)}/cancel`, "POST", { reason });
}

// ─── Customers ───────────────────────────────────────────────────────────────

export function fetchCustomers(): Promise<CustomerView[]> {
  return apiFetch("/api/customers");
}

export function fetchCustomer(customerId: string): Promise<CustomerView> {
  return apiFetch(`/api/customers/${segment(customerId)}`);
}

/**
 * `POST /api/customers`: the id is the client's to pick. An ADMIN registers
 * anyone; any other caller only the customer whose id is their own X-User-Id.
 */
export function registerCustomer(body: {
  customerId: string;
  name: string;
  email: string;
  address: string;
  phone: string;
}): Promise<void> {
  return apiCommand("/api/customers", "POST", body);
}

/** `PUT /api/customers/{id}`: a field left out keeps its value. */
export function updateProfile(
  customerId: string,
  body: { name?: string; email?: string; address?: string; phone?: string }
): Promise<void> {
  return apiCommand(`/api/customers/${segment(customerId)}`, "PUT", body);
}

export function exportData(customerId: string): Promise<void> {
  return apiCommand(`/api/customers/${segment(customerId)}/export-data`, "POST");
}

/**
 * `POST /api/customers/{id}/forget`: `200` with the result once the customer is
 * fully erased; `500` with the same shape when the erasure stopped part-way,
 * which arrives here as an ApiError and is repeated by sending it again.
 */
export function forgetCustomer(customerId: string): Promise<ForgetResult> {
  return apiFetch(`/api/customers/${segment(customerId)}/forget`, { method: "POST" });
}

// ─── Events ──────────────────────────────────────────────────────────────────

export function fetchEvents(
  offset = 0,
  limit = 50
): Promise<EventEntry[]> {
  return apiFetch(`/api/events?offset=${offset}&limit=${limit}`);
}

export function fetchStreamEvents(
  aggregateType: string,
  aggregateId: string,
): Promise<EventEntry[]> {
  return apiFetch(
    `/api/events/${segment(aggregateType)}/${segment(aggregateId)}`,
  );
}

// ─── Audit ───────────────────────────────────────────────────────────────────

export function fetchAuditLog(limit = 100): Promise<AuditEntry[]> {
  return apiFetch(`/api/audit/commands?limit=${limit}`);
}

// ─── Admin ───────────────────────────────────────────────────────────────────

export function fetchCircuitBreaker(): Promise<CircuitBreakerState> {
  return apiFetch("/api/admin/circuit-breaker");
}

export function fetchDeadLetters(): Promise<DeadLetterEntry[]> {
  return apiFetch("/api/admin/dead-letters");
}

/**
 * `POST /api/admin/dead-letters/{commandId}/retry`: `200` with the outcome
 * `REPLAYED` once the command ran. A refusal arrives as an ApiError: `409` when
 * the backend cannot tell a replay from a second execution (the command ran
 * without an idempotency key, or its key may already be gone from the inbox) —
 * its `detail` says what to check, and `force` replays the entry anyway
 * (`discardDeadLetter` removes it instead); `404`
 * when the entry is gone; `503` while the command bus admits nothing more.
 */
export function retryDeadLetter(
  commandId: string,
  force = false
): Promise<DeadLetterRetryAnswer> {
  const query = force ? "?force=true" : "";
  return apiFetch(`/api/admin/dead-letters/${segment(commandId)}/retry${query}`, {
    method: "POST",
  });
}

/**
 * `DELETE /api/admin/dead-letters/{commandId}`: removes an entry whose command
 * already ran (its effect is present), so that it is never replayed. `200` with
 * the outcome `DISCARDED`; `404` when the entry is gone.
 */
export function discardDeadLetter(commandId: string): Promise<DeadLetterRetryAnswer> {
  return apiFetch(`/api/admin/dead-letters/${segment(commandId)}`, { method: "DELETE" });
}

export function fetchOutbox(): Promise<OutboxEntry[]> {
  return apiFetch("/api/admin/outbox");
}

export function fetchPaymentFailure(): Promise<PaymentFailureState> {
  return apiFetch("/api/admin/payment-failure");
}

export function togglePaymentFailure(): Promise<PaymentFailureState> {
  return apiFetch("/api/admin/payment-failure/toggle", { method: "POST" });
}

/**
 * `GET /api/admin/health`: the one health answer the three apps share, open to
 * every caller and under `/api`, the path the backend's CORS mapping covers.
 */
export function fetchHealth(): Promise<HealthState> {
  return apiFetch("/api/admin/health");
}

// ─── Saga ────────────────────────────────────────────────────────────────────

export function fetchSagaList(limit = 50): Promise<SagaListEntry[]> {
  return apiFetch(`/api/saga/fulfillments?limit=${limit}`);
}

export function fetchSaga(id: string): Promise<OrderFulfillmentState> {
  return apiFetch(`/api/saga/fulfillments/${segment(id)}`);
}

/** Flips the same switch as `togglePaymentFailure`; the saga id is not used by the backend. */
export function injectSagaFailure(id: string): Promise<PaymentFailureState> {
  return apiFetch(`/api/saga/fulfillments/${segment(id)}/inject-failure`, {
    method: "POST",
  });
}

/**
 * Splits a stream filter typed as {@code type:id} at the first colon. Returns {@code null} when
 * there is no colon or either side is empty.
 */
export function parseStreamFilter(
  input: string,
): { aggregateType: string; aggregateId: string } | null {
  const colon = input.indexOf(":");
  if (colon < 0) return null;
  const aggregateType = input.slice(0, colon).trim();
  const aggregateId = input.slice(colon + 1).trim();
  if (!aggregateType || !aggregateId) return null;
  return { aggregateType, aggregateId };
}
