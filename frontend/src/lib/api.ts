import type {
  ProductView,
  OrderView,
  CustomerView,
  EventEntry,
  AuditEntry,
  CircuitBreakerState,
  DeadLetterEntry,
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
 * every other status the backend's own words (a `400` carries the decider's or
 * the validation's message, e.g. "Insufficient stock for product: prod-1").
 */
export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, detail: string) {
    super(describeRefusal(status, detail));
    this.name = "ApiError";
    this.status = status;
  }
}

function describeRefusal(status: number, detail: string): string {
  if (status === 403) {
    return `Not allowed for the selected role (403): ${detail || "Forbidden"}`;
  }
  if (status === 400) {
    return `The backend refused the request (400): ${detail || "Bad Request"}`;
  }
  return `API error ${status}: ${detail}`;
}

/**
 * What an error answer says. The apps answer a refusal with the message as
 * plain text, and a request Spring could not read with its JSON error object.
 */
async function refusalDetail(res: Response): Promise<string> {
  const text = await res.text().catch(() => "");
  if ((res.headers.get("content-type") ?? "").includes("json")) {
    try {
      const body = JSON.parse(text) as Record<string, unknown>;
      const detail = body.message ?? body.detail ?? body.error;
      if (typeof detail === "string" && detail) return detail;
    } catch {
      // not JSON after all: the text is the detail
    }
  }
  return text || res.statusText;
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
    throw new ApiError(res.status, await refusalDetail(res));
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
async function apiCommand(path: string, method: "POST" | "PUT", body?: unknown): Promise<void> {
  await apiFetch<string>(path, {
    method,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

// ─── Products ────────────────────────────────────────────────────────────────

export function fetchProducts(): Promise<ProductView[]> {
  return apiFetch("/api/products");
}

export function fetchProduct(productId: string): Promise<ProductView> {
  return apiFetch(`/api/products/${productId}`);
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

/** `PUT /api/products/{id}/stock`: `quantity` is the change, positive to add, negative to remove. */
export function adjustStock(
  productId: string,
  body: { quantity: number; reason?: string }
): Promise<void> {
  return apiCommand(`/api/products/${productId}/stock`, "PUT", body);
}

/** `PUT /api/products/{id}/price`: the new price as a plain USD amount. */
export function updatePrice(productId: string, body: { price: number }): Promise<void> {
  return apiCommand(`/api/products/${productId}/price`, "PUT", body);
}

export function discontinueProduct(productId: string): Promise<void> {
  return apiCommand(`/api/products/${productId}/discontinue`, "POST");
}

// ─── Inventory ───────────────────────────────────────────────────────────────

/**
 * `POST /api/inventory/{productId}/receive`: adds to the inventory the
 * fulfillment saga reserves from. The inventory is its own aggregate; the
 * stock column of the catalogue is the product's and does not move with it.
 */
export function receiveShipment(
  productId: string,
  body: { quantity: number }
): Promise<void> {
  return apiCommand(`/api/inventory/${productId}/receive`, "POST", body);
}

// ─── Orders ──────────────────────────────────────────────────────────────────

export function fetchOrders(customerId?: string): Promise<OrderView[]> {
  const qs = customerId ? `?customerId=${encodeURIComponent(customerId)}` : "";
  return apiFetch(`/api/orders${qs}`);
}

export function fetchOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${orderId}`);
}

/**
 * `POST /api/orders`: the id is the client's to pick, and each line carries
 * the unit price as a plain USD amount (the price of the product picked).
 */
export function placeOrder(body: {
  orderId: string;
  customerId: string;
  lines: { productId: string; quantity: number; unitPrice: number }[];
}): Promise<void> {
  return apiCommand("/api/orders", "POST", body);
}

export function confirmOrder(orderId: string): Promise<void> {
  return apiCommand(`/api/orders/${orderId}/confirm`, "POST");
}

export function shipOrder(orderId: string): Promise<void> {
  return apiCommand(`/api/orders/${orderId}/ship`, "POST");
}

export function deliverOrder(orderId: string): Promise<void> {
  return apiCommand(`/api/orders/${orderId}/deliver`, "POST");
}

/** `POST /api/orders/{id}/cancel`: the body is required and holds the reason. */
export function cancelOrder(orderId: string, reason: string): Promise<void> {
  return apiCommand(`/api/orders/${orderId}/cancel`, "POST", { reason });
}

// ─── Customers ───────────────────────────────────────────────────────────────

export function fetchCustomers(): Promise<CustomerView[]> {
  return apiFetch("/api/customers");
}

export function fetchCustomer(customerId: string): Promise<CustomerView> {
  return apiFetch(`/api/customers/${customerId}`);
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
  return apiCommand(`/api/customers/${customerId}`, "PUT", body);
}

export function exportData(customerId: string): Promise<void> {
  return apiCommand(`/api/customers/${customerId}/export-data`, "POST");
}

/**
 * `POST /api/customers/{id}/forget`: `200` with the result once the customer is
 * fully erased; `500` with the same shape when the erasure stopped part-way,
 * which arrives here as an ApiError and is repeated by sending it again.
 */
export function forgetCustomer(customerId: string): Promise<ForgetResult> {
  return apiFetch(`/api/customers/${customerId}/forget`, { method: "POST" });
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
    `/api/events/${encodeURIComponent(aggregateType)}/${encodeURIComponent(aggregateId)}`,
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

/** `POST /api/admin/dead-letters/{commandId}/retry`: `404` when the entry is gone. */
export function retryDeadLetter(commandId: string): Promise<void> {
  return apiCommand(`/api/admin/dead-letters/${encodeURIComponent(commandId)}/retry`, "POST");
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
  return apiFetch(`/api/saga/fulfillments/${id}`);
}

/** Flips the same switch as `togglePaymentFailure`; the saga id is not used by the backend. */
export function injectSagaFailure(id: string): Promise<PaymentFailureState> {
  return apiFetch(`/api/saga/fulfillments/${id}/inject-failure`, {
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
