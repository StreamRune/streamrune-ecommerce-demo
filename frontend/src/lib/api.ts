import type {
  ProductView,
  OrderView,
  CustomerView,
  EventEntry,
  AuditEntry,
  DeadLetterEntry,
  OutboxEntry,
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
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API error ${res.status}: ${text}`);
  }
  const contentType = res.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return res.json() as Promise<T>;
  }
  return res.text() as unknown as Promise<T>;
}

// ─── Products ────────────────────────────────────────────────────────────────

export function fetchProducts(): Promise<ProductView[]> {
  return apiFetch("/api/products");
}

export function fetchProduct(productId: string): Promise<ProductView> {
  return apiFetch(`/api/products/${productId}`);
}

export function createProduct(body: {
  name: string;
  description: string;
  category: string;
  price: { amount: number; currency: string };
  initialStock: number;
}): Promise<ProductView> {
  return apiFetch("/api/products", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function adjustStock(
  productId: string,
  body: { delta: number; reason?: string }
): Promise<ProductView> {
  return apiFetch(`/api/products/${productId}/stock`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

export function updatePrice(
  productId: string,
  body: { amount: number; currency: string }
): Promise<ProductView> {
  return apiFetch(`/api/products/${productId}/price`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

export function discontinueProduct(productId: string): Promise<ProductView> {
  return apiFetch(`/api/products/${productId}/discontinue`, {
    method: "POST",
  });
}

// ─── Inventory ───────────────────────────────────────────────────────────────

export function receiveShipment(
  productId: string,
  body: { quantity: number }
): Promise<void> {
  return apiFetch(`/api/inventory/${productId}/receive`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

// ─── Orders ──────────────────────────────────────────────────────────────────

export function fetchOrders(customerId?: string): Promise<OrderView[]> {
  const qs = customerId ? `?customerId=${encodeURIComponent(customerId)}` : "";
  return apiFetch(`/api/orders${qs}`);
}

export function fetchOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${orderId}`);
}

export function placeOrder(body: {
  customerId: string;
  lines: { productId: string; quantity: number }[];
}): Promise<OrderView> {
  return apiFetch("/api/orders", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function confirmOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${orderId}/confirm`, { method: "POST" });
}

export function shipOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${orderId}/ship`, { method: "POST" });
}

export function deliverOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${orderId}/deliver`, { method: "POST" });
}

export function cancelOrder(orderId: string): Promise<OrderView> {
  return apiFetch(`/api/orders/${orderId}/cancel`, { method: "POST" });
}

// ─── Customers ───────────────────────────────────────────────────────────────

export function fetchCustomers(): Promise<CustomerView[]> {
  return apiFetch("/api/customers");
}

export function fetchCustomer(customerId: string): Promise<CustomerView> {
  return apiFetch(`/api/customers/${customerId}`);
}

export function registerCustomer(body: {
  name: string;
  email: string;
  address: string;
  phone: string;
}): Promise<CustomerView> {
  return apiFetch("/api/customers", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updateProfile(
  customerId: string,
  body: { name?: string; email?: string; address?: string; phone?: string }
): Promise<CustomerView> {
  return apiFetch(`/api/customers/${customerId}`, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

export function exportData(customerId: string): Promise<void> {
  return apiFetch(`/api/customers/${customerId}/export-data`, {
    method: "POST",
  });
}

export function forgetCustomer(customerId: string): Promise<void> {
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

export function fetchCircuitBreaker(): Promise<Record<string, unknown>> {
  return apiFetch("/api/admin/circuit-breaker");
}

export function fetchDeadLetters(): Promise<DeadLetterEntry[]> {
  return apiFetch("/api/admin/dead-letters");
}

export function retryDeadLetter(id: string): Promise<void> {
  return apiFetch(`/api/admin/dead-letters/${id}/retry`, { method: "POST" });
}

export function fetchOutbox(): Promise<OutboxEntry[]> {
  return apiFetch("/api/admin/outbox");
}

export function fetchPaymentFailure(): Promise<{ enabled: boolean }> {
  return apiFetch("/api/admin/payment-failure");
}

export function togglePaymentFailure(): Promise<{ enabled: boolean }> {
  return apiFetch("/api/admin/payment-failure/toggle", { method: "POST" });
}

export function fetchHealth(): Promise<Record<string, unknown>> {
  return apiFetch("/actuator/health");
}

// ─── Saga ────────────────────────────────────────────────────────────────────

export function fetchSagaList(limit = 50): Promise<SagaListEntry[]> {
  return apiFetch(`/api/saga/fulfillments?limit=${limit}`);
}

export function fetchSaga(id: string): Promise<OrderFulfillmentState> {
  return apiFetch(`/api/saga/fulfillments/${id}`);
}

export function injectSagaFailure(id: string): Promise<void> {
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
