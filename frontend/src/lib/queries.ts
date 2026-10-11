import { useQuery } from "@tanstack/react-query";
import {
  fetchProducts,
  fetchProduct,
  fetchOrders,
  fetchOrder,
  fetchCustomers,
  fetchCustomer,
  fetchEvents,
  fetchStreamEvents,
  fetchAuditLog,
  fetchCircuitBreaker,
  fetchDeadLetters,
  fetchOutbox,
  fetchPaymentFailure,
  fetchHealth,
  fetchSagaList,
  fetchSaga,
} from "./api";

export function useProducts() {
  return useQuery({
    queryKey: ["products"],
    queryFn: fetchProducts,
  });
}

export function useProduct(id: string) {
  return useQuery({
    queryKey: ["products", id],
    queryFn: () => fetchProduct(id),
    enabled: Boolean(id),
  });
}

/**
 * Orders are served to an authenticated caller only: every order to an ADMIN,
 * a CUSTOMER's own orders to that customer (who may not name another customer
 * in `customerId`). The caller passes `enabled: false` for a GUEST and no
 * request is sent.
 */
export function useOrders(customerId?: string, enabled = true) {
  return useQuery({
    queryKey: customerId ? ["orders", customerId] : ["orders"],
    queryFn: () => fetchOrders(customerId),
    enabled,
  });
}

export function useOrder(id: string) {
  return useQuery({
    queryKey: ["orders", id],
    queryFn: () => fetchOrder(id),
    enabled: Boolean(id),
  });
}

/**
 * Customers are served to an authenticated caller only: every customer to an
 * ADMIN, a CUSTOMER's own record to that customer. The caller passes
 * `enabled: false` for a GUEST and no request is sent.
 */
export function useCustomers(enabled = true) {
  return useQuery({
    queryKey: ["customers"],
    queryFn: fetchCustomers,
    enabled,
  });
}

export function useCustomer(id: string) {
  return useQuery({
    queryKey: ["customers", id],
    queryFn: () => fetchCustomer(id),
    enabled: Boolean(id),
  });
}

/**
 * The event history is ADMIN only on the backend (it returns decrypted
 * payloads), so the caller passes `enabled: false` for any other role and no
 * request is sent.
 */
export function useEvents(offset = 0, limit = 50, enabled = true) {
  return useQuery({
    queryKey: ["events", offset, limit],
    queryFn: () => fetchEvents(offset, limit),
    enabled,
  });
}

export function useStreamEvents(
  aggregateType: string | null,
  aggregateId: string | null,
  enabled = true,
) {
  return useQuery({
    queryKey: ["events", "stream", aggregateType, aggregateId],
    queryFn: () => fetchStreamEvents(aggregateType as string, aggregateId as string),
    enabled: enabled && !!aggregateType && !!aggregateId,
  });
}

/** The audit log is ADMIN only on the backend; `enabled: false` sends no request. */
export function useAuditLog(limit = 100, enabled = true) {
  return useQuery({
    queryKey: ["audit", limit],
    queryFn: () => fetchAuditLog(limit),
    enabled,
  });
}

export function useCircuitBreaker(enabled = true) {
  return useQuery({
    queryKey: ["admin", "circuit-breaker"],
    queryFn: fetchCircuitBreaker,
    enabled,
  });
}

export function useDeadLetters(enabled = true) {
  return useQuery({
    queryKey: ["admin", "dead-letters"],
    queryFn: fetchDeadLetters,
    enabled,
  });
}

export function useOutbox(enabled = true) {
  return useQuery({
    queryKey: ["admin", "outbox"],
    queryFn: fetchOutbox,
    enabled,
  });
}

export function usePaymentFailure(enabled = true) {
  return useQuery({
    queryKey: ["admin", "payment-failure"],
    queryFn: fetchPaymentFailure,
    enabled,
  });
}

export function useHealth(enabled = true) {
  return useQuery({
    queryKey: ["admin", "health"],
    queryFn: fetchHealth,
    enabled,
  });
}

/** The saga list is ADMIN only on the backend; `enabled: false` sends no request. */
export function useSagaList(limit = 50, enabled = true) {
  return useQuery({
    queryKey: ["fulfillment", "sagas", limit],
    queryFn: () => fetchSagaList(limit),
    enabled,
  });
}

export function useSaga(id: string) {
  return useQuery({
    queryKey: ["fulfillment", "sagas", id],
    queryFn: () => fetchSaga(id),
    enabled: Boolean(id),
  });
}
