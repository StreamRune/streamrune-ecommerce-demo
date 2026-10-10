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

export function useOrders(customerId?: string) {
  return useQuery({
    queryKey: customerId ? ["orders", customerId] : ["orders"],
    queryFn: () => fetchOrders(customerId),
  });
}

export function useOrder(id: string) {
  return useQuery({
    queryKey: ["orders", id],
    queryFn: () => fetchOrder(id),
    enabled: Boolean(id),
  });
}

export function useCustomers() {
  return useQuery({
    queryKey: ["customers"],
    queryFn: fetchCustomers,
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

export function useAuditLog(limit = 100) {
  return useQuery({
    queryKey: ["audit", limit],
    queryFn: () => fetchAuditLog(limit),
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

export function useSagaList(limit = 50) {
  return useQuery({
    queryKey: ["fulfillment", "sagas", limit],
    queryFn: () => fetchSagaList(limit),
  });
}

export function useSaga(id: string) {
  return useQuery({
    queryKey: ["fulfillment", "sagas", id],
    queryFn: () => fetchSaga(id),
    enabled: Boolean(id),
  });
}
