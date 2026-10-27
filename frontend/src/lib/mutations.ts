import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { QueryKey } from "@tanstack/react-query";
import {
  createProduct,
  adjustStock,
  updatePrice,
  discontinueProduct,
  receiveShipment,
  placeOrder,
  shipOrder,
  deliverOrder,
  cancelOrder,
  registerCustomer,
  updateProfile,
  exportData,
  forgetCustomer,
  retryDeadLetter,
  discardDeadLetter,
  togglePaymentFailure,
  injectSagaFailure,
  ApiError,
} from "./api";
import { readAgainUntilSettled } from "./read-model-sync";

/**
 * A command and the queries its result shows in. The backend answers a command
 * once its events are stored; the read models follow, so the queries are read
 * again at once and a few more times while the read models settle.
 */
function useInvalidatingMutation<TData, TVariables>(
  mutationFn: (vars: TVariables) => Promise<TData>,
  invalidateKeys: QueryKey[],
  retry: (failureCount: number, error: Error) => boolean = () => false
) {
  const queryClient = useQueryClient();
  return useMutation<TData, Error, TVariables>({
    mutationFn,
    retry,
    onSuccess: () => readAgainUntilSettled(queryClient, invalidateKeys),
  });
}

/**
 * The page's own retry of a command sent with an `Idempotency-Key`: twice more
 * when no answer arrived (the request or its answer was lost) or the backend
 * failed (`5xx`), a command it queued for a replay (`503 QUEUED_FOR_REPLAY`)
 * included. A retry sends the same variables, so the same key, and the
 * backend applies the change once whichever attempt, or the replay, reached it
 * first. A refusal (`4xx`) is the backend's answer and is not sent again.
 */
function retryKeyedCommand(failureCount: number, error: Error): boolean {
  if (failureCount >= 2) return false;
  return !(error instanceof ApiError) || error.status >= 500;
}

// ─── Products ────────────────────────────────────────────────────────────────

export function useCreateProduct() {
  return useInvalidatingMutation(createProduct, [["products"]]);
}

/** The caller sends the key of the intended change (`pending-operations.ts`) with every request of it. */
export function useAdjustStock() {
  return useInvalidatingMutation(
    ({
      productId,
      body,
      idempotencyKey,
    }: {
      productId: string;
      body: { quantity: number; reason?: string };
      idempotencyKey: string;
    }) => adjustStock(productId, body, idempotencyKey),
    [["products"]],
    retryKeyedCommand
  );
}

export function useUpdatePrice() {
  return useInvalidatingMutation(
    ({ productId, body }: { productId: string; body: { price: number } }) =>
      updatePrice(productId, body),
    [["products"]]
  );
}

export function useDiscontinueProduct() {
  return useInvalidatingMutation(
    (productId: string) => discontinueProduct(productId),
    [["products"]]
  );
}

// ─── Inventory ───────────────────────────────────────────────────────────────

/** The caller sends the key of the intended change (`pending-operations.ts`) with every request of it. */
export function useReceiveShipment() {
  return useInvalidatingMutation(
    ({
      productId,
      body,
      idempotencyKey,
    }: {
      productId: string;
      body: { quantity: number };
      idempotencyKey: string;
    }) => receiveShipment(productId, body, idempotencyKey),
    [["products"]],
    retryKeyedCommand
  );
}

// ─── Orders ──────────────────────────────────────────────────────────────────

export function usePlaceOrder() {
  return useInvalidatingMutation(placeOrder, [["orders"], ["fulfillment"]]);
}

export function useShipOrder() {
  return useInvalidatingMutation(
    (orderId: string) => shipOrder(orderId),
    [["orders"]]
  );
}

export function useDeliverOrder() {
  return useInvalidatingMutation(
    (orderId: string) => deliverOrder(orderId),
    [["orders"]]
  );
}

export function useCancelOrder() {
  return useInvalidatingMutation(
    ({ orderId, reason }: { orderId: string; reason: string }) => cancelOrder(orderId, reason),
    [["orders"], ["fulfillment"]]
  );
}

// ─── Customers ───────────────────────────────────────────────────────────────

export function useRegisterCustomer() {
  return useInvalidatingMutation(registerCustomer, [["customers"]]);
}

export function useUpdateProfile() {
  return useInvalidatingMutation(
    ({
      customerId,
      body,
    }: {
      customerId: string;
      body: { name?: string; address?: string; phone?: string };
    }) => updateProfile(customerId, body),
    [["customers"]]
  );
}

export function useExportData() {
  return useInvalidatingMutation(
    (customerId: string) => exportData(customerId),
    [["customers"]]
  );
}

export function useForgetCustomer() {
  return useInvalidatingMutation(
    (customerId: string) => forgetCustomer(customerId),
    [["customers"]]
  );
}

// ─── Admin ───────────────────────────────────────────────────────────────────

/** `force` replays an entry the backend refused with `409`, once the admin has confirmed it. */
export function useRetryDeadLetter() {
  return useInvalidatingMutation(
    ({ id, force }: { id: string; force: boolean }) => retryDeadLetter(id, force),
    [["admin", "dead-letters"]]
  );
}

/** Removes an entry whose effect is present, once the admin has checked it. */
export function useDiscardDeadLetter() {
  return useInvalidatingMutation(
    (id: string) => discardDeadLetter(id),
    [["admin", "dead-letters"]]
  );
}

export function useTogglePaymentFailure() {
  return useInvalidatingMutation(togglePaymentFailure, [
    ["admin", "payment-failure"],
  ]);
}

// ─── Saga ────────────────────────────────────────────────────────────────────

export function useInjectSagaFailure() {
  return useInvalidatingMutation(
    (id: string) => injectSagaFailure(id),
    [["fulfillment"]]
  );
}
