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
  isQueuedForReplay,
} from "./api";
import { sendHeldOperation } from "./pending-operations";
import { readAgainUntilSettled } from "./read-model-sync";

/**
 * A command and the queries its result shows in. The backend answers a command
 * once its events are stored; the read models follow, so the queries are read
 * again at once and a few more times while the read models settle.
 */
function useInvalidatingMutation<TData, TVariables>(
  mutationFn: (vars: TVariables) => Promise<TData>,
  invalidateKeys: QueryKey[],
  retry: (failureCount: number, error: Error) => boolean = () => false,
  retryDelay?: (failureCount: number, error: Error) => number
) {
  const queryClient = useQueryClient();
  return useMutation<TData, Error, TVariables>({
    mutationFn,
    retry,
    retryDelay,
    onSuccess: () => readAgainUntilSettled(queryClient, invalidateKeys),
  });
}

/**
 * The page's own retry of a command sent with an `Idempotency-Key`. A retry
 * sends the same variables, so the same key, and the backend applies the change
 * once whichever attempt reached it.
 *
 * - **No answer, or a `5xx` that says nothing** (the request or its answer was
 *   lost, a `500`): twice more.
 * - **A refusal** (`503` with `CIRCUIT_BREAKER_OPEN`, a busy aggregate, a
 *   closing bus): once more, after its `Retry-After`. The backend wrote nothing,
 *   and an attempt before then would meet the same refusal.
 * - **`QUEUED_FOR_REPLAY`**: not again. The backend recorded the change for a
 *   replay; another attempt at once would most likely fail the same way, leave
 *   one more entry and count once more toward the command bus's circuit
 *   breaker, which opens after three failures in a row. The dialog keeps the
 *   key, and the user's own send of the same change is the deliberate re-send.
 * - **Any other `4xx`**: not again. It is the backend's answer.
 */
function retryKeyedCommand(failureCount: number, error: Error): boolean {
  if (isQueuedForReplay(error)) return false;
  if (error instanceof ApiError && error.status === 503) return failureCount < 1;
  if (error instanceof ApiError && error.status < 500) return false;
  return failureCount < 2;
}

/**
 * How long the page waits before its retry: the answer's `Retry-After` when it
 * names one, never less than the backoff react-query uses by default (1 s, then
 * 2 s).
 */
function keyedCommandRetryDelay(failureCount: number, error: Error): number {
  const backoff = Math.min(1000 * 2 ** failureCount, 30_000);
  const retryAfter = error instanceof ApiError ? error.retryAfterSeconds : undefined;
  return retryAfter === undefined ? backoff : Math.max(backoff, retryAfter * 1000);
}

// ─── Products ────────────────────────────────────────────────────────────────

export function useCreateProduct() {
  return useInvalidatingMutation(createProduct, [["products"]]);
}

/**
 * The caller sends the key of the intended change (`pending-operations.ts`),
 * which records what each attempt's answer says about the change.
 */
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
    }) =>
      sendHeldOperation("adjust-stock", productId, idempotencyKey, () =>
        adjustStock(productId, body, idempotencyKey)
      ),
    [["products"]],
    retryKeyedCommand,
    keyedCommandRetryDelay
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

/**
 * The caller sends the key of the intended change (`pending-operations.ts`),
 * which records what each attempt's answer says about the change.
 */
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
    }) =>
      sendHeldOperation("receive-shipment", productId, idempotencyKey, () =>
        receiveShipment(productId, body, idempotencyKey)
      ),
    [["products"]],
    retryKeyedCommand,
    keyedCommandRetryDelay
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
