import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { QueryKey } from "@tanstack/react-query";
import {
  createProduct,
  adjustStock,
  updatePrice,
  discontinueProduct,
  receiveShipment,
  placeOrder,
  confirmOrder,
  shipOrder,
  deliverOrder,
  cancelOrder,
  registerCustomer,
  updateProfile,
  exportData,
  forgetCustomer,
  retryDeadLetter,
  togglePaymentFailure,
  injectSagaFailure,
} from "./api";
import { refreshAndAgainOnceSettled } from "./read-model-sync";

/**
 * A command and the queries its result shows in. The backend answers a command
 * once its events are stored; the read models follow, so the queries are read
 * again at once and once more when the read models have settled.
 */
function useInvalidatingMutation<TData, TVariables>(
  mutationFn: (vars: TVariables) => Promise<TData>,
  invalidateKeys: QueryKey[]
) {
  const queryClient = useQueryClient();
  return useMutation<TData, Error, TVariables>({
    mutationFn,
    onSuccess: () => refreshAndAgainOnceSettled(queryClient, invalidateKeys),
  });
}

// ─── Products ────────────────────────────────────────────────────────────────

export function useCreateProduct() {
  return useInvalidatingMutation(createProduct, [["products"]]);
}

export function useAdjustStock() {
  return useInvalidatingMutation(
    ({ productId, body }: { productId: string; body: { quantity: number; reason?: string } }) =>
      adjustStock(productId, body),
    [["products"]]
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

export function useReceiveShipment() {
  return useInvalidatingMutation(
    ({ productId, body }: { productId: string; body: { quantity: number } }) =>
      receiveShipment(productId, body),
    [["products"]]
  );
}

// ─── Orders ──────────────────────────────────────────────────────────────────

export function usePlaceOrder() {
  return useInvalidatingMutation(placeOrder, [["orders"], ["fulfillment"]]);
}

export function useConfirmOrder() {
  return useInvalidatingMutation(
    (orderId: string) => confirmOrder(orderId),
    [["orders"]]
  );
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

export function useRetryDeadLetter() {
  return useInvalidatingMutation(
    (id: string) => retryDeadLetter(id),
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
