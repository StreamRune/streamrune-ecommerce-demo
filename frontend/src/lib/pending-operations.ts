import { ApiError, isQueuedForReplay } from "./api";
import { newIdempotencyKey } from "./ids";

/**
 * The changes of a quantity the page sent and has no final answer for, one per
 * product and kind, each with the `Idempotency-Key` it was sent under.
 *
 * A key belongs to the change the user intends, not to a dialog: the backend
 * runs a command once per key, and a command that failed on infrastructure is
 * recorded with its key and replayed under it later. A dialog that minted a new
 * key when it was opened again would send the same change as a second one, and
 * the replay of the first would apply it beside it. So the operation is held
 * here from its first request until the backend's answer is final — a `2xx`
 * applied it, a `4xx` refused it — or the user starts a new one. While it is
 * held, the same amount is the same operation and goes out under the same key,
 * whether the dialog stays open, is closed and opened again, or its fields are
 * edited in between; a different amount is a new operation with a key of its
 * own. The operations live as long as the page: a reload forgets them.
 */
export type OperationKind = "adjust-stock" | "receive-shipment";

/** An operation the page has no final answer for. */
export interface PendingOperation {
  /** The key every request of the operation carries. */
  key: string;
  /** The amount: a different one is another operation. */
  quantity: number;
  /** `queued`: the backend recorded it for a replay (`QUEUED_FOR_REPLAY`); `unknown`: no answer says. */
  state: "unknown" | "queued";
}

const pending = new Map<string, PendingOperation>();

function slot(kind: OperationKind, productId: string): string {
  return `${kind}:${productId}`;
}

/** The operation of this kind on this product the page has no final answer for, if any. */
export function pendingOperation(
  kind: OperationKind,
  productId: string
): PendingOperation | undefined {
  return pending.get(slot(kind, productId));
}

/**
 * The key to send `quantity` under: the held operation's when it is the same
 * amount, a new key otherwise. The operation is held from here on.
 */
export function keyForOperation(kind: OperationKind, productId: string, quantity: number): string {
  const held = pending.get(slot(kind, productId));
  if (held && held.quantity === quantity) {
    return held.key;
  }
  const key = newIdempotencyKey();
  pending.set(slot(kind, productId), { key, quantity, state: "unknown" });
  return key;
}

/** The backend applied the operation sent under `key`: the page forgets it. */
export function operationApplied(kind: OperationKind, productId: string, key: string): void {
  if (pending.get(slot(kind, productId))?.key === key) {
    pending.delete(slot(kind, productId));
  }
}

/**
 * What a failed request of the operation sent under `key` leaves held: nothing
 * after a `4xx`, the backend's final refusal; the operation, `queued`, after a
 * `503 QUEUED_FOR_REPLAY`; the operation, `unknown`, after any other failure —
 * a `5xx` or no answer — which says nothing about whether it was applied.
 */
export function operationFailed(
  kind: OperationKind,
  productId: string,
  key: string,
  error: unknown
): PendingOperation | undefined {
  const held = pending.get(slot(kind, productId));
  if (!held || held.key !== key) {
    return held;
  }
  if (error instanceof ApiError && error.status >= 400 && error.status < 500) {
    pending.delete(slot(kind, productId));
    return undefined;
  }
  const after: PendingOperation = {
    ...held,
    state: isQueuedForReplay(error) || held.state === "queued" ? "queued" : "unknown",
  };
  pending.set(slot(kind, productId), after);
  return after;
}

/** The user starts a new operation: the held one keeps its key, and the next send gets a new one. */
export function startNewOperation(kind: OperationKind, productId: string): void {
  pending.delete(slot(kind, productId));
}
