import { ApiError, isQueuedForReplay, isRefusal } from "./api";
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
 * own.
 *
 * The operations are kept in the tab's `sessionStorage`, so a reload of the
 * page keeps them too: the replay of a queued change runs on the backend's
 * schedule, a minute or two later, and a reload in between must not turn the
 * same change into a new one. A tab of its own starts without them. When the
 * storage cannot be read or written (a private window that refuses it), the
 * operations live as long as the page.
 */
export type OperationKind = "adjust-stock" | "receive-shipment";

/** An operation the page has no final answer for. */
export interface PendingOperation {
  /** The key every request of the operation carries. */
  key: string;
  /** The amount: a different one is another operation. */
  quantity: number;
  /** An attempt was answered `QUEUED_FOR_REPLAY`: the backend recorded it for a replay. */
  queued: boolean;
  /** An attempt got no answer or a `5xx` that says nothing: it may have been applied. */
  uncertain: boolean;
  /** A request is on its way, or was when the page went away: its outcome is unknown. */
  inFlight: boolean;
}

/**
 * What the page knows of a held operation: `queued` for a replay; `unknown`, an
 * attempt may have been applied; `refused`, every attempt was refused before
 * the backend wrote anything.
 */
export type Outcome = "queued" | "unknown" | "refused";

export function outcomeOf(operation: PendingOperation): Outcome {
  if (operation.queued) {
    return "queued";
  }
  return operation.uncertain || operation.inFlight ? "unknown" : "refused";
}

const STORAGE_KEY = "streamrune.pending-operations";

let pending: Map<string, PendingOperation> | null = null;

/** The held operations, read from the tab's storage the first time they are needed. */
function held(): Map<string, PendingOperation> {
  if (pending === null) {
    pending = new Map();
    try {
      const saved = typeof window === "undefined" ? null : sessionStorage.getItem(STORAGE_KEY);
      if (saved) {
        const entries = Object.entries(JSON.parse(saved) as Record<string, PendingOperation>);
        entries.forEach(([slot, operation]) => pending?.set(slot, operation));
      }
    } catch {
      // Nothing saved, or the storage cannot be read: the page starts without held operations.
    }
  }
  return pending;
}

function save(): void {
  try {
    if (typeof window !== "undefined") {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(Object.fromEntries(held())));
    }
  } catch {
    // The storage refuses the write: the operations stay held for as long as the page lives.
  }
}

function slot(kind: OperationKind, productId: string): string {
  return `${kind}:${productId}`;
}

/** The operation of this kind on this product the page has no final answer for, if any. */
export function pendingOperation(
  kind: OperationKind,
  productId: string
): PendingOperation | undefined {
  return held().get(slot(kind, productId));
}

/**
 * The key to send `quantity` under: the held operation's when it is the same
 * amount, a new key otherwise. The operation is held from here on.
 */
export function keyForOperation(kind: OperationKind, productId: string, quantity: number): string {
  const current = held().get(slot(kind, productId));
  if (current && current.quantity === quantity) {
    return current.key;
  }
  const key = newIdempotencyKey();
  held().set(slot(kind, productId), {
    key,
    quantity,
    queued: false,
    uncertain: false,
    inFlight: false,
  });
  save();
  return key;
}

/** Applies `change` to the operation held under `key`; another key's operation is left alone. */
function update(
  kind: OperationKind,
  productId: string,
  key: string,
  change: (operation: PendingOperation) => PendingOperation | undefined
): void {
  const current = held().get(slot(kind, productId));
  if (!current || current.key !== key) {
    return;
  }
  const next = change(current);
  if (next) {
    held().set(slot(kind, productId), next);
  } else {
    held().delete(slot(kind, productId));
  }
  save();
}

/**
 * Sends one request of the operation held under `key` and records what its
 * answer says: a `2xx` applied the operation and a `4xx` refused it for good,
 * and either way the page lets it go; `QUEUED_FOR_REPLAY` marks it queued; a
 * refusal before anything was written changes nothing; no answer or any other
 * `5xx` marks it as possibly applied. Every attempt of the page's own retry
 * goes through here, so a refusal that follows an attempt without an answer
 * still reads as possibly applied.
 */
export async function sendHeldOperation<T>(
  kind: OperationKind,
  productId: string,
  key: string,
  send: () => Promise<T>
): Promise<T> {
  update(kind, productId, key, (operation) => ({ ...operation, inFlight: true }));
  try {
    const answer = await send();
    update(kind, productId, key, () => undefined);
    return answer;
  } catch (error) {
    update(kind, productId, key, (operation) =>
      error instanceof ApiError && error.status >= 400 && error.status < 500
        ? undefined
        : {
            ...operation,
            inFlight: false,
            queued: operation.queued || isQueuedForReplay(error),
            uncertain: operation.uncertain || !(isQueuedForReplay(error) || isRefusal(error)),
          }
    );
    throw error;
  }
}

/** The user starts a new operation: the held one keeps its key, and the next send gets a new one. */
export function startNewOperation(kind: OperationKind, productId: string): void {
  held().delete(slot(kind, productId));
  save();
}
