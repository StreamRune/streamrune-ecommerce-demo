"use client";

import { useState } from "react";
import { toast } from "sonner";
import { isQueuedForReplay } from "@/lib/api";
import {
  keyForOperation,
  outcomeOf,
  pendingOperation,
  startNewOperation,
  type OperationKind,
  type PendingOperation,
} from "@/lib/pending-operations";
import { Button } from "@/components/ui/button";

/** `+5`, `-3`: an amount as a dialog shows it. */
export function signed(quantity: number): string {
  return quantity > 0 ? `+${quantity}` : `${quantity}`;
}

/** What the notice says of a held change, by what the backend answered it so far. */
const OUTCOME_TEXT = {
  queued: "is queued for a replay; its outcome shows in the admin dead-letter list.",
  unknown: "has no answer yet: it may have been applied.",
  refused: "was not applied: the backend refused it for now and wrote nothing.",
} as const;

/**
 * The notice of a dialog whose change of this product has no final answer: the
 * amount, what the backend answered it so far, what sending it again does, and
 * the way to start a new change instead — which, while the held change may
 * still be applied, is a second change beside it, and the notice says so.
 */
export function PendingNotice({
  pending,
  noun,
  onStartNew,
}: {
  pending: PendingOperation;
  noun: string;
  onStartNew: () => void;
}) {
  const amount = signed(pending.quantity);
  const outcome = outcomeOf(pending);
  return (
    <div role="status" className="rounded-md border border-yellow-500/30 bg-yellow-500/10 p-3 text-sm space-y-2">
      <p>
        The {noun} of {amount} {OUTCOME_TEXT[outcome]} Sending {amount} again sends it
        under the same key, so it is applied once. A different amount is a new {noun}; so is the
        same amount after Start a new change.
        {pending.reason !== undefined && <> Its reason is &ldquo;{pending.reason}&rdquo;.</>}
        {outcome !== "refused" && (
          <> Start a new change only for another {noun} beside this one: this one may still be
          applied.</>
        )}
      </p>
      <Button type="button" variant="outline" size="sm" onClick={onStartNew}>
        Start a new change
      </Button>
    </div>
  );
}

/**
 * The toast after a failed send: a change queued for a replay is no error; any
 * other failure shows the backend's words.
 */
export function reportFailure(err: unknown, fallback: string) {
  if (isQueuedForReplay(err)) {
    toast.info(err instanceof Error ? err.message : "Queued for replay");
  } else {
    toast.error(err instanceof Error ? err.message : fallback);
  }
}

/**
 * A change of one product's quantity sent under the key of the intended
 * operation (`pending-operations.ts`): the dialog opens on the change that has
 * no final answer yet, if there is one, and reads it again after each send.
 */
export function usePendingOperation(kind: OperationKind, productId: string) {
  const [pending, setPending] = useState(() => pendingOperation(kind, productId));
  return {
    pending,
    keyFor: (quantity: number, reason?: string) =>
      keyForOperation(kind, productId, quantity, reason),
    refresh: () => setPending(pendingOperation(kind, productId)),
    startNew: () => {
      startNewOperation(kind, productId);
      setPending(undefined);
    },
  };
}

/** The message of a field the form refused before sending anything, tied to its input. */
export function FieldError({ id, message }: { id: string; message: string | null }) {
  if (!message) return null;
  return (
    <p id={id} role="alert" className="text-xs text-destructive">
      {message}
    </p>
  );
}
