"use client";

import { useRef, useState } from "react";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import { Settings, RefreshCw } from "lucide-react";
import {
  useCircuitBreaker,
  useDeadLetters,
  useOutbox,
  usePaymentFailure,
  useHealth,
} from "@/lib/queries";
import {
  useDiscardDeadLetter,
  useRetryDeadLetter,
  useTogglePaymentFailure,
} from "@/lib/mutations";
import { ApiError } from "@/lib/api";
import { useRole } from "@/providers/role-provider";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

// ─── Circuit Breaker Card ─────────────────────────────────────────────────────

function CircuitBreakerCard({ locked }: { locked: boolean }) {
  const { data, isLoading } = useCircuitBreaker(!locked);

  const state = isLoading ? "LOADING" : (data?.state ?? "UNKNOWN");

  const dotColor =
    state === "CLOSED"
      ? "bg-green-500"
      : state === "OPEN"
        ? "bg-red-500"
        : state === "HALF_OPEN"
          ? "bg-yellow-500"
          : "bg-muted-foreground";

  return (
    <Card className={cn(locked && "opacity-50")}>
      <CardHeader>
        <CardTitle>Circuit Breaker</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-2">
          <span className={cn("size-3 rounded-full shrink-0", dotColor)} />
          <span className="font-semibold">{state}</span>
        </div>
        {data && (
          <pre className="mt-3 text-[10px] bg-muted/50 rounded p-2 overflow-auto max-h-24 font-mono">
            {JSON.stringify(data, null, 2)}
          </pre>
        )}
      </CardContent>
    </Card>
  );
}

// ─── Dead Letter Queue Card ───────────────────────────────────────────────────

/** An entry the backend refused to replay with `409`, waiting for the admin's decision. */
interface PendingConfirmation {
  commandId: string;
  commandType: string;
  /** The backend's reason: what the admin has to check before deciding. */
  reason: string;
}

/**
 * The command dead-letter queue. Retry asks the backend to replay one entry. The
 * backend refuses (`409`) an entry it cannot tell from a second execution — a
 * command that ran without an idempotency key, whose caller may already have
 * retried it — and the card then shows the backend's reason and asks the admin
 * to check the command's effect: when it is absent, the entry is replayed with
 * `force`; when it is present, the entry is discarded. The backend records
 * either decision in the audit log before it acts on it. Keyed entries the
 * backend replays on its own schedule.
 */
function DeadLetterCard({ locked }: { locked: boolean }) {
  const { data: entries = [], isLoading } = useDeadLetters(!locked);
  const { mutateAsync: retry, isPending: isRetrying } = useRetryDeadLetter();
  const { mutateAsync: discard, isPending: isDiscarding } = useDiscardDeadLetter();
  const queryClient = useQueryClient();
  const [pending, setPending] = useState<PendingConfirmation | null>(null);
  // One request at a time: a second click lands before the pending state disables the buttons.
  const inFlight = useRef(false);
  const busy = isRetrying || isDiscarding;

  /** What every answer but the expected ones means; `true` when it was handled. */
  async function handleCommonRefusal(err: unknown): Promise<boolean> {
    if (err instanceof ApiError && err.status === 503 && err.reason === "READER_BEHIND") {
      // A backend still on the previous release, during a rolling deployment of an event-schema
      // change: nothing ran, and the entry waits for a backend that reads the events.
      toast.error(
        "This backend reads the command's events at an older schema version than they were stored with: the entry keeps its attempt count and is replayed again after its backoff, once an upgraded backend runs it"
      );
      return true;
    }
    if (err instanceof ApiError && err.status === 503) {
      toast.error("The backend is busy and admits no more commands: try again in a few seconds");
      return true;
    }
    if (err instanceof ApiError && err.status === 404) {
      toast.error("This entry is gone: it was replayed or removed in the meantime");
      await queryClient.invalidateQueries({ queryKey: ["admin", "dead-letters"] });
      return true;
    }
    return false;
  }

  async function handleRetry(entry: { commandId: string; commandType: string }, force: boolean) {
    if (inFlight.current) return;
    inFlight.current = true;
    try {
      await retry({ id: entry.commandId, force });
      setPending(null);
      toast.success(force ? "Dead letter replayed after your confirmation" : "Dead letter replayed");
    } catch (err) {
      if (err instanceof ApiError && err.status === 409 && !force) {
        setPending({ ...entry, reason: err.detail });
        return;
      }
      setPending(null);
      if (!(await handleCommonRefusal(err))) {
        toast.error(err instanceof Error ? err.message : "Failed to retry");
      }
    } finally {
      inFlight.current = false;
    }
  }

  async function handleDiscard(entry: { commandId: string }) {
    if (inFlight.current) return;
    inFlight.current = true;
    try {
      await discard(entry.commandId);
      setPending(null);
      toast.success("Dead letter discarded: its command already ran");
    } catch (err) {
      setPending(null);
      if (!(await handleCommonRefusal(err))) {
        toast.error(err instanceof Error ? err.message : "Failed to discard");
      }
    } finally {
      inFlight.current = false;
    }
  }

  return (
    <Card className={cn("col-span-full lg:col-span-2", locked && "opacity-50")}>
      <CardHeader>
        <CardTitle>Dead Letter Queue ({entries.length})</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {isLoading ? (
          <p className="text-sm text-muted-foreground px-4 pb-4">Loading…</p>
        ) : entries.length === 0 ? (
          <p className="text-sm text-muted-foreground px-4 pb-4">No dead letters. All good!</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Type</TableHead>
                <TableHead>Error</TableHead>
                <TableHead>Timestamp</TableHead>
                <TableHead className="w-20" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {entries.map((entry) => (
                <TableRow key={entry.commandId}>
                  <TableCell className="text-sm">{entry.commandType.split(/[.$]/).pop()}</TableCell>
                  <TableCell className="text-xs text-muted-foreground max-w-48 truncate">{entry.errorMessage}</TableCell>
                  <TableCell className="text-xs text-muted-foreground tabular-nums">
                    {new Date(entry.firstAttemptAt).toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="outline"
                      size="xs"
                      onClick={() => handleRetry(entry, false)}
                      disabled={busy || locked}
                    >
                      <RefreshCw className="size-3" />
                      Retry
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
      <Dialog open={pending !== null} onOpenChange={(open) => { if (!open) setPending(null); }}>
        {pending && (
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Replay or discard {pending.commandType.split(/[.$]/).pop()}?</DialogTitle>
              <DialogDescription>The backend did not replay this entry.</DialogDescription>
            </DialogHeader>
            <p className="text-sm">{pending.reason}</p>
            <p className="text-sm text-muted-foreground">
              Check the command&apos;s effect first — for a stock adjustment, whether the
              product&apos;s stock changed. If the effect is absent, replay the entry. If it is
              present, the command already ran and a replay applies it a second time: discard the
              entry. Either decision is recorded in the audit log under your id.
            </p>
            <DialogFooter showCloseButton>
              <Button variant="outline" onClick={() => handleDiscard(pending)} disabled={busy}>
                {isDiscarding ? "Discarding…" : "The effect is present: discard"}
              </Button>
              <Button onClick={() => handleRetry(pending, true)} disabled={busy}>
                {isRetrying ? "Replaying…" : "The effect is absent: replay"}
              </Button>
            </DialogFooter>
          </DialogContent>
        )}
      </Dialog>
    </Card>
  );
}

// ─── Outbox Card ──────────────────────────────────────────────────────────────

function OutboxCard({ locked }: { locked: boolean }) {
  const { data: entries = [], isLoading } = useOutbox(!locked);

  return (
    <Card className={cn("col-span-full lg:col-span-2", locked && "opacity-50")}>
      <CardHeader>
        <CardTitle>Outbox ({entries.length} pending)</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {isLoading ? (
          <p className="text-sm text-muted-foreground px-4 pb-4">Loading…</p>
        ) : entries.length === 0 ? (
          <p className="text-sm text-muted-foreground px-4 pb-4">No pending outbox entries.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ID</TableHead>
                <TableHead>Event Type</TableHead>
                <TableHead>Created At</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {entries.map((entry) => (
                <TableRow key={entry.id}>
                  <TableCell className="font-mono text-xs">{entry.id}</TableCell>
                  <TableCell className="text-sm">{entry.payloadType}</TableCell>
                  <TableCell className="text-xs text-muted-foreground tabular-nums">
                    {new Date(entry.createdAt).toLocaleString()}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}

// ─── Payment Failure Card ─────────────────────────────────────────────────────

function PaymentFailureCard({ locked }: { locked: boolean }) {
  const { data, isLoading } = usePaymentFailure(!locked);
  const { mutateAsync: toggle, isPending } = useTogglePaymentFailure();

  async function handleToggle() {
    try {
      const next = await toggle(undefined);
      toast.success(next.failureInjected ? "Payment failure injection enabled" : "Payment failure disabled");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to toggle");
    }
  }

  return (
    <Card className={cn(locked && "opacity-50")}>
      <CardHeader>
        <CardTitle>Payment Failure Injection</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <p className="text-sm text-muted-foreground">Loading…</p>
        ) : (
          <div className="flex items-center gap-3">
            <Switch
              checked={data?.failureInjected ?? false}
              onCheckedChange={handleToggle}
              disabled={isPending || locked}
            />
            <Label className={cn("text-sm", data?.failureInjected ? "text-red-400" : "text-muted-foreground")}>
              {data?.failureInjected ? "Enabled — payments will fail" : "Disabled"}
            </Label>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ─── System Health Card ───────────────────────────────────────────────────────

function SystemHealthCard({ locked }: { locked: boolean }) {
  const { data, isLoading } = useHealth(!locked);
  const status = isLoading ? "CHECKING" : (data?.status ?? "UNKNOWN");
  const isUp = status === "UP";

  return (
    <Card className={cn(locked && "opacity-50")}>
      <CardHeader>
        <CardTitle>System Health</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-2">
          <span className={cn("size-3 rounded-full shrink-0", isUp ? "bg-green-500" : "bg-red-500")} />
          <span className={cn("font-semibold", isUp ? "text-green-400" : "text-red-400")}>{status}</span>
        </div>
        {data && (
          <pre className="mt-3 text-[10px] bg-muted/50 rounded p-2 overflow-auto max-h-24 font-mono">
            {JSON.stringify(data, null, 2)}
          </pre>
        )}
      </CardContent>
    </Card>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function AdminPage() {
  const { hasPermission } = useRole();
  const isAdmin = hasPermission("ADMIN_PANEL");

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Admin</h1>
        <p className="text-sm text-muted-foreground">System administration and diagnostics</p>
      </div>

      {!isAdmin && (
        <div className="flex items-center gap-2 rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 text-sm text-yellow-400">
          <Settings className="size-4 shrink-0" />
          <span>Admin panel requires the ADMIN role. Cards are shown read-only.</span>
        </div>
      )}

      <div className="grid gap-4 grid-cols-1 sm:grid-cols-2 lg:grid-cols-4">
        <CircuitBreakerCard locked={!isAdmin} />
        <SystemHealthCard locked={!isAdmin} />
        <PaymentFailureCard locked={!isAdmin} />
        <DeadLetterCard locked={!isAdmin} />
        <OutboxCard locked={!isAdmin} />
      </div>
    </div>
  );
}
