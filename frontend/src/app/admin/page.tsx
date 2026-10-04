"use client";

import { toast } from "sonner";
import { Settings, RefreshCw } from "lucide-react";
import {
  useCircuitBreaker,
  useDeadLetters,
  useOutbox,
  usePaymentFailure,
  useHealth,
} from "@/lib/queries";
import { useRetryDeadLetter, useTogglePaymentFailure } from "@/lib/mutations";
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
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

// ─── Circuit Breaker Card ─────────────────────────────────────────────────────

function CircuitBreakerCard({ locked }: { locked: boolean }) {
  const { data, isLoading } = useCircuitBreaker(!locked);

  const state = isLoading
    ? "LOADING"
    : ((data as Record<string, unknown>)?.state as string | undefined) ?? "UNKNOWN";

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

function DeadLetterCard({ locked }: { locked: boolean }) {
  const { data: entries = [], isLoading } = useDeadLetters(!locked);
  const { mutateAsync: retry, isPending } = useRetryDeadLetter();

  async function handleRetry(id: string) {
    try {
      await retry(id);
      toast.success("Retrying dead letter");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to retry");
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
                <TableRow key={entry.id}>
                  <TableCell className="text-sm">{entry.commandType}</TableCell>
                  <TableCell className="text-xs text-muted-foreground max-w-48 truncate">{entry.error}</TableCell>
                  <TableCell className="text-xs text-muted-foreground tabular-nums">
                    {new Date(entry.timestamp).toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="outline"
                      size="xs"
                      onClick={() => handleRetry(entry.id)}
                      disabled={isPending || locked}
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
                  <TableCell className="font-mono text-xs">{entry.id.slice(0, 8)}…</TableCell>
                  <TableCell className="text-sm">{entry.eventType}</TableCell>
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
      await toggle(undefined);
      toast.success("Payment failure toggled");
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
              checked={data?.enabled ?? false}
              onCheckedChange={handleToggle}
              disabled={isPending || locked}
            />
            <Label className={cn("text-sm", data?.enabled ? "text-red-400" : "text-muted-foreground")}>
              {data?.enabled ? "Enabled — payments will fail" : "Disabled"}
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
  const status = isLoading
    ? "CHECKING"
    : ((data as Record<string, unknown>)?.status as string | undefined) ?? "UNKNOWN";
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
