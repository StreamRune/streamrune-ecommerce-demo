"use client";

import { useState } from "react";
import { ShieldOff } from "lucide-react";
import { useAuditLog } from "@/lib/queries";
import { useRole } from "@/providers/role-provider";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

const LIMIT_OPTIONS = [25, 50, 100];

export default function AuditPage() {
  const { hasPermission } = useRole();
  const [limit, setLimit] = useState(25);
  const { data: entries = [], isLoading } = useAuditLog(limit);

  if (!hasPermission("AUDIT_VIEW")) {
    return (
      <div className="flex flex-col items-center justify-center py-24 gap-4 text-center">
        <ShieldOff className="size-12 text-muted-foreground opacity-40" />
        <div>
          <h2 className="text-lg font-semibold">Access Restricted</h2>
          <p className="text-sm text-muted-foreground mt-1">Requires ADMIN role to view audit log.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Audit Log</h1>
          <p className="text-sm text-muted-foreground">{entries.length} entries</p>
        </div>
        <div className="flex items-center gap-2">
          <Label htmlFor="audit-limit" className="text-sm">Show</Label>
          <Select value={limit.toString()} onValueChange={(v) => { if (v) setLimit(parseInt(v as string, 10)); }}>
            <SelectTrigger id="audit-limit" className="w-24">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {LIMIT_OPTIONS.map((n) => (
                <SelectItem key={n} value={n.toString()}>{n}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="rounded-xl border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Command ID</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>Aggregate</TableHead>
              <TableHead>User</TableHead>
              <TableHead>Timestamp</TableHead>
              <TableHead>Outcome</TableHead>
              <TableHead>Events</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                  Loading audit log…
                </TableCell>
              </TableRow>
            ) : entries.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                  No audit entries yet.
                </TableCell>
              </TableRow>
            ) : (
              entries.map((entry) => {
                const isSuccess = entry.outcome === "SUCCESS";
                return (
                  <TableRow key={entry.commandId}>
                    <TableCell className="font-mono text-xs">{entry.commandId.slice(0, 8)}…</TableCell>
                    <TableCell className="text-sm">{entry.commandType}</TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">{entry.aggregateId.slice(0, 8)}…</TableCell>
                    <TableCell className="text-xs text-muted-foreground">{entry.userId || "—"}</TableCell>
                    <TableCell className="text-xs text-muted-foreground tabular-nums">
                      {new Date(entry.occurredAt).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      <Badge
                        className={cn(
                          "text-xs",
                          isSuccess
                            ? "bg-green-500/10 text-green-400 border-green-500/20"
                            : "bg-red-500/10 text-red-400 border-red-500/20"
                        )}
                      >
                        {entry.outcome}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-center">{entry.eventCount}</TableCell>
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
