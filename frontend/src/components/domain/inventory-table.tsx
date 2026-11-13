"use client";

import { useState } from "react";
import { toast } from "sonner";
import { MoreHorizontal, Warehouse } from "lucide-react";
import { useInventory } from "@/lib/queries";
import { useAdjustInventory, useReceiveShipment } from "@/lib/mutations";
import type { InventoryView } from "@/lib/types";
import { isQueuedForReplay } from "@/lib/api";
import { useRole } from "@/providers/role-provider";
import { AccessRestricted } from "@/components/domain/access-restricted";
import { PermissionGate } from "@/components/domain/permission-gate";
import {
  PendingNotice,
  reportFailure,
  usePendingOperation,
} from "@/components/domain/held-operation";
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
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

// ─── Receive Shipment ─────────────────────────────────────────────────────────

function ReceiveShipmentForm({ row, onClose }: { row: InventoryView; onClose: () => void }) {
  const operation = usePendingOperation("receive-shipment", row.productId);
  const [quantity, setQuantity] = useState(() =>
    operation.pending ? String(operation.pending.quantity) : "1"
  );
  const { mutateAsync, isPending } = useReceiveShipment();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const amount = parseInt(quantity, 10);
    // One key per intended shipment: the page's own retry, and the same amount sent again from
    // this dialog, after it was closed or after a reload, carry it, so the backend applies the
    // shipment once.
    const idempotencyKey = operation.keyFor(amount);
    try {
      await mutateAsync({ productId: row.productId, body: { quantity: amount }, idempotencyKey });
      operation.refresh();
      toast.success("Shipment received into the inventory");
      onClose();
    } catch (err) {
      operation.refresh();
      reportFailure(err, "Failed to receive shipment");
      if (isQueuedForReplay(err)) onClose();
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <DialogHeader>
        <DialogTitle>Receive Shipment — {row.productName}</DialogTitle>
      </DialogHeader>
      <p className="text-sm text-muted-foreground">
        Adds units to the inventory, the one place that holds this product&apos;s stock and the one
        every order reserves from. Available now: {row.available}.
      </p>
      {operation.pending && (
        <PendingNotice pending={operation.pending} noun="shipment" onStartNew={operation.startNew} />
      )}
      <div className="space-y-1">
        <Label htmlFor="rs-qty">Quantity</Label>
        <Input
          id="rs-qty"
          type="number"
          min="1"
          value={quantity}
          onChange={(e) => setQuantity(e.target.value)}
          required
        />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Receive"}</Button>
      </DialogFooter>
    </form>
  );
}

// ─── Adjust ───────────────────────────────────────────────────────────────────

function AdjustInventoryForm({ row, onClose }: { row: InventoryView; onClose: () => void }) {
  const operation = usePendingOperation("adjust-inventory", row.productId);
  const [change, setChange] = useState(() =>
    operation.pending ? String(operation.pending.quantity) : ""
  );
  const [reason, setReason] = useState("");
  const { mutateAsync, isPending } = useAdjustInventory();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const quantityChange = parseInt(change, 10);
    // One key per intended adjustment, as for a shipment. The backend decides what it accepts:
    // a change of zero, a blank reason or a count below zero comes back as its 400 and reason.
    const idempotencyKey = operation.keyFor(quantityChange);
    try {
      await mutateAsync({
        productId: row.productId,
        body: { quantityChange, reason },
        idempotencyKey,
      });
      operation.refresh();
      toast.success("Inventory adjusted");
      onClose();
    } catch (err) {
      operation.refresh();
      reportFailure(err, "Failed to adjust the inventory");
      if (isQueuedForReplay(err)) onClose();
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <DialogHeader>
        <DialogTitle>Adjust Inventory — {row.productName}</DialogTitle>
      </DialogHeader>
      <p className="text-sm text-muted-foreground">
        Corrects the available units, after a stocktake for example. Reserved and committed units
        stay as they are. Available now: {row.available}.
      </p>
      {operation.pending && (
        <PendingNotice pending={operation.pending} noun="adjustment" onStartNew={operation.startNew} />
      )}
      <div className="space-y-1">
        <Label htmlFor="ai-change">Quantity change (positive adds, negative removes)</Label>
        <Input
          id="ai-change"
          type="number"
          value={change}
          onChange={(e) => setChange(e.target.value)}
          required
        />
      </div>
      <div className="space-y-1">
        <Label htmlFor="ai-reason">Reason</Label>
        <Input
          id="ai-reason"
          value={reason}
          onChange={(e) => setReason(e.target.value)}
          placeholder="e.g. stocktake: two damaged"
          required
        />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Adjust"}</Button>
      </DialogFooter>
    </form>
  );
}

// ─── Row Actions ──────────────────────────────────────────────────────────────

type ActionDialogMode = "receive-shipment" | "adjust-inventory" | null;

function InventoryRowActions({ row }: { row: InventoryView }) {
  const [dialogMode, setDialogMode] = useState<ActionDialogMode>(null);
  const close = () => setDialogMode(null);

  return (
    <Dialog open={dialogMode !== null} onOpenChange={(open) => { if (!open) close(); }}>
      <PermissionGate permission="INVENTORY_MANAGE">
        <DropdownMenu>
          <DropdownMenuTrigger render={<Button variant="ghost" size="icon-sm" />}>
            <MoreHorizontal className="size-4" />
            <span className="sr-only">Actions</span>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => setDialogMode("receive-shipment")}>
              Receive Shipment
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setDialogMode("adjust-inventory")}>
              Adjust
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </PermissionGate>
      {dialogMode === "receive-shipment" && (
        <DialogContent>
          <ReceiveShipmentForm row={row} onClose={close} />
        </DialogContent>
      )}
      {dialogMode === "adjust-inventory" && (
        <DialogContent>
          <AdjustInventoryForm row={row} onClose={close} />
        </DialogContent>
      )}
    </Dialog>
  );
}

// ─── Main Table ───────────────────────────────────────────────────────────────

/**
 * Admin → Inventory: every product's units, read from the inventory read
 * model. The live feed's inventory events read the table again, so an order's
 * reservation, its confirmation or its release shows here by itself.
 */
export function InventoryTable() {
  const { hasPermission } = useRole();
  const allowed = hasPermission("INVENTORY_MANAGE");
  const { data: rows = [], isLoading } = useInventory(allowed);

  if (!allowed) {
    return <AccessRestricted message="Requires ADMIN role to view the inventory." />;
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <Warehouse className="size-5 mr-2 animate-pulse" />
        Loading inventory…
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Inventory</h1>
        <p className="text-sm text-muted-foreground">
          The units of every product: available to order, reserved for orders in fulfillment,
          committed to confirmed orders
        </p>
      </div>

      <div className="rounded-xl border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Product</TableHead>
              <TableHead className="text-right">Available</TableHead>
              <TableHead className="text-right">Reserved</TableHead>
              <TableHead className="text-right">Committed</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                  No products yet. Create one in Admin → Catalog.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row) => (
                <TableRow key={row.productId}>
                  <TableCell>
                    <span className="font-medium">{row.productName}</span>
                    <p className="font-mono text-xs text-muted-foreground">{row.productId}</p>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{row.available}</TableCell>
                  <TableCell className="text-right tabular-nums">{row.reserved}</TableCell>
                  <TableCell className="text-right tabular-nums">{row.committed}</TableCell>
                  <TableCell>
                    <InventoryRowActions row={row} />
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
