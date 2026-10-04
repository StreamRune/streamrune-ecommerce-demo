"use client";

import { useState } from "react";
import { toast } from "sonner";
import { Plus, ShoppingCart, ChevronDown, ChevronRight } from "lucide-react";
import { useOrders, useProducts, useCustomers } from "@/lib/queries";
import {
  usePlaceOrder,
  useConfirmOrder,
  useShipOrder,
  useDeliverOrder,
  useCancelOrder,
} from "@/lib/mutations";
import type { OrderView, OrderStatus } from "@/lib/types";
import { PermissionGate } from "@/components/domain/permission-gate";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn, formatMoney } from "@/lib/utils";

const STATUS_STYLES: Record<OrderStatus, string> = {
  CREATED: "bg-blue-500/10 text-blue-400 border-blue-500/20",
  CONFIRMED: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  SHIPPED: "bg-orange-500/10 text-orange-400 border-orange-500/20",
  DELIVERED: "bg-green-500/10 text-green-400 border-green-500/20",
  CANCELLED: "bg-red-500/10 text-red-400 border-red-500/20",
};

// ─── Place Order Dialog ───────────────────────────────────────────────────────

interface OrderLine {
  productId: string;
  quantity: number;
}

function PlaceOrderDialog({ initialCustomerId }: { initialCustomerId?: string }) {
  const [open, setOpen] = useState(false);
  const [customerId, setCustomerId] = useState(initialCustomerId ?? "");
  const [lines, setLines] = useState<OrderLine[]>([{ productId: "", quantity: 1 }]);
  const { mutateAsync, isPending } = usePlaceOrder();
  const { data: products = [] } = useProducts();
  const { data: customers = [] } = useCustomers();

  function addLine() {
    setLines((l) => [...l, { productId: "", quantity: 1 }]);
  }

  function removeLine(i: number) {
    setLines((l) => l.filter((_, idx) => idx !== i));
  }

  function updateLine(i: number, field: keyof OrderLine, value: string | number) {
    setLines((l) => l.map((line, idx) => idx === i ? { ...line, [field]: value } : line));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const validLines = lines.filter((l) => l.productId && l.quantity > 0);
    if (validLines.length === 0) {
      toast.error("Add at least one product line");
      return;
    }
    try {
      await mutateAsync({ customerId, lines: validLines });
      toast.success("Order placed");
      setOpen(false);
      setLines([{ productId: "", quantity: 1 }]);
      if (!initialCustomerId) setCustomerId("");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to place order");
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <PermissionGate permission="ORDER_PLACE">
        <DialogTrigger render={<Button />}>
          <Plus className="size-4" />
          Place Order
        </DialogTrigger>
      </PermissionGate>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Place Order</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-1">
            <Label htmlFor="po-customer">Customer</Label>
            {customers.length > 0 ? (
              <Select value={customerId} onValueChange={(v) => { if (v) setCustomerId(v as string); }}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select customer…" />
                </SelectTrigger>
                <SelectContent>
                  {customers.map((c) => (
                    <SelectItem key={c.customerId} value={c.customerId}>
                      {c.name} ({c.email})
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : (
              <Input
                id="po-customer"
                placeholder="Customer ID"
                value={customerId}
                onChange={(e) => setCustomerId(e.target.value)}
                required
              />
            )}
          </div>
          <div className="space-y-2">
            <Label>Order Lines</Label>
            {lines.map((line, i) => (
              <div key={i} className="flex gap-2 items-end">
                <div className="flex-1 space-y-1">
                  <Select value={line.productId} onValueChange={(v) => { if (v) updateLine(i, "productId", v as string); }}>
                    <SelectTrigger className="w-full">
                      <SelectValue placeholder="Select product…" />
                    </SelectTrigger>
                    <SelectContent>
                      {products.filter((p) => p.status !== "DISCONTINUED").map((p) => (
                        <SelectItem key={p.productId} value={p.productId}>
                          {p.name} ({formatMoney(p.price.amount, p.price.currency)})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="w-20">
                  <Input
                    type="number"
                    min="1"
                    value={line.quantity}
                    onChange={(e) => updateLine(i, "quantity", parseInt(e.target.value, 10))}
                  />
                </div>
                {lines.length > 1 && (
                  <Button variant="ghost" size="icon-sm" type="button" onClick={() => removeLine(i)}>
                    ×
                  </Button>
                )}
              </div>
            ))}
            <Button type="button" variant="outline" size="sm" onClick={addLine}>
              Add Line
            </Button>
          </div>
          <DialogFooter showCloseButton>
            <Button type="submit" disabled={isPending || !customerId}>
              {isPending ? "Placing…" : "Place Order"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

// ─── Cancel Dialog ────────────────────────────────────────────────────────────

function CancelOrderDialog({ orderId, onClose }: { orderId: string; onClose: () => void }) {
  const { mutateAsync, isPending } = useCancelOrder();

  async function handleConfirm() {
    try {
      await mutateAsync(orderId);
      toast.success("Order cancelled");
      onClose();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to cancel order");
    }
  }

  return (
    <>
      <DialogHeader>
        <DialogTitle>Cancel Order</DialogTitle>
      </DialogHeader>
      <p className="text-sm text-muted-foreground">
        Are you sure you want to cancel this order? This action cannot be undone.
      </p>
      <DialogFooter showCloseButton>
        <Button variant="destructive" onClick={handleConfirm} disabled={isPending}>
          {isPending ? "Cancelling…" : "Cancel Order"}
        </Button>
      </DialogFooter>
    </>
  );
}

// ─── Order Row ────────────────────────────────────────────────────────────────

function OrderRow({ order }: { order: OrderView }) {
  const [expanded, setExpanded] = useState(false);
  const [cancelOpen, setCancelOpen] = useState(false);
  const { mutateAsync: confirm, isPending: isConfirming } = useConfirmOrder();
  const { mutateAsync: ship, isPending: isShipping } = useShipOrder();
  const { mutateAsync: deliver, isPending: isDelivering } = useDeliverOrder();

  async function handleAction(action: "confirm" | "ship" | "deliver") {
    try {
      if (action === "confirm") await confirm(order.orderId);
      else if (action === "ship") await ship(order.orderId);
      else await deliver(order.orderId);
      toast.success(`Order ${action}ed`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed to ${action} order`);
    }
  }

  return (
    <>
      <TableRow>
        <TableCell>
          <button
            onClick={() => setExpanded((e) => !e)}
            className="flex items-center gap-1 text-left hover:text-foreground text-muted-foreground"
          >
            {expanded ? <ChevronDown className="size-3.5" /> : <ChevronRight className="size-3.5" />}
          </button>
        </TableCell>
        <TableCell className="font-mono text-xs">{order.orderId.slice(0, 8)}…</TableCell>
        <TableCell className="font-mono text-xs">{order.customerId.slice(0, 8)}…</TableCell>
        <TableCell>{formatMoney(order.total.amount, order.total.currency)}</TableCell>
        <TableCell>
          <Badge className={cn("text-xs", STATUS_STYLES[order.status])}>
            {order.status}
          </Badge>
        </TableCell>
        <TableCell>
          <div className="flex gap-1">
            {order.status === "CREATED" && (
              <>
                <PermissionGate permission="ORDER_CANCEL_CONFIRMED">
                  <Button variant="outline" size="xs" onClick={() => handleAction("confirm")} disabled={isConfirming}>
                    Confirm
                  </Button>
                </PermissionGate>
                <PermissionGate permission="ORDER_CANCEL_OWN">
                  <Button variant="ghost" size="xs" onClick={() => setCancelOpen(true)}>
                    Cancel
                  </Button>
                </PermissionGate>
              </>
            )}
            {order.status === "CONFIRMED" && (
              <PermissionGate permission="ORDER_SHIP">
                <Button variant="outline" size="xs" onClick={() => handleAction("ship")} disabled={isShipping}>
                  Ship
                </Button>
              </PermissionGate>
            )}
            {order.status === "SHIPPED" && (
              <PermissionGate permission="ORDER_DELIVER">
                <Button variant="outline" size="xs" onClick={() => handleAction("deliver")} disabled={isDelivering}>
                  Deliver
                </Button>
              </PermissionGate>
            )}
          </div>
        </TableCell>
      </TableRow>
      {expanded && (
        <TableRow className="bg-muted/30">
          <TableCell colSpan={6} className="p-4">
            <div className="space-y-1">
              <p className="text-xs font-medium text-muted-foreground mb-2">Order Lines</p>
              {order.lines.map((line, i) => (
                <div key={i} className="flex gap-4 text-sm">
                  <span className="font-mono text-xs text-muted-foreground flex-1">{line.productId}</span>
                  <span>×{line.quantity}</span>
                  <span>{formatMoney(line.unitPrice.amount, line.unitPrice.currency)}</span>
                </div>
              ))}
            </div>
          </TableCell>
        </TableRow>
      )}
      <Dialog open={cancelOpen} onOpenChange={setCancelOpen}>
        <DialogContent>
          <CancelOrderDialog orderId={order.orderId} onClose={() => setCancelOpen(false)} />
        </DialogContent>
      </Dialog>
    </>
  );
}

// ─── Main Table ───────────────────────────────────────────────────────────────

interface OrderTableProps {
  customerId?: string;
}

export function OrderTable({ customerId }: OrderTableProps) {
  const { data: orders = [], isLoading } = useOrders(customerId);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <ShoppingCart className="size-5 mr-2 animate-pulse" />
        Loading orders…
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Orders</h1>
          <p className="text-sm text-muted-foreground">
            {orders.length} total{customerId ? ` for ${customerId}` : ""}
          </p>
        </div>
        <PlaceOrderDialog initialCustomerId={customerId} />
      </div>

      <div className="rounded-xl border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-8" />
              <TableHead>Order ID</TableHead>
              <TableHead>Customer</TableHead>
              <TableHead>Total</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {orders.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                  No orders yet. Place one!
                </TableCell>
              </TableRow>
            ) : (
              orders.map((order) => <OrderRow key={order.orderId} order={order} />)
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
