"use client";

import { Package, ShoppingCart, Users, Zap } from "lucide-react";
import { StatCard } from "@/components/domain/stat-card";
import { EventFeed } from "@/components/domain/event-feed";
import { useProducts } from "@/lib/queries";
import { useOrders } from "@/lib/queries";
import { useCustomers } from "@/lib/queries";
import { useSSE } from "@/providers/sse-provider";
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
import { Badge } from "@/components/ui/badge";
import { cn, formatMoney } from "@/lib/utils";
import type { OrderStatus } from "@/lib/types";

const ORDER_STATUS_STYLES: Record<OrderStatus, string> = {
  CREATED: "bg-blue-500/10 text-blue-400 border-blue-500/20",
  CONFIRMED: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  SHIPPED: "bg-orange-500/10 text-orange-400 border-orange-500/20",
  DELIVERED: "bg-green-500/10 text-green-400 border-green-500/20",
  CANCELLED: "bg-red-500/10 text-red-400 border-red-500/20",
};

export default function DashboardPage() {
  const { data: products = [] } = useProducts();
  const { data: orders = [] } = useOrders();
  const { data: customers = [] } = useCustomers();
  const { recentEvents } = useSSE();

  const recentOrders = orders.slice(0, 5);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <p className="text-muted-foreground mt-1 text-sm">StreamRune E-Commerce Demo</p>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard
          title="Products"
          value={products.length}
          subtitle={`${products.filter((p) => p.status === "AVAILABLE").length} available`}
          icon={Package}
          href="/products"
        />
        <StatCard
          title="Orders"
          value={orders.length}
          subtitle={`${orders.filter((o) => o.status === "CREATED").length} pending`}
          icon={ShoppingCart}
          href="/orders"
        />
        <StatCard
          title="Customers"
          value={customers.length}
          subtitle={`${customers.filter((c) => c.status === "ACTIVE").length} active`}
          icon={Users}
          href="/customers"
        />
        <StatCard
          title="Live Events"
          value={recentEvents.length}
          subtitle="Last 50 events"
          icon={Zap}
          href="/events"
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {/* Recent Orders */}
        <Card>
          <CardHeader>
            <CardTitle>Recent Orders</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {recentOrders.length === 0 ? (
              <p className="text-sm text-muted-foreground px-4 pb-4">No orders yet.</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Order</TableHead>
                    <TableHead>Customer</TableHead>
                    <TableHead>Total</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {recentOrders.map((order) => (
                    <TableRow key={order.orderId}>
                      <TableCell className="font-mono text-xs">{order.orderId.slice(0, 8)}…</TableCell>
                      <TableCell className="font-mono text-xs">{order.customerId.slice(0, 8)}…</TableCell>
                      <TableCell>{formatMoney(order.total.amount, order.total.currency)}</TableCell>
                      <TableCell>
                        <Badge className={cn("text-xs", ORDER_STATUS_STYLES[order.status])}>
                          {order.status}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        {/* Live Event Feed */}
        <Card>
          <CardHeader>
            <CardTitle>Live Event Feed</CardTitle>
          </CardHeader>
          <CardContent>
            <EventFeed maxItems={15} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
