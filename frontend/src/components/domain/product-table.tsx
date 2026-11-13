"use client";

import { Package } from "lucide-react";
import { useProducts } from "@/lib/queries";
import type { Availability } from "@/lib/types";
import { useRole } from "@/providers/role-provider";
import { PlaceOrderDialog } from "@/components/domain/order-table";
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

const AVAILABILITY_STYLES: Record<Availability, string> = {
  IN_STOCK: "bg-green-500/10 text-green-400 border-green-500/20",
  LOW_STOCK: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  OUT_OF_STOCK: "bg-red-500/10 text-red-400 border-red-500/20",
  DISCONTINUED: "bg-muted text-muted-foreground",
};

const AVAILABILITY_LABELS: Record<Availability, string> = {
  IN_STOCK: "In stock",
  LOW_STOCK: "Low stock",
  OUT_OF_STOCK: "Out of stock",
  DISCONTINUED: "Discontinued",
};

/** A product a customer can order now: sold, and with units the saga can reserve. */
function orderable(availability: Availability): boolean {
  return availability === "IN_STOCK" || availability === "LOW_STOCK";
}

/**
 * The storefront: what the shop sells, for how much, and whether it can be
 * ordered now. The availability is a badge; the number of units is the
 * inventory's business (Admin → Inventory), and so are the catalogue's
 * actions (Admin → Catalog): this page offers none of them, for any role.
 */
export function ProductTable() {
  const { data: products = [], isLoading } = useProducts();
  const { role, userId } = useRole();
  // A CUSTOMER orders for themself; an ADMIN picks the customer in the dialog.
  const customerId = role === "CUSTOMER" ? (userId ?? undefined) : undefined;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <Package className="size-5 mr-2 animate-pulse" />
        Loading products…
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Products</h1>
        <p className="text-sm text-muted-foreground">{products.length} total</p>
      </div>

      <div className="rounded-xl border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Category</TableHead>
              <TableHead>Price</TableHead>
              <TableHead>Availability</TableHead>
              <TableHead className="w-20" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {products.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                  No products yet.
                </TableCell>
              </TableRow>
            ) : (
              products.map((product) => (
                <TableRow
                  key={product.productId}
                  className={cn(product.availability === "DISCONTINUED" && "opacity-50")}
                >
                  <TableCell>
                    <span
                      className={cn(
                        "font-medium",
                        product.availability === "DISCONTINUED" && "line-through"
                      )}
                    >
                      {product.name}
                    </span>
                    {product.description && (
                      <p className="text-xs text-muted-foreground truncate max-w-48">{product.description}</p>
                    )}
                  </TableCell>
                  <TableCell className="text-muted-foreground">{product.category}</TableCell>
                  <TableCell>{formatMoney(product.price.amount, product.price.currency)}</TableCell>
                  <TableCell>
                    <Badge className={cn("text-xs", AVAILABILITY_STYLES[product.availability])}>
                      {AVAILABILITY_LABELS[product.availability]}
                    </Badge>
                  </TableCell>
                  <TableCell>
                    <PlaceOrderDialog
                      key={customerId ?? "any"}
                      initialCustomerId={customerId}
                      initialProductId={product.productId}
                      triggerLabel="Order"
                      triggerDisabled={!orderable(product.availability)}
                    />
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
