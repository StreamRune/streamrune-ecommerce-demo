"use client";

import { useState } from "react";
import { toast } from "sonner";
import { Plus, MoreHorizontal, Package } from "lucide-react";
import {
  useProducts,
} from "@/lib/queries";
import {
  useCreateProduct,
  useAdjustStock,
  useUpdatePrice,
  useDiscontinueProduct,
  useReceiveShipment,
} from "@/lib/mutations";
import type { ProductView, ProductStatus } from "@/lib/types";
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn, formatMoney } from "@/lib/utils";

const STATUS_STYLES: Record<ProductStatus, string> = {
  AVAILABLE: "bg-green-500/10 text-green-400 border-green-500/20",
  LOW_STOCK: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  OUT_OF_STOCK: "bg-red-500/10 text-red-400 border-red-500/20",
  DISCONTINUED: "bg-muted text-muted-foreground",
};

// ─── Create Product Dialog ────────────────────────────────────────────────────

function CreateProductDialog() {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [category, setCategory] = useState("");
  const [price, setPrice] = useState("");
  const [stock, setStock] = useState("0");
  const { mutateAsync, isPending } = useCreateProduct();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      await mutateAsync({
        name,
        description,
        category,
        price: { amount: parseFloat(price), currency: "USD" },
        initialStock: parseInt(stock, 10),
      });
      toast.success("Product created");
      setOpen(false);
      setName(""); setDescription(""); setCategory(""); setPrice(""); setStock("0");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to create product");
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <PermissionGate permission="PRODUCT_MANAGE">
        <DialogTrigger render={<Button />}>
          <Plus className="size-4" />
          Add Product
        </DialogTrigger>
      </PermissionGate>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Create Product</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-1">
            <Label htmlFor="cp-name">Name</Label>
            <Input id="cp-name" value={name} onChange={(e) => setName(e.target.value)} required />
          </div>
          <div className="space-y-1">
            <Label htmlFor="cp-desc">Description</Label>
            <Input id="cp-desc" value={description} onChange={(e) => setDescription(e.target.value)} />
          </div>
          <div className="space-y-1">
            <Label htmlFor="cp-cat">Category</Label>
            <Input id="cp-cat" value={category} onChange={(e) => setCategory(e.target.value)} required />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1">
              <Label htmlFor="cp-price">Price (USD)</Label>
              <Input id="cp-price" type="number" step="0.01" min="0" value={price} onChange={(e) => setPrice(e.target.value)} required />
            </div>
            <div className="space-y-1">
              <Label htmlFor="cp-stock">Initial Stock</Label>
              <Input id="cp-stock" type="number" min="0" value={stock} onChange={(e) => setStock(e.target.value)} required />
            </div>
          </div>
          <DialogFooter showCloseButton>
            <Button type="submit" disabled={isPending}>
              {isPending ? "Creating…" : "Create"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

// ─── Row Action Dialogs ───────────────────────────────────────────────────────

function AdjustStockForm({ product, onClose }: { product: ProductView; onClose: () => void }) {
  const [delta, setDelta] = useState("0");
  const [reason, setReason] = useState("");
  const { mutateAsync, isPending } = useAdjustStock();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      await mutateAsync({ productId: product.productId, body: { delta: parseInt(delta, 10), reason: reason || undefined } });
      toast.success("Stock adjusted");
      onClose();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to adjust stock");
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <DialogHeader>
        <DialogTitle>Adjust Stock — {product.name}</DialogTitle>
      </DialogHeader>
      <p className="text-sm text-muted-foreground">Current stock: {product.stock}</p>
      <div className="space-y-1">
        <Label htmlFor="as-delta">Delta (positive = add, negative = remove)</Label>
        <Input id="as-delta" type="number" value={delta} onChange={(e) => setDelta(e.target.value)} required />
      </div>
      <div className="space-y-1">
        <Label htmlFor="as-reason">Reason (optional)</Label>
        <Input id="as-reason" value={reason} onChange={(e) => setReason(e.target.value)} />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Adjust"}</Button>
      </DialogFooter>
    </form>
  );
}

function UpdatePriceForm({ product, onClose }: { product: ProductView; onClose: () => void }) {
  const [amount, setAmount] = useState(product.price.amount.toString());
  const { mutateAsync, isPending } = useUpdatePrice();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      await mutateAsync({ productId: product.productId, body: { amount: parseFloat(amount), currency: product.price.currency } });
      toast.success("Price updated");
      onClose();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to update price");
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <DialogHeader>
        <DialogTitle>Update Price — {product.name}</DialogTitle>
      </DialogHeader>
      <div className="space-y-1">
        <Label htmlFor="up-amount">New Price ({product.price.currency})</Label>
        <Input id="up-amount" type="number" step="0.01" min="0" value={amount} onChange={(e) => setAmount(e.target.value)} required />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Update"}</Button>
      </DialogFooter>
    </form>
  );
}

function ReceiveShipmentForm({ product, onClose }: { product: ProductView; onClose: () => void }) {
  const [quantity, setQuantity] = useState("1");
  const { mutateAsync, isPending } = useReceiveShipment();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      await mutateAsync({ productId: product.productId, body: { quantity: parseInt(quantity, 10) } });
      toast.success("Shipment received");
      onClose();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to receive shipment");
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <DialogHeader>
        <DialogTitle>Receive Shipment — {product.name}</DialogTitle>
      </DialogHeader>
      <div className="space-y-1">
        <Label htmlFor="rs-qty">Quantity</Label>
        <Input id="rs-qty" type="number" min="1" value={quantity} onChange={(e) => setQuantity(e.target.value)} required />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Receive"}</Button>
      </DialogFooter>
    </form>
  );
}

// ─── Row Actions ──────────────────────────────────────────────────────────────

type ActionDialogMode = "adjust-stock" | "update-price" | "receive-shipment" | null;

function ProductRowActions({ product }: { product: ProductView }) {
  const [dialogMode, setDialogMode] = useState<ActionDialogMode>(null);
  const { mutateAsync: discontinue, isPending: isDiscontinuing } = useDiscontinueProduct();

  async function handleDiscontinue() {
    try {
      await discontinue(product.productId);
      toast.success("Product discontinued");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to discontinue product");
    }
  }

  const isDiscontinued = product.status === "DISCONTINUED";

  return (
    <Dialog open={dialogMode !== null} onOpenChange={(open) => { if (!open) setDialogMode(null); }}>
      <PermissionGate permission="PRODUCT_MANAGE">
        <DropdownMenu>
          <DropdownMenuTrigger render={<Button variant="ghost" size="icon-sm" />}>
            <MoreHorizontal className="size-4" />
            <span className="sr-only">Actions</span>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => setDialogMode("adjust-stock")}>
              Adjust Stock
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setDialogMode("update-price")}>
              Update Price
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setDialogMode("receive-shipment")}>
              Receive Shipment
            </DropdownMenuItem>
            {!isDiscontinued && (
              <DropdownMenuItem
                className="text-destructive"
                onClick={handleDiscontinue}
                disabled={isDiscontinuing}
              >
                Discontinue
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </PermissionGate>
      {dialogMode === "adjust-stock" && (
        <DialogContent>
          <AdjustStockForm product={product} onClose={() => setDialogMode(null)} />
        </DialogContent>
      )}
      {dialogMode === "update-price" && (
        <DialogContent>
          <UpdatePriceForm product={product} onClose={() => setDialogMode(null)} />
        </DialogContent>
      )}
      {dialogMode === "receive-shipment" && (
        <DialogContent>
          <ReceiveShipmentForm product={product} onClose={() => setDialogMode(null)} />
        </DialogContent>
      )}
    </Dialog>
  );
}

// ─── Main Table ───────────────────────────────────────────────────────────────

export function ProductTable() {
  const { data: products = [], isLoading } = useProducts();

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
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Products</h1>
          <p className="text-sm text-muted-foreground">{products.length} total</p>
        </div>
        <CreateProductDialog />
      </div>

      <div className="rounded-xl border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Category</TableHead>
              <TableHead>Price</TableHead>
              <TableHead>Stock</TableHead>
              <TableHead>Status</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {products.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                  No products yet. Create one!
                </TableCell>
              </TableRow>
            ) : (
              products.map((product) => (
                <TableRow
                  key={product.productId}
                  className={cn(product.status === "DISCONTINUED" && "opacity-50")}
                >
                  <TableCell>
                    <span className={cn("font-medium", product.status === "DISCONTINUED" && "line-through")}>
                      {product.name}
                    </span>
                    {product.description && (
                      <p className="text-xs text-muted-foreground truncate max-w-48">{product.description}</p>
                    )}
                  </TableCell>
                  <TableCell className="text-muted-foreground">{product.category}</TableCell>
                  <TableCell>{formatMoney(product.price.amount, product.price.currency)}</TableCell>
                  <TableCell>{product.stock}</TableCell>
                  <TableCell>
                    <Badge className={cn("text-xs", STATUS_STYLES[product.status])}>
                      {product.status.replace("_", " ")}
                    </Badge>
                  </TableCell>
                  <TableCell>
                    <ProductRowActions product={product} />
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
