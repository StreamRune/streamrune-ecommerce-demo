"use client";

import { useState } from "react";
import { toast } from "sonner";
import { Plus, MoreHorizontal, BookOpen } from "lucide-react";
import { useProducts } from "@/lib/queries";
import {
  useCreateProduct,
  useUpdatePrice,
  useDiscontinueProduct,
  useReceiveShipment,
} from "@/lib/mutations";
import type { ProductView, ProductStatus } from "@/lib/types";
import { newId } from "@/lib/ids";
import { isQueuedForReplay } from "@/lib/api";
import { keyForOperation } from "@/lib/pending-operations";
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
  DISCONTINUED: "bg-muted text-muted-foreground",
};

// ─── Create Product Dialog ────────────────────────────────────────────────────

/** Where the two steps of a create with opening stock stand, as the dialog shows them. */
type Step = "idle" | "creating" | "receiving";

/**
 * Add Product. A product holds no stock, so the opening stock is not part of
 * CreateProduct: when the field holds a number above zero, the dialog sends
 * ReceiveShipment for it once the product exists, as a second command of its
 * own. Each step says how it went. The shipment goes out under the key of the
 * intended change (`pending-operations.ts`), so when it fails the Receive
 * Shipment dialog of Admin → Inventory opens on it and sends it again under
 * the same key.
 */
function CreateProductDialog() {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [category, setCategory] = useState("");
  const [price, setPrice] = useState("");
  const [openingStock, setOpeningStock] = useState("");
  const [step, setStep] = useState<Step>("idle");
  const { mutateAsync: create } = useCreateProduct();
  const { mutateAsync: receive } = useReceiveShipment();

  function reset() {
    setName(""); setDescription(""); setCategory(""); setPrice(""); setOpeningStock("");
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const units = openingStock.trim() === "" ? 0 : parseInt(openingStock, 10);
    // Step 1: the backend takes the product id from the request and the price as a plain USD
    // amount.
    let productId: string;
    setStep("creating");
    try {
      productId = newId("prod");
      await create({ productId, name, description, category, price: parseFloat(price) });
      toast.success(`Product created: ${productId}`);
    } catch (err) {
      setStep("idle");
      toast.error(err instanceof Error ? err.message : "Failed to create product");
      return;
    }
    // Step 2: the opening stock, received into the inventory like any shipment. The product
    // exists whatever this step answers, so the dialog closes either way.
    if (units > 0) {
      setStep("receiving");
      try {
        await receive({
          productId,
          body: { quantity: units },
          idempotencyKey: keyForOperation("receive-shipment", productId, units),
        });
        toast.success(
          `Opening stock received: ${units} ${units === 1 ? "unit" : "units"} of ${productId}`
        );
      } catch (err) {
        const answer = err instanceof Error ? err.message : "no answer";
        const message = `Product ${productId} was created, but its opening stock of ${units} was not received (${answer}). Receive it from Admin → Inventory.`;
        if (isQueuedForReplay(err)) toast.info(message);
        else toast.error(message);
      }
    }
    setStep("idle");
    setOpen(false);
    reset();
  }

  const busy = step !== "idle";

  return (
    <Dialog open={open} onOpenChange={(next) => { if (!busy) setOpen(next); }}>
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
              <Input id="cp-price" type="number" step="0.01" min="0.01" value={price} onChange={(e) => setPrice(e.target.value)} required />
            </div>
            <div className="space-y-1">
              <Label htmlFor="cp-opening">Opening stock (optional)</Label>
              <Input
                id="cp-opening"
                type="number"
                min="0"
                placeholder="0"
                value={openingStock}
                onChange={(e) => setOpeningStock(e.target.value)}
              />
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            The catalogue holds no stock. An opening stock is received into the inventory as a
            shipment, a second command sent once the product exists.
          </p>
          {busy && (
            <ol role="status" className="text-sm space-y-1">
              <li>
                Step 1: create the product — {step === "creating" ? "sending…" : "done"}
              </li>
              {step === "receiving" && (
                <li>Step 2: receive the opening stock into the inventory — sending…</li>
              )}
            </ol>
          )}
          <DialogFooter showCloseButton>
            <Button type="submit" disabled={busy}>
              {busy ? "Creating…" : "Create"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

// ─── Row Actions ──────────────────────────────────────────────────────────────

function UpdatePriceForm({ product, onClose }: { product: ProductView; onClose: () => void }) {
  const [amount, setAmount] = useState(product.price.amount.toString());
  const { mutateAsync, isPending } = useUpdatePrice();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      // The backend's field is `price`, a plain amount; the currency is USD on the backend.
      await mutateAsync({ productId: product.productId, body: { price: parseFloat(amount) } });
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
        <Input id="up-amount" type="number" step="0.01" min="0.01" value={amount} onChange={(e) => setAmount(e.target.value)} required />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Update"}</Button>
      </DialogFooter>
    </form>
  );
}

function CatalogRowActions({ product }: { product: ProductView }) {
  const [priceOpen, setPriceOpen] = useState(false);
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
    <Dialog open={priceOpen} onOpenChange={setPriceOpen}>
      <PermissionGate permission="PRODUCT_MANAGE">
        <DropdownMenu>
          <DropdownMenuTrigger render={<Button variant="ghost" size="icon-sm" />}>
            <MoreHorizontal className="size-4" />
            <span className="sr-only">Actions</span>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => setPriceOpen(true)}>Update Price</DropdownMenuItem>
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
      {priceOpen && (
        <DialogContent>
          <UpdatePriceForm product={product} onClose={() => setPriceOpen(false)} />
        </DialogContent>
      )}
    </Dialog>
  );
}

// ─── Main Table ───────────────────────────────────────────────────────────────

/**
 * Admin → Catalog: what the shop sells and for how much. The catalogue holds
 * no stock; the units are the inventory's (Admin → Inventory).
 */
export function CatalogTable() {
  const { data: products = [], isLoading } = useProducts();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <BookOpen className="size-5 mr-2 animate-pulse" />
        Loading catalog…
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Catalog</h1>
          <p className="text-sm text-muted-foreground">
            {products.length} products. Their stock is kept in Admin → Inventory.
          </p>
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
              <TableHead>Status</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {products.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
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
                    <p className="font-mono text-xs text-muted-foreground">{product.productId}</p>
                  </TableCell>
                  <TableCell className="text-muted-foreground">{product.category}</TableCell>
                  <TableCell>{formatMoney(product.price.amount, product.price.currency)}</TableCell>
                  <TableCell>
                    <Badge className={cn("text-xs", STATUS_STYLES[product.status])}>
                      {product.status}
                    </Badge>
                  </TableCell>
                  <TableCell>
                    <CatalogRowActions product={product} />
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
