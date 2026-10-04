"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import { Plus, Users, MoreHorizontal } from "lucide-react";
import { useCustomers } from "@/lib/queries";
import {
  useRegisterCustomer,
  useUpdateProfile,
  useExportData,
  useForgetCustomer,
} from "@/lib/mutations";
import type { CustomerView } from "@/lib/types";
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
import { cn } from "@/lib/utils";

// ─── Register Customer Dialog ─────────────────────────────────────────────────

function RegisterCustomerDialog() {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [address, setAddress] = useState("");
  const [phone, setPhone] = useState("");
  const { mutateAsync, isPending } = useRegisterCustomer();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      await mutateAsync({ name, email, address, phone });
      toast.success("Customer registered");
      setOpen(false);
      setName(""); setEmail(""); setAddress(""); setPhone("");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to register customer");
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <PermissionGate permission="CUSTOMER_MANAGE">
        <DialogTrigger render={<Button />}>
          <Plus className="size-4" />
          Register Customer
        </DialogTrigger>
      </PermissionGate>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Register Customer</DialogTitle>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-1">
            <Label htmlFor="rc-name">Name</Label>
            <Input id="rc-name" value={name} onChange={(e) => setName(e.target.value)} required />
          </div>
          <div className="space-y-1">
            <Label htmlFor="rc-email">Email</Label>
            <Input id="rc-email" type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
          </div>
          <div className="space-y-1">
            <Label htmlFor="rc-address">Address</Label>
            <Input id="rc-address" value={address} onChange={(e) => setAddress(e.target.value)} />
          </div>
          <div className="space-y-1">
            <Label htmlFor="rc-phone">Phone</Label>
            <Input id="rc-phone" type="tel" value={phone} onChange={(e) => setPhone(e.target.value)} />
          </div>
          <DialogFooter showCloseButton>
            <Button type="submit" disabled={isPending}>
              {isPending ? "Registering…" : "Register"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

// ─── Edit Profile Dialog ──────────────────────────────────────────────────────

function EditProfileForm({ customer, onClose }: { customer: CustomerView; onClose: () => void }) {
  const [name, setName] = useState(customer.name);
  const [address, setAddress] = useState(customer.address);
  const [phone, setPhone] = useState(customer.phone);
  const { mutateAsync, isPending } = useUpdateProfile();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    try {
      await mutateAsync({ customerId: customer.customerId, body: { name, address, phone } });
      toast.success("Profile updated");
      onClose();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to update profile");
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <DialogHeader>
        <DialogTitle>Edit Profile — {customer.name}</DialogTitle>
      </DialogHeader>
      <div className="space-y-1">
        <Label htmlFor="ep-name">Name</Label>
        <Input id="ep-name" value={name} onChange={(e) => setName(e.target.value)} />
      </div>
      <div className="space-y-1">
        <Label htmlFor="ep-address">Address</Label>
        <Input id="ep-address" value={address} onChange={(e) => setAddress(e.target.value)} />
      </div>
      <div className="space-y-1">
        <Label htmlFor="ep-phone">Phone</Label>
        <Input id="ep-phone" type="tel" value={phone} onChange={(e) => setPhone(e.target.value)} />
      </div>
      <DialogFooter showCloseButton>
        <Button type="submit" disabled={isPending}>{isPending ? "Saving…" : "Save"}</Button>
      </DialogFooter>
    </form>
  );
}

// ─── Forget Dialog ────────────────────────────────────────────────────────────

function ForgetCustomerForm({ customer, onClose }: { customer: CustomerView; onClose: () => void }) {
  const { mutateAsync, isPending } = useForgetCustomer();

  async function handleConfirm() {
    try {
      await mutateAsync(customer.customerId);
      toast.success("Customer forgotten (GDPR)");
      onClose();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to forget customer");
    }
  }

  return (
    <>
      <DialogHeader>
        <DialogTitle>Forget Customer</DialogTitle>
      </DialogHeader>
      <p className="text-sm text-muted-foreground">
        This will permanently erase all personal data for <strong>{customer.name}</strong> (GDPR right to erasure). This cannot be undone.
      </p>
      <DialogFooter showCloseButton>
        <Button variant="destructive" onClick={handleConfirm} disabled={isPending}>
          {isPending ? "Forgetting…" : "Forget Customer"}
        </Button>
      </DialogFooter>
    </>
  );
}

// ─── Row Actions ──────────────────────────────────────────────────────────────

type ActionMode = "edit" | "forget" | null;

function CustomerRowActions({ customer }: { customer: CustomerView }) {
  const [dialogMode, setDialogMode] = useState<ActionMode>(null);
  const { mutateAsync: exportData, isPending: isExporting } = useExportData();

  const isForgotten = customer.status === "FORGOTTEN";

  async function handleExport() {
    try {
      await exportData(customer.customerId);
      toast.success("Data export requested");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to export data");
    }
  }

  return (
    <Dialog open={dialogMode !== null} onOpenChange={(open) => { if (!open) setDialogMode(null); }}>
      <PermissionGate permission="CUSTOMER_MANAGE">
        <DropdownMenu>
          <DropdownMenuTrigger render={<Button variant="ghost" size="icon-sm" />}>
            <MoreHorizontal className="size-4" />
            <span className="sr-only">Actions</span>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {!isForgotten && (
              <DropdownMenuItem onClick={() => setDialogMode("edit")}>
                Edit Profile
              </DropdownMenuItem>
            )}
            <DropdownMenuItem onClick={handleExport} disabled={isExporting}>
              Export Data
            </DropdownMenuItem>
            {!isForgotten && (
              <DropdownMenuItem
                className="text-destructive"
                onClick={() => setDialogMode("forget")}
              >
                Forget (GDPR)
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </PermissionGate>
      {dialogMode === "edit" && (
        <DialogContent>
          <EditProfileForm customer={customer} onClose={() => setDialogMode(null)} />
        </DialogContent>
      )}
      {dialogMode === "forget" && (
        <DialogContent>
          <ForgetCustomerForm customer={customer} onClose={() => setDialogMode(null)} />
        </DialogContent>
      )}
    </Dialog>
  );
}

// ─── Main Table ───────────────────────────────────────────────────────────────

export function CustomerTable() {
  const { data: customers = [], isLoading } = useCustomers();
  const router = useRouter();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12 text-muted-foreground">
        <Users className="size-5 mr-2 animate-pulse" />
        Loading customers…
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Customers</h1>
          <p className="text-sm text-muted-foreground">{customers.length} total</p>
        </div>
        <RegisterCustomerDialog />
      </div>

      <div className="rounded-xl border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Email</TableHead>
              <TableHead>Phone</TableHead>
              <TableHead>Status</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {customers.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                  No customers yet.
                </TableCell>
              </TableRow>
            ) : (
              customers.map((customer) => {
                const isForgotten = customer.status === "FORGOTTEN";
                return (
                  <TableRow
                    key={customer.customerId}
                    className={cn("cursor-pointer", isForgotten && "opacity-50")}
                    onClick={() => router.push(`/orders?customerId=${customer.customerId}`)}
                  >
                    <TableCell className="font-medium">
                      {isForgotten ? "[REDACTED]" : customer.name}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {isForgotten ? "[REDACTED]" : customer.email}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {isForgotten ? "[REDACTED]" : (customer.phone || "—")}
                    </TableCell>
                    <TableCell>
                      <Badge
                        className={cn(
                          "text-xs",
                          isForgotten
                            ? "bg-muted text-muted-foreground"
                            : "bg-green-500/10 text-green-400 border-green-500/20"
                        )}
                      >
                        {customer.status}
                      </Badge>
                    </TableCell>
                    <TableCell onClick={(e) => e.stopPropagation()}>
                      <CustomerRowActions customer={customer} />
                    </TableCell>
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
