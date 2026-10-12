"use client";

import { toast } from "sonner";
import { Activity, AlertTriangle } from "lucide-react";
import { useSagaList } from "@/lib/queries";
import { usePaymentFailure } from "@/lib/queries";
import { useTogglePaymentFailure } from "@/lib/mutations";
import { SagaCard } from "@/components/domain/saga-card";
import { AccessRestricted } from "@/components/domain/access-restricted";
import { useRole } from "@/providers/role-provider";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

export default function FulfillmentPage() {
  const { role, hasPermission } = useRole();
  // A saga names the order, the customer and the payment of one purchase. The backend serves the
  // list to an authenticated caller only: every saga to an ADMIN, the sagas of their own orders
  // to a CUSTOMER. A GUEST sends no request. The payment-failure flag is an ADMIN's alone.
  const allowed = hasPermission("FULFILLMENT_VIEW");
  const ownOnly = allowed && role !== "ADMIN";
  const { data: sagas = [], isLoading } = useSagaList(50, allowed);
  const { data: paymentFailure } = usePaymentFailure(hasPermission("ADMIN_PANEL"));
  const { mutateAsync: toggleFailure, isPending: isToggling } = useTogglePaymentFailure();

  async function handleToggle() {
    try {
      await toggleFailure(undefined);
      toast.success(paymentFailure?.enabled ? "Payment failure disabled" : "Payment failure injection enabled");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to toggle payment failure");
    }
  }

  if (!allowed) {
    return <AccessRestricted message="Requires CUSTOMER or ADMIN role to view fulfillment sagas." />;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">Fulfillment</h1>
          <p className="text-sm text-muted-foreground">
            {ownOnly
              ? "Saga-based fulfillment status of your own orders"
              : "Saga-based order fulfillment status"}
          </p>
        </div>
        {hasPermission("ADMIN_PANEL") && (
          <div className="flex items-center gap-2">
            <Label htmlFor="pf-toggle" className="text-sm">Inject Payment Failure</Label>
            <Switch
              id="pf-toggle"
              checked={paymentFailure?.enabled ?? false}
              onCheckedChange={handleToggle}
              disabled={isToggling}
            />
          </div>
        )}
      </div>

      {paymentFailure?.enabled && (
        <div className="flex items-center gap-2 rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 text-sm text-yellow-400">
          <AlertTriangle className="size-4 shrink-0" />
          <span>Payment failure injection is active. New orders will fail at the payment step.</span>
        </div>
      )}

      {isLoading ? (
        <div className={cn("flex items-center justify-center py-12 text-muted-foreground")}>
          <Activity className="size-5 mr-2 animate-pulse" />
          Loading sagas…
        </div>
      ) : sagas.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground">
          <Activity className="size-10 mx-auto mb-3 opacity-30" />
          <p>
            {ownOnly
              ? "No sagas yet. Place an order to see its fulfillment."
              : "No sagas yet. Place an order to see fulfillment tracking."}
          </p>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {sagas.map((saga) => (
            <SagaCard key={saga.sagaId} saga={saga} />
          ))}
        </div>
      )}
    </div>
  );
}
