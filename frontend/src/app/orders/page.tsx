"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { OrderTable } from "@/components/domain/order-table";

function OrdersContent() {
  const searchParams = useSearchParams();
  const customerId = searchParams.get("customerId") ?? undefined;

  return <OrderTable customerId={customerId} />;
}

export default function OrdersPage() {
  return (
    <Suspense fallback={<div className="p-6 text-muted-foreground">Loading…</div>}>
      <OrdersContent />
    </Suspense>
  );
}
