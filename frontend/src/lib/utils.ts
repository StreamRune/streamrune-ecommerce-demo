import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatMoney(amount: number, currency: string) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency }).format(amount);
}

export function getAggregateBadgeClass(eventType: string): string {
  if (["ProductCreated", "PriceUpdated", "StockAdjusted", "ProductDiscontinued"].includes(eventType)) {
    return "bg-purple-500/10 text-purple-400 border-purple-500/20";
  }
  if (["OrderPlaced", "OrderConfirmed", "OrderShipped", "OrderDelivered", "OrderCancelled"].includes(eventType)) {
    return "bg-blue-500/10 text-blue-400 border-blue-500/20";
  }
  if (["CustomerRegistered", "ProfileUpdated", "DataExportRequested", "CustomerForgotten"].includes(eventType)) {
    return "bg-green-500/10 text-green-400 border-green-500/20";
  }
  if (["ShipmentReceived", "StockReserved", "StockReleased", "ReservationConfirmed"].includes(eventType)) {
    return "bg-orange-500/10 text-orange-400 border-orange-500/20";
  }
  if (["PaymentInitiated", "PaymentCaptured", "PaymentRefunded", "PaymentFailed"].includes(eventType)) {
    return "bg-yellow-500/10 text-yellow-400 border-yellow-500/20";
  }
  return "bg-muted text-muted-foreground";
}
