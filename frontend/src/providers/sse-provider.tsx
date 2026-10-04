"use client";

import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { EventEntry } from "@/lib/types";
import { API_URL } from "@/lib/api";

const MAX_RECENT = 50;
const BASE_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;

interface SSEContextValue {
  connected: boolean;
  recentEvents: EventEntry[];
}

const SSEContext = createContext<SSEContextValue>({
  connected: false,
  recentEvents: [],
});

function invalidationKeysFor(eventType: string): string[][] {
  const keys: string[][] = [["dashboard"], ["events"]];

  if (
    ["ProductCreated", "PriceUpdated", "StockAdjusted", "ProductDiscontinued"].includes(
      eventType
    )
  ) {
    keys.push(["products"]);
  }
  if (
    [
      "OrderPlaced",
      "OrderConfirmed",
      "OrderShipped",
      "OrderDelivered",
      "OrderCancelled",
    ].includes(eventType)
  ) {
    keys.push(["orders"]);
  }
  if (
    [
      "CustomerRegistered",
      "ProfileUpdated",
      "DataExportRequested",
      "CustomerForgotten",
    ].includes(eventType)
  ) {
    keys.push(["customers"]);
  }
  if (
    [
      "PaymentInitiated",
      "PaymentCaptured",
      "PaymentRefunded",
      "PaymentFailed",
    ].includes(eventType)
  ) {
    keys.push(["fulfillment"]);
  }
  if (
    [
      "StockReserved",
      "StockReleased",
      "ReservationConfirmed",
      "ShipmentReceived",
    ].includes(eventType)
  ) {
    keys.push(["products"]);
    keys.push(["fulfillment"]);
  }

  // deduplicate
  const seen = new Set<string>();
  return keys.filter((k) => {
    const key = k.join(",");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function SSEProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [connected, setConnected] = useState(false);
  const [recentEvents, setRecentEvents] = useState<EventEntry[]>([]);
  const retryRef = useRef(0);
  const esRef = useRef<EventSource | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    function connect() {
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }

      const es = new EventSource(`${API_URL}/api/events/sse`);
      esRef.current = es;

      es.onopen = () => {
        setConnected(true);
        retryRef.current = 0;
      };

      es.onmessage = (event) => {
        try {
          const entry = JSON.parse(event.data as string) as EventEntry;
          setRecentEvents((prev) => [entry, ...prev].slice(0, MAX_RECENT));

          const keys = invalidationKeysFor(entry.eventType);
          for (const k of keys) {
            void queryClient.invalidateQueries({ queryKey: k });
          }
        } catch {
          // ignore parse errors
        }
      };

      es.onerror = () => {
        setConnected(false);
        es.close();
        esRef.current = null;

        const backoff = Math.min(
          BASE_BACKOFF_MS * 2 ** retryRef.current,
          MAX_BACKOFF_MS
        );
        retryRef.current += 1;
        timeoutRef.current = setTimeout(() => connect(), backoff);
      };
    }

    connect();
    return () => {
      esRef.current?.close();
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, [queryClient]);

  return (
    <SSEContext.Provider value={{ connected, recentEvents }}>
      {children}
    </SSEContext.Provider>
  );
}

export function useSSE(): SSEContextValue {
  return useContext(SSEContext);
}
