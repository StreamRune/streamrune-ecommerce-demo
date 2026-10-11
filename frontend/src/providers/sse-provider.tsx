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
import type { LiveEvent } from "@/lib/types";
import { API_URL } from "@/lib/api";
import { refreshAndAgainOnceSettled } from "@/lib/read-model-sync";

const MAX_RECENT = 50;
const BASE_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;

interface SSEContextValue {
  connected: boolean;
  recentEvents: LiveEvent[];
}

const SSEContext = createContext<SSEContextValue>({
  connected: false,
  recentEvents: [],
});

/**
 * The queries an event of the given type makes stale. A frame names no stream,
 * so the decision is by event type alone and every query of a kind is read
 * again, each with the session's identity headers: the backend answers with
 * what that identity may see.
 */
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
    // The saga starts with OrderPlaced and ends with OrderConfirmed or OrderCancelled.
    keys.push(["fulfillment"]);
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
  const [recentEvents, setRecentEvents] = useState<LiveEvent[]>([]);
  // The feed is live: a connection starts at the head of the event store and is sent what is
  // stored from then on. A frame at or below this offset has been counted and is skipped.
  const lastOffsetRef = useRef(0);
  const retryRef = useRef(0);
  const hasConnectedRef = useRef(false);
  const esRef = useRef<EventSource | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    function connect() {
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }

      // An EventSource cannot send request headers, so the feed is the one endpoint the browser
      // calls without X-User-Id and X-User-Role. The backend keeps it open to everyone and
      // sends frames that name no stream and carry no payload (see LiveEvent); the frames are
      // unnamed, so onmessage receives them all.
      const es = new EventSource(`${API_URL}/api/events/sse`);
      esRef.current = es;

      es.onopen = () => {
        setConnected(true);
        retryRef.current = 0;
        // What was stored while the page was disconnected is not sent after the reconnect: a
        // connection starts at the head. Every list is read again instead, each with the
        // session's identity headers.
        if (hasConnectedRef.current) {
          void queryClient.invalidateQueries();
        }
        hasConnectedRef.current = true;
      };

      es.onmessage = (event) => {
        try {
          const entry = JSON.parse(event.data as string) as LiveEvent;
          if (entry.globalOffset <= lastOffsetRef.current) return;
          lastOffsetRef.current = entry.globalOffset;
          setRecentEvents((prev) => [entry, ...prev].slice(0, MAX_RECENT));

          // The frame arrives within a second of the event; the read models the queries show
          // follow the event store by seconds. Read at once, and once more when they have settled.
          refreshAndAgainOnceSettled(queryClient, invalidationKeysFor(entry.eventType));
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
