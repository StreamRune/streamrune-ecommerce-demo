"use client";

import { useState } from "react";
import { Zap, Search, ShieldOff } from "lucide-react";
import { useEvents, useStreamEvents } from "@/lib/queries";
import { parseStreamFilter } from "@/lib/api";
import { useSSE } from "@/providers/sse-provider";
import { useRole } from "@/providers/role-provider";
import type { EventEntry, LiveEvent } from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { cn, getAggregateBadgeClass } from "@/lib/utils";

function EventRow({ event }: { event: EventEntry }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="border-b border-border last:border-0">
      <button
        className="w-full flex items-center gap-3 px-4 py-2.5 text-left hover:bg-muted/30 transition-colors"
        onClick={() => setExpanded((e) => !e)}
      >
        <span className="text-xs text-muted-foreground tabular-nums w-12 shrink-0">
          #{event.globalOffset}
        </span>
        <Badge className={cn("text-xs shrink-0", getAggregateBadgeClass(event.eventType))}>
          {event.eventType}
        </Badge>
        <span className="text-xs text-muted-foreground truncate flex-1">{event.streamId}</span>
        <span className="text-xs text-muted-foreground shrink-0 tabular-nums">
          {new Date(event.timestamp).toLocaleTimeString()}
        </span>
      </button>
      {expanded && (
        <div className="px-4 pb-3">
          <pre className="text-[11px] bg-muted/50 rounded p-3 overflow-auto max-h-48 font-mono">
            {JSON.stringify(event.payload, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

/** A live feed row: the event's type, stream and version. The feed carries no payload. */
function LiveEventRow({ event }: { event: LiveEvent }) {
  return (
    <div className="flex items-center gap-3 px-4 py-2.5 border-b border-border last:border-0">
      <span className="text-xs text-muted-foreground tabular-nums w-12 shrink-0">
        #{event.globalOffset}
      </span>
      <Badge className={cn("text-xs shrink-0", getAggregateBadgeClass(event.eventType))}>
        {event.eventType}
      </Badge>
      <span className="text-xs text-muted-foreground truncate flex-1">{event.streamId}</span>
      <span className="text-xs text-muted-foreground shrink-0 tabular-nums">v{event.version}</span>
      <span className="text-xs text-muted-foreground shrink-0 tabular-nums">
        {new Date(event.timestamp).toLocaleTimeString()}
      </span>
    </div>
  );
}

// ─── Historical Browser ───────────────────────────────────────────────────────

const PAGE_SIZE = 25;

function HistoricalBrowser() {
  const { hasPermission } = useRole();
  // The history returns each event's payload decrypted, so the backend serves it to ADMIN only.
  const allowed = hasPermission("EVENT_HISTORY_VIEW");
  const [streamInput, setStreamInput] = useState("");
  const [streamFilter, setStreamFilter] = useState<{
    aggregateType: string;
    aggregateId: string;
  } | null>(null);
  const [filterHint, setFilterHint] = useState(false);
  const [offset, setOffset] = useState(0);

  const { data: globalEvents = [], isLoading: globalLoading } = useEvents(
    offset,
    PAGE_SIZE,
    allowed,
  );
  const { data: streamEvents = [], isLoading: streamLoading } = useStreamEvents(
    streamFilter?.aggregateType ?? null,
    streamFilter?.aggregateId ?? null,
    allowed,
  );

  if (!allowed) {
    return (
      <div className="flex flex-col items-center justify-center py-12 gap-3 text-center rounded-xl border border-border">
        <ShieldOff className="size-10 text-muted-foreground opacity-40" />
        <div>
          <p className="text-sm font-medium">Requires ADMIN role to browse the event history.</p>
          <p className="text-xs text-muted-foreground mt-1">
            The history shows each event with its payload, decrypted. The live feed above lists
            events without their payload.
          </p>
        </div>
      </div>
    );
  }

  const events = streamFilter ? streamEvents : globalEvents;
  const isLoading = streamFilter ? streamLoading : globalLoading;

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    const parsed = parseStreamFilter(streamInput.trim());
    setFilterHint(parsed === null);
    setStreamFilter(parsed);
    setOffset(0);
  }

  function clearFilter() {
    setStreamFilter(null);
    setStreamInput("");
    setFilterHint(false);
  }

  return (
    <div className="space-y-3">
      <form onSubmit={handleSearch} className="flex gap-2 items-end">
        <div className="flex-1 space-y-1">
          <Label htmlFor="stream-filter">Filter by stream</Label>
          <Input
            id="stream-filter"
            placeholder="type:id (e.g. product:p-1)"
            value={streamInput}
            onChange={(e) => setStreamInput(e.target.value)}
          />
        </div>
        <Button type="submit" variant="outline">
          <Search className="size-4" />
          Search
        </Button>
        {streamFilter && (
          <Button type="button" variant="ghost" onClick={clearFilter}>
            Clear
          </Button>
        )}
      </form>

      {filterHint && (
        <p className="text-xs text-muted-foreground">
          Enter a stream as type:id, e.g. product:p-1
        </p>
      )}

      {streamFilter && (
        <p className="text-xs text-muted-foreground">
          Showing events for stream:{" "}
          <code className="font-mono">
            {streamFilter.aggregateType}:{streamFilter.aggregateId}
          </code>
        </p>
      )}

      <div className="rounded-xl border border-border overflow-hidden">
        {isLoading ? (
          <div className="flex items-center justify-center py-8 text-muted-foreground text-sm">
            Loading events…
          </div>
        ) : events.length === 0 ? (
          <div className="flex items-center justify-center py-8 text-muted-foreground text-sm">
            No events found.
          </div>
        ) : (
          events.map((event) => (
            <EventRow key={`${event.globalOffset}-${event.streamId}`} event={event} />
          ))
        )}
      </div>

      {!streamFilter && (
        <div className="flex items-center justify-between">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
            disabled={offset === 0}
          >
            Previous
          </Button>
          <span className="text-xs text-muted-foreground">
            Offset {offset}–{offset + PAGE_SIZE}
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setOffset(offset + PAGE_SIZE)}
            disabled={globalEvents.length < PAGE_SIZE}
          >
            Next
          </Button>
        </div>
      )}
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function EventsPage() {
  const { connected, recentEvents } = useSSE();

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold">Event Explorer</h1>
        <p className="text-sm text-muted-foreground">Browse and inspect domain events</p>
      </div>

      {/* Live Feed */}
      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-lg font-semibold">Live Feed</h2>
          <span className={cn("size-2 rounded-full shrink-0", connected ? "bg-green-500 animate-pulse" : "bg-muted-foreground")} />
          <span className="text-xs text-muted-foreground">{connected ? "Connected" : "Disconnected"}</span>
          <span className="text-xs text-muted-foreground ml-auto">{recentEvents.length} events</span>
        </div>
        <div className="rounded-xl border border-border overflow-hidden max-h-72 overflow-y-auto">
          {recentEvents.length === 0 ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground text-sm">
              <Zap className="size-4 mr-2 opacity-50" />
              Waiting for live events…
            </div>
          ) : (
            recentEvents.map((event) => (
              <LiveEventRow key={`live-${event.globalOffset}-${event.streamId}`} event={event} />
            ))
          )}
        </div>
      </section>

      {/* Historical Browser */}
      <section className="space-y-3">
        <h2 className="text-lg font-semibold">Historical Browser</h2>
        <HistoricalBrowser />
      </section>
    </div>
  );
}
