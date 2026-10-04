"use client";

import { useSSE } from "@/providers/sse-provider";
import { Badge } from "@/components/ui/badge";
import { cn, getAggregateBadgeClass } from "@/lib/utils";

function relativeTime(timestamp: string): string {
  const diff = Date.now() - new Date(timestamp).getTime();
  const seconds = Math.floor(diff / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}

interface EventFeedProps {
  maxItems?: number;
  className?: string;
}

export function EventFeed({ maxItems = 20, className }: EventFeedProps) {
  const { connected, recentEvents } = useSSE();
  const events = recentEvents.slice(0, maxItems);

  return (
    <div className={cn("flex flex-col gap-2", className)}>
      <div className="flex items-center gap-2 mb-1">
        <span
          className={cn(
            "size-2 rounded-full shrink-0",
            connected ? "bg-green-500 animate-pulse" : "bg-muted-foreground"
          )}
        />
        <span className="text-xs text-muted-foreground">
          {connected ? "Live" : "Disconnected"}
        </span>
      </div>
      {events.length === 0 ? (
        <p className="text-sm text-muted-foreground py-4 text-center">
          No events yet. Waiting for activity...
        </p>
      ) : (
        <div className="space-y-1.5">
          {events.map((event) => (
            <div
              key={`${event.globalOffset}-${event.streamId}-${event.eventType}`}
              className="flex items-center gap-2 text-xs"
            >
              <Badge
                className={cn(
                  "shrink-0 text-xs",
                  getAggregateBadgeClass(event.eventType)
                )}
              >
                {event.eventType}
              </Badge>
              <span className="text-muted-foreground truncate flex-1">{event.streamId}</span>
              <span className="text-muted-foreground shrink-0 tabular-nums">
                {relativeTime(event.timestamp)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
