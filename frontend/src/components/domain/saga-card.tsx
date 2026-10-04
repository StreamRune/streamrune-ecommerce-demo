"use client";

import { useState } from "react";
import { CheckCircle, XCircle, Circle, Loader2, ChevronDown, ChevronRight } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { SagaListEntry, SagaLifecycleStatus } from "@/lib/types";

type StepState = "completed" | "failed" | "active" | "pending";

interface SagaStep {
  label: string;
  state: StepState;
}

// SagaCard is fed SagaListEntry (see fulfillment/page.tsx), whose `status` is the framework
// lifecycle status (SagaStatus on the backend), not the domain OrderFulfillmentStatus — the list
// endpoint reads the raw saga_state.status column, shared by every saga type. Style every value
// the card can actually receive, including ones this saga's derivation doesn't produce today
// (COMPENSATED/FAULTED), since other saga types or future changes can reach them via the same
// list endpoint/card.
const STATUS_STYLES: Record<SagaLifecycleStatus, string> = {
  STARTED: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  RUNNING: "bg-blue-500/10 text-blue-400 border-blue-500/20",
  COMPLETED: "bg-green-500/10 text-green-400 border-green-500/20",
  COMPENSATING: "bg-orange-500/10 text-orange-400 border-orange-500/20",
  COMPENSATED: "bg-orange-500/10 text-orange-400 border-orange-500/20",
  FAILED: "bg-red-500/10 text-red-400 border-red-500/20",
  FAULTED: "bg-red-500/10 text-red-400 border-red-500/20",
};

// Coarse step tracker for the framework lifecycle status. The list endpoint can't tell us which
// domain step (payment/inventory/confirmation/cancelling) a RUNNING saga is on — that detail only
// lives in the (unparsed) `state` JSON string — so RUNNING just shows as "in progress" on a
// generic step rather than pretending to know the domain step. Notably, the domain's CANCELLING
// status (PaymentFailed -> CANCELLING -> CancelOrder) maps to framework RUNNING, not COMPENSATING
// (see OrderFulfillmentState.status()), so a cancelling saga renders as "active"/in-progress here,
// not failed — only COMPENSATING/COMPENSATED/FAILED/FAULTED render the current step as failed.
function getSteps(status: SagaLifecycleStatus): SagaStep[] {
  const steps = [
    { label: "Started" },
    { label: "In Progress" },
    { label: "Completed" },
  ];

  const ORDER: SagaLifecycleStatus[] = ["STARTED", "RUNNING", "COMPLETED"];
  const currentIndex = ORDER.indexOf(status);
  const isFailed = status === "FAILED" || status === "FAULTED" || status === "COMPENSATING" || status === "COMPENSATED";

  return steps.map((step, i) => {
    if (isFailed) {
      // Mark steps before current as completed, current as failed, rest as pending. Compensation
      // statuses are treated as having gotten at least past "Started".
      const failedIndex = Math.max(currentIndex, 1);
      if (i < failedIndex) return { label: step.label, state: "completed" };
      if (i === failedIndex) return { label: step.label, state: "failed" };
      return { label: step.label, state: "pending" };
    }
    if (status === "COMPLETED") {
      return { label: step.label, state: "completed" };
    }
    if (i < currentIndex) return { label: step.label, state: "completed" };
    if (i === currentIndex) return { label: step.label, state: "active" };
    return { label: step.label, state: "pending" };
  });
}

function StepIcon({ state }: { state: StepState }) {
  switch (state) {
    case "completed":
      return <CheckCircle className="size-4 text-green-400 shrink-0" />;
    case "failed":
      return <XCircle className="size-4 text-red-400 shrink-0" />;
    case "active":
      return <Loader2 className="size-4 text-blue-400 animate-spin shrink-0" />;
    case "pending":
      return <Circle className="size-4 text-muted-foreground shrink-0" />;
  }
}

interface SagaCardProps {
  saga: SagaListEntry;
  stateJson?: Record<string, unknown>;
}

export function SagaCard({ saga, stateJson }: SagaCardProps) {
  const [expanded, setExpanded] = useState(false);
  const steps = getSteps(saga.status);

  return (
    <Card size="sm">
      <CardHeader>
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <CardTitle className="font-mono text-xs text-muted-foreground truncate">{saga.sagaId}</CardTitle>
            <p className="text-xs text-muted-foreground mt-0.5">{saga.sagaType}</p>
          </div>
          <Badge className={cn("text-xs shrink-0", STATUS_STYLES[saga.status] ?? "bg-muted text-muted-foreground")}>
            {saga.status}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Steps */}
        <div className="flex items-center gap-1">
          {steps.map((step, i) => (
            <div key={step.label} className="flex items-center gap-1 flex-1">
              <div className="flex flex-col items-center gap-0.5 flex-1">
                <StepIcon state={step.state} />
                <span className="text-[10px] text-muted-foreground text-center">{step.label}</span>
              </div>
              {i < steps.length - 1 && (
                <div className={cn(
                  "h-px flex-1 -mt-4",
                  steps[i + 1].state === "pending" ? "bg-muted" : "bg-primary/30"
                )} />
              )}
            </div>
          ))}
        </div>

        {/* JSON expand */}
        {stateJson && (
          <button
            onClick={() => setExpanded((e) => !e)}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            {expanded ? <ChevronDown className="size-3" /> : <ChevronRight className="size-3" />}
            {expanded ? "Hide" : "Show"} state
          </button>
        )}
        {expanded && stateJson && (
          <pre className="text-[10px] bg-muted/50 rounded p-2 overflow-auto max-h-40 font-mono">
            {JSON.stringify(stateJson, null, 2)}
          </pre>
        )}
      </CardContent>
    </Card>
  );
}
