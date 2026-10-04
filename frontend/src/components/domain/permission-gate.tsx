"use client";

import { type ReactNode } from "react";
import { Shield } from "lucide-react";
import { toast } from "sonner";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useRole } from "@/providers/role-provider";
import { requiredRoleFor } from "@/lib/permissions";

interface PermissionGateProps {
  permission: string;
  children: ReactNode;
}

export function PermissionGate({ permission, children }: PermissionGateProps) {
  const { hasPermission } = useRole();
  const allowed = hasPermission(permission);

  if (allowed) {
    return <>{children}</>;
  }

  const required = requiredRoleFor(permission);

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger
          render={
            <div
              className="relative inline-flex opacity-50 cursor-not-allowed select-none"
              onClick={() => {
                toast.error(`This action requires the ${required} role.`);
              }}
            />
          }
        >
          <span className="pointer-events-none">{children}</span>
          <Shield className="absolute -top-1 -right-1 size-3.5 text-muted-foreground pointer-events-none" />
        </TooltipTrigger>
        <TooltipContent>Requires {required} role</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
