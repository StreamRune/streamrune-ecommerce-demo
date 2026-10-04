"use client";

import { useRole, DEFAULT_USERS } from "@/providers/role-provider";
import type { Role } from "@/lib/types";
import {
  Select,
  SelectTrigger,
  SelectContent,
  SelectItem,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";

const ROLE_BADGE_CLASS: Record<Role, string> = {
  ADMIN: "bg-red-500/20 text-red-400 border-red-500/30",
  CUSTOMER: "bg-blue-500/20 text-blue-400 border-blue-500/30",
  GUEST: "bg-muted text-muted-foreground border-border",
};

export function RoleSwitcher() {
  const { role, userId, setRole, setUserId } = useRole();

  return (
    <div className="space-y-2">
      <div className="text-xs text-muted-foreground px-1">Role</div>
      <Select value={role} onValueChange={(v) => setRole(v as Role)}>
        <SelectTrigger className="w-full text-xs h-8">
          <SelectValue>
            <span className="flex items-center gap-2">
              <Badge
                className={ROLE_BADGE_CLASS[role]}
                variant="outline"
              >
                {role}
              </Badge>
            </span>
          </SelectValue>
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="ADMIN">
            <Badge className={ROLE_BADGE_CLASS["ADMIN"]} variant="outline">
              ADMIN
            </Badge>
          </SelectItem>
          <SelectItem value="CUSTOMER">
            <Badge className={ROLE_BADGE_CLASS["CUSTOMER"]} variant="outline">
              CUSTOMER
            </Badge>
          </SelectItem>
          <SelectItem value="GUEST">
            <Badge className={ROLE_BADGE_CLASS["GUEST"]} variant="outline">
              GUEST
            </Badge>
          </SelectItem>
        </SelectContent>
      </Select>

      {role === "CUSTOMER" && (
        <>
          <div className="text-xs text-muted-foreground px-1">Identity</div>
          <Select
            value={userId ?? ""}
            onValueChange={(v) => setUserId(v || null)}
          >
            <SelectTrigger className="w-full text-xs h-8">
              <SelectValue placeholder="Select user" />
            </SelectTrigger>
            <SelectContent>
              {Object.entries(DEFAULT_USERS).map(([id, user]) => (
                <SelectItem key={id} value={id}>
                  {user.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </>
      )}
    </div>
  );
}
