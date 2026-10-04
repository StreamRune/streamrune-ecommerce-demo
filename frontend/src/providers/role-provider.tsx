"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { Role } from "@/lib/types";
import { setApiAuth } from "@/lib/api";
import { userIdForRole } from "@/lib/identity";
import { getPermissions, hasPermission as checkPermission } from "@/lib/permissions";

export const DEFAULT_USERS = {
  "cust-alice": { name: "Alice Smith", email: "alice@example.com" },
  "cust-bob": { name: "Bob Jones", email: "bob@example.com" },
} as const;

interface RoleContextValue {
  role: Role;
  userId: string | null;
  permissions: Set<string>;
  setRole: (role: Role) => void;
  setUserId: (userId: string | null) => void;
  hasPermission: (permission: string) => boolean;
}

const RoleContext = createContext<RoleContextValue | null>(null);

const STORAGE_KEY_ROLE = "sr_role";
const STORAGE_KEY_USER = "sr_userId";

export function RoleProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [role, setRoleState] = useState<Role>(() => {
    if (typeof window === "undefined") return "GUEST";
    const saved = localStorage.getItem(STORAGE_KEY_ROLE);
    if (saved === "ADMIN" || saved === "CUSTOMER" || saved === "GUEST") {
      return saved;
    }
    return "GUEST";
  });

  const [userId, setUserIdState] = useState<string | null>(() => {
    if (typeof window === "undefined") return null;
    const savedRole = localStorage.getItem(STORAGE_KEY_ROLE) as Role | null;
    const savedUser = localStorage.getItem(STORAGE_KEY_USER);
    const effectiveRole =
      savedRole === "ADMIN" || savedRole === "CUSTOMER" || savedRole === "GUEST"
        ? savedRole
        : "GUEST";
    return userIdForRole(effectiveRole, savedUser);
  });

  const permissions = getPermissions(role);

  // Sync auth state immediately on mount (before any queries fire)
  setApiAuth(role, userId);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY_ROLE, role);
    if (userId) {
      localStorage.setItem(STORAGE_KEY_USER, userId);
    } else {
      localStorage.removeItem(STORAGE_KEY_USER);
    }
    setApiAuth(role, userId);
    // Invalidate all cached data when role changes — previous data may differ by role
    void queryClient.invalidateQueries();
  }, [role, userId, queryClient]);

  const setRole = useCallback((newRole: Role) => {
    setRoleState(newRole);
    setUserIdState(userIdForRole(newRole));
  }, []);

  const setUserId = useCallback((newUserId: string | null) => {
    setUserIdState(newUserId);
  }, []);

  const hasPerm = useCallback(
    (permission: string) => checkPermission(role, permission),
    [role]
  );

  return (
    <RoleContext.Provider
      value={{ role, userId, permissions, setRole, setUserId, hasPermission: hasPerm }}
    >
      {children}
    </RoleContext.Provider>
  );
}

export function useRole(): RoleContextValue {
  const ctx = useContext(RoleContext);
  if (!ctx) throw new Error("useRole must be used within a RoleProvider");
  return ctx;
}
