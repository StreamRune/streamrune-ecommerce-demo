"use client";

import {
  createContext,
  useContext,
  useEffect,
  useCallback,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { Role } from "@/lib/types";
import { setApiAuth } from "@/lib/api";
import { userIdForRole } from "@/lib/identity";
import { getPermissions, hasPermission as checkPermission } from "@/lib/permissions";

export const DEFAULT_USERS = {
  "cust-alice": { name: "Alice Johnson", email: "alice@example.com" },
  "cust-bob": { name: "Bob Smith", email: "bob@example.com" },
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

// ─── The session as an external store ────────────────────────────────────────
//
// The role and the identity live in the browser's localStorage. The server that renders a page
// first does not see it, so a component that read it while React hydrates would render another
// tree than the server sent. useSyncExternalStore is React's way to read such a value: the
// server's snapshot (a GUEST) for the server render and for the hydration, the browser's
// snapshot right after it.

const sessionListeners = new Set<() => void>();

function subscribeToSession(onChange: () => void): () => void {
  sessionListeners.add(onChange);
  // Another tab of the same browser switched the role.
  window.addEventListener("storage", onChange);
  return () => {
    sessionListeners.delete(onChange);
    window.removeEventListener("storage", onChange);
  };
}

function savedRole(): Role {
  const saved = localStorage.getItem(STORAGE_KEY_ROLE);
  return saved === "ADMIN" || saved === "CUSTOMER" || saved === "GUEST" ? saved : "GUEST";
}

function savedUserId(): string | null {
  return localStorage.getItem(STORAGE_KEY_USER);
}

function serverRole(): Role {
  return "GUEST";
}

function serverUserId(): string | null {
  return null;
}

function saveSession(role: Role, userId: string | null): void {
  localStorage.setItem(STORAGE_KEY_ROLE, role);
  if (userId) {
    localStorage.setItem(STORAGE_KEY_USER, userId);
  } else {
    localStorage.removeItem(STORAGE_KEY_USER);
  }
  sessionListeners.forEach((listener) => listener());
}

export function RoleProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const role = useSyncExternalStore(subscribeToSession, savedRole, serverRole);
  const savedUser = useSyncExternalStore(subscribeToSession, savedUserId, serverUserId);
  const userId = userIdForRole(role, savedUser);

  const permissions = getPermissions(role);

  // Sync auth state during the render, before the queries of the children fire.
  setApiAuth(role, userId);

  useEffect(() => {
    // What the backend returns depends on the identity (a CUSTOMER gets their own orders, an
    // ADMIN everyone's), so the cache of the identity that was active is dropped, not just
    // marked stale: nothing it loaded is shown under the next one.
    void queryClient.resetQueries();
  }, [role, userId, queryClient]);

  const setRole = useCallback((newRole: Role) => {
    saveSession(newRole, userIdForRole(newRole));
  }, []);

  const setUserId = useCallback(
    (newUserId: string | null) => {
      saveSession(role, newUserId);
    },
    [role]
  );

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
