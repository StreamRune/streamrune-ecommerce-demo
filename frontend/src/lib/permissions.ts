import type { Role } from "./types";

const ROLE_PERMISSIONS: Record<Role, string[]> = {
  ADMIN: [
    "ORDER_CANCEL_CONFIRMED",
    "ORDER_SHIP",
    "ORDER_DELIVER",
    "PRODUCT_MANAGE",
    "CUSTOMER_MANAGE",
    "AUDIT_VIEW",
    "ADMIN_PANEL",
  ],
  CUSTOMER: ["ORDER_PLACE", "ORDER_CANCEL_OWN", "PROFILE_MANAGE"],
  GUEST: [],
};

export function getPermissions(role: Role): Set<string> {
  return new Set(ROLE_PERMISSIONS[role]);
}

export function hasPermission(role: Role, permission: string): boolean {
  return ROLE_PERMISSIONS[role].includes(permission);
}

export function requiredRoleFor(permission: string): Role {
  if (ROLE_PERMISSIONS.CUSTOMER.includes(permission)) return "CUSTOMER";
  if (ROLE_PERMISSIONS.ADMIN.includes(permission)) return "ADMIN";
  return "ADMIN";
}
