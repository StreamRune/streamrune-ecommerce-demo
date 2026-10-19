import type { Role } from "./types";

/**
 * What each role may do. The backend decides; this table mirrors it so a page
 * can show a "Requires … role" state instead of sending a request that would
 * be answered 403.
 *
 * - OWN_DATA_VIEW: read customers and orders. The backend serves them to an
 *   authenticated caller only: every row to an ADMIN, a CUSTOMER's own record
 *   and own orders to that customer. A GUEST has no identity and is refused.
 * - ORDER_PLACE, ORDER_CANCEL: an order is placed and cancelled by its
 *   customer or by an ADMIN; the fulfillment saga confirms it. A CUSTOMER's
 *   table holds their own orders only, so the gate needs no check per row.
 * - ORDER_SHIP, ORDER_DELIVER: an ADMIN's, whoever the order belongs to.
 * - PRODUCT_MANAGE: creating a product, its stock and price, discontinuing it
 *   and receiving a shipment are an ADMIN's; everyone reads the catalogue.
 * - PROFILE_MANAGE: a CUSTOMER may edit, export and forget their own record.
 * - FULFILLMENT_VIEW: read the fulfillment sagas. A saga belongs to its order,
 *   so the backend serves them as it serves orders: every saga to an ADMIN, the
 *   sagas of their own orders to a CUSTOMER. A GUEST is refused. The
 *   payment-failure switch on the same page is ADMIN_PANEL.
 */
const ROLE_PERMISSIONS: Record<Role, string[]> = {
  ADMIN: [
    "ORDER_PLACE",
    "ORDER_CANCEL",
    "ORDER_SHIP",
    "ORDER_DELIVER",
    "PRODUCT_MANAGE",
    "CUSTOMER_MANAGE",
    "AUDIT_VIEW",
    "EVENT_HISTORY_VIEW",
    "FULFILLMENT_VIEW",
    "OWN_DATA_VIEW",
    "ADMIN_PANEL",
  ],
  CUSTOMER: [
    "ORDER_PLACE",
    "ORDER_CANCEL",
    "PROFILE_MANAGE",
    "OWN_DATA_VIEW",
    "FULFILLMENT_VIEW",
  ],
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
