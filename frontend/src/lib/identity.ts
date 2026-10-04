import type { Role } from "./types";

/** The customer a CUSTOMER session acts as until another one is picked. */
export const DEFAULT_CUSTOMER_ID = "cust-alice";

/** The identity an ADMIN session acts as (also the admin in the tutorial's curl examples). */
export const ADMIN_USER_ID = "admin-1";

/**
 * The X-User-Id a session sends for a role, or null for none.
 *
 * The backends run in StreamRune's trusted-gateway mode, so X-User-Id is the
 * caller's identity and the frontend stands in for the gateway that would set
 * it. Every role that sends a guarded command needs one: a @RequireRole
 * command with no X-User-Id is refused with "Authentication required",
 * whatever X-User-Role says. A GUEST sends none and stays anonymous.
 *
 * @param savedUserId the customer picked earlier (CUSTOMER only)
 */
export function userIdForRole(
  role: Role,
  savedUserId: string | null = null
): string | null {
  switch (role) {
    case "ADMIN":
      return ADMIN_USER_ID;
    case "CUSTOMER":
      return savedUserId ?? DEFAULT_CUSTOMER_ID;
    default:
      return null;
  }
}
