/**
 * The identity headers every API call carries, per role. Runs in Node: no
 * browser and no backend (fetch is stubbed).
 *
 * The demo backends run in StreamRune's trusted-gateway mode
 * (streamrune.security.trust-user-id-header=true): X-User-Id is the caller's
 * identity, and the frontend stands in for the gateway that would set it. A
 * command guarded by @RequireRole (ship, deliver, discontinue) is refused with
 * "Authentication required" when no X-User-Id arrives, whatever X-User-Role
 * says, so the ADMIN role must carry an identity too.
 */

import { test, expect } from "@playwright/test";
import { fetchProducts, setApiAuth } from "../../src/lib/api";
import { userIdForRole } from "../../src/lib/identity";
import type { Role } from "../../src/lib/types";

async function headersSentAs(
  role: Role,
  savedUserId: string | null = null
): Promise<Record<string, string>> {
  let sent: Record<string, string> = {};
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (_input, init) => {
    sent = { ...(init?.headers as Record<string, string>) };
    return new Response("[]", {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  try {
    setApiAuth(role, userIdForRole(role, savedUserId));
    await fetchProducts();
  } finally {
    globalThis.fetch = realFetch;
  }
  return sent;
}

test.describe("Identity headers", () => {
  test("ADMIN sends an X-User-Id, so its guarded commands are authenticated", async () => {
    const headers = await headersSentAs("ADMIN");
    expect(headers["X-User-Role"]).toBe("ADMIN");
    expect(headers["X-User-Id"]).toBe("admin-1");
  });

  test("CUSTOMER sends the default customer's id, or the one picked earlier", async () => {
    expect((await headersSentAs("CUSTOMER"))["X-User-Id"]).toBe("cust-alice");
    expect((await headersSentAs("CUSTOMER", "cust-bob"))["X-User-Id"]).toBe("cust-bob");
  });

  test("GUEST sends no X-User-Id", async () => {
    const headers = await headersSentAs("GUEST");
    expect(headers["X-User-Role"]).toBe("GUEST");
    expect(headers["X-User-Id"]).toBeUndefined();
  });
});
