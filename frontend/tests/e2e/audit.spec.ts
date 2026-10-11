/**
 * Audit Log page smoke test.
 *
 * Uses mocked backend so the test always runs.  Verifies the page renders
 * the heading and does not crash.
 */

import { test, expect } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  collectConsoleErrors,
  recordApiRequests,
  requestsTo,
} from "./helpers";

test.describe("Audit Log page", () => {
  test("renders heading", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/audit");

    // Audit Log requires ADMIN role.  GUEST (default when localStorage is
    // empty) sees "Access Restricted" instead of the heading — both are valid
    // rendered states that prove the page loaded without crashing.
    const headingOrGate = page
      .getByRole("heading", { name: "Audit Log" })
      .or(page.getByRole("heading", { name: "Access Restricted" }));
    await expect(headingOrGate.first()).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });

  test("a GUEST and a CUSTOMER send no audit request; an ADMIN does", async ({ page }) => {
    await mockBackendRoutes(page);
    const requests = recordApiRequests(page);

    await page.goto("/audit");
    await expect(page.getByText("Requires ADMIN role to view audit log.")).toBeVisible();

    await actAs(page, "CUSTOMER");
    await page.goto("/audit");
    await expect(page.getByText("Requires ADMIN role to view audit log.")).toBeVisible();
    expect(requestsTo(requests, "/api/audit")).toHaveLength(0);

    await actAs(page, "ADMIN");
    await page.goto("/audit");
    await expect(page.getByRole("heading", { name: "Audit Log" })).toBeVisible();
    const sent = requestsTo(requests, "/api/audit");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.role).toBe("ADMIN");
      expect(request.userId).toBe("admin-1");
    }
  });
});
