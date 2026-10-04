/**
 * Audit Log page smoke test.
 *
 * Uses mocked backend so the test always runs.  Verifies the page renders
 * the heading and does not crash.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, collectConsoleErrors } from "./helpers";

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
});
