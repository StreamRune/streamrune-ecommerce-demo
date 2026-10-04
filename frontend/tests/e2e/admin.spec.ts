/**
 * Admin page smoke test — read-only surface check only.
 *
 * The admin page renders for all roles (GUEST sees a "read-only" notice,
 * ADMIN sees action buttons).  We only verify the page renders without
 * crashing — no data-mutation assertions.
 *
 * Uses mocked backend so the test always runs.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, collectConsoleErrors } from "./helpers";

test.describe("Admin page", () => {
  test("renders heading (read-only check, no mutations)", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/admin");

    await expect(page.getByRole("heading", { name: "Admin" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });
});
