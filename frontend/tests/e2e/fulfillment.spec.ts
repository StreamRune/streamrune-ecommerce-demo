/**
 * Fulfillment page smoke test.
 *
 * Uses mocked backend so the test always runs regardless of whether the
 * Spring Boot backend is up.  Verifies the page renders without crashing.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, collectConsoleErrors } from "./helpers";

test.describe("Fulfillment page", () => {
  test("renders heading", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/fulfillment");

    await expect(page.getByRole("heading", { name: "Fulfillment" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });
});
