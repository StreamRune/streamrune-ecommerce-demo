/**
 * Customers page smoke tests.
 *
 * Surface test: mocked backend — always runs.
 * Backend-dependent block: skipped when Spring Boot is not reachable.
 * When backend IS up the seed customers ("Alice Johnson", "Bob Smith") should
 * appear, OR an empty-state message is visible.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, isBackendReachable, collectConsoleErrors } from "./helpers";

test.describe("Customers page — surface (no backend required)", () => {
  test("renders page heading with mocked data", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });
});

test.describe("Customers page — backend-dependent", () => {
  let backendAvailable = false;

  test.beforeAll(async () => {
    backendAvailable = await isBackendReachable();
    if (!backendAvailable) {
      console.log(
        "[customers.spec] Backend not reachable at localhost:8080 — skipping backend-dependent tests"
      );
    }
  });

  test("seed customers Alice Johnson and Bob Smith or empty-state are visible", async ({
    page,
  }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();

    // Either seed customers or the empty-state message must be present
    const customersOrEmpty = page
      .locator('text="Alice Johnson"')
      .or(page.getByText("No customers yet."));
    await expect(customersOrEmpty.first()).toBeVisible();

    // If Alice is visible, Bob should be too
    const aliceVisible = await page.locator('text="Alice Johnson"').isVisible().catch(() => false);
    if (aliceVisible) {
      await expect(page.getByText("Bob Smith")).toBeVisible();
    }
  });
});
