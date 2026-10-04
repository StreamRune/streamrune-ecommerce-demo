/**
 * Orders page smoke tests.
 *
 * Surface test: mocked backend — always runs.
 * Backend-dependent block: skipped when Spring Boot is not reachable.
 * When backend IS up the seed order ("order-seed-1") should appear, OR an
 * empty-state message is visible (if seed data was not loaded yet).
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, isBackendReachable, collectConsoleErrors } from "./helpers";

test.describe("Orders page — surface (no backend required)", () => {
  test("renders page heading with mocked data", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/orders");

    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });
});

test.describe("Orders page — backend-dependent", () => {
  let backendAvailable = false;

  test.beforeAll(async () => {
    backendAvailable = await isBackendReachable();
    if (!backendAvailable) {
      console.log(
        "[orders.spec] Backend not reachable at localhost:8080 — skipping backend-dependent tests"
      );
    }
  });

  test("seed order (order-seed-1) or empty-state is visible", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }

    await page.goto("/orders");

    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();

    // The order table shows orderId.slice(0, 8) — "order-seed-1" appears as
    // "order-se" in the table cell.  Either that cell OR the empty-state must
    // be visible to confirm the page loaded correctly.
    const orderRowOrEmpty = page
      .locator('td.font-mono', { hasText: "order-se" })
      .or(page.getByText("No orders yet. Place one!"));
    await expect(orderRowOrEmpty.first()).toBeVisible();
  });
});
