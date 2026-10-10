/**
 * Dashboard smoke test — loads "/" with mocked backend so it works even when
 * port 8080 is not running.  Verifies:
 *   - Page heading is visible
 *   - The SSE status indicator is present in the sidebar (Event Explorer nav item)
 *   - No console.error during initial load
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, collectConsoleErrors, BACKEND_URL } from "./helpers";

test.describe("Dashboard page", () => {
  test("the live event feed lists a payload-free frame", async ({ page }) => {
    await mockBackendRoutes(page);
    // An unnamed frame, as the backend sends it: an id and a data line without a payload.
    await page.route(`${BACKEND_URL}/api/events/sse`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        body:
          "id:3\n" +
          'data:{"globalOffset":3,"streamId":"product:prod-widget","aggregateType":"product",' +
          '"aggregateId":"prod-widget","eventType":"ProductCreated","version":1,' +
          '"timestamp":"2026-10-04T00:00:00Z"}\n\n',
      })
    );

    await page.goto("/");

    await expect(page.getByText("ProductCreated")).toBeVisible();
    await expect(page.getByText("product:prod-widget")).toBeVisible();
  });

  test("renders heading and SSE indicator without console errors", async ({ page }) => {
    // Mock backend before navigation so no real fetch fires
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/");

    // Main heading
    await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();

    // Subtitle
    await expect(page.getByText("StreamRune E-Commerce Demo")).toBeVisible();

    // SSE indicator lives on the "Event Explorer" nav item (a dot next to the label)
    // The span has title="SSE connected" or "SSE disconnected"
    const sseIndicator = page
      .locator('nav a[href="/events"] span[title]')
      .first();
    await expect(sseIndicator).toBeVisible();

    // Stat cards should render (even with empty data from mocks).
    // Use the sidebar nav links which have unique accessible names.
    await expect(page.getByRole("link", { name: /Products/ }).first()).toBeVisible();
    await expect(page.getByRole("link", { name: /Orders/ }).first()).toBeVisible();
    await expect(page.getByRole("link", { name: /Customers/ }).first()).toBeVisible();
    // Live Events stat card
    await expect(page.getByText("Live Events")).toBeVisible();

    // No console.error during load
    const errors = getErrors();
    // Filter out expected "SSE disconnected" fetch errors from mocked EventSource
    const unexpectedErrors = errors.filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("SSE") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(unexpectedErrors, `Unexpected console errors: ${unexpectedErrors.join("\n")}`).toHaveLength(0);
  });
});
