/**
 * Event Explorer page smoke test.
 *
 * Uses mocked backend so the test always runs.  Verifies the page renders
 * the heading and does not crash.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, collectConsoleErrors, BACKEND_URL } from "./helpers";

test.describe("Event Explorer page", () => {
  test("renders heading", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/events");

    await expect(page.getByRole("heading", { name: "Event Explorer" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });

  test("filters one stream by type:id over the two-segment route", async ({ page }) => {
    await mockBackendRoutes(page);
    const requested: string[] = [];
    await page.route(`${BACKEND_URL}/api/events/product/p-1`, (route) => {
      requested.push(route.request().url());
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            globalOffset: 1,
            streamId: "product:p-1",
            aggregateType: "product",
            aggregateId: "p-1",
            eventType: "ProductCreated",
            timestamp: "2026-10-04T00:00:00Z",
            payload: {},
          },
        ]),
      });
    });
    await page.goto("/events");
    const filter = page.getByPlaceholder(/type:id/);
    await filter.fill("product:p-1");
    await filter.press("Enter");
    await expect(page.getByText("ProductCreated")).toBeVisible();
    expect(requested).toHaveLength(1);
  });

  test("an input without ':' shows the hint and sends no stream request", async ({ page }) => {
    await mockBackendRoutes(page);
    const streamRequests: string[] = [];
    page.on("request", (r) => {
      const path = new URL(r.url()).pathname;
      if (/^\/api\/events\/[^/]+\/[^/]+$/.test(path)) streamRequests.push(path);
    });
    await page.goto("/events");
    const filter = page.getByPlaceholder(/type:id/);
    await filter.fill("p-1");
    await filter.press("Enter");
    await expect(page.getByText("Enter a stream as type:id, e.g. product:p-1")).toBeVisible();
    expect(streamRequests).toHaveLength(0);
  });
});
