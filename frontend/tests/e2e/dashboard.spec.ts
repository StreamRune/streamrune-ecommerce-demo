/**
 * Dashboard smoke test — loads "/" with mocked backend so it works even when
 * port 8080 is not running.  Verifies:
 *   - Page heading is visible
 *   - The SSE status indicator is present in the sidebar (Event Explorer nav item)
 *   - No console.error during initial load
 */

import { test, expect } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  mockFeedThatSendsOnce,
  collectConsoleErrors,
  recordApiRequests,
  requestsTo,
} from "./helpers";

test.describe("Dashboard page", () => {
  test("the live event feed lists a frame that names no stream", async ({ page }) => {
    await mockBackendRoutes(page);
    // An unnamed frame, as the backend sends it: an id and a data line with the offset, the two
    // types and the timestamp, and no stream id, aggregate id or payload.
    await mockFeedThatSendsOnce(
      page,
      "id:3\n" +
        'data:{"globalOffset":3,"aggregateType":"product","eventType":"ProductCreated",' +
        '"timestamp":"2026-10-04T00:00:00Z"}\n\n'
    );

    await page.goto("/");

    await expect(page.getByText("ProductCreated")).toBeVisible();
    await expect(page.getByText("product", { exact: true })).toBeVisible();
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

  test("a GUEST sends no order or customer request and sees what the cards require", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    const requests = recordApiRequests(page);

    await page.goto("/");

    await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();
    await expect(page.getByText("Requires CUSTOMER or ADMIN role", { exact: true })).toHaveCount(2);
    await expect(page.getByText("Requires CUSTOMER or ADMIN role to view orders.")).toBeVisible();
    // The product catalogue is open to everyone.
    await expect.poll(() => requestsTo(requests, "/api/products").length).toBeGreaterThan(0);
    expect(requestsTo(requests, "/api/orders")).toHaveLength(0);
    expect(requestsTo(requests, "/api/customers")).toHaveLength(0);
  });

  test("a CUSTOMER loads their own orders and record, as cust-alice", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "CUSTOMER");
    const requests = recordApiRequests(page);

    await page.goto("/");

    await expect(page.getByText("Your own record")).toBeVisible();
    await expect(page.getByText("0 pending, your own")).toBeVisible();
    const sent = [
      ...requestsTo(requests, "/api/orders"),
      ...requestsTo(requests, "/api/customers"),
    ];
    expect(sent.length).toBeGreaterThanOrEqual(2);
    for (const request of sent) {
      expect(request.target).not.toContain("customerId");
      expect(request.role).toBe("CUSTOMER");
      expect(request.userId).toBe("cust-alice");
    }
  });
});
