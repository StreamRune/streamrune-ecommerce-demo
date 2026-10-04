/**
 * Cross-page navigation smoke flow.
 *
 * Starts at "/", then navigates to each page via the sidebar link href,
 * asserting that:
 *   1. The target page loads (its heading or access-gate becomes visible).
 *   2. No unexpected console.error was emitted during the navigation.
 *
 * Uses mocked backend throughout — runs without the Spring Boot backend.
 *
 * Note: Audit Log requires ADMIN role; GUEST sees "Access Restricted".
 * Both are valid rendered states that prove the page loaded without crashing.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes } from "./helpers";

/** Nav items: [href, expected heading text OR fallback gate text] */
const NAV_ITEMS: Array<{ href: string; heading: string; fallback?: string }> = [
  { href: "/orders", heading: "Orders" },
  { href: "/products", heading: "Products" },
  { href: "/customers", heading: "Customers" },
  { href: "/fulfillment", heading: "Fulfillment" },
  { href: "/events", heading: "Event Explorer" },
  { href: "/audit", heading: "Audit Log", fallback: "Access Restricted" },
  { href: "/admin", heading: "Admin" },
];

test.describe("Cross-page navigation flow", () => {
  test("navigate through all pages without console errors", async ({ page }) => {
    const errors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        errors.push(msg.text());
      }
    });

    // Mock backend once — route mocks persist for the lifetime of the page
    await mockBackendRoutes(page);

    // Start at Dashboard
    await page.goto("/");
    await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();

    // Navigate to each page by href and assert it renders
    for (const { href, heading, fallback } of NAV_ITEMS) {
      await page.goto(href);

      const primary = page.getByRole("heading", { name: heading });
      const target = fallback
        ? primary.or(page.getByRole("heading", { name: fallback }))
        : primary;

      await expect(target.first()).toBeVisible();
    }

    // Filter noise: SSE / EventSource / network errors are expected with mocked backend
    const unexpectedErrors = errors.filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch") &&
        !e.includes("SSE")
    );

    expect(
      unexpectedErrors,
      `Unexpected console errors during navigation:\n${unexpectedErrors.join("\n")}`
    ).toHaveLength(0);
  });
});
