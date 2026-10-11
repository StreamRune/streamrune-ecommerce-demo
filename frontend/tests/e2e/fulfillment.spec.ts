/**
 * Fulfillment page tests.
 *
 * Uses mocked backend so the tests always run. The backend serves the saga
 * list and the payment-failure flag to the ADMIN role only: a saga names the
 * order, the customer and the payment of one purchase. The page follows.
 */

import { test, expect } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  collectConsoleErrors,
  recordApiRequests,
  requestsTo,
} from "./helpers";

test.describe("Fulfillment page", () => {
  test("an ADMIN sees the page", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
    const getErrors = collectConsoleErrors(page);
    const requests = recordApiRequests(page);

    await page.goto("/fulfillment");

    await expect(page.getByRole("heading", { name: "Fulfillment" })).toBeVisible();
    await expect(
      page.getByText("No sagas yet. Place an order to see fulfillment tracking.")
    ).toBeVisible();
    const sent = requestsTo(requests, "/api/saga/fulfillments");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.role).toBe("ADMIN");
      expect(request.userId).toBe("admin-1");
    }

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });

  for (const role of ["GUEST", "CUSTOMER"] as const) {
    test(`a ${role} is told the page needs ADMIN and sends no saga request`, async ({ page }) => {
      await mockBackendRoutes(page);
      await actAs(page, role);
      const requests = recordApiRequests(page);

      await page.goto("/fulfillment");

      await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
      await expect(page.getByText("Requires ADMIN role to view fulfillment sagas.")).toBeVisible();
      expect(requestsTo(requests, "/api/saga")).toHaveLength(0);
      expect(requestsTo(requests, "/api/admin")).toHaveLength(0);
    });
  }
});
