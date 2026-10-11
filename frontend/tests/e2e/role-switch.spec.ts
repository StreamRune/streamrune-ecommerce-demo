/**
 * Switching the role inside one session.
 *
 * What the backend returns depends on the identity: every order to an ADMIN, a
 * CUSTOMER's own orders to that customer, 403 to a caller without an identity.
 * The role provider therefore drops the query cache when the role or the
 * identity changes, so that nothing loaded as one identity is shown under the
 * next. Every other spec fixes the role before the page loads; this one uses
 * the role switcher, as a person does.
 *
 * Mocked backend, which answers by the identity headers it is sent — always
 * runs.
 */

import { test, expect, type Page } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  recordApiRequests,
  requestsTo,
  BACKEND_URL,
} from "./helpers";

function order(orderId: string, customerId: string) {
  return {
    orderId,
    customerId,
    lines: [
      { productId: "prod-widget", quantity: 1, unitPrice: { amount: 9.99, currency: "USD" } },
    ],
    total: { amount: 9.99, currency: "USD" },
    status: "CONFIRMED",
    createdAt: "2026-10-04T00:00:00Z",
    updatedAt: "2026-10-04T00:00:00Z",
  };
}

/** Picks a role in the sidebar's role switcher, the first select of the page. */
async function switchRole(page: Page, role: "ADMIN" | "CUSTOMER" | "GUEST") {
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: role }).click();
}

test.describe("Role switch inside a session — surface (no backend required)", () => {
  test("the rows of one identity are gone under the next, and each list is asked for again", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    // The backend's answer by caller: an ADMIN gets every order, cust-alice her own, and a
    // request without an identity is refused.
    await page.route(`${BACKEND_URL}/api/orders`, (route) => {
      const headers = route.request().headers();
      const userId = headers["x-user-id"];
      if (userId === "admin-1" && headers["x-user-role"] === "ADMIN") {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify([order("order-a1", "cust-alice"), order("order-b1", "cust-bob")]),
        });
      }
      if (userId === "cust-alice") {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify([order("order-a1", "cust-alice")]),
        });
      }
      return route.fulfill({
        status: 403,
        contentType: "text/plain",
        body: "Only an ADMIN or the customer themself may do this",
      });
    });
    await actAs(page, "ADMIN");
    const requests = recordApiRequests(page);

    await page.goto("/orders");

    await expect(page.getByText("2 total")).toBeVisible();
    await expect(page.getByText(/order-b1/)).toBeVisible();

    // ADMIN → CUSTOMER: the list is asked for again as cust-alice, and Bob's order is gone.
    await switchRole(page, "CUSTOMER");

    await expect(page.getByText("1 total for cust-alice (your own orders)")).toBeVisible();
    await expect(page.getByText(/order-a1/)).toBeVisible();
    await expect(page.getByText(/order-b1/)).toHaveCount(0);
    const asCustomer = requestsTo(requests, "/api/orders").filter((r) => r.role === "CUSTOMER");
    expect(asCustomer.length).toBeGreaterThan(0);
    for (const request of asCustomer) {
      expect(request.userId).toBe("cust-alice");
      expect(request.target).toBe("/api/orders");
    }

    // CUSTOMER → GUEST: no identity, no rows, and no order request at all.
    const sentBeforeGuest = requestsTo(requests, "/api/orders").length;
    await switchRole(page, "GUEST");

    await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
    await expect(page.getByText(/order-a1/)).toHaveCount(0);
    expect(requestsTo(requests, "/api/orders")).toHaveLength(sentBeforeGuest);
    expect(requestsTo(requests, "/api/orders").filter((r) => r.role === "GUEST")).toHaveLength(0);
  });
});
