/**
 * Orders page tests.
 *
 * The backend serves orders to an authenticated caller only: every order to an
 * ADMIN (narrowed to one customer by ?customerId=), a CUSTOMER's own orders to
 * that customer, 403 to a caller without an identity and to a CUSTOMER who
 * names another customer in the filter. The page follows.
 *
 * Surface tests: mocked backend — always run.
 * Backend-dependent block: skipped when Spring Boot is not reachable. It
 * expects the seed order "order-seed-1", placed by cust-alice.
 */

import { test, expect } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  isBackendReachable,
  collectConsoleErrors,
  recordApiRequests,
  requestsTo,
  BACKEND_URL,
} from "./helpers";

function order(orderId: string, customerId: string, paymentStatus: string | null = "CAPTURED") {
  return {
    orderId,
    customerId,
    lines: [
      { productId: "prod-widget", quantity: 1, unitPrice: { amount: 9.99, currency: "USD" } },
    ],
    total: { amount: 9.99, currency: "USD" },
    status: "CONFIRMED",
    paymentStatus,
    createdAt: "2026-10-04T00:00:00Z",
    updatedAt: "2026-10-04T00:00:00Z",
  };
}

test.describe("Orders page — surface (no backend required)", () => {
  test("a GUEST is told what the page requires and sends no order or customer request", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);
    const requests = recordApiRequests(page);

    await page.goto("/orders");

    await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
    await expect(page.getByText("Requires CUSTOMER or ADMIN role to view orders.")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Orders" })).toHaveCount(0);
    expect(requestsTo(requests, "/api/orders")).toHaveLength(0);
    expect(requestsTo(requests, "/api/customers")).toHaveLength(0);

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });

  test("an ADMIN lists every order and may narrow the list to one customer", async ({ page }) => {
    await mockBackendRoutes(page);
    await page.route(`${BACKEND_URL}/api/orders`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([order("order-a1", "cust-alice"), order("order-b1", "cust-bob")]),
      })
    );
    await page.route(`${BACKEND_URL}/api/orders?customerId=cust-bob`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([order("order-b1", "cust-bob")]),
      })
    );
    await actAs(page, "ADMIN");
    const requests = recordApiRequests(page);

    await page.goto("/orders");

    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
    await expect(page.getByText("2 total")).toBeVisible();

    await page.goto("/orders?customerId=cust-bob");

    await expect(page.getByText("1 total for cust-bob")).toBeVisible();
    const sent = requestsTo(requests, "/api/orders");
    expect(sent.map((r) => r.target)).toContain("/api/orders?customerId=cust-bob");
    for (const request of sent) {
      expect(request.role).toBe("ADMIN");
      expect(request.userId).toBe("admin-1");
    }
  });

  test("a CUSTOMER lists their own orders and never sends another customer as the filter", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    // What the backend answers a CUSTOMER: the caller's own orders and nothing else.
    await page.route(`${BACKEND_URL}/api/orders`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([order("order-a1", "cust-alice")]),
      })
    );
    await actAs(page, "CUSTOMER");
    const requests = recordApiRequests(page);

    // The address names another customer; the page asks for the caller's own orders all the same.
    await page.goto("/orders?customerId=cust-bob");

    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
    await expect(page.getByText("1 total for cust-alice (your own orders)")).toBeVisible();

    const sent = requestsTo(requests, "/api/orders");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.target).toBe("/api/orders");
      expect(request.role).toBe("CUSTOMER");
      expect(request.userId).toBe("cust-alice");
    }
  });
});

test.describe("Orders page — the payment of an order (no backend required)", () => {
  test("an order row shows its payment status", async ({ page }) => {
    await mockBackendRoutes(page);
    await page.route(`${BACKEND_URL}/api/orders`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          { ...order("order-a1", "cust-alice", "REFUNDED"), status: "CANCELLED" },
          order("order-a2", "cust-alice", null),
        ]),
      })
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");

    const refunded = page.getByRole("row").filter({ hasText: "order-a1" });
    await expect(refunded.getByLabel("Payment status")).toHaveText("REFUNDED");
    await expect(refunded).toContainText("CANCELLED");
    // An order whose payment is not initiated yet shows no payment status.
    const unpaid = page.getByRole("row").filter({ hasText: "order-a2" });
    await expect(unpaid).toBeVisible();
    await expect(unpaid.getByLabel("Payment status")).toHaveCount(0);
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

  // The order table shows orderId.slice(0, 8): "order-seed-1" appears as "order-se".
  test("an ADMIN and the customer who placed it see the seed order (order-seed-1)", async ({
    page,
  }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    await actAs(page, "ADMIN");
    await page.goto("/orders");
    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
    await expect(page.locator("td.font-mono", { hasText: "order-se" }).first()).toBeVisible();

    await actAs(page, "CUSTOMER", "cust-alice");
    await page.goto("/orders");
    await expect(page.getByText("(your own orders)")).toBeVisible();
    await expect(page.locator("td.font-mono", { hasText: "order-se" }).first()).toBeVisible();
  });

  test("another customer does not see the seed order", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    await actAs(page, "CUSTOMER", "cust-bob");

    await page.goto("/orders");

    await expect(page.getByText("(your own orders)")).toBeVisible();
    await expect(page.locator("td.font-mono", { hasText: "order-se" })).toHaveCount(0);
  });

  test("a GUEST sends no order request", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    const requests = recordApiRequests(page);

    await page.goto("/orders");

    await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
    expect(requestsTo(requests, "/api/orders")).toHaveLength(0);
  });
});
