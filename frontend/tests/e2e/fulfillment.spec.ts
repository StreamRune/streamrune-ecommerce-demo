/**
 * Fulfillment page tests.
 *
 * Uses mocked backend so the tests always run. The backend serves the saga
 * list to an authenticated caller only: every saga to an ADMIN, the sagas of
 * their own orders to a CUSTOMER, and nothing to a GUEST. The payment-failure
 * flag is an ADMIN's alone. The page follows.
 */

import { test, expect, type Page } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  collectConsoleErrors,
  recordApiRequests,
  requestsTo,
  BACKEND_URL,
} from "./helpers";

/** One row of GET /api/saga/fulfillments, as the backend sends it. */
function sagaRow(orderId: string, customerId: string, status: string) {
  return {
    sagaId: `fulfillment-${orderId}`,
    sagaType: "org.streamrune.ecommerce.commands.saga.OrderFulfillmentState",
    status,
    state: JSON.stringify({
      sagaId: { value: `fulfillment-${orderId}` },
      orderId,
      customerId,
      paymentId: `pay-${orderId}`,
    }),
  };
}

/**
 * Answers the saga list as the backend does: the rows of the caller named in
 * X-User-Id, every row for an ADMIN, 403 without an identity.
 */
async function mockSagaList(page: Page): Promise<void> {
  const rows = [
    sagaRow("ord-alice-1", "cust-alice", "COMPLETED"),
    sagaRow("ord-bob-1", "cust-bob", "RUNNING"),
  ];
  await page.route(`${BACKEND_URL}/api/saga/fulfillments?*`, (route) => {
    const headers = route.request().headers();
    const userId = headers["x-user-id"];
    if (!userId) {
      return route.fulfill({ status: 403, contentType: "application/json", body: "{}" });
    }
    const visible =
      headers["x-user-role"] === "ADMIN"
        ? rows
        : rows.filter((row) => JSON.parse(row.state).customerId === userId);
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(visible),
    });
  });
}

test.describe("Fulfillment page", () => {
  test("an ADMIN sees every saga and the payment-failure switch", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockSagaList(page);
    await actAs(page, "ADMIN");
    const getErrors = collectConsoleErrors(page);
    const requests = recordApiRequests(page);

    await page.goto("/fulfillment");

    await expect(page.getByRole("heading", { name: "Fulfillment" })).toBeVisible();
    await expect(page.getByText("fulfillment-ord-alice-1")).toBeVisible();
    await expect(page.getByText("fulfillment-ord-bob-1")).toBeVisible();
    await expect(page.getByRole("switch", { name: "Inject Payment Failure" })).toBeVisible();
    const sent = requestsTo(requests, "/api/saga/fulfillments");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.role).toBe("ADMIN");
      expect(request.userId).toBe("admin-1");
    }
    expect(requestsTo(requests, "/api/admin/payment-failure").length).toBeGreaterThan(0);

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });

  test("an ADMIN without sagas is told to place an order", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");

    await page.goto("/fulfillment");

    await expect(
      page.getByText("No sagas yet. Place an order to see fulfillment tracking.")
    ).toBeVisible();
  });

  test("a CUSTOMER sees the sagas of their own orders and no admin control", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockSagaList(page);
    await actAs(page, "CUSTOMER");
    const requests = recordApiRequests(page);

    await page.goto("/fulfillment");

    await expect(page.getByRole("heading", { name: "Fulfillment" })).toBeVisible();
    await expect(page.getByText("Saga-based fulfillment status of your own orders")).toBeVisible();
    await expect(page.getByText("fulfillment-ord-alice-1")).toBeVisible();
    await expect(page.getByText("fulfillment-ord-bob-1")).toHaveCount(0);
    // The switch and the flag behind it are an ADMIN's: neither shown nor asked for.
    await expect(page.getByRole("switch", { name: "Inject Payment Failure" })).toHaveCount(0);
    const sent = requestsTo(requests, "/api/saga/fulfillments");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.role).toBe("CUSTOMER");
      expect(request.userId).toBe("cust-alice");
    }
    expect(requestsTo(requests, "/api/admin")).toHaveLength(0);
  });

  test("another CUSTOMER sees their own sagas", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockSagaList(page);
    await actAs(page, "CUSTOMER", "cust-bob");

    await page.goto("/fulfillment");

    await expect(page.getByText("fulfillment-ord-bob-1")).toBeVisible();
    await expect(page.getByText("fulfillment-ord-alice-1")).toHaveCount(0);
  });

  test("a CUSTOMER without orders is told to place one", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "CUSTOMER");

    await page.goto("/fulfillment");

    await expect(
      page.getByText("No sagas yet. Place an order to see its fulfillment.")
    ).toBeVisible();
  });

  test("a GUEST is told the page needs an identity and sends no saga request", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "GUEST");
    const requests = recordApiRequests(page);

    await page.goto("/fulfillment");

    await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
    await expect(
      page.getByText("Requires CUSTOMER or ADMIN role to view fulfillment sagas.")
    ).toBeVisible();
    expect(requestsTo(requests, "/api/saga")).toHaveLength(0);
    expect(requestsTo(requests, "/api/admin")).toHaveLength(0);
  });
});
