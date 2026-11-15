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

import { test, expect, type Locator, type Page } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  isBackendReachable,
  collectConsoleErrors,
  recordApiRequests,
  requestsTo,
  isOrderList,
  orderPage,
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
  test("a cancelled order shows why it was cancelled", async ({ page }) => {
    await mockBackendRoutes(page);
    const reason = "Insufficient stock for prod-widget: requested 3";
    await page.route(isOrderList, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage([
          { ...order("order-a1", "cust-alice", "REFUNDED"), status: "CANCELLED", reason },
          { ...order("order-a2", "cust-alice"), reason: null },
        ]),
      })
    );
    await actAs(page, "ADMIN");

    await page.goto("/orders");

    const cancelled = page.getByRole("row").filter({ hasText: "order-a1" });
    await expect(cancelled).toContainText("CANCELLED");
    await expect(cancelled.getByText(reason)).toBeVisible();
    await expect(cancelled).toContainText(`Cancellation reason: ${reason}`);
    // A confirmed order has no reason to show.
    await expect(page.getByRole("row").filter({ hasText: "order-a2" })).not.toContainText(
      "Cancellation reason"
    );
  });

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
    await page.route(isOrderList, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage([order("order-a1", "cust-alice"), order("order-b1", "cust-bob")]),
      })
    );
    await page.route(
      (url) => isOrderList(url) && url.searchParams.get("customerId") === "cust-bob",
      (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage([order("order-b1", "cust-bob")]),
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
    expect(sent.map((r) => r.target)).toContain("/api/orders?customerId=cust-bob&page=0&size=20");
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
    await page.route(isOrderList, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage([order("order-a1", "cust-alice")]),
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
      expect(request.target).toBe("/api/orders?page=0&size=20");
      expect(request.role).toBe("CUSTOMER");
      expect(request.userId).toBe("cust-alice");
    }
  });
});

test.describe("Orders page — one page at a time (no backend required)", () => {
  test("the page reads the first page and loads the next one on Load more", async ({ page }) => {
    await mockBackendRoutes(page);
    // 25 orders: a first page of 20 and a second of 5.
    const all = Array.from({ length: 25 }, (_, i) =>
      order(`order-${String(i).padStart(2, "0")}`, "cust-alice")
    );
    await page.route(isOrderList, (route) => {
      const url = new URL(route.request().url());
      const number = Number(url.searchParams.get("page"));
      const size = Number(url.searchParams.get("size"));
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage(all.slice(number * size, (number + 1) * size), {
          page: number,
          size,
          total: all.length,
        }),
      });
    });
    await actAs(page, "ADMIN");
    const requests = recordApiRequests(page);

    await page.goto("/orders");

    await expect(page.getByText("25 total, 20 shown")).toBeVisible();
    await expect(page.getByRole("row").filter({ hasText: "order-19" })).toBeVisible();
    await expect(page.getByRole("row").filter({ hasText: "order-20" })).toHaveCount(0);
    // Only the first page is asked for (perhaps more than once, as the page settles).
    expect(new Set(requestsTo(requests, "/api/orders").map((r) => r.target))).toEqual(
      new Set(["/api/orders?page=0&size=20"])
    );

    await page.getByRole("button", { name: "Load more" }).click();

    await expect(page.getByRole("row").filter({ hasText: "order-24" })).toBeVisible();
    await expect(page.getByText("25 total", { exact: true })).toBeVisible();
    // The last page says no page follows it: the button is gone.
    await expect(page.getByRole("button", { name: "Load more" })).toHaveCount(0);
    expect(requestsTo(requests, "/api/orders").map((r) => r.target)).toContain(
      "/api/orders?page=1&size=20"
    );
  });

  test("an order a newer one pushed onto the next page is shown once", async ({ page }) => {
    await mockBackendRoutes(page);
    const firstPage = Array.from({ length: 20 }, (_, i) =>
      order(`order-${String(i).padStart(2, "0")}`, "cust-alice")
    );
    // Between the two reads an order was placed: the second page starts with the first page's
    // last order, and the total has grown by one.
    const secondPage = [order("order-19", "cust-alice"), order("order-20", "cust-alice")];
    await page.route(isOrderList, (route) => {
      const number = Number(new URL(route.request().url()).searchParams.get("page"));
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body:
          number === 0
            ? orderPage(firstPage, { total: 21 })
            : orderPage(secondPage, { page: 1, total: 22 }),
      });
    });
    await actAs(page, "ADMIN");
    const errors = collectConsoleErrors(page);

    await page.goto("/orders");
    await expect(page.getByText("21 total, 20 shown")).toBeVisible();
    await page.getByRole("button", { name: "Load more" }).click();

    await expect(page.getByRole("row").filter({ hasText: "order-20" })).toBeVisible();
    await expect(page.getByRole("row").filter({ hasText: "order-19" })).toHaveCount(1);
    // The total is the last page's: 22 orders, 21 of them shown.
    await expect(page.getByText("22 total, 21 shown")).toBeVisible();
    expect(errors().filter((e) => e.includes("same key"))).toHaveLength(0);
  });

  test("the helper of the live specs loads page after page until a row many pages deep shows", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    // 130 orders, seven pages; every page takes a moment to answer, as a real backend does.
    const all = Array.from({ length: 130 }, (_, i) =>
      order(`order-${String(i).padStart(3, "0")}`, "cust-alice")
    );
    const asked: number[] = [];
    await page.route(isOrderList, async (route) => {
      const url = new URL(route.request().url());
      const number = Number(url.searchParams.get("page"));
      const size = Number(url.searchParams.get("size"));
      asked.push(number);
      await new Promise((resolve) => setTimeout(resolve, 150));
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage(all.slice(number * size, (number + 1) * size), {
          page: number,
          size,
          total: all.length,
        }),
      });
    });
    await actAs(page, "ADMIN");

    await page.goto("/orders");
    await showOrder(page, "order-129");

    expect(Math.max(...asked)).toBe(6);
    await expect(page.getByText("130 total", { exact: true })).toBeVisible();
  });
});

test.describe("Orders page — the payment of an order (no backend required)", () => {
  test("an order row shows its payment status", async ({ page }) => {
    await mockBackendRoutes(page);
    await page.route(isOrderList, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: orderPage([
          { ...order("order-a1", "cust-alice", "REFUNDED"), status: "CANCELLED" },
          order("order-a2", "cust-alice", null),
        ]),
      })
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");

    const refunded = page.getByRole("row").filter({ hasText: "order-a1" });
    // The badge reads "REFUNDED"; its name for a screen reader is "Payment REFUNDED".
    await expect(refunded.getByText("Payment REFUNDED")).toBeVisible();
    await expect(refunded).toContainText("CANCELLED");
    // An order whose payment is not initiated yet shows no payment status.
    const unpaid = page.getByRole("row").filter({ hasText: "order-a2" });
    await expect(unpaid).toBeVisible();
    await expect(unpaid.getByText(/^Payment /)).toHaveCount(0);
  });
});

/**
 * Loads pages of the order table with "Load more" until `until` shows, or, without `until`, until
 * the last page is loaded; at most 100 pages. While a page is fetched the button reads
 * "Loading…"; only a list with neither button is at its end. Each step waits for the fetch it
 * started to end: the next page's rows shown, or the button gone.
 */
async function loadOrderPages(page: Page, until?: Locator): Promise<void> {
  const loadMore = page.getByRole("button", { name: "Load more", exact: true });
  const loading = page.getByRole("button", { name: "Loading…", exact: true });
  const anyButton = page.getByRole("button", { name: /^(Load more|Loading…)$/ });
  const rows = page.getByRole("row");
  await expect(page.getByText(/\d+ total/)).toBeVisible();
  for (let pages = 1; pages <= 100; pages++) {
    await expect(loading).toHaveCount(0);
    if (until && (await until.isVisible())) return;
    if ((await anyButton.count()) === 0) return;
    const before = await rows.count();
    await loadMore.click();
    await expect
      .poll(async () => (await rows.count()) > before || (await anyButton.count()) === 0)
      .toBe(true);
  }
  if (!until) await expect(anyButton).toHaveCount(0);
}

/** Loads pages of the order table until a row whose id cell holds `idPrefix` shows. */
async function showOrder(page: Page, idPrefix: string): Promise<void> {
  const row = page.locator("td.font-mono", { hasText: idPrefix }).first();
  await loadOrderPages(page, row);
  await expect(row).toBeVisible();
}

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

  // The order table shows orderId.slice(0, 8): "order-seed-1" appears as "order-se". The list is
  // newest first and the seed order is the oldest, so it is on the last page of a long list.
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
    await showOrder(page, "order-se");

    await actAs(page, "CUSTOMER", "cust-alice");
    await page.goto("/orders");
    await expect(page.getByText("(your own orders)")).toBeVisible();
    await showOrder(page, "order-se");
  });

  test("another customer does not see the seed order", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    await actAs(page, "CUSTOMER", "cust-bob");

    await page.goto("/orders");

    await expect(page.getByText("(your own orders)")).toBeVisible();
    // Every page of Bob's list, which may be empty: the seed order is on none of them.
    await loadOrderPages(page);
    await expect(page.locator("td.font-mono", { hasText: "order-se" })).toHaveCount(0);
    // Asked for by its id, the seed order is refused to Bob.
    const answer = await page.request.get(`${BACKEND_URL}/api/orders/order-seed-1`, {
      headers: { "X-User-Id": "cust-bob", "X-User-Role": "CUSTOMER" },
    });
    expect(answer.status()).toBe(403);
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
