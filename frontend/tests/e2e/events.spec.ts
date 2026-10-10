/**
 * Event Explorer page tests.
 *
 * Uses mocked backend so the tests always run. The backend serves the event
 * history (payloads included, decrypted) to the ADMIN role only, and an open
 * live feed whose frames carry no payload. The page follows: the history is
 * browsed as ADMIN, the live feed is shown to everyone.
 */

import { test, expect, type Page } from "@playwright/test";
import { mockBackendRoutes, collectConsoleErrors, BACKEND_URL } from "./helpers";

/** Opens the page as the given role, as the role switcher would have saved it. */
async function actAs(page: Page, role: "ADMIN" | "CUSTOMER") {
  await page.addInitScript((r) => {
    window.localStorage.setItem("sr_role", r);
  }, role);
}

/** Requests for the event history: the list or one stream, never the live feed. */
function historyRequests(page: Page): string[] {
  const seen: string[] = [];
  page.on("request", (r) => {
    const path = new URL(r.url()).pathname;
    if (path === "/api/events" || /^\/api\/events\/[^/]+\/[^/]+$/.test(path)) {
      seen.push(path);
    }
  });
  return seen;
}

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

  test("a GUEST is told the history needs ADMIN and sends no history request", async ({ page }) => {
    await mockBackendRoutes(page);
    const requests = historyRequests(page);

    await page.goto("/events");

    await expect(page.getByText("Requires ADMIN role to browse the event history.")).toBeVisible();
    await expect(page.getByPlaceholder(/type:id/)).toHaveCount(0);
    // The live feed is still there for everyone.
    await expect(page.getByRole("heading", { name: "Live Feed" })).toBeVisible();
    expect(requests).toHaveLength(0);
  });

  test("an ADMIN browses the history and the request carries the role headers", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
    const sent: Record<string, string>[] = [];
    await page.route(`${BACKEND_URL}/api/events?*`, (route) => {
      sent.push(route.request().headers());
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            globalOffset: 7,
            streamId: "customer:cust-alice",
            aggregateType: "customer",
            aggregateId: "cust-alice",
            eventType: "CustomerRegistered",
            timestamp: "2026-10-04T00:00:00Z",
            payload: { customerId: "cust-alice", name: "Alice Johnson" },
          },
        ]),
      });
    });

    await page.goto("/events");

    await page.getByRole("button", { name: /CustomerRegistered/ }).click();
    await expect(page.getByText(/"name": "Alice Johnson"/)).toBeVisible();
    expect(sent.length).toBeGreaterThan(0);
    expect(sent[0]["x-user-role"]).toBe("ADMIN");
    expect(sent[0]["x-user-id"]).toBe("admin-1");
  });

  test("the live feed lists a payload-free frame for every role", async ({ page }) => {
    await mockBackendRoutes(page);
    // An unnamed frame, as the backend sends it: an id and a data line without a payload.
    await page.route(`${BACKEND_URL}/api/events/sse`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        body:
          "id:7\n" +
          'data:{"globalOffset":7,"streamId":"customer:cust-alice","aggregateType":"customer",' +
          '"aggregateId":"cust-alice","eventType":"CustomerRegistered","version":1,' +
          '"timestamp":"2026-10-04T00:00:00Z"}\n\n',
      })
    );

    await page.goto("/events");

    const liveFeed = page.locator("section", {
      has: page.getByRole("heading", { name: "Live Feed" }),
    });
    await expect(liveFeed.getByText("CustomerRegistered")).toBeVisible();
    await expect(liveFeed.getByText("customer:cust-alice")).toBeVisible();
    await expect(liveFeed.getByText("v1", { exact: true })).toBeVisible();
    // The mocked feed ends after its one frame and the page reconnects to the same frame; an
    // event already listed is not listed again.
    await page.waitForTimeout(2_500);
    await expect(liveFeed.getByText("CustomerRegistered")).toHaveCount(1);
  });

  test("filters one stream by type:id over the two-segment route", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
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
    await actAs(page, "ADMIN");
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
