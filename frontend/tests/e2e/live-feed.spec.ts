/**
 * The page and the live feed's connection, with a mocked backend.
 *
 * The feed is live: a connection starts at the head of the event store, so what
 * was stored while the page was disconnected is not sent to it afterwards. The
 * page reads its lists again when the feed is back.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, BACKEND_URL } from "./helpers";

test.describe("Live feed connection", () => {
  test("a page that just connected shows no event and reads no list again", async ({ page }) => {
    await mockBackendRoutes(page);
    let productReads = 0;
    page.on("request", (r) => {
      if (new URL(r.url()).pathname === "/api/products") productReads += 1;
    });

    await page.goto("/");

    await expect(page.getByText("No events yet. Waiting for activity...")).toBeVisible();
    await expect.poll(() => productReads).toBeGreaterThan(0);
    await page.waitForTimeout(1_000);
    const readsOfTheLoad = productReads;
    // Nothing arrives and the connection stays as it is: the list is not read again.
    await page.waitForTimeout(2_500);
    expect(productReads).toBe(readsOfTheLoad);
  });

  test("a page whose feed connection ended reads its lists again when the feed is back", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    // What the page asked the backend for, in order.
    const asked: string[] = [];
    page.on("request", (r) => {
      if (new URL(r.url()).pathname === "/api/products") asked.push("products");
    });
    // The first two connections open and end at once, as a backend that restarts does; the page
    // reconnects after a second. The third one stays open, so the page connects twice in all
    // before it settles.
    let connections = 0;
    await page.route(`${BACKEND_URL}/api/events/sse`, (route) => {
      connections += 1;
      asked.push(`feed connection ${connections}`);
      if (connections <= 2) {
        return route.fulfill({ status: 200, contentType: "text/event-stream", body: "" });
      }
      // Left unanswered: the connection stays as it is.
    });

    await page.goto("/products");
    await expect(page.getByRole("heading", { name: "Products" })).toBeVisible();

    // The second connection is the feed being back: the list is read again after it.
    const askedSinceTheFeedWasBack = () => {
      const back = asked.indexOf("feed connection 2");
      return back < 0 ? [] : asked.slice(back + 1);
    };
    await expect.poll(askedSinceTheFeedWasBack, { timeout: 10_000 }).toContain("products");
  });
});
