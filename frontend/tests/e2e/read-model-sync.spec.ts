/**
 * How often the page reads a list when the live feed announces events.
 *
 * A frame is a signal to read again, and the read models follow the event
 * store by seconds. A burst of frames must not turn into a burst of reads, and
 * a read model that is late must still show up without another event.
 *
 * Mocked backend and a mocked feed — always runs.
 */

import { test, expect, type Page } from "@playwright/test";
import { actAs, mockBackendRoutes, BACKEND_URL } from "./helpers";

function frame(offset: number, eventType: string): string {
  const data = JSON.stringify({
    globalOffset: offset,
    aggregateType: "order",
    eventType,
    timestamp: "2026-10-04T00:00:00Z",
  });
  return `id:${offset}\ndata:${data}\n\n`;
}

const ORDER = {
  orderId: "order-late-1",
  customerId: "cust-alice",
  lines: [{ productId: "prod-widget", quantity: 1, unitPrice: { amount: 9.99, currency: "USD" } }],
  total: { amount: 9.99, currency: "USD" },
  status: "CONFIRMED",
  createdAt: "2026-10-04T00:00:00Z",
  updatedAt: "2026-10-04T00:00:00Z",
};

/**
 * A feed that sends the given frames after `delayMs` and then ends; the page's reconnects stay
 * unanswered, so the frames arrive once and nothing but them makes the page read. `sentAt`
 * resolves with the time the frames were sent.
 */
async function feedThatSendsOnce(
  page: Page,
  frames: string,
  delayMs: number
): Promise<{ sentAt: Promise<number> }> {
  let connections = 0;
  let frameSent: (at: number) => void = () => {};
  const sentAt = new Promise<number>((resolve) => {
    frameSent = resolve;
  });
  await page.route(`${BACKEND_URL}/api/events/sse`, async (route) => {
    connections += 1;
    if (connections > 1) return;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    const at = Date.now();
    await route.fulfill({ status: 200, contentType: "text/event-stream", body: frames });
    frameSent(at);
  });
  return { sentAt };
}

test.describe("Reads of a list after feed frames", () => {
  test("a burst of fifty frames is answered by one read of the list, not fifty", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
    const readsAt: number[] = [];
    page.on("request", (r) => {
      if (new URL(r.url()).pathname === "/api/orders") readsAt.push(Date.now());
    });
    let frames = "";
    for (let offset = 1; offset <= 50; offset++) {
      frames += frame(offset, offset % 2 === 0 ? "OrderConfirmed" : "OrderPlaced");
    }
    // The frames arrive 1.5 s after the page asked for the feed: the page has loaded by then.
    const feed = await feedThatSendsOnce(page, frames, 1_500);

    await page.goto("/orders");
    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
    const burstAt = await feed.sentAt;
    const readsSinceTheBurst = () => readsAt.filter((at) => at >= burstAt).length;

    // One read at once for the whole burst; the next read is the first re-read, two seconds on.
    await page.waitForTimeout(1_200);
    expect(readsSinceTheBurst()).toBe(1);

    // The re-reads are few and spaced: by eight seconds after the burst, the one at once, the
    // one after two seconds and the one after seven.
    await page.waitForTimeout(7_000);
    expect(readsSinceTheBurst()).toBe(3);
  });

  test("a read model that is ten seconds late shows up without another event", async ({
    page,
  }) => {
    test.setTimeout(60_000);
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
    // The order's row reaches the read model ten seconds after its frame.
    let frameSentAt = Number.POSITIVE_INFINITY;
    await page.route(`${BACKEND_URL}/api/orders`, (route) => {
      const rowIsThere = Date.now() - frameSentAt >= 10_000;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(rowIsThere ? [ORDER] : []),
      });
    });
    let connections = 0;
    await page.route(`${BACKEND_URL}/api/events/sse`, async (route) => {
      connections += 1;
      if (connections > 1) return;
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      frameSentAt = Date.now();
      await route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        body: frame(1, "OrderPlaced"),
      });
    });

    await page.goto("/orders");
    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();

    // No further frame arrives. The re-read fifteen seconds after the frame finds the row.
    await expect(page.getByRole("row").filter({ hasText: ORDER.orderId })).toBeVisible({
      timeout: 25_000,
    });
  });

  test("the re-reads one identity was waiting for are dropped when the role changes", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
    const readsAt: number[] = [];
    page.on("request", (r) => {
      if (new URL(r.url()).pathname === "/api/orders") readsAt.push(Date.now());
    });
    const feed = await feedThatSendsOnce(page, frame(1, "OrderPlaced"), 1_000);

    await page.goto("/orders");
    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
    await feed.sentAt;
    await page.waitForTimeout(300);

    // The role switcher is the first select of the page; the keyboard picks CUSTOMER, whose
    // first letter no other role shares.
    const switcher = page.getByRole("combobox").first();
    await switcher.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("option", { name: "CUSTOMER" })).toBeVisible();
    await page.keyboard.type("C");
    await page.keyboard.press("Enter");
    await expect(switcher).toContainText("CUSTOMER");
    await page.waitForTimeout(500);
    const readsAfterTheSwitch = readsAt.length;

    // The frame's re-reads at two and at seven seconds were the ADMIN's: none of them is sent
    // as the customer.
    await page.waitForTimeout(8_000);
    expect(readsAt.length).toBe(readsAfterTheSwitch);
  });

  test("the re-reads of a list stop for a page that was left", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "ADMIN");
    const readsAt: number[] = [];
    page.on("request", (r) => {
      if (new URL(r.url()).pathname === "/api/orders") readsAt.push(Date.now());
    });
    const feed = await feedThatSendsOnce(page, frame(1, "OrderPlaced"), 1_000);

    await page.goto("/orders");
    await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
    await feed.sentAt;
    await page.waitForTimeout(500);
    // Leave the Orders page through the sidebar, without a reload: the providers stay mounted.
    await page.getByRole("link", { name: "Products" }).first().click();
    await expect(page.getByRole("heading", { name: "Products" })).toBeVisible();
    const readsWhenLeft = readsAt.length;

    // The re-reads at two and at seven seconds fall into this wait and ask for nothing.
    await page.waitForTimeout(8_000);
    expect(readsAt.length).toBe(readsWhenLeft);
  });
});
