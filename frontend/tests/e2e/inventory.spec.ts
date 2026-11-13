/**
 * Admin → Inventory and Admin → Catalog: what the pages show and to whom.
 *
 * The inventory is the one place that holds a product's units; its read model
 * is served to an ADMIN only (GET /api/inventory), with the units available,
 * reserved and committed. The table follows the live feed: an inventory event
 * reads it again, and so does a product's creation, which opens a row. The
 * storefront's availability is derived from the same inventory events, so they
 * read the products again too.
 *
 * Mocked backend and a mocked feed — always runs. The commands of both pages
 * are in `write-contracts.spec.ts`.
 */

import { test, expect, type Page } from "@playwright/test";
import {
  actAs,
  mockBackendRoutes,
  recordApiRequests,
  requestsTo,
  BACKEND_URL,
} from "./helpers";

function frame(offset: number, aggregateType: string, eventType: string): string {
  const data = JSON.stringify({
    globalOffset: offset,
    aggregateType,
    eventType,
    timestamp: "2026-10-04T00:00:00Z",
  });
  return `id:${offset}\ndata:${data}\n\n`;
}

/**
 * A feed that sends the given frames after `delayMs` and then ends; the page's reconnects stay
 * unanswered, so nothing but the frames makes the page read again. `beforeSend` runs just before
 * the frames go out; `sentAt` resolves with the time they were sent.
 */
async function feedThatSendsOnce(
  page: Page,
  frames: string,
  delayMs: number,
  beforeSend: () => void = () => {}
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
    beforeSend();
    const at = Date.now();
    await route.fulfill({ status: 200, contentType: "text/event-stream", body: frames });
    frameSent(at);
  });
  return { sentAt };
}

const WIDGET_UNITS = {
  productId: "prod-widget",
  productName: "Widget",
  available: 100,
  reserved: 2,
  committed: 5,
};

const GADGET_UNITS = {
  productId: "prod-gadget",
  productName: "Gadget",
  available: 0,
  reserved: 0,
  committed: 0,
};

async function mockInventory(page: Page, rows: () => unknown[]): Promise<void> {
  await page.route(`${BACKEND_URL}/api/inventory`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(rows()) })
  );
}

function row(page: Page, text: string) {
  return page.getByRole("row").filter({ hasText: text });
}

test.describe("Admin → Inventory", () => {
  test("an ADMIN sees every product's available, reserved and committed units", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockInventory(page, () => [WIDGET_UNITS, GADGET_UNITS]);
    await actAs(page, "ADMIN");

    await page.goto("/admin/inventory");

    await expect(page.getByRole("heading", { name: "Inventory" })).toBeVisible();
    const cells = (name: string) => row(page, name).getByRole("cell");
    await expect(cells("Widget").nth(0)).toContainText("prod-widget");
    await expect(cells("Widget").nth(1)).toHaveText("100");
    await expect(cells("Widget").nth(2)).toHaveText("2");
    await expect(cells("Widget").nth(3)).toHaveText("5");
    // A product nobody has received stock for is listed with no units.
    await expect(cells("Gadget").nth(1)).toHaveText("0");
    // The sidebar marks Inventory, not Admin, as the current page.
    await expect(page.getByRole("link", { name: "Inventory" })).toHaveAttribute("aria-current", "page");
    await expect(page.getByRole("link", { name: "Admin", exact: true })).not.toHaveAttribute(
      "aria-current",
      "page"
    );
  });

  for (const role of ["CUSTOMER", "GUEST"] as const) {
    test(`a ${role} is told the inventory is an ADMIN's, and no inventory request is sent`, async ({
      page,
    }) => {
      await mockBackendRoutes(page);
      const requests = recordApiRequests(page);
      await actAs(page, role);

      await page.goto("/admin/inventory");

      await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
      await expect(page.getByText("Requires ADMIN role to view the inventory.")).toBeVisible();
      await expect(page.getByRole("button", { name: "Actions" })).toHaveCount(0);
      expect(requestsTo(requests, "/api/inventory")).toHaveLength(0);
    });
  }

  // Every event that moves a count, and the product's creation, which opens a row.
  for (const [aggregateType, eventType] of [
    ["inventory", "ShipmentReceived"],
    ["inventory", "InventoryAdjusted"],
    ["inventory", "StockReserved"],
    ["inventory", "StockReleased"],
    ["inventory", "ReservationConfirmed"],
    ["inventory", "StockReservationRefused"],
    ["product", "ProductCreated"],
  ]) {
    const article = /^[AEIOU]/.test(eventType) ? "an" : "a";
    test(`${article} ${eventType} frame reads the inventory again and the row follows`, async ({
      page,
    }) => {
      await mockBackendRoutes(page);
      await actAs(page, "ADMIN");
      let frameSent = false;
      await mockInventory(page, () =>
        frameSent ? [{ ...WIDGET_UNITS, available: 97, reserved: 5 }] : [WIDGET_UNITS]
      );
      // The read model has the change by the time the frame announces it.
      const feed = await feedThatSendsOnce(page, frame(1, aggregateType, eventType), 1_500, () => {
        frameSent = true;
      });

      await page.goto("/admin/inventory");
      const cells = row(page, "Widget").getByRole("cell");
      await expect(cells.nth(1)).toHaveText("100");

      await feed.sentAt;
      // The re-read follows the frame at once; without it nothing would read the list again.
      await expect(cells.nth(1)).toHaveText("97", { timeout: 1_500 });
      await expect(cells.nth(2)).toHaveText("5");
    });
  }

  test("an inventory frame reads the storefront's products again: their availability follows", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    const readsAt: number[] = [];
    page.on("request", (r) => {
      if (new URL(r.url()).pathname === "/api/products") readsAt.push(Date.now());
    });
    const feed = await feedThatSendsOnce(page, frame(1, "inventory", "InventoryAdjusted"), 1_500);

    await page.goto("/products");
    await expect(page.getByRole("heading", { name: "Products" })).toBeVisible();
    const sentAt = await feed.sentAt;

    await expect
      .poll(() => readsAt.filter((at) => at >= sentAt).length, { timeout: 1_500 })
      .toBeGreaterThan(0);
  });
});

test.describe("Admin → Catalog", () => {
  test("lists the catalogue with its status, and offers its actions to an ADMIN only", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await page.route(`${BACKEND_URL}/api/products`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          {
            productId: "prod-widget",
            name: "Widget",
            description: "A widget",
            category: "Electronics",
            price: { amount: 19.99, currency: "USD" },
            status: "DISCONTINUED",
            available: 731,
            availability: "DISCONTINUED",
            createdAt: "2026-10-04T00:00:00Z",
            updatedAt: "2026-10-04T00:00:00Z",
          },
        ]),
      })
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/admin/catalog");

    const widget = row(page, "Widget");
    await expect(widget).toContainText("Electronics");
    await expect(widget).toContainText("$19.99");
    await expect(widget).toContainText("DISCONTINUED");
    // The catalogue holds no stock: no units on this page either.
    await expect(page.getByRole("table")).not.toContainText("731");
    await expect(page.getByRole("link", { name: "Catalog" })).toHaveAttribute("aria-current", "page");

    // A CUSTOMER is told that Add Product is an ADMIN's.
    await page.getByText("Add Product", { exact: true }).click({ force: true });
    await expect(page.getByText("This action requires the ADMIN role.").first()).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(0);
  });
});
