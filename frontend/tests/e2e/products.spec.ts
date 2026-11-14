/**
 * Products page: the storefront.
 *
 * Surface tests: mocked backend — always run. The storefront shows each
 * product's name, category, price and availability badge, and an Order action;
 * no number of units (the public product view carries none), and none of the
 * catalogue's or the inventory's actions, for any role.
 * Backend-dependent block: skipped when Spring Boot at localhost:8080 is not
 * reachable; when backend IS running it asserts seed products (Widget, Gadget,
 * Doohickey) are visible in the table.
 */

import { test, expect, type Page } from "@playwright/test";
import {
  actAs,
  answerCommands,
  mockBackendRoutes,
  isBackendReachable,
  collectConsoleErrors,
  BACKEND_URL,
} from "./helpers";

function product(productId: string, name: string, availability: string, status = "AVAILABLE") {
  return {
    productId,
    name,
    description: `The ${name}`,
    category: "Electronics",
    price: { amount: 19.99, currency: "USD" },
    status,
    availability,
    createdAt: "2026-10-04T00:00:00Z",
    updatedAt: "2026-10-04T00:00:00Z",
  };
}

/** Four products, one per availability. */
const STOREFRONT = [
  product("prod-widget", "Widget", "IN_STOCK"),
  product("prod-gadget", "Gadget", "LOW_STOCK"),
  product("prod-doohickey", "Doohickey", "OUT_OF_STOCK"),
  product("prod-gizmo", "Gizmo", "DISCONTINUED", "DISCONTINUED"),
];

const ALICE = {
  customerId: "cust-alice",
  name: "Alice Johnson",
  email: "alice@example.com",
  address: "123 Main St",
  phone: "+1-555-0101",
  status: "ACTIVE",
  createdAt: "2026-10-04T00:00:00Z",
  updatedAt: "2026-10-04T00:00:00Z",
};

async function mockStorefront(page: Page): Promise<void> {
  await mockBackendRoutes(page);
  await page.route(`${BACKEND_URL}/api/products`, (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(STOREFRONT) })
      : route.fallback()
  );
  await page.route(`${BACKEND_URL}/api/customers`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([ALICE]) })
  );
}

function row(page: Page, name: string) {
  return page.getByRole("row").filter({ hasText: name });
}

test.describe("Products page — surface (no backend required)", () => {
  test("renders page heading with mocked data", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/products");

    await expect(page.getByRole("heading", { name: "Products" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });
});

test.describe("Products page — the storefront", () => {
  test("each product shows its availability as a badge, never its number of units", async ({
    page,
  }) => {
    await mockStorefront(page);
    await actAs(page, "ADMIN");

    await page.goto("/products");

    await expect(row(page, "Widget")).toContainText("In stock");
    await expect(row(page, "Widget")).toContainText("Electronics");
    await expect(row(page, "Widget")).toContainText("$19.99");
    await expect(row(page, "Gadget")).toContainText("Low stock");
    await expect(row(page, "Doohickey")).toContainText("Out of stock");
    await expect(row(page, "Gizmo")).toContainText("Discontinued");
    // The badge is the stock column, for every role: no column of units, not even for an ADMIN.
    await expect(row(page, "Gadget").getByRole("cell").nth(3)).toHaveText("Low stock");
    await expect(page.getByRole("columnheader", { name: /Stock/ })).toHaveCount(0);
  });

  for (const role of ["ADMIN", "CUSTOMER", "GUEST"] as const) {
    test(`the storefront offers no catalogue or inventory action to ${role}`, async ({ page }) => {
      await mockStorefront(page);
      await actAs(page, role);

      await page.goto("/products");
      await expect(row(page, "Widget")).toBeVisible();

      await expect(page.getByRole("button", { name: "Actions" })).toHaveCount(0);
      for (const action of ["Add Product", "Update Price", "Discontinue", "Receive Shipment", "Adjust"]) {
        await expect(page.getByText(action, { exact: true })).toHaveCount(0);
      }
      // The one action is Order.
      await expect(row(page, "Widget").getByRole("button", { name: "Order" })).toHaveCount(1);
    });
  }

  test("Order is offered for a product in stock or low in stock, not for one out of stock or discontinued", async ({
    page,
  }) => {
    await mockStorefront(page);
    await actAs(page, "CUSTOMER");

    await page.goto("/products");

    await expect(row(page, "Widget").getByRole("button", { name: "Order" })).toBeEnabled();
    await expect(row(page, "Gadget").getByRole("button", { name: "Order" })).toBeEnabled();
    await expect(row(page, "Doohickey").getByRole("button", { name: "Order" })).toBeDisabled();
    await expect(row(page, "Gizmo").getByRole("button", { name: "Order" })).toBeDisabled();
  });

  test("Order opens Place Order on the row's product and sends the order for the customer", async ({
    page,
  }) => {
    await mockStorefront(page);
    const sent = await answerCommands(page, "/api/orders");
    await actAs(page, "CUSTOMER");

    await page.goto("/products");
    await row(page, "Gadget").getByRole("button", { name: "Order" }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByRole("combobox").filter({ hasText: "Gadget" })).toBeVisible();
    await dialog.getByRole("spinbutton").fill("2");
    await dialog.getByRole("button", { name: "Place Order" }).click();

    await expect(page.getByText(/Order placed: order-[0-9a-f]{8}/)).toBeVisible();
    expect(sent.map((c) => [c.method, c.path, c.userId])).toEqual([
      ["POST", "/api/orders", "cust-alice"],
    ]);
    expect(sent[0].body?.customerId).toBe("cust-alice");
    expect(sent[0].body?.lines).toEqual([{ productId: "prod-gadget", quantity: 2 }]);
  });

  test("a GUEST is told that ordering requires a customer", async ({ page }) => {
    await mockStorefront(page);
    const sent = await answerCommands(page, "/api/orders");
    await actAs(page, "GUEST");

    await page.goto("/products");
    await row(page, "Widget").getByText("Order", { exact: true }).click({ force: true });

    await expect(page.getByText("This action requires the CUSTOMER role.")).toBeVisible();
    expect(sent).toHaveLength(0);
  });
});

test.describe("Products page — backend-dependent", () => {
  let backendAvailable = false;

  test.beforeAll(async () => {
    backendAvailable = await isBackendReachable();
    if (!backendAvailable) {
      console.log(
        "[products.spec] Backend not reachable at localhost:8080 — skipping backend-dependent tests"
      );
    }
  });

  test("seed products Widget, Gadget, Doohickey are visible", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }

    await page.goto("/products");

    await expect(page.getByRole("heading", { name: "Products" })).toBeVisible();

    // Wait for data to load — use exact:true to avoid strict-mode violations
    // (product description also contains the product name as a substring)
    await expect(page.getByText("Widget", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Gadget", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Doohickey", { exact: true }).first()).toBeVisible();
  });
});
