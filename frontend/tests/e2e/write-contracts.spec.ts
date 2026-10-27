/**
 * The write actions of the pages, request by request.
 *
 * Every command the frontend sends has a contract on the backend: a method, a
 * path, the two identity headers and a JSON body whose field names are those
 * of the controller's request record. A body with another field name is not
 * refused by every app: an unknown field is ignored, so "delta" in place of
 * "quantity" adjusts the stock by zero and answers 200. These tests drive
 * each action through the page and compare the request it sends, field by
 * field, with what the three apps' controllers read.
 *
 * Mocked backend, which records the command and answers as the backend does
 * for one it accepted (200, empty body) — always runs. `writes.live.spec.ts`
 * sends the same actions to a running backend.
 */

import { test, expect, type Page, type Route } from "@playwright/test";
import { actAs, answerCommands, mockBackendRoutes, BACKEND_URL } from "./helpers";

const WIDGET = {
  productId: "prod-widget",
  name: "Widget",
  description: "A versatile widget for all occasions",
  category: "Electronics",
  price: { amount: 19.99, currency: "USD" },
  stock: 100,
  status: "AVAILABLE",
  createdAt: "2026-10-04T00:00:00Z",
  updatedAt: "2026-10-04T00:00:00Z",
};

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

function order(orderId: string, status: string) {
  return {
    orderId,
    customerId: "cust-alice",
    lines: [
      { productId: "prod-widget", quantity: 1, unitPrice: { amount: 19.99, currency: "USD" } },
    ],
    total: { amount: 19.99, currency: "USD" },
    status,
    paymentStatus: status === "CREATED" ? "PENDING" : "CAPTURED",
    createdAt: "2026-10-04T00:00:00Z",
    updatedAt: "2026-10-04T00:00:00Z",
  };
}

async function mockList(page: Page, path: string, rows: unknown[]): Promise<void> {
  await page.route(`${BACKEND_URL}${path}`, (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(rows) })
      : route.fallback()
  );
}

/** Opens the row menu of the table's only row and picks an item. */
async function pickRowAction(page: Page, item: string): Promise<void> {
  await page.getByRole("button", { name: "Actions" }).click();
  await page.getByRole("menuitem", { name: item }).click();
}

test.describe("Orders — the commands the page sends", () => {
  test("Place Order sends an order id, the product picked and its quantity, and no price", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await mockList(page, "/api/customers", [ALICE]);
    const sent = await answerCommands(page, "/api/orders");
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Place Order" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("combobox").filter({ hasText: "Select product" }).click();
    await page.getByRole("option", { name: /Widget/ }).click();
    await dialog.getByRole("spinbutton").fill("2");
    await dialog.getByRole("button", { name: "Place Order" }).click();

    await expect(page.getByText(/Order placed: order-[0-9a-f]{8}/)).toBeVisible();
    expect(sent).toHaveLength(1);
    expect(sent[0].method).toBe("POST");
    expect(sent[0].path).toBe("/api/orders");
    expect(sent[0].role).toBe("CUSTOMER");
    expect(sent[0].userId).toBe("cust-alice");
    expect(sent[0].contentType).toContain("application/json");
    // PlaceOrderRequest(orderId, customerId, lines[OrderLineRequest(productId, quantity)]): the
    // catalogue sets the price.
    expect(Object.keys(sent[0].body ?? {}).sort()).toEqual(["customerId", "lines", "orderId"]);
    expect(sent[0].body?.orderId).toMatch(/^order-[0-9a-f]{8}$/);
    expect(sent[0].body?.customerId).toBe("cust-alice");
    expect(sent[0].body?.lines).toEqual([{ productId: "prod-widget", quantity: 2 }]);
  });

  test("Place Order sends products and quantities, merged per product", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await mockList(page, "/api/customers", [ALICE]);
    const sent = await answerCommands(page, "/api/orders");
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Place Order" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("combobox").filter({ hasText: "Select product" }).click();
    await page.getByRole("option", { name: /Widget/ }).click();
    await dialog.getByRole("button", { name: "Add Line" }).click();
    await dialog.getByRole("combobox").filter({ hasText: "Select product" }).click();
    await page.getByRole("option", { name: /Widget/ }).click();
    await dialog.getByRole("spinbutton").nth(1).fill("2");
    await dialog.getByRole("button", { name: "Place Order" }).click();

    await expect(page.getByText(/Order placed: order-[0-9a-f]{8}/)).toBeVisible();
    expect(sent).toHaveLength(1);
    // The backend takes one line per product: two lines of one product become one line, the
    // quantities summed.
    expect(sent[0].body?.lines).toEqual([{ productId: "prod-widget", quantity: 3 }]);
    for (const line of sent[0].body?.lines as Record<string, unknown>[]) {
      expect(Object.keys(line).sort()).toEqual(["productId", "quantity"]);
    }
  });

  test("Cancel sends a body with the reason, for a confirmed order too", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [order("order-a1", "CONFIRMED")]);
    const sent = await answerCommands(page, "/api/orders/*/cancel");
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Cancel" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Reason (optional)").fill("changed my mind");
    await dialog.getByRole("button", { name: "Cancel Order" }).click();

    await expect(page.getByText("Order cancelled")).toBeVisible();
    expect(sent).toHaveLength(1);
    expect(sent[0].method).toBe("POST");
    expect(sent[0].path).toBe("/api/orders/order-a1/cancel");
    expect(sent[0].userId).toBe("cust-alice");
    // CancelOrderRequest(reason): the body is required
    expect(sent[0].body).toEqual({ reason: "changed my mind" });
  });

  test("Cancel without a typed reason still sends one", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [order("order-a1", "CREATED")]);
    const sent = await answerCommands(page, "/api/orders/*/cancel");
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Cancel" }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Cancel Order" }).click();

    await expect(page.getByText("Order cancelled")).toBeVisible();
    expect(Object.keys(sent[0].body ?? {})).toEqual(["reason"]);
    expect(sent[0].body?.reason).toBeTruthy();
  });

  test("Cancel says that the order's stock is released and its payment voided or refunded", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [
      order("order-a0", "CREATED"),
      order("order-a1", "CONFIRMED"),
    ]);
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    const row = (orderId: string) => page.getByRole("row").filter({ hasText: orderId });
    const dialog = page.getByRole("dialog");
    const note = dialog.getByRole("note");

    // Every cancellation is undone, whether the saga has confirmed the order or not.
    for (const orderId of ["order-a1", "order-a0"]) {
      await row(orderId).getByRole("button", { name: "Cancel" }).click();
      await expect(dialog.getByText("Are you sure you want to cancel order")).toBeVisible();
      await expect(note).toHaveText(
        "Cancelling releases the order's stock and voids or refunds its payment in full."
      );
      await dialog.getByRole("button", { name: "Close" }).first().click();
      await expect(dialog).toHaveCount(0);
    }
  });

  test("an id is sent as one path segment, whatever characters it holds", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [order("order a/1?x", "CONFIRMED")]);
    const paths: string[] = [];
    await page.route(`${BACKEND_URL}/api/orders/**`, (route) => {
      paths.push(new URL(route.request().url()).pathname);
      return route.fulfill({ status: 200, body: "" });
    });
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Cancel" }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Cancel Order" }).click();

    await expect(page.getByText("Order cancelled")).toBeVisible();
    expect(paths).toEqual(["/api/orders/order%20a%2F1%3Fx/cancel"]);
  });

  test("Ship and Deliver send no body and are an ADMIN's; a CREATED order offers no Confirm", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [
      order("order-a0", "CREATED"),
      order("order-a1", "CONFIRMED"),
      order("order-a2", "SHIPPED"),
    ]);
    const shipped = await answerCommands(page, "/api/orders/*/ship");
    const delivered = await answerCommands(page, "/api/orders/*/deliver");
    await actAs(page, "ADMIN");

    await page.goto("/orders");
    // The fulfillment saga confirms an order; the page offers no way to confirm one by hand.
    await expect(page.getByRole("row").filter({ hasText: "order-a0" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Confirm" })).toHaveCount(0);
    await page.getByRole("button", { name: "Ship" }).click();
    await expect(page.getByText("Order shipped")).toBeVisible();
    await page.getByRole("button", { name: "Deliver" }).click();
    await expect(page.getByText("Order delivered")).toBeVisible();

    expect(shipped.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/orders/order-a1/ship", "admin-1", null],
    ]);
    expect(delivered.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/orders/order-a2/deliver", "admin-1", null],
    ]);
  });
});

test.describe("A command's result reaches the page", () => {
  test("the list is read again when the read model has settled, and shows the new order", async ({
    page,
  }) => {
    test.setTimeout(60_000);
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await mockList(page, "/api/customers", [ALICE]);
    // The backend answers the command at once; the order read model has the row some seconds
    // later, as a projection that polls the event store does.
    let placedAt = 0;
    let placedId = "";
    let listReads = 0;
    await page.route(`${BACKEND_URL}/api/orders`, (route) => {
      const req = route.request();
      if (req.method() === "POST") {
        placedAt = Date.now();
        placedId = (req.postDataJSON() as { orderId: string }).orderId;
        return route.fulfill({ status: 200, body: "" });
      }
      listReads += 1;
      const settled = placedAt > 0 && Date.now() - placedAt > 4_000;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(settled ? [order(placedId, "CONFIRMED")] : []),
      });
    });
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await expect(page.getByText("No orders yet. Place one!")).toBeVisible();
    await page.getByRole("button", { name: "Place Order" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("combobox").filter({ hasText: "Select product" }).click();
    await page.getByRole("option", { name: /Widget/ }).click();
    await dialog.getByRole("button", { name: "Place Order" }).click();
    await expect(page.getByText(/Order placed: order-[0-9a-f]{8}/)).toBeVisible();

    // Read at once, the list is the one from before the command.
    await expect(page.getByText("No orders yet. Place one!")).toBeVisible();
    const readsRightAfter = listReads;

    // No reload and no further event: the page reads the list again by itself.
    await expect(page.getByRole("row").filter({ hasText: placedId })).toBeVisible({
      timeout: 15_000,
    });
    await expect(page.getByRole("row").filter({ hasText: placedId })).toContainText("CONFIRMED");
    expect(listReads).toBeGreaterThan(readsRightAfter);
  });
});

test.describe("Orders — what each role is offered", () => {
  test("a CUSTOMER may cancel their order, and is told that Ship is an ADMIN's", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [order("order-a1", "CONFIRMED")]);
    const shipped = await answerCommands(page, "/api/orders/*/ship");
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await expect(page.getByRole("button", { name: "Cancel" })).toBeEnabled();
    // The gate around Ship takes the click and says which role the action requires.
    await page.getByText("Ship", { exact: true }).click({ force: true });

    await expect(page.getByText("This action requires the ADMIN role.")).toBeVisible();
    expect(shipped).toHaveLength(0);
  });
});

test.describe("Customers — the commands the page sends", () => {
  test("Register Customer sends a customer id", async ({ page }) => {
    await mockBackendRoutes(page);
    const sent = await answerCommands(page, "/api/customers");
    await actAs(page, "ADMIN");

    await page.goto("/customers");
    await page.getByRole("button", { name: "Register Customer" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Name").fill("Carol Example");
    await dialog.getByLabel("Email").fill("carol@example.com");
    await dialog.getByLabel("Address").fill("7 Elm St");
    await dialog.getByLabel("Phone").fill("+1-555-0103");
    await dialog.getByRole("button", { name: "Register" }).click();

    await expect(page.getByText(/Customer registered: cust-[0-9a-f]{8}/)).toBeVisible();
    expect(sent).toHaveLength(1);
    expect(sent[0].method).toBe("POST");
    expect(sent[0].path).toBe("/api/customers");
    expect(sent[0].role).toBe("ADMIN");
    expect(sent[0].userId).toBe("admin-1");
    // RegisterRequest(customerId, name, email, address, phone)
    expect(Object.keys(sent[0].body ?? {}).sort()).toEqual([
      "address",
      "customerId",
      "email",
      "name",
      "phone",
    ]);
    expect(sent[0].body?.customerId).toMatch(/^cust-[0-9a-f]{8}$/);
    expect(sent[0].body).toMatchObject({
      name: "Carol Example",
      email: "carol@example.com",
      address: "7 Elm St",
      phone: "+1-555-0103",
    });
  });

  test("Edit Profile, Export Data and Forget send what the customer routes read", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/customers", [ALICE]);
    const edited = await answerCommands(page, "/api/customers/cust-alice");
    const exported = await answerCommands(page, "/api/customers/cust-alice/export-data");
    const forgotten = await answerCommands(page, "/api/customers/cust-alice/forget", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ keyDeleted: true, fullyErased: true, failedPurgers: [] }),
      })
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/customers");
    await pickRowAction(page, "Edit Profile");
    await page.getByRole("dialog").getByLabel("Name").fill("Alice Smith");
    await page.getByRole("dialog").getByRole("button", { name: "Save" }).click();
    await expect(page.getByText("Profile updated")).toBeVisible();

    await pickRowAction(page, "Export Data");
    await expect(page.getByText("Data export requested")).toBeVisible();

    await pickRowAction(page, "Forget (GDPR)");
    await page.getByRole("dialog").getByRole("button", { name: "Forget Customer" }).click();
    await expect(page.getByText("Customer forgotten (GDPR)")).toBeVisible();

    // UpdateProfileRequest(name, email, address, phone): a field left out keeps its value
    expect(edited.map((c) => [c.method, c.path, c.userId])).toEqual([
      ["PUT", "/api/customers/cust-alice", "cust-alice"],
    ]);
    expect(edited[0].body).toEqual({
      name: "Alice Smith",
      address: "123 Main St",
      phone: "+1-555-0101",
    });
    expect(exported.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/customers/cust-alice/export-data", "cust-alice", null],
    ]);
    expect(forgotten.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/customers/cust-alice/forget", "cust-alice", null],
    ]);
  });
});

test.describe("Products — the commands the page sends", () => {
  test("Add Product sends a product id and the price as a plain amount", async ({ page }) => {
    await mockBackendRoutes(page);
    const sent = await answerCommands(page, "/api/products");
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await page.getByRole("button", { name: "Add Product" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Name").fill("Sprocket");
    await dialog.getByLabel("Description").fill("A sprocket");
    await dialog.getByLabel("Category").fill("Hardware");
    await dialog.getByLabel("Price (USD)").fill("12.50");
    await dialog.getByLabel("Initial Stock").fill("7");
    await dialog.getByRole("button", { name: "Create" }).click();

    await expect(page.getByText(/Product created: prod-[0-9a-f]{8}/)).toBeVisible();
    expect(sent).toHaveLength(1);
    expect(sent[0].method).toBe("POST");
    expect(sent[0].path).toBe("/api/products");
    expect(sent[0].role).toBe("ADMIN");
    expect(sent[0].userId).toBe("admin-1");
    // CreateProductRequest(productId, name, description, category, price, initialStock)
    expect(Object.keys(sent[0].body ?? {}).sort()).toEqual([
      "category",
      "description",
      "initialStock",
      "name",
      "price",
      "productId",
    ]);
    expect(sent[0].body?.productId).toMatch(/^prod-[0-9a-f]{8}$/);
    expect(sent[0].body).toMatchObject({
      name: "Sprocket",
      description: "A sprocket",
      category: "Hardware",
      price: 12.5,
      initialStock: 7,
    });
  });

  test("Update Price, Adjust Stock, Receive Shipment and Discontinue send the backend's field names", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    const priced = await answerCommands(page, "/api/products/prod-widget/price");
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock");
    const received = await answerCommands(page, "/api/inventory/prod-widget/receive");
    const discontinued = await answerCommands(page, "/api/products/prod-widget/discontinue");
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Update Price");
    await page.getByRole("dialog").getByLabel("New Price (USD)").fill("24.50");
    await page.getByRole("dialog").getByRole("button", { name: "Update" }).click();
    await expect(page.getByText("Price updated")).toBeVisible();

    await pickRowAction(page, "Adjust Stock");
    await page.getByRole("dialog").getByLabel(/Delta/).fill("-3");
    await page.getByRole("dialog").getByLabel("Reason (optional)").fill("damaged");
    await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();

    await pickRowAction(page, "Receive Shipment");
    await page.getByRole("dialog").getByLabel("Quantity").fill("5");
    await page.getByRole("dialog").getByRole("button", { name: "Receive" }).click();
    await expect(page.getByText("Shipment received into the inventory")).toBeVisible();

    await pickRowAction(page, "Discontinue");
    await expect(page.getByText("Product discontinued")).toBeVisible();

    // UpdatePriceRequest(price)
    expect(priced.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["PUT", "/api/products/prod-widget/price", "admin-1", { price: 24.5 }],
    ]);
    // AdjustStockRequest(quantity, reason)
    expect(adjusted.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["PUT", "/api/products/prod-widget/stock", "admin-1", { quantity: -3, reason: "damaged" }],
    ]);
    // ReceiveShipmentRequest(quantity)
    expect(received.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/inventory/prod-widget/receive", "admin-1", { quantity: 5 }],
    ]);
    expect(discontinued.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/products/prod-widget/discontinue", "admin-1", null],
    ]);
  });

  test("Adjust Stock and Receive Shipment send a new Idempotency-Key for each applied change, and the page's retry of a lost answer sends the same one", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    // The first adjustment's first request gets no answer (the connection fails); the page sends
    // it again. Every later request is answered 200.
    let lost = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      lost++ === 0 ? route.abort("failed") : route.fulfill({ status: 200, body: "" })
    );
    const received = await answerCommands(page, "/api/inventory/prod-widget/receive");
    await actAs(page, "ADMIN");

    await page.goto("/products");
    for (const delta of ["5", "2"]) {
      await pickRowAction(page, "Adjust Stock");
      await page.getByRole("dialog").getByLabel(/Delta/).fill(delta);
      await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();
      await expect(page.getByText("Stock adjusted").first()).toBeVisible();
      await expect(page.getByRole("dialog")).toBeHidden();
    }
    await pickRowAction(page, "Receive Shipment");
    await page.getByRole("dialog").getByLabel("Quantity").fill("5");
    await page.getByRole("dialog").getByRole("button", { name: "Receive" }).click();
    await expect(page.getByText("Shipment received into the inventory")).toBeVisible();

    const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
    const keys = adjusted.map((c) => c.idempotencyKey);
    expect(adjusted.map((c) => c.body)).toEqual([
      { quantity: 5 },
      { quantity: 5 },
      { quantity: 2 },
    ]);
    keys.forEach((key) => expect(key).toMatch(uuid));
    expect(keys[1], "the retry of the lost request carries its key").toBe(keys[0]);
    expect(keys[2], "a new submit is a new adjustment").not.toBe(keys[0]);
    expect(received).toHaveLength(1);
    expect(received[0].idempotencyKey).toMatch(uuid);
    expect(received[0].idempotencyKey).not.toBe(keys[0]);
  });

  test("after the page gives up, a resubmit of the unchanged Adjust Stock dialog sends the same Idempotency-Key, an edited one a new key", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    // The first submit and the page's two retries of it get no answer; then the backend answers.
    let lost = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      lost++ < 3 ? route.abort("failed") : route.fulfill({ status: 200, body: "" })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Adjust Stock");
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText(/Failed to fetch|API error/).first()).toBeVisible({ timeout: 15_000 });
    expect(adjusted).toHaveLength(3);

    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();

    await pickRowAction(page, "Adjust Stock");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByLabel(/Delta/).fill("6");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted").first()).toBeVisible();

    const keys = adjusted.map((c) => c.idempotencyKey);
    expect(keys).toHaveLength(5);
    expect(new Set(keys.slice(0, 4)).size, "one adjustment, one key").toBe(1);
    expect(keys[4], "an edited dialog is another adjustment").not.toBe(keys[0]);
    expect(adjusted.map((c) => c.body)).toEqual([
      { quantity: 5 },
      { quantity: 5 },
      { quantity: 5 },
      { quantity: 5 },
      { quantity: 6 },
    ]);
  });
});

/** The answer of a keyed command the backend recorded for a replay under its key. */
const QUEUED_FOR_REPLAY = {
  status: 503,
  headers: { "Retry-After": "60" },
  contentType: "application/json",
  body: JSON.stringify({
    reason: "QUEUED_FOR_REPLAY",
    commandId: "cmd-queued-1",
    message:
      "The command is not applied yet: it is queued for a replay under its Idempotency-Key." +
      " Wait for the replay, or send it again with the same key; a new key is a new operation.",
  }),
};

/**
 * A refusal of the command bus (nothing written) with a `Retry-After` of `seconds`, which the
 * backend exposes to the page's origin.
 */
function refused(reason: string, seconds: number) {
  return {
    status: 503,
    headers: { "Retry-After": String(seconds), "Access-Control-Expose-Headers": "Retry-After" },
    contentType: "application/json",
    body: JSON.stringify({ reason, message: "Refused for now; nothing was written." }),
  };
}

test.describe("Products — one Idempotency-Key per intended change", () => {
  test("Adjust Stock closed after a lost answer and opened again sends the same amount under the same key, also after a field edit", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    // The first submit and the page's two retries of it get no answer; then the backend answers.
    let lost = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      lost++ < 3 ? route.abort("failed") : route.fulfill({ status: 200, body: "" })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Adjust Stock");
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText(/Failed to fetch|API error/).first()).toBeVisible({ timeout: 15_000 });
    expect(adjusted).toHaveLength(3);

    // The admin closes the dialog and opens it again: it opens on the change without an answer.
    await page.keyboard.press("Escape");
    await expect(dialog).toBeHidden();
    await pickRowAction(page, "Adjust Stock");
    await expect(dialog.getByRole("status")).toContainText(
      "The adjustment of +5 has no answer yet: it may have been applied."
    );
    await expect(dialog.getByLabel(/Delta/)).toHaveValue("5");
    await dialog.getByLabel(/Delta/).fill("6");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByLabel(/Reason/).fill("recount");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();

    // Applied: the next adjustment is a new one, of the same amount or not.
    await pickRowAction(page, "Adjust Stock");
    await expect(dialog.getByRole("status")).toHaveCount(0);
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect.poll(() => adjusted.length).toBe(5);

    const keys = adjusted.map((c) => c.idempotencyKey);
    expect(new Set(keys.slice(0, 4)).size, "one intended adjustment, one key").toBe(1);
    expect(keys[4], "an adjustment after the first was applied is a new one").not.toBe(keys[0]);
    expect(adjusted.map((c) => c.body)).toEqual([
      { quantity: 5 },
      { quantity: 5 },
      { quantity: 5 },
      { quantity: 5, reason: "recount" },
      { quantity: 5 },
    ]);
  });

  test("a 503 QUEUED_FOR_REPLAY is sent once and says where its outcome shows, and the dialog opened again sends it under the same key", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    // The first request of each change is queued for a replay; the next one is applied.
    let stockAnswers = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      stockAnswers++ < 1 ? route.fulfill(QUEUED_FOR_REPLAY) : route.fulfill({ status: 200, body: "" })
    );
    let receiveAnswers = 0;
    const received = await answerCommands(page, "/api/inventory/prod-widget/receive", (route) =>
      receiveAnswers++ < 1
        ? route.fulfill(QUEUED_FOR_REPLAY)
        : route.fulfill({ status: 200, body: "" })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    const dialog = page.getByRole("dialog");
    await pickRowAction(page, "Adjust Stock");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(
      page
        .getByText(
          "Queued for replay; the outcome shows in the admin dead-letter list (503): The command is not applied yet"
        )
        .first()
    ).toBeVisible();
    await expect(dialog).toBeHidden();
    // The page does not send a queued change again on its own: the backend holds it.
    await page.waitForTimeout(3_000);
    expect(adjusted).toHaveLength(1);

    await pickRowAction(page, "Adjust Stock");
    await expect(dialog.getByRole("status")).toContainText(
      "The adjustment of +5 is queued for a replay; its outcome shows in the admin dead-letter list."
    );
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();
    expect(new Set(adjusted.map((c) => c.idempotencyKey)).size, "one key").toBe(1);
    expect(adjusted).toHaveLength(2);

    await pickRowAction(page, "Receive Shipment");
    await dialog.getByLabel("Quantity").fill("7");
    await dialog.getByRole("button", { name: "Receive" }).click();
    await expect(dialog).toBeHidden();
    await pickRowAction(page, "Receive Shipment");
    await expect(dialog.getByRole("status")).toContainText(
      "The shipment of +7 is queued for a replay; its outcome shows in the admin dead-letter list."
    );
    await expect(dialog.getByLabel("Quantity")).toHaveValue("7");
    await dialog.getByRole("button", { name: "Receive" }).click();
    await expect(page.getByText("Shipment received into the inventory")).toBeVisible();
    expect(received.map((c) => c.body)).toEqual([{ quantity: 7 }, { quantity: 7 }]);
    expect(new Set(received.map((c) => c.idempotencyKey)).size, "one key").toBe(1);
    expect(received[0].idempotencyKey).not.toBe(adjusted[0].idempotencyKey);
  });

  test("Start a new change makes the same amount a new adjustment with a new key", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    let answers = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      answers++ < 1 ? route.fulfill(QUEUED_FOR_REPLAY) : route.fulfill({ status: 200, body: "" })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    const dialog = page.getByRole("dialog");
    await pickRowAction(page, "Adjust Stock");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(dialog).toBeHidden();

    // The admin means a second delivery of five, not the queued one: a new adjustment.
    await pickRowAction(page, "Adjust Stock");
    await dialog.getByRole("button", { name: "Start a new change" }).click();
    await expect(dialog.getByRole("status")).toHaveCount(0);
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();

    const keys = adjusted.map((c) => c.idempotencyKey);
    expect(keys).toHaveLength(2);
    expect(keys[1], "started as a new adjustment").not.toBe(keys[0]);
  });

  test("a change held by the page survives a reload of the page", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    let answers = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      answers++ < 1 ? route.fulfill(QUEUED_FOR_REPLAY) : route.fulfill({ status: 200, body: "" })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    const dialog = page.getByRole("dialog");
    await pickRowAction(page, "Adjust Stock");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(dialog).toBeHidden();

    // While the backend's replay is due, the admin reloads the page: the change is still held.
    await page.reload();
    await pickRowAction(page, "Adjust Stock");
    await expect(dialog.getByRole("status")).toContainText(
      "The adjustment of +5 is queued for a replay"
    );
    await expect(dialog.getByLabel(/Delta/)).toHaveValue("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();

    expect(adjusted).toHaveLength(2);
    expect(adjusted[1].idempotencyKey, "the key survives the reload").toBe(
      adjusted[0].idempotencyKey
    );
  });

  test("a 503 refusal is sent again once, after its Retry-After", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    const sentAt: number[] = [];
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) => {
      sentAt.push(Date.now());
      return sentAt.length === 1
        ? route.fulfill(refused("CIRCUIT_BREAKER_OPEN", 3))
        : route.fulfill({ status: 200, body: "" });
    });
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Adjust Stock");
    await page.getByRole("dialog").getByLabel(/Delta/).fill("5");
    await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible({ timeout: 15_000 });

    expect(adjusted).toHaveLength(2);
    expect(adjusted[1].idempotencyKey).toBe(adjusted[0].idempotencyKey);
    expect(sentAt[1] - sentAt[0], "the retry waits for Retry-After: 3").toBeGreaterThanOrEqual(2_900);
  });

  test("a change refused twice is not sent a third time, says it was not applied, and goes out again under its key", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    let answers = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      answers++ < 2
        ? route.fulfill(refused("AGGREGATE_LOCK_UNAVAILABLE", 1))
        : route.fulfill({ status: 200, body: "" })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    const dialog = page.getByRole("dialog");
    await pickRowAction(page, "Adjust Stock");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText(/The backend is busy, try again in a few seconds \(503\)/)).toBeVisible({
      timeout: 15_000,
    });
    await page.waitForTimeout(3_000);
    expect(adjusted, "the first attempt and one retry").toHaveLength(2);

    await expect(dialog.getByRole("status")).toContainText(
      "The adjustment of +5 was not applied: the backend refused it for now and wrote nothing."
    );
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();
    expect(new Set(adjusted.map((c) => c.idempotencyKey)).size, "one key").toBe(1);
  });

  test("a refusal that follows an attempt without an answer still says the change may have been applied", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    let answers = 0;
    const adjusted = await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      answers++ === 0 ? route.abort("failed") : route.fulfill(refused("CIRCUIT_BREAKER_OPEN", 1))
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    const dialog = page.getByRole("dialog");
    await pickRowAction(page, "Adjust Stock");
    await dialog.getByLabel(/Delta/).fill("5");
    await dialog.getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText(/The backend is busy/)).toBeVisible({ timeout: 15_000 });

    await expect(dialog.getByRole("status")).toContainText(
      "The adjustment of +5 has no answer yet: it may have been applied."
    );
    expect(adjusted.length).toBeGreaterThanOrEqual(2);
  });
});

test.describe("Admin and Fulfillment — what the page reads and sends", () => {
  test("the payment-failure switch reads failureInjected and follows the toggle's answer", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    let failureInjected = true;
    await page.route(`${BACKEND_URL}/api/admin/payment-failure`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ failureInjected }),
      })
    );
    const toggled = await answerCommands(page, "/api/admin/payment-failure/toggle", (route) => {
      failureInjected = !failureInjected;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ failureInjected }),
      });
    });
    await actAs(page, "ADMIN");

    await page.goto("/fulfillment");
    const toggle = page.getByRole("switch", { name: "Inject Payment Failure" });
    await expect(toggle).toBeChecked();
    await expect(page.getByText("Payment failure injection is active.")).toBeVisible();

    await toggle.click();

    await expect(page.getByText("Payment failure disabled")).toBeVisible();
    await expect(toggle).not.toBeChecked();
    await expect(page.getByText("Payment failure injection is active.")).toHaveCount(0);
    expect(toggled.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/admin/payment-failure/toggle", "admin-1", null],
    ]);
  });

  test("the admin page reads the health, the dead letters and the outbox in the backend's shapes", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    const healthRequests: string[] = [];
    page.on("request", (r) => {
      const path = new URL(r.url()).pathname;
      if (path.endsWith("/health")) healthRequests.push(path);
    });
    await mockList(page, "/api/admin/dead-letters", [
      {
        commandId: "cmd_0149t5g0p6KunF1kBsysAVmeg",
        commandType: "org.streamrune.ecommerce.domain.customer.CustomerCommand$UpdateProfile",
        commandPayload: "{}",
        streamId: "customer:cust-alice",
        errorType: "java.sql.SQLTransientConnectionException",
        errorMessage: "Connection is not available",
        attempts: 3,
        firstAttemptAt: "2026-10-04T00:00:00Z",
        publishedAt: "2026-10-04T00:00:01Z",
        dlqAttempts: 1,
        lastAttemptAt: "2026-10-04T00:01:00Z",
      },
    ]);
    await mockList(page, "/api/admin/outbox", [
      {
        id: "out_01",
        payload: "{}",
        payloadType: "OrderConfirmed",
        streamId: "order:order-a1",
        status: "PENDING",
        attempts: 0,
        lastError: null,
        createdAt: "2026-10-04T00:00:00Z",
        processedAt: null,
      },
    ]);
    const retried = await answerCommands(page, "/api/admin/dead-letters/*/retry", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ outcome: "REPLAYED" }),
      })
    );
    await actAs(page, "ADMIN");

    await page.goto("/admin");

    // The health card asks the backend's own health route, below /api.
    await expect(page.getByText("UP", { exact: true })).toBeVisible();
    expect(healthRequests).toContain("/api/admin/health");
    expect(healthRequests).not.toContain("/actuator/health");

    await expect(page.getByText("UpdateProfile", { exact: true })).toBeVisible();
    await expect(page.getByText("Connection is not available")).toBeVisible();
    await expect(page.getByText("OrderConfirmed", { exact: true })).toBeVisible();
    await expect(page.getByText("out_01")).toBeVisible();

    await page.getByRole("button", { name: "Retry" }).click();
    await expect(page.getByText("Dead letter replayed")).toBeVisible();
    expect(retried.map((c) => [c.method, c.path, c.userId, c.body])).toEqual([
      ["POST", "/api/admin/dead-letters/cmd_0149t5g0p6KunF1kBsysAVmeg/retry", "admin-1", null],
    ]);
  });
});

test.describe("A refused command is shown", () => {
  test("a 500 AWAITING_OPERATOR says that an operator decides", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await answerCommands(page, "/api/products/prod-widget/price", (route) =>
      route.fulfill({
        status: 500,
        contentType: "application/json",
        body: JSON.stringify({
          reason: "AWAITING_OPERATOR",
          commandId: "cmd-awaiting-1",
          message:
            "The command failed and is recorded for an operator, who replays or discards it." +
            " Sending it again would apply it a second time if the operator replays it.",
        }),
      })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Update Price");
    await page.getByRole("dialog").getByLabel(/New Price/).fill("21.50");
    await page.getByRole("dialog").getByRole("button", { name: "Update" }).click();

    await expect(
      page.getByText(
        "Not applied: an operator decides whether it is (500): The command failed and is recorded for an operator"
      )
    ).toBeVisible();
    await expect(page.getByText("Price updated")).toHaveCount(0);
  });

  /** The backend's answer to a command it refused for a reason that passes. */
  function busy(route: Route, reason: string, message: string): Promise<void> {
    return route.fulfill({
      status: 503,
      contentType: "application/json",
      headers: { "Retry-After": "5" },
      body: JSON.stringify({ reason, message }),
    });
  }

  test("a 503 on the shop page says the backend is busy, with its reason", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await mockList(page, "/api/customers", [ALICE]);
    await answerCommands(page, "/api/orders", (route) =>
      busy(
        route,
        "CIRCUIT_BREAKER_OPEN",
        "The command bus refuses commands while its circuit breaker is open."
      )
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Place Order" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("combobox").filter({ hasText: "Select product" }).click();
    await page.getByRole("option", { name: /Widget/ }).click();
    await dialog.getByRole("button", { name: "Place Order" }).click();

    await expect(
      page.getByText(
        "The backend is busy, try again in a few seconds (503): The command bus refuses commands while its circuit breaker is open."
      )
    ).toBeVisible();
    await expect(page.getByText(/Order placed/)).toHaveCount(0);
  });

  test("a 503 on an admin action says the backend is busy, with its reason", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await answerCommands(page, "/api/products/prod-widget/price", (route) =>
      busy(route, "AGGREGATE_LOCK_UNAVAILABLE", "The aggregate's lock could not be taken in time.")
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Update Price");
    await page.getByRole("dialog").getByLabel(/New Price/).fill("24.99");
    await page.getByRole("dialog").getByRole("button", { name: "Update" }).click();

    await expect(
      page.getByText(
        "The backend is busy, try again in a few seconds (503): The aggregate's lock could not be taken in time."
      )
    ).toBeVisible();
  });

  test("a 403 says the role may not do it, with the backend's words", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/orders", [order("order-a1", "CONFIRMED")]);
    await answerCommands(page, "/api/orders/*/cancel", (route) =>
      route.fulfill({
        status: 403,
        contentType: "text/plain",
        body: "Only an ADMIN or the customer themself may do this",
      })
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/orders");
    await page.getByRole("button", { name: "Cancel" }).click();
    await page.getByRole("dialog").getByRole("button", { name: "Cancel Order" }).click();

    await expect(
      page.getByText(
        "Not allowed for the selected role (403): Only an ADMIN or the customer themself may do this"
      )
    ).toBeVisible();
    await expect(page.getByText("Order cancelled")).toHaveCount(0);
  });

  test("a forget that stopped part-way says what is left and what to do", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/customers", [ALICE]);
    // The backend's answer when a purger failed after the key was deleted: 500 with the result.
    await answerCommands(page, "/api/customers/cust-alice/forget", (route) =>
      route.fulfill({
        status: 500,
        contentType: "application/json",
        body: JSON.stringify({
          keyDeleted: true,
          fullyErased: false,
          failedPurgers: ["customers_view"],
        }),
      })
    );
    await actAs(page, "CUSTOMER");

    await page.goto("/customers");
    await pickRowAction(page, "Forget (GDPR)");
    await page.getByRole("dialog").getByRole("button", { name: "Forget Customer" }).click();

    await expect(
      page.getByText(
        "API error 500: The erasure is not complete: the key is deleted, a copy is left in: customers_view. Send Forget again."
      )
    ).toBeVisible();
    await expect(page.getByText("Customer forgotten (GDPR)")).toHaveCount(0);
  });

  test("an id that cannot be generated is reported, and no request is sent", async ({ page }) => {
    await mockBackendRoutes(page);
    const sent = await answerCommands(page, "/api/customers");
    await actAs(page, "ADMIN");
    // A page opened over plain http under another host name has no crypto.randomUUID.
    await page.addInitScript(() => {
      Object.defineProperty(window.crypto, "randomUUID", {
        value: () => {
          throw new Error("crypto.randomUUID is not available here");
        },
      });
    });

    await page.goto("/customers");
    await page.getByRole("button", { name: "Register Customer" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Name").fill("Carol White");
    await dialog.getByLabel("Email").fill("carol@example.com");
    await dialog.getByRole("button", { name: "Register" }).click();

    await expect(page.getByText("crypto.randomUUID is not available here")).toBeVisible();
    expect(sent).toHaveLength(0);
  });

  test("a 400 shows the backend's reason", async ({ page }) => {
    await mockBackendRoutes(page);
    await mockList(page, "/api/products", [WIDGET]);
    await answerCommands(page, "/api/products/prod-widget/stock", (route) =>
      route.fulfill({
        status: 400,
        contentType: "text/plain",
        body: "Insufficient stock for product: prod-widget",
      })
    );
    await actAs(page, "ADMIN");

    await page.goto("/products");
    await pickRowAction(page, "Adjust Stock");
    await page.getByRole("dialog").getByLabel(/Delta/).fill("-500");
    await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();

    await expect(
      page.getByText(
        "The backend refused the request (400): Insufficient stock for product: prod-widget"
      )
    ).toBeVisible();
    await expect(page.getByText("Stock adjusted")).toHaveCount(0);
  });
});
