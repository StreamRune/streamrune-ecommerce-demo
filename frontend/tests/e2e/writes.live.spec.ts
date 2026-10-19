/**
 * The write actions of the pages, end to end against a running backend.
 *
 * Each test performs a write through the page, as a person does, and asserts
 * its result through the page: the row a registration adds, the order that
 * appears in its customer's list and is confirmed by the fulfillment saga, the
 * price and the stock a product shows. Nothing is mocked, so a request the
 * backend does not accept fails the test with the backend's own answer.
 *
 * Backend-dependent: skipped when the backend is not reachable. Run it with
 * the stack of the README's Quick Start up (`docker compose up -d --wait`):
 *
 *   npm run test:live
 *
 * The tests run in order and share what they create: the customer the first
 * one registers places the orders of the later ones. The suite brings what it
 * consumes: before the first test it creates a product of its own through the
 * API and receives ten units of it into the inventory, and every order that
 * needs stock takes it from there. It changes no seed data, so it passes run
 * after run on the same database.
 *
 * What a run leaves behind: a forgotten customer, two products (the one it
 * ordered from, with the units no order took, and a discontinued one), four
 * orders with their payments and sagas (one delivered, one confirmed, one
 * cancelled after its confirmation, its payment refunded and its unit
 * released, one cancelled by a failed payment), and the events of all of
 * them. The payment-failure switch is left off.
 */

import { test, expect, request, type Browser, type Page } from "@playwright/test";
import { actAs, isBackendReachable, BACKEND_URL } from "./helpers";

/**
 * How long a page may take to show the outcome of a write.
 *
 * With the backend's defaults an order is confirmed by its saga within two
 * seconds, and the order read model, which polls every five seconds plus up to
 * one of jitter, has the row within eight. The page reads its lists again at
 * once and 2, 7, 15 and 30 seconds after the last frame of a change
 * (`read-model-sync.ts`). 45 seconds is above the backend's worst case several
 * times over and above the page's last re-read, so a run fails here only when
 * the outcome does not come, not when a poll falls badly.
 */
const SETTLED = { timeout: 45_000 };

const ADMIN_HEADERS = { "X-User-Id": "admin-1", "X-User-Role": "ADMIN" };

/** What the earlier tests created, for the later ones. */
const created = {
  customerId: "",
  customerName: "",
  orderId: "",
  productId: "",
  productName: "",
  /** The product the suite's orders take their stock from. */
  stockedProductId: "",
  stockedProductName: "",
};

const STOCKED_PRICE = 19.99;

/** The JSON body of the command the page sends to `path` while `action` runs. */
async function commandSentBy(
  page: Page,
  path: string,
  action: () => Promise<void>
): Promise<Record<string, unknown>> {
  const [sent] = await Promise.all([
    page.waitForRequest(
      (r) => r.method() === "POST" && new URL(r.url()).pathname === path
    ),
    action(),
  ]);
  return sent.postDataJSON() as Record<string, unknown>;
}

/** A page of its own for the given identity, next to the test's page. */
async function pageAs(
  browser: Browser,
  role: "ADMIN" | "CUSTOMER",
  userId?: string
): Promise<Page> {
  const context = await browser.newContext();
  const page = await context.newPage();
  await actAs(page, role, userId);
  return page;
}

/** Places an order for one unit of a product through the Orders page and returns its id. */
async function placeOrder(page: Page, productName: string): Promise<string> {
  await page.goto("/orders");
  await page.getByRole("button", { name: "Place Order" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("combobox").filter({ hasText: "Select product" }).click();
  await page.getByRole("option", { name: new RegExp(productName) }).click();
  const body = await commandSentBy(page, "/api/orders", () =>
    dialog.getByRole("button", { name: "Place Order" }).click()
  );
  const orderId = body.orderId as string;
  await expect(page.getByText(`Order placed: ${orderId}`)).toBeVisible();
  return orderId;
}

function row(page: Page, text: string) {
  return page.getByRole("row").filter({ hasText: text });
}

async function pickRowAction(page: Page, rowText: string, item: string): Promise<void> {
  await row(page, rowText).getByRole("button", { name: "Actions" }).click();
  await page.getByRole("menuitem", { name: item }).click();
}

test.describe("Writes against a running backend", () => {
  test.describe.configure({ mode: "serial", timeout: 120_000 });

  let backendAvailable = false;

  test.beforeAll(async () => {
    backendAvailable = await isBackendReachable();
    if (!backendAvailable) {
      console.log(
        "[writes.live.spec] Backend not reachable at localhost:8080 — skipping the write tests"
      );
      return;
    }
    // A product of the run's own, with stock in the inventory the fulfillment saga reserves
    // from: the orders below consume it, not the seed data's.
    const suffix = Date.now().toString(36);
    created.stockedProductId = `prod-live-${suffix}`;
    created.stockedProductName = `Live Gadget ${suffix}`;
    const api = await request.newContext({ extraHTTPHeaders: ADMIN_HEADERS });
    const product = await api.post(`${BACKEND_URL}/api/products`, {
      data: {
        productId: created.stockedProductId,
        name: created.stockedProductName,
        description: "Ordered by the write tests",
        category: "Tests",
        price: STOCKED_PRICE,
        initialStock: 0,
      },
    });
    expect(product.status(), await product.text()).toBe(200);
    const shipment = await api.post(
      `${BACKEND_URL}/api/inventory/${created.stockedProductId}/receive`,
      { data: { quantity: 10 } }
    );
    expect(shipment.status(), await shipment.text()).toBe(200);
    // No page is open yet to hear the feed announce the product: wait until the catalogue, a
    // read model, lists it, so that the first Place Order dialog offers it.
    await expect
      .poll(
        async () => (await api.get(`${BACKEND_URL}/api/products/${created.stockedProductId}`)).status(),
        SETTLED
      )
      .toBe(200);
    await api.dispose();
  });

  test.beforeEach(() => {
    test.skip(!backendAvailable, "the backend is not running");
  });

  test.afterAll(async () => {
    if (!backendAvailable) return;
    // Whatever a failed test left behind, the next order must reach a working payment gateway.
    const api = await request.newContext({ extraHTTPHeaders: ADMIN_HEADERS });
    const state = await api.get(`${BACKEND_URL}/api/admin/payment-failure`);
    if (state.ok() && ((await state.json()) as { failureInjected: boolean }).failureInjected) {
      await api.post(`${BACKEND_URL}/api/admin/payment-failure/toggle`);
    }
    await api.dispose();
  });

  test("an ADMIN registers a customer, who is then on the list", async ({ page }) => {
    await actAs(page, "ADMIN");
    await page.goto("/customers");
    await expect(row(page, "cust-alice")).toBeVisible();

    const suffix = Date.now().toString(36);
    created.customerName = `Carol Writes ${suffix}`;
    await page.getByRole("button", { name: "Register Customer" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Name").fill(created.customerName);
    await dialog.getByLabel("Email").fill(`carol.${suffix}@example.com`);
    await dialog.getByLabel("Address").fill("7 Elm St");
    await dialog.getByLabel("Phone").fill("+1-555-0103");
    const body = await commandSentBy(page, "/api/customers", () =>
      dialog.getByRole("button", { name: "Register" }).click()
    );
    created.customerId = body.customerId as string;

    await expect(page.getByText(`Customer registered: ${created.customerId}`)).toBeVisible();
    const registered = row(page, created.customerId);
    await expect(registered).toBeVisible(SETTLED);
    await expect(registered).toContainText(created.customerName);
    await expect(registered).toContainText(`carol.${suffix}@example.com`);
    await expect(registered).toContainText("ACTIVE");
  });

  test("the customer places an order: it is on their list, and its fulfillment completes", async ({
    page,
  }) => {
    await actAs(page, "CUSTOMER", created.customerId);

    created.orderId = await placeOrder(page, created.stockedProductName);

    // The order is the customer's own, and the fulfillment saga confirms it.
    const placed = row(page, created.orderId);
    await expect(placed).toBeVisible(SETTLED);
    await expect(placed).toContainText(created.customerId);
    await expect(placed).toContainText("$19.99");
    await expect(placed).toContainText("CONFIRMED", SETTLED);

    // The saga of the order is on the customer's Fulfillment page, completed.
    await page.goto("/fulfillment");
    const saga = page
      .locator('[data-slot="card"]')
      .filter({ hasText: `fulfillment-${created.orderId}` });
    await expect(saga).toBeVisible(SETTLED);
    await expect(saga).toContainText("COMPLETED", SETTLED);
    // Their own sagas only: the seed order is another customer's.
    await expect(page.getByText("fulfillment-order-seed-1")).toHaveCount(0);
  });

  test("the customer cancels a confirmed order: its stock is released and its payment refunded", async ({
    page,
  }) => {
    await actAs(page, "CUSTOMER", created.customerId);
    const orderId = await placeOrder(page, created.stockedProductName);
    const placed = row(page, orderId);
    await expect(placed).toContainText("CONFIRMED", SETTLED);

    // The dialog says what a cancel does.
    await placed.getByRole("button", { name: "Cancel" }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByRole("note")).toHaveText(
      "Cancelling releases the order's stock and voids or refunds its payment in full."
    );
    await dialog.getByLabel("Reason (optional)").fill("changed my mind");
    await dialog.getByRole("button", { name: "Cancel Order" }).click();

    await expect(page.getByText("Order cancelled")).toBeVisible();
    await expect(placed).toContainText("CANCELLED", SETTLED);
    await expect(placed.getByRole("button", { name: "Cancel" })).toHaveCount(0);
    // The row's payment badge follows the refund.
    await expect(placed.getByLabel("Payment status")).toHaveText("REFUNDED", SETTLED);

    // The order's saga was complete before the cancel and stays so. The cancellation's undo
    // refunds the payment in full and releases the reserved unit.
    await page.goto("/fulfillment");
    const saga = page.locator('[data-slot="card"]').filter({ hasText: `fulfillment-${orderId}` });
    await expect(saga).toContainText("COMPLETED", SETTLED);
    const api = await request.newContext({ extraHTTPHeaders: ADMIN_HEADERS });
    const eventTypesOf = async (stream: string) => {
      const events = await api.get(`${BACKEND_URL}/api/events/${stream}`);
      expect(events.status()).toBe(200);
      return ((await events.json()) as { eventType: string; payload: { orderId?: string } }[])
        .filter((event) => event.payload.orderId === undefined || event.payload.orderId === orderId)
        .map((event) => event.eventType);
    };
    expect(await eventTypesOf(`order/${orderId}`)).toEqual([
      "OrderPlaced",
      "OrderConfirmed",
      "OrderCancelled",
    ]);
    await expect
      .poll(() => eventTypesOf(`payment/pay-${orderId}`), SETTLED)
      .toEqual(["PaymentInitiated", "PaymentCaptured", "PaymentRefunded"]);
    await expect
      .poll(() => eventTypesOf(`inventory/${created.stockedProductId}`), SETTLED)
      .toContain("StockReleased");
    await api.dispose();
  });

  test("an ADMIN ships and delivers the customer's order", async ({ page }) => {
    await actAs(page, "ADMIN");
    await page.goto(`/orders?customerId=${created.customerId}`);
    const placed = row(page, created.orderId);
    await expect(placed).toContainText("CONFIRMED", SETTLED);

    await placed.getByRole("button", { name: "Ship" }).click();
    await expect(page.getByText("Order shipped")).toBeVisible();
    await expect(placed).toContainText("SHIPPED", SETTLED);

    await placed.getByRole("button", { name: "Deliver" }).click();
    await expect(page.getByText("Order delivered")).toBeVisible();
    await expect(placed).toContainText("DELIVERED", SETTLED);
  });

  test("an ADMIN creates a product and changes its price and its stock", async ({ page }) => {
    await actAs(page, "ADMIN");
    await page.goto("/products");
    await expect(row(page, "Widget")).toBeVisible();

    created.productName = `Sprocket ${Date.now().toString(36)}`;
    await page.getByRole("button", { name: "Add Product" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Name").fill(created.productName);
    await dialog.getByLabel("Description").fill("Made by the write tests");
    await dialog.getByLabel("Category").fill("Hardware");
    await dialog.getByLabel("Price (USD)").fill("12.50");
    await dialog.getByLabel("Initial Stock").fill("7");
    const body = await commandSentBy(page, "/api/products", () =>
      dialog.getByRole("button", { name: "Create" }).click()
    );
    created.productId = body.productId as string;

    await expect(page.getByText(`Product created: ${created.productId}`)).toBeVisible();
    const product = row(page, created.productName);
    await expect(product).toBeVisible(SETTLED);
    await expect(product).toContainText("Hardware");
    await expect(product).toContainText("$12.50");
    await expect(product.getByRole("cell").nth(3)).toHaveText("7");

    await pickRowAction(page, created.productName, "Update Price");
    await page.getByRole("dialog").getByLabel("New Price (USD)").fill("24.50");
    await page.getByRole("dialog").getByRole("button", { name: "Update" }).click();
    await expect(page.getByText("Price updated")).toBeVisible();
    await expect(product).toContainText("$24.50", SETTLED);

    await pickRowAction(page, created.productName, "Adjust Stock");
    await page.getByRole("dialog").getByLabel(/Delta/).fill("5");
    await page.getByRole("dialog").getByLabel("Reason (optional)").fill("found in the back");
    await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText("Stock adjusted")).toBeVisible();
    await expect(product.getByRole("cell").nth(3)).toHaveText("12", SETTLED);

    await pickRowAction(page, created.productName, "Adjust Stock");
    await page.getByRole("dialog").getByLabel(/Delta/).fill("-2");
    await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();
    await expect(product.getByRole("cell").nth(3)).toHaveText("10", SETTLED);

    // More than there is: the backend refuses, and the page says why.
    await pickRowAction(page, created.productName, "Adjust Stock");
    await page.getByRole("dialog").getByLabel(/Delta/).fill("-500");
    await page.getByRole("dialog").getByRole("button", { name: "Adjust" }).click();
    await expect(page.getByText(/refused the request \(400\): Insufficient stock/)).toBeVisible();
  });

  test("a shipment received into the inventory lets an order for the new product be fulfilled", async ({
    page,
    browser,
  }) => {
    await actAs(page, "ADMIN");
    await page.goto("/products");
    await pickRowAction(page, created.productName, "Receive Shipment");
    await page.getByRole("dialog").getByLabel("Quantity").fill("3");
    await page.getByRole("dialog").getByRole("button", { name: "Receive" }).click();
    await expect(page.getByText("Shipment received into the inventory")).toBeVisible();

    // The saga reserves from that inventory: the order is confirmed at the catalogue's price.
    const customer = await pageAs(browser, "CUSTOMER", created.customerId);
    const orderId = await placeOrder(customer, created.productName);
    const placed = row(customer, orderId);
    await expect(placed).toContainText("$24.50", SETTLED);
    await expect(placed).toContainText("CONFIRMED", SETTLED);
    await customer.context().close();

    await pickRowAction(page, created.productName, "Discontinue");
    await expect(page.getByText("Product discontinued")).toBeVisible();
    await expect(row(page, created.productName)).toContainText("DISCONTINUED", SETTLED);
  });

  test("the payment-failure switch fails the next order's payment and is switched off again", async ({
    page,
    browser,
  }) => {
    await actAs(page, "ADMIN");
    await page.goto("/fulfillment");
    const toggle = page.getByRole("switch", { name: "Inject Payment Failure" });
    await expect(toggle).not.toBeChecked();

    await toggle.click();
    await expect(page.getByText("Payment failure injection enabled")).toBeVisible();
    await expect(toggle).toBeChecked();
    await expect(page.getByText("Payment failure injection is active.")).toBeVisible();

    // The switch is the backend's: a page loaded afterwards reads it as on.
    await page.reload();
    await expect(toggle).toBeChecked();

    const customer = await pageAs(browser, "CUSTOMER", created.customerId);
    const orderId = await placeOrder(customer, created.stockedProductName);
    await expect(row(customer, orderId)).toContainText("CANCELLED", SETTLED);

    // The saga is read on the customer's own Fulfillment page, which lists the sagas of their
    // orders. An ADMIN's page lists fifty sagas of everyone, the highest saga ids first, and on
    // a database that has seen many runs a new order's saga need not be among them.
    await customer.goto("/fulfillment");
    const saga = customer
      .locator('[data-slot="card"]')
      .filter({ hasText: `fulfillment-${orderId}` });
    await expect(saga).toContainText("FAILED", SETTLED);
    await customer.context().close();

    await toggle.click();
    await expect(page.getByText("Payment failure disabled")).toBeVisible();
    await expect(toggle).not.toBeChecked();
    await expect(page.getByText("Payment failure injection is active.")).toHaveCount(0);
  });

  test("the customer edits their profile", async ({ page }) => {
    await actAs(page, "CUSTOMER", created.customerId);
    await page.goto("/customers");
    await expect(row(page, created.customerId)).toContainText(created.customerName, SETTLED);

    const renamed = `${created.customerName} Edited`;
    await pickRowAction(page, created.customerId, "Edit Profile");
    await page.getByRole("dialog").getByLabel("Name").fill(renamed);
    await page.getByRole("dialog").getByLabel("Phone").fill("+1-555-0199");
    await page.getByRole("dialog").getByRole("button", { name: "Save" }).click();

    await expect(page.getByText("Profile updated")).toBeVisible();
    await expect(row(page, created.customerId)).toContainText(renamed, SETTLED);
    await expect(row(page, created.customerId)).toContainText("+1-555-0199");
    created.customerName = renamed;
  });

  test("the customer asks for an export of their data", async ({ page, browser }) => {
    await actAs(page, "CUSTOMER", created.customerId);
    await page.goto("/customers");
    await pickRowAction(page, created.customerId, "Export Data");
    await expect(page.getByText("Data export requested")).toBeVisible();

    // The request is an event of the customer's stream, which an ADMIN reads in the history.
    const admin = await pageAs(browser, "ADMIN");
    await admin.goto("/events");
    const filter = admin.getByPlaceholder(/type:id/);
    await filter.fill(`customer:${created.customerId}`);
    await filter.press("Enter");
    await expect(admin.getByRole("button", { name: /DataExportRequested/ })).toBeVisible(SETTLED);
    await admin.context().close();
  });

  test("the customer is forgotten and their record is gone", async ({ page, browser }) => {
    await actAs(page, "CUSTOMER", created.customerId);
    await page.goto("/customers");
    await pickRowAction(page, created.customerId, "Forget (GDPR)");
    await page.getByRole("dialog").getByRole("button", { name: "Forget Customer" }).click();

    await expect(page.getByText("Customer forgotten (GDPR)")).toBeVisible();
    await expect(page.getByText(`No customer record for ${created.customerId}.`)).toBeVisible(
      SETTLED
    );

    const admin = await pageAs(browser, "ADMIN");
    await admin.goto("/customers");
    await expect(row(admin, "cust-alice")).toBeVisible();
    await expect(row(admin, created.customerId)).toHaveCount(0);
    await admin.context().close();
  });
});
