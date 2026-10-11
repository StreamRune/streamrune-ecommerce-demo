/**
 * Customers page tests.
 *
 * The backend serves customers to an authenticated caller only: every customer
 * to an ADMIN, a CUSTOMER's own record to that customer, 403 to a caller
 * without an identity. The page follows: a GUEST is told what the page
 * requires and sends no request.
 *
 * Surface tests: mocked backend — always run.
 * Backend-dependent block: skipped when Spring Boot is not reachable. It
 * expects the seed customers "Alice Johnson" (cust-alice) and "Bob Smith"
 * (cust-bob).
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

const BOB = {
  ...ALICE,
  customerId: "cust-bob",
  name: "Bob Smith",
  email: "bob@example.com",
};

test.describe("Customers page — surface (no backend required)", () => {
  test("a GUEST is told what the page requires and sends no customer request", async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);
    const requests = recordApiRequests(page);

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
    await expect(
      page.getByText("Requires CUSTOMER or ADMIN role to view customers.")
    ).toBeVisible();
    await expect(page.getByRole("heading", { name: "Customers" })).toHaveCount(0);
    expect(requestsTo(requests, "/api/customers")).toHaveLength(0);

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });

  test("an ADMIN lists every customer, as admin-1", async ({ page }) => {
    await mockBackendRoutes(page);
    await page.route(`${BACKEND_URL}/api/customers`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([ALICE, BOB]),
      })
    );
    await actAs(page, "ADMIN");
    const requests = recordApiRequests(page);

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();
    await expect(page.getByText("2 total")).toBeVisible();
    await expect(page.getByText("Alice Johnson")).toBeVisible();
    await expect(page.getByText("Bob Smith")).toBeVisible();

    const sent = requestsTo(requests, "/api/customers");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.role).toBe("ADMIN");
      expect(request.userId).toBe("admin-1");
    }
  });

  test("a CUSTOMER sees their own record and may manage it", async ({ page }) => {
    await mockBackendRoutes(page);
    // What the backend answers a CUSTOMER: the caller's own record and nothing else.
    await page.route(`${BACKEND_URL}/api/customers`, (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([ALICE]),
      })
    );
    await actAs(page, "CUSTOMER");
    const requests = recordApiRequests(page);

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();
    await expect(page.getByText("Your own record (cust-alice)")).toBeVisible();
    await expect(page.getByText("Alice Johnson")).toBeVisible();
    await expect(page.getByText("Bob Smith")).toHaveCount(0);

    const sent = requestsTo(requests, "/api/customers");
    expect(sent.length).toBeGreaterThan(0);
    for (const request of sent) {
      expect(request.role).toBe("CUSTOMER");
      expect(request.userId).toBe("cust-alice");
    }

    // The row's actions are open to the customer the row belongs to.
    await page.getByRole("button", { name: "Actions" }).click();
    await expect(page.getByRole("menuitem", { name: "Edit Profile" })).toBeVisible();
    await expect(page.getByRole("menuitem", { name: "Export Data" })).toBeVisible();
    await expect(page.getByRole("menuitem", { name: "Forget (GDPR)" })).toBeVisible();
  });

  test("a CUSTOMER without a customer record is told so", async ({ page }) => {
    await mockBackendRoutes(page);
    await actAs(page, "CUSTOMER", "cust-bob");

    await page.goto("/customers");

    await expect(page.getByText("No customer record for cust-bob.")).toBeVisible();
  });
});

test.describe("Customers page — backend-dependent", () => {
  let backendAvailable = false;

  test.beforeAll(async () => {
    backendAvailable = await isBackendReachable();
    if (!backendAvailable) {
      console.log(
        "[customers.spec] Backend not reachable at localhost:8080 — skipping backend-dependent tests"
      );
    }
  });

  test("an ADMIN sees the seed customers Alice Johnson and Bob Smith", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    await actAs(page, "ADMIN");

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();
    await expect(page.locator('text="Alice Johnson"')).toBeVisible();
    await expect(page.locator('text="Bob Smith"')).toBeVisible();
  });

  test("a CUSTOMER sees only their own record", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    await actAs(page, "CUSTOMER", "cust-bob");

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();
    await expect(page.locator('text="Bob Smith"')).toBeVisible();
    await expect(page.locator('text="Alice Johnson"')).toHaveCount(0);
  });

  test("a GUEST sends no customer request", async ({ page }) => {
    if (!backendAvailable) {
      test.skip();
      return;
    }
    const requests = recordApiRequests(page);

    await page.goto("/customers");

    await expect(page.getByRole("heading", { name: "Access Restricted" })).toBeVisible();
    expect(requestsTo(requests, "/api/customers")).toHaveLength(0);
  });
});
