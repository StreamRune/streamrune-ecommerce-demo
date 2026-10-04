/**
 * Products page smoke tests.
 *
 * Surface test: mocked backend — always runs, verifies the page renders.
 * Backend-dependent block: skipped when Spring Boot at localhost:8080 is not
 * reachable; when backend IS running it asserts seed products (Widget, Gadget,
 * Doohickey) are visible in the table.
 */

import { test, expect } from "@playwright/test";
import { mockBackendRoutes, isBackendReachable, collectConsoleErrors } from "./helpers";

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
