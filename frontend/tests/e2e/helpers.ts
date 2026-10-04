/**
 * Shared helpers for StreamRune E2E smoke tests.
 *
 * Backend-dependent tests skip themselves when the Spring Boot backend at
 * http://localhost:8080 is not reachable.  Set BACKEND_URL env var to
 * override the health-check target (defaults to "http://localhost:8080").
 */

import { type Page, type TestInfo, request } from "@playwright/test";

export const BACKEND_URL =
  process.env.BACKEND_URL ?? "http://localhost:8080";

/**
 * Returns true when the backend actuator health endpoint responds 200.
 * Used in beforeAll hooks to decide whether to skip backend-dependent suites.
 */
export async function isBackendReachable(): Promise<boolean> {
  try {
    const ctx = await request.newContext();
    const res = await ctx.get(`${BACKEND_URL}/actuator/health`, {
      timeout: 3_000,
      // Don't throw on non-2xx — we just check status manually
    }).catch(() => null);
    await ctx.dispose();
    return res !== null && res.status() === 200;
  } catch {
    return false;
  }
}

/**
 * Collect console errors on the page during a test.
 * Returns the array of error messages captured so far.
 */
export function collectConsoleErrors(page: Page): () => string[] {
  const errors: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      errors.push(msg.text());
    }
  });
  return () => errors;
}

/**
 * Mock all backend API calls so the page renders without port 8080.
 * Intercepts /api/** and /actuator/** with minimal empty-body responses.
 */
export async function mockBackendRoutes(page: Page): Promise<void> {
  // Empty arrays for list endpoints
  await page.route(`${BACKEND_URL}/api/products`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/orders`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/customers`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/events**`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/audit**`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/sagas**`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/admin/**`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "{}" })
  );
  await page.route(`${BACKEND_URL}/actuator/**`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "UP" }),
    })
  );
  // SSE endpoint — fulfill immediately so EventSource doesn't hang
  await page.route(`${BACKEND_URL}/api/events/sse`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body: "",
    })
  );
}
