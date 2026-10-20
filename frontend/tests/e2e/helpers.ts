/**
 * Shared helpers for StreamRune E2E tests.
 *
 * Backend-dependent tests skip themselves when the backend at
 * http://localhost:8080 is not reachable.  Set BACKEND_URL env var to
 * override the health-check target (defaults to "http://localhost:8080").
 */

import { type Page, type Route, request } from "@playwright/test";

export const BACKEND_URL =
  process.env.BACKEND_URL ?? "http://localhost:8080";

/**
 * Returns true when the backend's health endpoint responds 200: GET
 * /api/admin/health, the one the three apps share and answer to every caller.
 * Used in beforeAll hooks to decide whether to skip backend-dependent suites.
 * With E2E_REQUIRE_BACKEND=1 an unreachable backend throws instead.
 */
export async function isBackendReachable(): Promise<boolean> {
  const reachable = await healthAnswers();
  if (!reachable && process.env.E2E_REQUIRE_BACKEND === "1") {
    // `npm run test:live` and the CI job that starts the stack: a backend that does not answer
    // is a failure there, not a reason to skip.
    throw new Error(
      `E2E_REQUIRE_BACKEND=1, but ${BACKEND_URL}/api/admin/health does not answer 200`
    );
  }
  return reachable;
}

async function healthAnswers(): Promise<boolean> {
  try {
    const ctx = await request.newContext();
    const res = await ctx.get(`${BACKEND_URL}/api/admin/health`, {
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
 * Opens the pages of a test as the given role, as the role switcher would have
 * saved it: ADMIN acts as admin-1, CUSTOMER as cust-alice unless `userId`
 * names another seeded customer, GUEST (also the default of an empty
 * localStorage) as nobody.
 */
export async function actAs(
  page: Page,
  role: "ADMIN" | "CUSTOMER" | "GUEST",
  userId?: string
): Promise<void> {
  await page.addInitScript(
    ([r, id]) => {
      window.localStorage.setItem("sr_role", r as string);
      if (id) {
        window.localStorage.setItem("sr_userId", id as string);
      } else {
        window.localStorage.removeItem("sr_userId");
      }
    },
    [role, userId ?? null]
  );
}

/** One request the page sent to the backend API. */
export interface ApiRequest {
  /** Path and query, e.g. "/api/orders?customerId=cust-bob". */
  target: string;
  role: string | undefined;
  userId: string | undefined;
}

/**
 * Records every request the page sends to the backend API from now on, with
 * the identity headers it carried.
 */
export function recordApiRequests(page: Page): ApiRequest[] {
  const seen: ApiRequest[] = [];
  page.on("request", (r) => {
    const url = new URL(r.url());
    if (url.origin === new URL(BACKEND_URL).origin && url.pathname.startsWith("/api/")) {
      const headers = r.headers();
      seen.push({
        target: url.pathname + url.search,
        role: headers["x-user-role"],
        userId: headers["x-user-id"],
      });
    }
  });
  return seen;
}

/** The requests among `seen` whose path is `path` or lies below it. */
export function requestsTo(seen: ApiRequest[], path: string): ApiRequest[] {
  return seen.filter((r) => {
    const pathname = r.target.split("?")[0];
    return pathname === path || pathname.startsWith(`${path}/`);
  });
}

/**
 * Mock all backend API calls so the page renders without port 8080.
 * Intercepts /api/** with minimal responses in the shapes the backend sends.
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
  await page.route(`${BACKEND_URL}/api/saga/**`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
  );
  await page.route(`${BACKEND_URL}/api/admin/**`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "{}" })
  );
  // The admin reads, each in its own shape: two lists, the switch, the breakers, the health.
  for (const list of ["dead-letters", "outbox"]) {
    await page.route(`${BACKEND_URL}/api/admin/${list}`, (route) =>
      route.fulfill({ status: 200, contentType: "application/json", body: "[]" })
    );
  }
  await page.route(`${BACKEND_URL}/api/admin/payment-failure`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ failureInjected: false }),
    })
  );
  await page.route(`${BACKEND_URL}/api/admin/circuit-breaker`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ state: "CLOSED", paymentGateway: "CLOSED" }),
    })
  );
  await page.route(`${BACKEND_URL}/api/admin/health`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "UP" }),
    })
  );
  // The live feed is left unanswered: a quiet feed, whose connection stays as it is. A mocked
  // route can only answer with a complete body, which the page sees as a feed that ended, and
  // the page then reconnects and reads its lists again. A spec that wants frames or a reconnect
  // routes the feed itself.
  await page.route(`${BACKEND_URL}/api/events/sse`, () => {});
}

/**
 * Routes the live feed so that its first connection sends the given frames and
 * every later one stays quiet. A mocked route can only answer with a complete
 * body, so the first connection ends after its frames and the page reconnects;
 * a backend sends a frame once, as a connection starts at the head and is sent
 * nothing of what was sent before it.
 */
export async function mockFeedThatSendsOnce(page: Page, frames: string): Promise<void> {
  let connections = 0;
  await page.route(`${BACKEND_URL}/api/events/sse`, (route) => {
    connections += 1;
    if (connections === 1) {
      return route.fulfill({ status: 200, contentType: "text/event-stream", body: frames });
    }
    // Left unanswered: the connection stays as it is.
  });
}

/** One command the page sent: a POST or PUT, with its identity headers and its JSON body. */
export interface SentCommand {
  method: string;
  /** Path without the query, e.g. "/api/orders/order-a1/cancel". */
  path: string;
  role: string | undefined;
  userId: string | undefined;
  contentType: string | undefined;
  /** The `Idempotency-Key` header, which the stock and shipment writes send. */
  idempotencyKey: string | undefined;
  /** The parsed JSON body, or null for a request without one. */
  body: Record<string, unknown> | null;
}

/**
 * Answers the commands the page sends to `path` (a path below /api, or a glob
 * such as "/api/orders/*\/cancel") and records each one. The answer is the
 * backend's for a command it accepted: `200` with an empty body, unless
 * `answer` says otherwise.
 */
export async function answerCommands(
  page: Page,
  path: string,
  answer: (route: Route) => Promise<void> = (route) => route.fulfill({ status: 200, body: "" })
): Promise<SentCommand[]> {
  const sent: SentCommand[] = [];
  await page.route(`${BACKEND_URL}${path}`, (route) => {
    const req = route.request();
    if (req.method() !== "POST" && req.method() !== "PUT") {
      return route.fallback();
    }
    const headers = req.headers();
    const data = req.postData();
    sent.push({
      method: req.method(),
      path: new URL(req.url()).pathname,
      role: headers["x-user-role"],
      userId: headers["x-user-id"],
      contentType: headers["content-type"],
      idempotencyKey: headers["idempotency-key"],
      body: data ? (JSON.parse(data) as Record<string, unknown>) : null,
    });
    return answer(route);
  });
  return sent;
}
