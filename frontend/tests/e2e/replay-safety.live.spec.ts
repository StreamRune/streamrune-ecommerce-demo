/**
 * The two halves of replay safety, end to end against a running backend.
 *
 * Retry on the Admin page: a dead-lettered command that ran without an
 * idempotency key is not replayed on a plain Retry. The backend answers `409`
 * with its reason, the page shows it and asks the admin to confirm that the
 * command's effect is absent, and only the confirmed replay (`?force=true`)
 * runs the command — once, with an audit row naming the admin.
 *
 * The `Idempotency-Key` of a stock adjustment: the backend applies a request
 * sent twice with one key once.
 *
 * Backend-dependent: skipped when the backend is not reachable. Run it with
 * the stack of the README's Quick Start up (`docker compose up -d --wait`):
 *
 *   npm run test:live
 *
 * The backend writes a dead-letter entry only when a command fails on
 * infrastructure, which a running stack does not do on request. The test
 * therefore writes the entry into the `dead_letter_queue` table of the stack's
 * database (`docker compose exec postgres psql`), in the shape the command bus
 * writes for an unkeyed `PUT /api/products/{id}/stock` whose event could not be
 * stored. Everything after that is the backend's own behaviour. Without the
 * Compose database the dead-letter test is skipped, and with
 * E2E_REQUIRE_BACKEND=1 it fails instead.
 *
 * What a run leaves behind: two products of its own with their stock
 * adjustments, and one DLQ_FORCED_REPLAY row in the audit log.
 */

import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { test, expect, request, type APIRequestContext } from "@playwright/test";
import { actAs, isBackendReachable, BACKEND_URL } from "./helpers";

const SETTLED = { timeout: 45_000 };

const ADMIN_HEADERS = { "X-User-Id": "admin-1", "X-User-Role": "ADMIN" };

const COMPOSE_FILE = path.resolve(__dirname, "../../../docker-compose.yml");

/** One SQL statement against the Compose stack's database; its unaligned, tuples-only output. */
function sql(statement: string): string {
  return execFileSync(
    "docker",
    [
      "compose",
      "-f",
      COMPOSE_FILE,
      "exec",
      "-T",
      "postgres",
      "psql",
      "-v",
      "ON_ERROR_STOP=1",
      "-U",
      "postgres",
      "-d",
      "streamrune_ecommerce",
      "-tAq",
      "-c",
      statement,
    ],
    { encoding: "utf8" }
  ).trim();
}

function composeDatabaseAnswers(): boolean {
  try {
    return sql("SELECT 1") === "1";
  } catch {
    return false;
  }
}

async function createProduct(api: APIRequestContext, productId: string, stock: number) {
  const created = await api.post(`${BACKEND_URL}/api/products`, {
    data: {
      productId,
      name: `Replay ${productId}`,
      description: "Written by the replay-safety tests",
      category: "Tests",
      price: 9.99,
      initialStock: stock,
    },
  });
  expect(created.status(), await created.text()).toBe(200);
}

async function stockOf(api: APIRequestContext, productId: string): Promise<number | null> {
  const res = await api.get(`${BACKEND_URL}/api/products/${productId}`);
  return res.ok() ? ((await res.json()) as { stock: number }).stock : null;
}

test.describe("Replay safety against a running backend", () => {
  test.describe.configure({ mode: "serial", timeout: 120_000 });

  let backendAvailable = false;
  let api: APIRequestContext;

  test.beforeAll(async () => {
    backendAvailable = await isBackendReachable();
    if (!backendAvailable) {
      console.log(
        "[replay-safety.live.spec] Backend not reachable at localhost:8080 — skipping the tests"
      );
      return;
    }
    api = await request.newContext({ extraHTTPHeaders: ADMIN_HEADERS });
  });

  test.afterAll(async () => {
    await api?.dispose();
  });

  test.beforeEach(() => {
    test.skip(!backendAvailable, "the backend is not running");
  });

  test("Retry of an unkeyed dead letter asks the admin first, and the confirmed replay runs it once", async ({
    page,
  }) => {
    const database = composeDatabaseAnswers();
    if (!database && process.env.E2E_REQUIRE_BACKEND === "1") {
      throw new Error(
        `E2E_REQUIRE_BACKEND=1, but the database of ${COMPOSE_FILE} does not answer (docker compose exec postgres)`
      );
    }
    test.skip(!database, "the Compose stack's database is not reachable");

    const suffix = Date.now().toString(36);
    const productId = `prod-dlq-${suffix}`;
    const commandId = randomUUID();
    const marker = `replay-safety probe ${suffix}`;
    await createProduct(api, productId, 10);
    await expect.poll(() => stockOf(api, productId), SETTLED).toBe(10);

    // The entry of an adjustment by +4 whose event the store refused: no idempotency key.
    sql(
      `INSERT INTO dead_letter_queue (command_id, command_type, payload, aggregate_type,
         aggregate_id, error_type, error_message, attempts, first_attempt_at, user_id)
       VALUES ('${commandId}',
         'org.streamrune.ecommerce.domain.product.ProductCommand$AdjustStock',
         '{"productId":"${productId}","quantityChange":4,"reason":"replayed"}', 'product',
         '${productId}', 'java.lang.IllegalStateException', '${marker}', 3, NOW(), 'admin-1')`
    );

    await actAs(page, "ADMIN");
    await page.goto("/admin");
    const entry = page.getByRole("row").filter({ hasText: marker });
    await expect(entry).toBeVisible(SETTLED);

    const plain = page.waitForResponse(
      (r) => new URL(r.url()).pathname === `/api/admin/dead-letters/${commandId}/retry`
    );
    await entry.getByRole("button", { name: "Retry" }).click();
    expect((await plain).status(), "the backend refuses a plain replay of an unkeyed entry").toBe(
      409
    );

    const dialog = page.getByRole("dialog");
    await expect(dialog.getByRole("heading", { name: "Replay AdjustStock anyway?" })).toBeVisible();
    await expect(dialog.getByText(/without an idempotency key/)).toBeVisible();
    expect(await stockOf(api, productId), "the refused Retry ran nothing").toBe(10);

    const forced = page.waitForResponse(
      (r) =>
        new URL(r.url()).pathname === `/api/admin/dead-letters/${commandId}/retry` &&
        new URL(r.url()).searchParams.get("force") === "true"
    );
    await dialog.getByRole("button", { name: "The effect is absent: replay" }).click();
    expect((await forced).status()).toBe(200);
    await expect(page.getByText("Dead letter replayed after your confirmation")).toBeVisible();
    await expect(entry).toBeHidden(SETTLED);

    await expect.poll(() => stockOf(api, productId), SETTLED).toBe(14);
    expect(
      sql(
        `SELECT user_id || ' ' || outcome FROM audit_log
          WHERE command_id = '${commandId}' AND command_type = 'DLQ_FORCED_REPLAY'`
      ),
      "the forced replay is audited with the admin who confirmed it"
    ).toBe("admin-1 SUCCESS");
  });

  test("a stock adjustment sent twice with one Idempotency-Key changes the stock once", async () => {
    const productId = `prod-key-${Date.now().toString(36)}`;
    await createProduct(api, productId, 10);
    await expect.poll(() => stockOf(api, productId), SETTLED).toBe(10);

    const key = randomUUID();
    for (let i = 0; i < 2; i++) {
      const adjusted = await api.put(`${BACKEND_URL}/api/products/${productId}/stock`, {
        headers: { "Idempotency-Key": key },
        data: { quantity: 3, reason: "restock" },
      });
      expect(adjusted.status(), await adjusted.text()).toBe(200);
    }
    const another = await api.put(`${BACKEND_URL}/api/products/${productId}/stock`, {
      headers: { "Idempotency-Key": randomUUID() },
      data: { quantity: 1, reason: "restock" },
    });
    expect(another.status(), await another.text()).toBe(200);

    // 10 + 3 once + 1: the repeat was answered with the first request's result.
    await expect.poll(() => stockOf(api, productId), SETTLED).toBe(14);
  });
});
