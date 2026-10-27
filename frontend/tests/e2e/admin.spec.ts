/**
 * Admin page: the page renders for every role (GUEST sees a "read-only"
 * notice, ADMIN the action buttons), and the dead-letter Retry button handles
 * each answer of `POST /api/admin/dead-letters/{id}/retry`, and the discard
 * (`DELETE /api/admin/dead-letters/{id}`) the confirm dialog offers.
 *
 * Uses mocked backend so the tests always run. `replay-safety.live.spec.ts`
 * drives the same Retry and discard against a running backend.
 */

import { test, expect, type Page, type Route } from "@playwright/test";
import { actAs, mockBackendRoutes, collectConsoleErrors, BACKEND_URL } from "./helpers";

test.describe("Admin page", () => {
  test("renders heading (read-only check, no mutations)", async ({ page }) => {
    await mockBackendRoutes(page);
    const getErrors = collectConsoleErrors(page);

    await page.goto("/admin");

    await expect(page.getByRole("heading", { name: "Admin" })).toBeVisible();

    const errors = getErrors().filter(
      (e) =>
        !e.includes("EventSource") &&
        !e.includes("net::ERR_") &&
        !e.includes("Failed to fetch")
    );
    expect(errors, `Unexpected console errors: ${errors.join("\n")}`).toHaveLength(0);
  });
});

const ENTRY = {
  commandId: "6f1c2a3e-0000-4000-8000-000000000001",
  commandType: "org.streamrune.ecommerce.domain.product.ProductCommand$AdjustStock",
  errorType: "java.lang.IllegalStateException",
  errorMessage: "transient downstream failure",
  attempts: 3,
  dlqAttempts: 0,
  firstAttemptAt: "2026-10-04T00:00:00Z",
  lastAttemptAt: null,
};

const NOT_DEDUPLICATED =
  "The command ran without an idempotency key: if its caller already retried it, a replay applies it twice. Check whether its effect is present: if it is missing, retry with ?force=true; if it is present, discard the entry (DELETE /api/admin/dead-letters/{id}).";

/** One dead-letter entry listed, and every retry request answered by `answer` and recorded. */
async function deadLetterRetries(
  page: Page,
  answer: (route: Route, force: boolean) => Promise<void>
): Promise<string[]> {
  await mockBackendRoutes(page);
  await page.route(`${BACKEND_URL}/api/admin/dead-letters`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify([ENTRY]) })
  );
  const sent: string[] = [];
  await page.route(`${BACKEND_URL}/api/admin/dead-letters/*/retry**`, (route) => {
    const url = new URL(route.request().url());
    sent.push(url.pathname + url.search);
    return answer(route, url.searchParams.get("force") === "true");
  });
  await actAs(page, "ADMIN");
  return sent;
}

/** Answers every discard of the entry with `answer` and records it. */
async function deadLetterDiscards(
  page: Page,
  answer: (route: Route) => Promise<void>
): Promise<string[]> {
  const sent: string[] = [];
  await page.route(`${BACKEND_URL}/api/admin/dead-letters/${ENTRY.commandId}`, (route) => {
    if (route.request().method() !== "DELETE") return route.fallback();
    sent.push(new URL(route.request().url()).pathname);
    return answer(route);
  });
  return sent;
}

function json(route: Route, status: number, body: unknown, headers?: Record<string, string>) {
  return route.fulfill({
    status,
    contentType: "application/json",
    headers,
    body: JSON.stringify(body),
  });
}

test.describe("Admin page — dead-letter Retry", () => {
  const RETRY = `/api/admin/dead-letters/${ENTRY.commandId}/retry`;

  test("a replayed entry says so", async ({ page }) => {
    const sent = await deadLetterRetries(page, (route) => json(route, 200, { outcome: "REPLAYED" }));

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();

    await expect(page.getByText("Dead letter replayed")).toBeVisible();
    expect(sent).toEqual([RETRY]);
  });

  test("a 409 shows the backend's reason, and the confirmed replay is sent with force", async ({
    page,
  }) => {
    const sent = await deadLetterRetries(page, (route, force) =>
      force
        ? json(route, 200, { outcome: "REPLAYED" })
        : json(route, 409, { outcome: "NOT_DEDUPLICATED", message: NOT_DEDUPLICATED })
    );

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();

    const dialog = page.getByRole("dialog");
    await expect(dialog.getByRole("heading", { name: "Replay or discard AdjustStock?" })).toBeVisible();
    await expect(dialog.getByText(NOT_DEDUPLICATED)).toBeVisible();
    await expect(dialog.getByText(/If the effect is absent, replay the entry/)).toBeVisible();
    expect(sent, "nothing is forced before the admin confirms").toEqual([RETRY]);

    await dialog.getByRole("button", { name: "The effect is absent: replay" }).click();

    await expect(page.getByText("Dead letter replayed after your confirmation")).toBeVisible();
    await expect(dialog).toBeHidden();
    expect(sent).toEqual([RETRY, `${RETRY}?force=true`]);
  });

  test("a 409 the admin does not confirm sends nothing more", async ({ page }) => {
    const sent = await deadLetterRetries(page, (route) =>
      json(route, 409, { outcome: "STALE_REPLAY_BLOCKED", message: "The entry is older than the command-inbox retention window." })
    );

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText(/older than the command-inbox retention window/)).toBeVisible();
    await dialog.getByRole("button", { name: "Close" }).first().click();

    await expect(dialog).toBeHidden();
    expect(sent).toEqual([RETRY]);
  });

  test("a 503 says the backend is busy", async ({ page }) => {
    await deadLetterRetries(page, (route) =>
      json(route, 503, { outcome: "ADMISSION_REFUSED" }, { "Retry-After": "5" })
    );

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();

    await expect(page.getByText(/The backend is busy.*try again in a few seconds/)).toBeVisible();
    await expect(page.getByRole("dialog")).toBeHidden();
  });

  test("a 503 READER_BEHIND says this backend's code is behind the stored events", async ({
    page,
  }) => {
    const sent = await deadLetterRetries(page, (route) =>
      json(route, 503, { outcome: "READER_BEHIND" }, { "Retry-After": "5" })
    );
    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();

    await expect(
      page.getByText(/reads the command's events at an older schema.*the entry keeps its attempt count/)
    ).toBeVisible();
    await expect(page.getByText(/The backend is busy/)).toHaveCount(0);
    expect(sent).toHaveLength(1);
  });

  test("a 404 says the entry is gone", async ({ page }) => {
    await deadLetterRetries(page, (route) => route.fulfill({ status: 404, body: "" }));

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();

    await expect(page.getByText(/This entry is gone/)).toBeVisible();
  });

  test("a 409 whose effect is present is discarded, and nothing is replayed", async ({ page }) => {
    const retried = await deadLetterRetries(page, (route) =>
      json(route, 409, { outcome: "NOT_DEDUPLICATED", message: NOT_DEDUPLICATED })
    );
    const discarded = await deadLetterDiscards(page, (route) =>
      json(route, 200, { outcome: "DISCARDED" })
    );

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "The effect is present: discard" }).click();

    await expect(page.getByText("Dead letter discarded: its command already ran")).toBeVisible();
    await expect(dialog).toBeHidden();
    expect(retried, "the discard is not a replay").toEqual([RETRY]);
    expect(discarded).toEqual([`/api/admin/dead-letters/${ENTRY.commandId}`]);
  });

  test("a discard of an entry that is gone says so", async ({ page }) => {
    await deadLetterRetries(page, (route) =>
      json(route, 409, { outcome: "NOT_DEDUPLICATED", message: NOT_DEDUPLICATED })
    );
    await deadLetterDiscards(page, (route) => route.fulfill({ status: 404, body: "" }));

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();
    await page.getByRole("dialog").getByRole("button", { name: "The effect is present: discard" }).click();

    await expect(page.getByText(/This entry is gone/)).toBeVisible();
    await expect(page.getByRole("dialog")).toBeHidden();
  });

  test("while a decision is on its way, neither decision can be sent again", async ({ page }) => {
    const retried = await deadLetterRetries(page, (route) =>
      json(route, 409, { outcome: "NOT_DEDUPLICATED", message: NOT_DEDUPLICATED })
    );
    let answer: () => void = () => {};
    const answered = new Promise<void>((resolve) => (answer = resolve));
    const discarded = await deadLetterDiscards(page, async (route) => {
      await answered;
      await json(route, 200, { outcome: "DISCARDED" });
    });

    await page.goto("/admin");
    await page.getByRole("button", { name: "Retry" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "The effect is present: discard" }).click();

    await expect(dialog.getByRole("button", { name: "Discarding…" })).toBeDisabled();
    await expect(dialog.getByRole("button", { name: "The effect is absent: replay" })).toBeDisabled();
    answer();
    await expect(page.getByText("Dead letter discarded: its command already ran")).toBeVisible();
    expect(discarded).toHaveLength(1);
    expect(retried).toEqual([RETRY]);
  });
});
