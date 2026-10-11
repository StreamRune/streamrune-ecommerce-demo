/**
 * The first render in the browser matches what the server rendered.
 *
 * The role and the identity live in the browser's localStorage, which the
 * server does not see. A page that read them while it hydrated would render
 * another tree than the server sent and React would report a hydration error
 * and build the page again. The role provider reads them as an external store
 * instead: the server's snapshot for the hydration, the browser's right after.
 *
 * Mocked backend — always runs. Against a production build the error is React's
 * minified #418; in development it is spelled out.
 */

import { test, expect, type Page } from "@playwright/test";
import { actAs, mockBackendRoutes } from "./helpers";

const HYDRATION_ERROR = /hydrat|Minified React error #(418|423|425)/i;

/** Every console error and uncaught error of the page, from this call on. */
function collectErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  page.on("pageerror", (error) => errors.push(error.message));
  return errors;
}

for (const role of ["ADMIN", "CUSTOMER", "GUEST"] as const) {
  test(`the first load as ${role} reports no hydration error and shows that role`, async ({
    page,
  }) => {
    await mockBackendRoutes(page);
    await actAs(page, role);
    const errors = collectErrors(page);

    for (const path of ["/", "/orders"]) {
      await page.goto(path);
      // The role switcher shows the role the browser holds, not the server's default.
      await expect(page.getByRole("combobox").first()).toContainText(role);
      // An error of the hydration is reported right after it; give it a moment.
      await page.waitForTimeout(500);
    }

    expect(errors.filter((error) => HYDRATION_ERROR.test(error))).toEqual([]);
  });
}
