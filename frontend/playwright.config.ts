import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "tests/e2e",

  /* Timeout for each expect() assertion — backend may need a moment */
  expect: {
    timeout: 10_000,
  },

  /* Global test timeout */
  timeout: 30_000,

  /* Reporter */
  reporter: "list",

  use: {
    baseURL: "http://localhost:3000",
    viewport: { width: 1280, height: 720 },
    /* Collect trace on first retry to help diagnose CI failures */
    trace: "on-first-retry",
  },

  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],

  webServer: {
    command: "npm run dev",
    url: "http://localhost:3000",
    // Locally a dev server that is already running is used as it is. In CI the suite starts its
    // own, unless E2E_REUSE_SERVER=1 says that the frontend under test is already up at the URL
    // (the Docker Compose stack of the README's Quick Start).
    reuseExistingServer: !process.env.CI || process.env.E2E_REUSE_SERVER === "1",
    timeout: 120_000,
    stdout: "pipe",
    stderr: "pipe",
  },
});
