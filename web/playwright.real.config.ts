import { defineConfig, devices } from "@playwright/test";

const environmentBaseUrl = process.env.STS_BASE_URL;
const sharedUse = {
  ...(environmentBaseUrl ? { baseURL: environmentBaseUrl } : {}),
  acceptDownloads: true,
  screenshot: "only-on-failure" as const,
  trace: "retain-on-failure" as const,
  video: "retain-on-failure" as const,
};

export default defineConfig({
  testDir: "./tests",
  testMatch: "real-backend.spec.ts",
  fullyParallel: false,
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  timeout: 10 * 60_000,
  expect: {
    timeout: 30_000,
  },
  reporter: "list",
  outputDir: "test-results/real-backend",
  use: sharedUse,
  projects: [
    {
      name: "real-desktop-chromium",
      grep: /@desktop/,
      use: {
        ...devices["Desktop Chrome"],
        ...sharedUse,
        viewport: { width: 1280, height: 800 },
      },
    },
    {
      name: "real-compact-chromium",
      grep: /@compact/,
      use: {
        ...devices["Desktop Chrome"],
        ...sharedUse,
        viewport: { width: 768, height: 900 },
      },
    },
  ],
});
