import { expect, test } from "@playwright/test";

test("mounts the accessible application shell", async ({ page, browserName }) => {
  await page.route("**/api/v1/bootstrap", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ready",
        host_resources: {
          platform_system: "Darwin",
          platform_machine: "arm64",
          logical_cpu_count: 8,
          total_memory_bytes: 16 * 1024 ** 3,
          available_memory_bytes: 8 * 1024 ** 3,
          disk_total_bytes: 512 * 1024 ** 3,
          disk_free_bytes: 256 * 1024 ** 3,
          gpu_backend: "none",
          gpu_device_count: 0,
          gpu_name: null,
          gpu_memory_total_bytes: null,
        },
        resource_plan: {
          resource_profile: "auto_cpu",
          recommended_device: "cpu",
          worker_lease_bytes: 4 * 1024 ** 3,
          utility_max_rows: 250_000,
          duckdb_memory_limit_bytes: 2 * 1024 ** 3,
          max_concurrent_jobs: 1,
          disk_free_bytes: 256 * 1024 ** 3,
        },
      }),
    }),
  );
  await page.route("**/api/v1/datasets?limit=1", (route) =>
    route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({ version: "1.0", datasets: [] }),
    }),
  );
  await page.goto("/");

  await expect(page).toHaveTitle("Synthetic Table Studio");
  await expect(
    page.getByRole("heading", {
      level: 1,
      name: "Synthetic Table Studio",
    }),
  ).toBeVisible();
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("status")).toContainText("준비되었습니다");

  await page.keyboard.press(browserName === "webkit" ? "Alt+Tab" : "Tab");
  await expect(page.getByRole("link", { name: "Skip to main content" })).toBeFocused();
});
