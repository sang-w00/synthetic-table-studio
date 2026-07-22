import { expect, test } from "@playwright/test";

test("mounts the accessible application shell", async ({ page }) => {
  await page.goto("/");

  await expect(page).toHaveTitle("Synthetic Table Studio");
  await expect(
    page.getByRole("heading", {
      level: 1,
      name: "A careful workspace for synthetic data",
    }),
  ).toBeVisible();
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("status")).toContainText("Interface ready");

  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to main content" })).toBeFocused();
});
