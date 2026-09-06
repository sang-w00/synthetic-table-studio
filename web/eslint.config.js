import eslint from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

// eslint-plugin-react-hooks is a declared devDependency, but a checkout whose
// node_modules predates it must still lint; the hooks rules are added only when the
// plugin resolves.
let reactHooks = null;
try {
  reactHooks = (await import("eslint-plugin-react-hooks")).default;
} catch {
  console.warn("eslint-plugin-react-hooks is not installed; skipping react-hooks rules.");
}

export default tseslint.config(
  {
    ignores: ["dist", "playwright-report", "test-results"],
  },
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["src/**/*.{ts,tsx}"],
    languageOptions: {
      globals: globals.browser,
    },
  },
  ...(reactHooks
    ? [{
        files: ["src/**/*.{ts,tsx}"],
        plugins: { "react-hooks": reactHooks },
        rules: {
          "react-hooks/rules-of-hooks": "error",
          "react-hooks/exhaustive-deps": "warn",
        },
      }]
    : []),
  {
    files: ["*.config.{js,ts}", "tests/**/*.ts"],
    languageOptions: {
      globals: globals.node,
    },
  },
);
