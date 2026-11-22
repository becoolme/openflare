import { bindings, defineConfig } from "@cloudflare/config";
import * as entrypoint from "./src/index.js" with { type: "cf-worker" };

// Fixture for vrt-local's `cf_config_real_fixture_loads` test: it proves the
// official @cloudflare/config loader path end to end (mode branching, text
// bindings, cf-worker entrypoint synthesis) without executing the entrypoint.
export default defineConfig(({ mode }) => ({
  worker: {
    name: "cf-config-fixture",
    compatibilityDate: "2026-09-17",
    entrypoint,
    env: {
      GREETING: bindings.text("hello"),
      STAGE: mode === "staging" ? bindings.text("staging") : undefined,
    },
  },
}));
