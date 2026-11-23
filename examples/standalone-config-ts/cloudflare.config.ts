// Minimal standalone project using the programmatic config format.
//
// `of deploy` evaluates this file with `@cloudflare/config` (the trusted
// Node loader by default) and normalizes the result exactly like a
// wrangler file; `--config-sandbox=worker` evaluates it in a workerd
// sandbox instead. Both paths must produce the same RawConfig — the
// `config_ts_example_sandbox_parity` test asserts that on this project.
import { bindings, defineConfig, triggers } from "@cloudflare/config";
import * as entrypoint from "./src/index.js" with { type: "cf-worker" };

export default defineConfig(({ mode, isPreview }) => ({
  worker: {
    name: "config-ts",
    compatibilityDate: "2026-09-17",
    entrypoint,
    triggers: [triggers.fetch({ pattern: "config-ts.vrt.local/*" })],
    env: {
      GREETING: bindings.text("hello from cloudflare.config.ts"),
      STAGE: mode === "staging" ? bindings.text("staging") : undefined,
      PREVIEW: bindings.text(isPreview ? "preview" : "production"),
    },
  },
}));
