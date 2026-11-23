# `cloudflare.config.*` evaluation in a dynamic worker (spike)

Status: **PoC, not wired into `of deploy`.** This page records what was
built, what was measured, and what the remaining gaps are. The code lives
in [`spike/config-sandbox/`](../spike/config-sandbox/README.md); the only
production-adjacent change is an opt-in `alias` knob in the bundling wire
(`of-bundler`, used by nothing in the deploy path).

## Why

[`docs/wrangler-config.md`](wrangler-config.md) evaluates a
`cloudflare.config.ts` by running Cloudflare's own `@cloudflare/config`
loader in a Node child process with a fixed cwd, a cleared environment,
a deadline and output caps — but **that is not a sandbox**. The config is
arbitrary code with the invoking user's filesystem, network and process
rights.

This spike answers one question: can the config be *evaluated* inside a
workerd dynamic worker (`workerLoader`), with the resulting value carried
back as JSON and converted on the trusted side, so that the untrusted
code never runs with the user's privileges? It is a link test, not a
migration.

## Chain

```
 cloudflare.config.ts (project code, untrusted)
        │
        │ 1. of-bundler (rolldown, platform=neutral, ESM)
        │    alias: @cloudflare/config$ -> shims/cloudflare-config-public.mjs
        │           "./src/index.js"$ -> generated shim exporting the
        │            absolute specifier (mirrors the Node cf-worker hook)
        ▼
 single ESM module (4,008 B for the repo fixture)
        │
        │ 2. embedded into a bootstrap worker's `workerLoader` call
        │    (WorkerCode: mainModule + config module, limits.cpuMs)
        ▼
 workerd process ── HTTP loopback ──> bootstrap worker
        │                                  │ env.LOADER.get(...)
        │                                  ▼
        │                            dynamic worker (separate isolate)
        │                              wrapper: call default export with
        │                              {mode, isPreview}, normalize
        │                              cf-worker entrypoint, JSON.stringify
        │                                  │
        │                                  └─ 512 KiB payload cap
        ▼
 JSON {config} ── 3. trusted side ──> CloudflareConfigSchema (zod)
                                       -> convertToWranglerConfig()
                                       -> RawConfig
```

Step 1 needs one bundler feature: specifier aliases. `of-bundler` had
none, so this spike added `BundleOpts.alias` /
`BundleOptsSpec.alias` (`vrt-local/src/bundle.rs`,
`vrt-local/src/bundle_spec.rs`) wired to rolldown's `resolve.alias`
(`of-bundler/src/main.rs`). Keys are prefix matches, `key$` is exact
(oxc semantics). Values are absolute files.

Coverage added: `of-bundler` unit tests for a bare alias and a relative
alias, plus `vrt-local/tests/bundle.rs::bundle_alias_rewrites_specifier_before_resolution`,
which runs the real helper through the wire in `cargo test --workspace`.

## Commands

```sh
# helper + deps
cargo build --manifest-path of-bundler/Cargo.toml
pnpm install

# upstream workerd, same upstream commit as the pin (1.20261120.1, darwin arm64)
node spike/config-sandbox/run.mjs --workerd <workerd> --case parity

# pinned fork (linux arm64) under Docker
node spike/config-sandbox/run.mjs \
  --docker-image debian:12-slim \
  --workerd <linux-workerd> \
  --case parity

# every case
node spike/config-sandbox/verify.mjs --workerd <workerd>
```

The runner prints a JSON report with `--json`. It is not part of CI: it
needs a workerd binary and (for a real comparison) `@cloudflare/config`
in `node_modules`. CI covers the bundler change; the sandbox verdict is
local evidence, reproduced below.

## Measured results

Fixture: `vrt-local/tests/fixtures/cf-config` —
`defineConfig(({mode, isPreview}) => ...)`, `bindings.text`, and a
`with { type: "cf-worker" }` entrypoint.

`parity`, upstream workerd 2026-11-20 (darwin arm64):

| step | result | time |
| --- | --- | --- |
| bundle (alias on) | ok, 4,008 B | 101–430 ms (cold/warm helper) |
| workerd start + ready | ok | 105–559 ms |
| dynamic eval -> JSON | HTTP 200 | 25–256 ms |
| trusted zod + convert | ok | 3 ms |
| Node loader baseline | ok | 187–293 ms |
| parity | **identical `RawConfig`** | — |

The sandbox RawConfig and
`node scripts/load-cloudflare-config.mjs ... --mode staging` compared
deep-equal (after key sorting):

```json
{"name":"cf-config-fixture",
 "main":"<fixture>/src/index.js",
 "compatibility_date":"2026-09-17",
 "vars":{"GREETING":"hello","STAGE":"staging","PREVIEW":"production"}}
```

`--preview` reaches `isPreview` the same way (`PREVIEW: "preview"`), and
a missing `--mode` leaves `STAGE` absent, matching the Node path.

Repeating the whole chain with the **pinned fork** (`vrt-20261122-d21616b`,
linux arm64, inside `debian:12-slim`) produced the same identical
RawConfig: bundle 166 ms warm, eval 39 ms. Docker cold start (~9 s,
image + 166 MB binary via the mount) dominates and is not representative
of a local process launch. The fork publishes linux x64/arm64 assets
only, which is why the darwin measurements use the upstream npm build of
the same upstream commit.

### Alias: needed?

Reporting only: bundling the fixture **without** the `@cloudflare/config`
alias also succeeds and produces a byte-identical 4,008 B bundle. The
package declares `"sideEffects": false`, so rolldown drops the Node
loader half of the root entry and only `defineConfig`/`bindings` from
`./public` survive. The alias is still the intended route (it does not
depend on tree-shaking a third-party entry that may start pulling in
`node:module` in a future release), but the spike does not claim the
alias is strictly required today.

## Boundary results

### `node:fs` / host files

`import { readFileSync } from "node:fs"` **bundles** (the helper marks
`node:*` external). The dynamic worker loads it and executes
`readFileSync("/etc/passwd")`; workerd's virtual filesystem returns
`Error: no such file or directory, readAll '/etc/passwd'`. No host bytes
reach the config. Nothing in the bundler rejects the import, so the
isolation comes from workerd's FS, not from a static allowlist.

### Network

`fetch("http://169.254.169.254/latest/meta-data/")` inside the factory
fails: workerd logs `connecting to 169.254.169.254:80 is not allowed`.
A dynamic worker's `globalOutbound` defaults to the null client channel,
so the sandbox is not an SSRF pivot. Caveat: the JS-visible error is an
opaque `internal error; reference = ...`, so callers cannot distinguish
policy denial from a crash; the trusted side must treat any non-200 as a
generic evaluation failure.

### `while (true)`

The request never completes. Two different runtimes behave differently:

- **Upstream workerd (darwin arm64, npm 1.20261120.1):** the dynamic
  worker's `limits: {cpuMs: 250}` is *not* enforced. The process keeps
  spinning at 87–106% CPU for as long as it is left alive; sample at
  t+4 s showed 70.9% and rising.
- **Pinned fork (linux arm64):** the budget is enforced — CPU drops to
  ~0.5% after the limit (the isolate is terminated). But the RPC caller
  (`stub.getEntrypoint().fetch()`) **still never receives an error**;
  the outer request hangs past 30 s.

Control: the fork's per-Worker `limits = (cpuMs = 50)` on a *static*
worker returns HTTP 500 `Worker exceeded CPU time limit.` in 94 ms, so
the fork's enforcer itself works; the gap is the dynamic-worker RPC
outcome. Consequence for any design: **the caller must always impose a
wall-clock deadline and kill the workerd process.** A `workerLoader`
CPU budget alone is not a response-time bound.

`workerLoader` also requires `--experimental`; the fork adds
`maxWorkers` (`(name = "LOADER", workerLoader = (maxWorkers = 32))`),
which the runner omits for upstream compatibility. Production would use
the fork and keep the quota.

## Semantic differences from the Node path

These are the ones that matter for a future implementation; the PoC does
not close them.

1. **Validation placement.** The Node loader calls
   `CloudflareConfigSchema.safeParse` before `convertToWranglerConfig`.
   The worker returns raw JSON, and `convertToWranglerConfig` does **not**
   validate. The trusted side must re-validate (the PoC does) — otherwise
   a malformed config silently converts. Zod issue projection
   (`code`/`path`/`message`, capped) must be reimplemented or delegated.
2. **Nested definitions.** The Node loader resolves *any* config value
   that is a function (`worker`, `containers`, cross-worker `env`
   references) through `createDefinitionResolver`. The PoC only calls
   the top-level factory; `defineWorker(() => ...)`/`defineContainer`
   factories would serialize as `{}`. A faithful implementation has to
   mirror that resolver inside the worker.
3. **`cf-worker` imports.** These are import attributes, not modules.
   The Node hook synthesizes a module exporting the specifier; rolldown
   has no equivalent. The PoC scans the config source and aliases the
   specifier to a generated shim. That requires reading the source in
   the bundler driver and only handles the literal attribute form (no
   computed specifiers).
4. **`process.env`.** The Node path forwards a small allowlist, so a
   config can read `NODE_ENV`/`CI`/etc. workerd has no `process.env`;
   such branches silently take the `undefined` path. A sandbox has to
   decide which env values (if any) are exposed and inject them
   explicitly (e.g. as bindings).
5. **Filesystem and modules.** `import.meta.url`, `__dirname`, and
   relative reads resolve against a virtual FS that contains no project
   files. Configs that read a JSON/TOML file next to themselves (or
   import one) will fail or see nothing. The bundler can inline JSON
   imports, but arbitrary reads cannot work.
6. **`dependencies`.** The Node helper reports every imported file.
   `of` validates and currently discards it, so today this is harmless;
   if watch/rebuild starts using it, the sandbox path has no equivalent
   and the bundler would need to report the module graph.
7. **Module cache.** `loadConfig` re-imports with a `cf-no-cache` UUID;
   `workerLoader.get(name, ...)` caches by name. A repeated evaluation
   must use `load()` (one-shot) or a fresh name/loader id, or it will
   return the first config forever.
8. **Error shape.** The Node path maps zod failures to
   `VRT_CONFIG_INVALID` with structured issues and loader failures to
   `VRT_CONFIG_LOADER_FAILED`/`VRT_CONFIG_LOADER_TIMEOUT`. The sandbox
   path yields (a) bundler errors, (b) an HTTP status plus an opaque
   workerd error string, (c) trusted-side validation issues. Those need a
   stable mapping before `of` can surface them.
9. **Package version.** The PoC bundles and converts with the repo's
   `@cloudflare/config` (`1.0.0-rc.3`) on both sides. `of` must pin the
   same version for the sandbox bundle and the trusted conversion (or a
   schema/conversion drift becomes silent).
10. **Single-file bundles only.** The runner embeds `main_source` only;
    code-split chunks (dynamic `import()`) and rule-typed externals are
    ignored, so a config using them would fail inside the worker. A real
    implementation has to ship every returned module.

The PoC runner prints the evaluated config in its report (values
included). That is acceptable for a local spike only: the Node path is
deliberately value-free in its diagnostics, and a real implementation
must keep it that way (zod issue projection without echoing values).

## Is it worth continuing?

**Yes, with conditions.** The hard part works: a config bundle can be
evaluated in a dynamic worker, the value crosses the boundary as JSON,
and the result is byte-for-byte the same `RawConfig` the Node path
produces — at roughly the same latency (~0.5–1.1 s total; the Node
baseline alone is ~0.2–0.3 s). Isolation is real: no host FS, no
outbound network, and a per-invocation CPU budget on the pinned fork.

What makes it a spike, not a shippable path:

- The caller must bound wall clock and kill workerd; the RPC caller does
  not observe a CPU-limit failure.
- Validation and (ideally) `convertToWranglerConfig` must move to the
  trusted side. `convertToWranglerConfig` is pure JS and could run in the
  worker, but its output is untrusted input to `of`; the smallest safe
  design is: worker returns `CloudflareConfig` JSON, Rust validates, and
  `CloudflareConfig -> RawConfig` is ported (a mechanical mapping that
  is small compared with the existing wrangler normalizer), or a Node
  helper keeps doing only the pure conversion (no untrusted execution).
- The sandbox needs a workerd binary at every place `of deploy` runs.
  The platform already pins the fork for its runtimes, so this is a
  packaging/availability question, not a new dependency.
- `cf-worker` entrypoints, nested factories and `process.env` need
  explicit design (items 2–4 above).

Suggested follow-up (not started): a `of-bundler` "config" mode that
returns the bundle plus the cf-worker specifier map; a one-shot
`workerLoader.load()` evaluation with a fresh loader id; trusted-side
validation with the same error codes as today; a `VRT_WORKERD_BIN`-gated
integration test next to the existing real-workerd suites.

## Changelog

- 2026-11-23: initial spike. `of-bundler` gains an opt-in specifier
  alias option with unit + wire tests; PoC runner under
  `spike/config-sandbox/`; measured bundle/eval/convert parity against
  the Node loader, plus FS, network and CPU-limit boundary behaviour on
  upstream workerd and the pinned fork.
