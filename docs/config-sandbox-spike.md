# `cloudflare.config.*` evaluation in a workerd sandbox

Status: **opt-in implementation.** `of deploy` / `of check` accept
`--config-sandbox=worker`; the default remains `node`, so existing
projects and scripts keep their current behaviour. The sandbox runs the
config bundle in a workerd dynamic worker — no host filesystem, no
outbound network — and keeps validation and `convertToWranglerConfig` on
the trusted side.

Code:

- `scripts/load-cloudflare-config-worker.mjs` — production driver
  (bundle → dynamic worker → JSON → trusted zod/convert);
- `scripts/load-cloudflare-config.mjs` — the Node loader (default path,
  unchanged);
- `scripts/shims/cloudflare-config-public.mjs` — Workers-safe
  `@cloudflare/config` shim;
- `vrt-local/src/project_cfg.rs` — sandbox selection, helper invocation,
  deadline/error mapping;
- `vrt-local/tests/config_sandbox.rs` and
  `spike/config-sandbox/verify.mjs` — the test matrix.

## Why

The Node path (`docs/wrangler-config.md`) is a *restricted child
process*, not a sandbox: a `cloudflare.config.ts` is arbitrary Node code
with the invoking user's filesystem, network and process rights. The
worker sandbox removes those rights from the untrusted evaluation step
without changing the resulting `RawConfig`.

## Chain

```
 cloudflare.config.ts (project code, untrusted)
        │
        │ 1. of-bundler (rolldown, platform=neutral, ESM)
        │    alias @cloudflare/config$  -> scripts/shims/cloudflare-config-public.mjs
        │    alias <cf-worker spec>$   -> generated module exporting the path
        ▼
 ESM modules (main + dynamic chunks)
        │
        │ 2. embedded into a bootstrap worker's workerLoader call
        ▼
 workerd process ── HTTP loopback ──> bootstrap worker
        │                                  │ env.LOADER.get(...)
        │                                  ▼
        │                            dynamic worker (separate isolate)
        │                              sandbox-env.js sets a frozen process
        │                              wrapper mirrors Cloudflare's definition
        │                              resolver (nested factories), JSON
        │                                  │
        │                                  └─ 512 KiB payload cap
        ▼
 JSON {config, issues} ── 3. trusted side ──> CloudflareConfigSchema (zod)
                                             -> convertToWranglerConfig()
                                             -> RawConfig
```

`scripts/load-cloudflare-config-worker.mjs` prints the same
`{raw, dependencies}` document on stdout and uses the same stderr
contract (`{error, message, issues?}` plus `phase`) as the Node helper.
`project_cfg.rs` therefore parses both paths with one implementation and
maps failures with one table.

## Usage

```sh
# default: Node loader (unchanged)
of deploy --root .vrt ./worker

# sandboxed evaluation
VRT_WORKERD_BIN=/path/to/workerd \
  of deploy --root .vrt --config-sandbox=worker ./worker

# pre-flight only
of check --config-sandbox=worker ./worker
```

`--env <name>` still maps to the loader `mode`, and `--preview` still
sets `isPreview`. Both flags are forwarded unchanged in either sandbox,
including remote (`--url`) and `--dry-run` packing. `vrt.toml` and
wrangler files are read, never evaluated, so the flag does not affect
them.

### Requirements and knobs

| What | Value |
| --- | --- |
| workerd binary | `VRT_WORKERD_BIN` (then `OF_WORKERD_BIN`); no fallback to the Node path. The binary must support `workerLoader`; the driver runs it with `--experimental` |
| bundler | resolved like every other bundle (`OF_BUNDLER`, embedded, in-repo, co-installed) and passed to the driver |
| sandbox driver override | `VRT_CF_CONFIG_SANDBOX_LOADER` (tests/ops) |
| one-evaluation deadline | `VRT_CF_CONFIG_SANDBOX_TIMEOUT_MS`, 100..30000 ms, default **5000 ms** |
| whole-helper deadline | `VRT_CF_CONFIG_LOADER_TIMEOUT_MS`, 100..600000 ms, default **60000 ms**; worker mode raises it to `sandbox deadline + 45 s` when that is larger |
| config source cap | 256 KiB |
| bundle cap | 8 MiB of emitted JS before embedding |
| result payload cap | 512 KiB |
| bundle / startup caps | 30 s / 15 s |

If workerd is missing the command fails closed with
`VRT_CONFIG_LOADER_FAILED` and tells the operator to install workerd or
use `--config-sandbox=node`; it never silently evaluates in Node.

## Timeout and isolation

The binding constraint is the caller's wall clock. Measurements on the
pinned fork show that `limits.cpuMs` terminates a `while (true)` isolate
(CPU drops to ~0.5%), but the `stub.getEntrypoint().fetch()` RPC **never
completes and never returns an error** — the outer request hangs. A CPU
budget alone is therefore not a response-time bound.

The driver handles this explicitly:

1. the outer `fetch` is aborted at `--timeout-ms`;
2. workerd is spawned `detached` (its own process group) and the whole
   group is `SIGKILL`ed on timeout, on helper exit, and from the driver's
   `exit` handler;
3. the caller receives `VRT_CONFIG_LOADER_TIMEOUT` with the deadline and
   the env knob — never a hang.

The dynamic worker also gets `limits: { cpuMs }` equal to the deadline as
a backstop. Upstream workerd ignores it; the pinned fork enforces it. It
is a defence in depth, not the guarantee.

Isolation results (reproduced by `verify.mjs`):

- `readFileSync("/etc/passwd")` fails with workerd's virtual-FS
  `no such file or directory`; no host bytes reach the config;
- `fetch("http://169.254.169.254/…")` and a raw
  `cloudflare:sockets` `connect()` to the same address both fail: a
  dynamic worker's `globalOutbound` defaults to the null client channel,
  so the sandbox is not an SSRF pivot. The JS-visible fetch error is an
  opaque `internal error; reference = …`; sockets report
  `cannot connect to the specified address`. Callers see a generic
  evaluation failure either way;
- the dynamic worker gets no bindings, secrets or inherited env beyond
  the explicit `process.env` map described below.

## `process.env` (decision)

workerd has no `process`. Rather than silently taking the
`undefined` branch, the sandbox **injects exactly the same allowlisted
environment the Node child receives** as a frozen `process.env`
(`CF_CHILD_ENV_ALLOW` in `project_cfg.rs`, mirrored in the driver),
plus `process.cwd()` (the canonical project directory) and
`process.platform`. `sandbox-env.js` is imported before the config
module, so top-level `process.env` reads work too.

Why this choice:

- **Parity.** A config that branches on `TZ`, `LANG`, `HOME` or
  `TMPDIR` behaves the same in both sandboxes. The Node child already
  clears everything else (`NODE_OPTIONS`, credentials, `VRT_*`), so the
  boundary is unchanged.
- **Explicit over absent.** Silent `undefined` would make a config deploy
  differently based on which flag was passed.
- **Defence in depth.** The driver re-filters before embedding the values;
  a non-allowlisted variable that somehow reaches the helper's own
  environment still never reaches the config (covered by a test).

`NODE_ENV` and `CI` are **not** allowlisted (they are not in the Node
child either). A config that needs build-time values should use
`defineConfig`'s `mode`/`isPreview` arguments, which are the supported
inputs in both paths. The `process` object has only `env`, `cwd`,
`platform`; other APIs (`exit`, `nextTick`, `argv`, …) are absent and a
config that calls them fails the evaluation.

## Nested factories

Cloudflare's loader resolves *any* definition — top-level, `worker`,
`containers`, cross-worker `env` references — through
`createDefinitionResolver` inside the config's process. Functions cannot
cross the JSON boundary, so the worker wrapper mirrors that resolver
(`normalizeWorkerEntrypoint`, `normalizeWorkerEnv`,
`normalizeContainerReferences`, `resolveAndParseConfig`) and sends the
resolved value plus any custom container issues. The trusted side then
runs the real `CloudflareConfigSchema` and merges the reported issues.

Covered by tests:

- `defineWorker(() => …)` and `defineContainer({…})` resolve;
- `exports: durableObject({ container })` becomes the container **name**;
- a container not listed in `containers` or passed as a string reports a
  `VRT_CONFIG_INVALID` issue with the same path the Node loader uses.

## `cf-worker` imports

`with { type: "cf-worker" }` is an import attribute, not a module. The
driver scans the config source for the literal form, resolves the
specifier, and aliases it to a generated module exporting the absolute
path. The referenced entry is **never bundled or executed** (the test
entrypoint throws if evaluated).

Only the literal attribute form is supported. A computed attribute
(`import(spec, { with: { type: "cf-worker" } })`) fails closed with
`VRT_CONFIG_LOADER_FAILED` instead of accidentally bundling the entry
module.

## `dependencies`

The driver reports every project source file rolldown put in the graph
(entry, relative imports, JSON modules, dynamic `import()` chunks) as a
sorted absolute list, excluding `node_modules` packages and the generated
shims. Static-import parity with the Node loader is covered by the
verification matrix; a dynamically imported source is reported by the
sandbox and not by the Node loader, which is a deliberate superset.
Modules that are fully tree-shaken are not part of the evaluated code and
are not reported.

`of` currently discards the list after validation (same as the Node
path); the sandbox makes it available so the later cache/reload work does
not need a second bundler change.

## Error mapping

The helper's stderr is a single JSON line; `project_cfg.rs` honours only
the public codes it defines.

| Phase | Helper detail | Public error | Exit |
| --- | --- | --- | --- |
| config file unreadable / wrong extension / too large | `VRT_CONFIG_LOADER_USAGE` | `VRT_CONFIG_LOADER_FAILED` | 3 |
| bundling (`of-bundler` error, unsupported cf-worker form) | bundle message + `phase: "bundle"` | `VRT_CONFIG_LOADER_FAILED` | 3 |
| workerd missing / cannot start / never listens | `phase: "startup"` | `VRT_CONFIG_LOADER_FAILED` | 3 |
| config factory throws / module load error | `phase: "eval"` + capped message | `VRT_CONFIG_LOADER_FAILED` | 3 |
| workerd aborts during eval (failed allocation, OOM kill) | `phase: "eval"` + exit code/signal + last workerd log line | `VRT_CONFIG_LOADER_FAILED` | 3 |
| non-200 inner response | status + capped message | `VRT_CONFIG_LOADER_FAILED` | 3 |
| payload above 512 KiB | `VRT_CONFIG_SANDBOX_PAYLOAD_TOO_LARGE` | `VRT_CONFIG_LOADER_FAILED` | 3 |
| wall-clock deadline | `VRT_CONFIG_LOADER_TIMEOUT` | `VRT_CONFIG_LOADER_TIMEOUT` | 3 |
| zod / container issues | `VRT_CONFIG_INVALID` + issues | `VRT_CONFIG_INVALID` | 2 |
| `convertToWranglerConfig` throws | `phase: "convert"` | `VRT_CONFIG_LOADER_FAILED` | 3 |
| helper itself died / bad stdout | no typed code | `VRT_CONFIG_LOADER_FAILED` | 3 |

Diagnostics are capped and value-free for schema failures (issue
`path`/`code`/`message` only). A message thrown by the config's own code
is capped text chosen by the config author; it is never the evaluated
config object. The helper never prints the config payload.

## Semantic differences from the Node path

These are deliberate and documented; everything else is expected to be
identical.

1. **Isolation.** The config cannot read host files, open sockets or
   spawn processes. A config that needs either is unusable in worker
   mode; it fails with `VRT_CONFIG_LOADER_FAILED`, not a silent fallback.
2. **`process`.** Only `env` (allowlisted), `cwd()` and `platform` exist.
   `process.argv`, `process.exit`, timers beyond workerd's globals and
   Node built-ins that need privileges are absent.
3. **Package version.** The sandbox bundles `@cloudflare/config` from
   the `of` install (repo pin `1.0.0-rc.3`) and converts with the same
   copy. The Node path uses the project's installed package and requires
   it to be resolvable from the config file. Target the version pinned by
   `of` when using the sandbox.
4. **Files and module URLs.** There is no host filesystem. Configs that
   read a JSON/TOML file next to themselves (or `import.meta.url`/resolve
   paths against a real file) fail or see nothing; importing a JSON module
   through the bundler still works.
5. **`cwd`.** `process.cwd()` is the canonicalized project directory;
   the dynamic worker's real FS is empty.
6. **Validation placement.** The worker returns raw JSON; zod runs in
   the trusted driver. `convertToWranglerConfig` never sees untrusted
   code, only validated data.
7. **One-shot evaluation.** `workerLoader.get("cf-config", …)` is called
   once per helper process; no module cache is reused across deploys.
8. **Environment.** Same allowlist as the Node child; no
   `NODE_ENV`/`NODE_PATH`/credentials.
9. **Cost.** One workerd start + bundle per config evaluation
   (~2 s warm on a developer machine; the Node path is ~0.1 s). It runs
   once per deploy/check, not per request.

## Tests

```sh
# whole matrix (needs a workerd binary + built of-bundler)
node spike/config-sandbox/verify.mjs --workerd /path/to/workerd

# Rust integration (CI `real-workerd` job sets VRT_WORKERD_BIN)
cargo test -p vrt-local --test config_sandbox

# bundler wire + dependency reporting
cargo test --manifest-path of-bundler/Cargo.toml
```

`verify.mjs` drives both production helpers and asserts:

| case | assertion |
| --- | --- |
| parity | staging / preview / no-mode `RawConfig` and `dependencies` deep-equal to Node |
| nested factories | `defineWorker` + `defineContainer` resolve; container reference becomes its name; parity |
| env injection | allowlisted `TZ` reaches both; non-allowlisted values never cross |
| cf-worker | entrypoint becomes `main`; the throwing entry module never executes; parity |
| dependencies | imported project helper is reported by both paths |
| dynamic import | a `import()` chunk is shipped, evaluated, and its source reported |
| host FS | `readFileSync` fails with `VRT_CONFIG_LOADER_FAILED` |
| network | `fetch` and `cloudflare:sockets` connects fail with `VRT_CONFIG_LOADER_FAILED` |
| oversized payload | 600 KiB result is refused before the trusted side buffers it |
| invalid config | `VRT_CONFIG_INVALID`, issue path `worker.name` |
| `while (true)` | `VRT_CONFIG_LOADER_TIMEOUT` within a bounded wall clock, no hang |
| missing workerd | fail closed with an actionable message |
| repeat + concurrency | 10 sequential + 2x2 concurrent runs succeed; no `of-cfg-sandbox-*` dir or workerd process left behind |
| memory exhaustion | an impossible `ArrayBuffer` fails closed with `VRT_CONFIG_LOADER_FAILED` inside the deadline (never succeeds, never hangs) |
| real example parity | `examples/standalone-config-ts` is deep-equal to the Node path (RawConfig + normalized), incl. routes and `main` |

`vrt-local/tests/config_sandbox.rs` additionally runs the 10x repeat and
both concurrency shapes (private roots and one shared `TMPDIR`) through
the real `of check --config-sandbox=worker` CLI, attributing leftover
workerd processes by the sandbox work dir they run in.

CI: the `real-workerd` job builds `of-bundler`, fetches the pinned
workerd, runs `cargo test -p vrt-local --test config_sandbox` through
`scripts/ci-real-workerd.sh`, then runs `verify.mjs`. The sandbox tests
self-skip when `VRT_WORKERD_BIN` is unset (same convention as the other
real-workerd suites), but that script exports `VRT_REQUIRE_WORKERD=1`
for this target: in CI a missing workerd or `of-bundler` **fails** the
job instead of turning the suite into silent skips.

## Known gaps

- **Not the default.** `node` stays the default until the sandbox has
  run against real projects; `worker` is opt-in per command.
- **workerd must be installed wherever deploys run.** It is the same
  pinned binary `of serve` uses, but the sandbox does not download it.
- **Package pinning.** A project that pins a different
  `@cloudflare/config` version than `of` may see `defineConfig`/schema
  drift; the sandbox uses `of`'s copy (documented above).
- **No partial fallback.** A config that needs Node-only APIs fails in
  worker mode; it is not retried in Node (fail closed).
- **Dependencies are reported but not consumed.** Watch/reload still
  needs the consumer side; the sandbox exposes the list.
- **Memory is not capped independently.** The wall-clock deadline and
  workerd's isolate limits bound CPU, and the matrix proves an *impossible*
  allocation fails closed (a catchable `RangeError` or a workerd abort,
  both mapped to `VRT_CONFIG_LOADER_FAILED`). workerd exposes no
  per-isolate memory limit, so a config that commits a large but
  satisfiable amount of memory is bounded only by the deadline, not by a
  quota.
- **Outer-deadline edge case.** The driver kills workerd's process group
  on every normal/timeout path. If the driver itself had to be SIGKILLed
  by the outer helper deadline (90 s+), a detached workerd could outlive
  it; the inner phases (30 s bundle / 15 s startup / 5 s eval) keep that
  path unreachable in practice.
- **Ephemeral port selection is not reserved.** `freePort()` binds and
  releases before workerd binds. The readiness probe carries a per-run
  nonce, so a stolen port fails startup with `VRT_CONFIG_LOADER_FAILED`
  (bounded by the 15 s startup timeout) instead of POSTing the config into
  another sandbox; it is still a rare spuriously-failed run rather than a
  retry.
- **Output cap is a hard failure.** A config materially larger than
  512 KiB must be split; the sandbox does not stream.

## Changelog

- 2026-11-23: robustness coverage. Adds an
  `examples/standalone-config-ts` real project and asserts Node/worker
  parity on its RawConfig *and* normalized output; adds 10x repeat,
  private-root and shared-root 2-way concurrency, impossible-allocation
  fail-closed, and residue checks (temp dirs + workerd processes) to the
  Rust suite and `verify.mjs`. A workerd crash during eval now reports
  its exit code/signal and last log line (with an allocation/OOM marker)
  instead of a bare `fetch failed`. CI's real-workerd job exports
  `VRT_REQUIRE_WORKERD=1` so this suite fails instead of skipping when
  the prerequisites are missing.
- 2026-11-23: implementation. `of deploy` / `of check` gain
  `--config-sandbox=node|worker` (default `node`); the worker path
  bundles with `of-bundler`, evaluates in a workerd dynamic worker with
  a 5 s caller deadline that kills the workerd process group, mirrors
  Cloudflare's nested-definition resolver, injects the allowlisted
  `process.env`, synthesises `cf-worker` specifiers, reports project
  dependencies, and maps every failure to the existing typed codes.
  Adds the Rust sandbox matrix, `verify.mjs`, CI coverage and docs.
- 2026-11-23: initial spike. `of-bundler` gains an opt-in specifier
  alias option with unit + wire tests; PoC runner under
  `spike/config-sandbox/`; measured bundle/eval/convert parity against
  the Node loader, plus FS, network and CPU-limit boundary behaviour on
  upstream workerd and the pinned fork.
