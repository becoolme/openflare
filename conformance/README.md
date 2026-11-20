# Binding-layer conformance

## Current coverage

Executable cases cover KV, R2, D1, Queues, Cache, Cron, Durable Objects,
static Assets, plain Vars, and the basic Rate Limiting binding result.
The areas that need infrastructure or protocols the runner cannot yet
exercise are listed in [`UNVERIFIED.md`](UNVERIFIED.md), including service
bindings, Hyperdrive, Secrets, WebSockets, and Workflows. A listed case
does not claim the entire binding API is covered.

OpenFlare runs unmodified workerd, so the workerd runtime itself does
not need re-testing. What this suite proves is the **binding layer**:
facade (vendored `KVNamespace` / `R2Transport` / d1-api / queue-broker
shims), `vrt-provider`, `vrt-sql`, DO storage, the queue dispatcher,
and the cron runner behave the way Cloudflare documents.

Cases are **golden-behavior fixtures**: every `expect` must cite where
the expected value comes from (Cloudflare docs URL or a repo test
file) in the case's `evidence` list. Behavior with no citation goes in
[`UNVERIFIED.md`](UNVERIFIED.md), never into an expectation.

## Layout

```
conformance/
  run.mjs                 the runner (pure Node, importable lib + CLI)
  deploy-standalone.sh    deploy hook for the docker compose stack
  cases/<binding>/<id>/   one deployable worker project per case
    case.json             declarative steps + expected responses + evidence
    worker.js             the probe worker
    wrangler.jsonc|vrt.toml  deploy manifest (bindings + hostnames/routes)
  real-projects.json      pinned OSS Workers projects for manual regression
  results/                generated reports (latest.json + latest.md)
  test/                   node:test unit tests (no workerd needed)
```

`conformance/` is deliberately **not** a workspace member — nothing
here affects `cargo test --workspace`.

## Running

Against the standalone compose stack (real workerd):

```sh
cp /path/to/workerd deploy/standalone/workerd   # pinned, digest-checked
WORKERD_SOURCE=file WORKERD_SHA256=<from deploy/docker/workerd.lock> \
  docker compose -f deploy/standalone/docker-compose.yml up --build --wait

node conformance/run.mjs \
  --url http://127.0.0.1:8080 \
  --deploy-cmd ./conformance/deploy-standalone.sh
```

The runner stages each case directory (manifest + worker modules,
minus `case.json`/docs), calls the deploy command once per case with
`CASE_DIR`/`CASE_ID`/`CASE_HOSTNAME` in the environment, waits for the
first step to pass within the case's readiness budget, then runs the
steps and writes `results/latest.json` + `results/latest.md`.

Options (`node conformance/run.mjs --help`):

| flag | meaning |
| --- | --- |
| `--url <base>` | ingress base URL (required) |
| `--deploy-cmd <shell>` | deploy hook; `$CASE_DIR` is the staged dir |
| `--no-deploy` | skip deploys; cases assumed already live |
| `--host <name>` | override the Host header for every request |
| `--stub` | the stack runs the stub runtime; `requiresWorkerd` cases report `skipped` (validates the runner itself without workerd) |
| `--only <regex>` | run a subset of case ids |
| `--ready-timeout <ms>` | per-case readiness budget (default 90 s) |
| `--timeout <ms>` | per-request timeout (default 15 s) |
| `--fail-fast` | stop a case at its first failed step |
| `--fail-on-skip` | exit 1 when any case is skipped (release gate: a skipped case did not prove its binding) |
| `--list` | print `real-projects.json` entries and exit |
| `--validate` | validate all manifests and exit |

Exit code: `0` when nothing failed (skips are fine unless
`--fail-on-skip` is passed), `1` on any failed step (or on a skip with
`--fail-on-skip`), `2` on usage/manifest errors.

## Release gate

`.github/workflows/conformance.yml` runs this suite weekly and on
`workflow_dispatch`: it builds the standalone compose stack with the
workerd binary pinned in `deploy/docker/workerd.lock`, deploys every
case, and runs with `--fail-on-skip`. A release must cite a green run of
that workflow (attach the `conformance-report` artifact); the per-PR
`ci` job only runs the runner's unit tests, which never touch a stack.

## Adding a case

1. `conformance/cases/<binding>/<id>/` — `<id>` is lower-kebab and must
   equal `case.json`'s `id`; the `<binding>` dir name must equal the
   case's `binding` field.
2. Write `worker.js` (the probe) and a manifest (`wrangler.jsonc` or
   `vrt.toml`) with a unique `name` and a `*.vrt.local` route/hostname.
   The manifest's first route/hostname is used for the `Host` header
   unless `case.json` sets `hostname` or `--host` is given.
3. Write `case.json`: `schemaVersion: 1`, `id`, `binding`, `title`,
   `requiresWorkerd`, `evidence` (doc URLs / repo test paths — at
   least one, required), and `steps`: `[{name, request, expect,
   retry?}]`.
   - `request`: `method` (default GET), `path`, `headers`, and one of
     `body` / `json` / `bodyBase64`.
   - `expect`: `status` (int or list), `headers` (exact,
     `{contains}`, `{matches}`, `{absent:true}`), `body`,
     `bodyContains`, `bodyMatches` (regex), `json` (recursive subset:
     objects recurse key-wise and ignore extra keys, arrays must have
     the same length and subset-match element-by-element),
     `jsonEquals` (exact, order-insensitive for objects).
   - `retry: {timeoutMs, intervalMs}` re-runs the request until the
     expect passes — use it for eventually-consistent effects (queue
     delivery, cron ticks, alarms). Step 0 always retries up to the
     case's readiness budget after a deploy.
4. Prefer **normalized responses**: the worker maps volatile values
   (etags, epochs, durations) onto stable booleans/shapes so the JSON
   expectation stays deterministic.
5. `node conformance/run.mjs --validate` must pass; `npm test` checks
   every manifest.

## Reading the report

`results/latest.json` — one record per case:
`{id, binding, status: passed|failed|skipped, reason?, steps:[{name,status,failures?,actual?}]}`.
`results/latest.md` — the same as a Markdown table plus a failure
detail section. `skipped` means the case could not run here (stub
stack), not that it passed.

## Changelog

- 2026-11-20: Added basic Assets, Vars, and Rate Limiting fixtures; listed
  unsupported multi-worker, database, secret, WebSocket, workflow, and
  advanced asset behaviors as unverified.

## real-projects.json

Pinned public Workers projects for manual regression. The runner does
not fetch them automatically; to run one: clone at the listed commit,
drop it onto a stack (`vrt deploy` in the compose `vrt` container —
see `deploy/standalone/README.md`), and compare behavior against the
recorded `expect` (`works|partial|fails|unknown`). Update the entry's
`expect`/`notes` when verified. `node conformance/run.mjs --list`
prints the manifest.
