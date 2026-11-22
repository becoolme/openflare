# OpenFlare

[![CI](https://github.com/becoolme/openflare/actions/workflows/ci.yml/badge.svg)](https://github.com/becoolme/openflare/actions/workflows/ci.yml)

OpenFlare is a self-hostable runtime for Cloudflare Workers–style applications.
It runs unmodified [`workerd`](https://github.com/cloudflare/workerd) (the
official open-source Workers runtime) and adds what workerd alone does not
provide: a management plane, signed deployments, bindings (KV, R2, D1, Queues,
Durable Objects, Workflows, Cron, Hyperdrive, rate limiting, static assets) and
per-tenant process isolation.

You can deploy a project straight from its `wrangler.toml` / `wrangler.json`.
The standalone mode runs the whole stack with one `docker compose up`.

> Status: early, actively developed. The wire formats and CLI may still change.

## Deployment modes

| | Standalone | Kubernetes + control plane |
| --- | --- | --- |
| Good for | local development, CI, a single owner on one machine | multi-cell, production deployments |
| Control plane | `vrt-local` in this repo (`vrt` CLI, `docker compose`) | [`openflare-control`](https://github.com/becoolme/openflare-control): signed desired snapshots, Postgres store, cell API |
| State | a local directory or S3 | Postgres plus S3/filesystem |
| Deploy | `docker compose up` or `vrt dev` | kustomize manifests from the control repository |
| Trust | one trusted owner | one owner per control plane; cells verify Ed25519-signed snapshots |

Both modes run the same `vrt-host`, `vrt-provider`, `vrt-sql` and
`cell-controller` components and the same signed-snapshot wire format.

- **Standalone** — the fastest way to try the runtime: `docker compose -f
  deploy/standalone/docker-compose.yml up --build --wait` (see
  [Quick start](#quick-start-docker-standalone)), or `vrt dev` for a
  no-container loop.
- **Kubernetes** — a Postgres-backed control plane publishes signed
  `compute.desired` documents to one or more cells. Manifests, the reference
  plane and the cell API live in
  [`openflare-control`](https://github.com/becoolme/openflare-control).

The Kubernetes manifests are intentionally not part of this repository: the
runtime stays a standalone-clean checkout, and the control plane releases on
its own cadence.

## Why

- **Real workerd, not an emulator.** Workers run on the same runtime
  Cloudflare open-sources, pinned by digest (`deploy/docker/workerd.lock`).
- **Signed desired state.** Every deployment is a content-addressed version
  bundle referenced by a signed, revision-fenced snapshot. Components verify
  signatures before applying anything and fail closed. A replayed or tampered
  envelope is rejected, not applied.
- **Per-tenant isolation.** `vrt-host` supervises one workerd process per
  tenant, with its own config, sockets and limits. Ingress routes by `Host`.
  Outbound `fetch` / `connect()` is filtered by a shared deny-range table:
  private, link-local and metadata ranges are blocked.
- **State lives outside the worker process.** KV, R2, D1 and Durable Object
  storage go through a binding provider. A worker process can be restarted,
  rescheduled or scaled to zero without losing data.

## Cloudflare compatibility

OpenFlare runs the real `workerd`, so the JavaScript runtime APIs behave as on
Cloudflare. The platform-level bindings differ where noted; the authoritative,
per-binding detail (wrangler config, backends, every known difference) lives in
[`docs/bindings/README.md`](docs/bindings/README.md), and platform-wide
differences in [`docs/compatibility.md`](docs/compatibility.md).

Legend: **Drop-in** — same API and configuration, differences limited to where
the data lives. **Partial** — runs with the differences listed below.
Anything not listed here is rejected at deploy with
`VRT_CONFIG_UNSUPPORTED_FIELD`; the complete matrix (including unsupported
bindings) is in [`docs/bindings/README.md`](docs/bindings/README.md).

| Service / binding | Status | Notes |
| --- | --- | --- |
| [KV](docs/bindings/kv.md) | ✅ Drop-in | Full `KVNamespace` on filesystem or Kvrocks; `cacheTtl` is validated but there is no edge-cache tier. |
| [R2](docs/bindings/r2.md) | 🟡 Partial | Objects, ranges, multipart, checksums; conditional writes depend on the S3 backend. `jurisdiction`/`storage_class` rejected; no SSE-C. |
| [D1](docs/bindings/d1.md) | 🟡 Partial | workerd's `d1-api` on `vrt-sql` (single-writer SQLite, fs/S3 persistence); opt-in read-only follower with bookmark-routed `/query` reads ([details](docs/d1-follower.md)); offline Time Travel restore by time/bookmark ([details](docs/d1-time-travel.md)); writes/`dump` stay on the writer; no failover. |
| [Durable Objects](docs/bindings/durable-objects.md) | 🟡 Partial | SQLite + KV storage, alarms, RPC, WebSockets, same-tenant `script_name`; no hibernation, `transferred_classes` rejected. |
| [Queues](docs/bindings/queues.md) | 🟡 Partial | Producers/consumers, retries, delay (including queue-level defaults), concurrency, DLQ; no pull consumer. |
| [Cron Triggers](docs/bindings/cron-triggers.md) | ✅ Drop-in | 5-field `triggers.crons` → `scheduled()`, at-most-once per `(worker, version, scheduledTime)`; wakes hibernated tenants. |
| [Workflows](docs/bindings/workflows.md) | 🟡 Partial | Cloudflare engine (miniflare lineage) as a DO; `limits.steps` capped at 100,000. |
| [Cache API](docs/bindings/cache-api.md) | 🟡 Partial | Per-tenant in-memory `caches.default`/`caches.open`; not persistent, requires `[cache]` in host config. |
| [Service bindings](docs/bindings/service-bindings.md) | 🟡 Partial | Worker-to-worker fetch/RPC plus named `WorkerEntrypoint` (`services[].entrypoint`), same tenant. |
| [Hyperdrive](docs/bindings/hyperdrive.md) | 🟡 Partial | Postgres wire protocol through the egress layer; credentials never enter the isolate. PostgreSQL only. |
| [Rate limiting](docs/bindings/rate-limiting.md) | 🟡 Partial | `[[ratelimits]]` fixed-window counter in the per-tenant process; approximate, not shared across hosts. |
| [Static assets](docs/bindings/static-assets.md) | 🟡 Partial | All four `html_handling` modes, `not_found_handling`, `run_worker_first`; content-addressed store; Cloudflare-aligned 20,000 files / 25 MiB each, plus a configurable total guard. |
| [WebSockets](docs/bindings/websockets.md) | 🟡 Partial | HTTP/1.1 upgrades to workers and DOs; 300 s idle, 4 h max, 4096 per ingress, 256 per tenant. |
| [Vars and secrets](docs/bindings/vars-and-secrets.md) | 🟡 Partial | Vars are `text` bindings (non-strings arrive JSON-encoded); secrets are `vrt secret put` name→ref maps. |
| [Tail Workers](docs/bindings/tail-workers.md) | 🟡 Partial | `tail_consumers` render as native workerd `tails`; same-tenant, best effort, no streaming tails. |
| [Worker loaders](docs/bindings/worker-loaders.md) | 🟡 Partial | Native `WorkerLoader` (`get`/`load`, named caches, dynamic entrypoints, DO facets); quota-bounded per loader. |

Platform-wide differences (no edge network, server-minted resource ids,
structural scale-to-zero, a whitelisted `request.cf`, WebSocket session
limits) are documented in [`docs/compatibility.md`](docs/compatibility.md).

## Components

| crate / dir | role |
| --- | --- |
| `snapshot-protocol` | signed desired-state snapshot protocol (Ed25519, revision fencing) |
| `vrt-config` | deploy manifest → workerd capnp config + content-addressed version bundles; wrangler config mapping |
| `vrt-host` | per-tenant workerd supervisor, HTTP ingress, scale-to-zero wake, logs, cron/queue delivery |
| `vrt-provider` | binding provider: KV, R2, Queues, rate limiting (memory / filesystem / Kvrocks / S3 backends) |
| `vrt-sql` | D1 and Durable Object SQL storage service |
| `cell-controller` | consumes signed desired state and orchestrates hosts (cron, queues, leases, assignment) |
| `vrt-local` | standalone management plane and `vrt` CLI (init, dev, deploy, rollback, secrets, hostnames, logs, doctor) |
| `vrt-securefs` | hardened shared-filesystem helpers (0700 dirs, atomic writes, safe path segments) |
| `vrt-objstore`, `vrt-metrics` | shared object-store and metrics helpers |
| `facade/` | TypeScript system workers that expose bindings to user code inside workerd |
| `contract/` | cross-language contract fixtures (TS ⇄ Rust) and drift checks |
| `third_party/open-compute` | vendored upstream code (Apache-2.0, see its `LICENSE` and `UPSTREAM.md`) |
| `deploy/` | Dockerfiles and the standalone Docker Compose stack (Kubernetes manifests live in `openflare-control`) |
| `examples/` | example workers: hello, KV + assets, D1, R2, queues, cron, Durable Objects, workflows, wrangler projects |

## Quick start (Docker, standalone)

workerd is never downloaded by the image build. You supply a pinned binary
and its digest is checked against `deploy/docker/workerd.lock`. For Linux
hosts the lock can also fetch it for you, verifying the same digests CI
does:

```sh
scripts/fetch-workerd.sh --out .workerd          # binary + workerd.capnp
cp .workerd/workerd deploy/standalone/workerd
```

or supply your own build of the pinned version:

```sh
cp /path/to/workerd deploy/standalone/workerd
WORKERD_SHA256=$(node -p "JSON.parse(require('fs').readFileSync('deploy/docker/workerd.lock','utf8')).artifacts['linux-x64'].binarySha256")
[ "$(shasum -a256 deploy/standalone/workerd | cut -d' ' -f1)" = "$WORKERD_SHA256" ] || echo "workerd does not match workerd.lock"

WORKERD_SOURCE=file WORKERD_SHA256=$WORKERD_SHA256 \
  docker compose -f deploy/standalone/docker-compose.yml up --build --wait
```

The compose stack is a single-machine quick start (dev/CI), not the
production topology. Because Docker named volumes share one device, it
passes the dev-only `vrt init --dev-allow-same-device-durability`
exemption; production must keep the D1 object store on a separate device
or use the s3 backend. Use `deploy/standalone/up.sh` in place of the bare
compose command to get per-service logs when the stack fails to boot.

Deploy the hello example. The management API reads a path local to the
server, so copy the project into the container first (or deploy from the
host — see "Local development and remote deploy" below). The example
declares a secret binding, so seed the secret before deploying:

```sh
dc="docker compose -f deploy/standalone/docker-compose.yml"
$dc exec vrt mkdir -p /tmp/examples
$dc cp examples/standalone-hello vrt:/tmp/examples/
printf 'my-secret' | $dc exec -T vrt vrt secret put --root /data greeting-secret
$dc exec vrt vrt deploy --root /data /tmp/examples/standalone-hello
curl -H 'Host: hello.vrt.local' http://127.0.0.1:8080/
```

To smoke-test without workerd, use the stub runtime:

```sh
WORKERD_SOURCE=stub docker compose \
  -f deploy/standalone/docker-compose.yml \
  -f deploy/standalone/docker-compose.stub.yml up --build --wait
```

[`deploy/standalone/README.md`](deploy/standalone/README.md) covers the rest:
day-2 operations (rollback, secrets, versions), backup, host-directory storage
and the end-to-end acceptance script `deploy/standalone/e2e.sh`.

## Local development and remote deploy

`vrt dev` runs the whole stack (vrt-local + vrt-host + vrt-provider +
vrt-sql) directly on your machine — no compose — with state under
`<worker_dir>/.vrt` (add it to your `.gitignore`). It deploys on
start, redeploys on file changes, streams worker logs to the terminal,
and binds a loopback ingress where any `Host` routes to your worker:

```sh
vrt dev examples/standalone-hello        # real workerd on PATH / VRT_WORKERD / --workerd
vrt dev examples/standalone-hello --stub # vrt-stub-workerd, no binary needed
curl http://127.0.0.1:8787/              # dev catch-all routes any Host
```

A real `workerd` is verified against `deploy/docker/workerd.lock`;
platforms without a pinned digest need the explicit
`--allow-unpinned-workerd` opt-out. `.dev.vars` in the project dir is
picked up as secrets/vars. `--no-watch`, `--no-logs`, `--no-color`,
`--listen <addr>` are supported; Ctrl+C shuts every child down.

`vrt deploy --url <http://host:port> --token-file <path> <worker_dir>`
deploys a local project to a remote management endpoint without
copying it first. The listener is off by default — `vrt serve`
enables it with `--mgmt-tcp-listen <addr>
--mgmt-tcp-token-file <path>` (0600, 64-hex token, bearer auth,
request-size cap, per-source-IP rate limit). Bind loopback and reach
it over an SSH tunnel or a TLS-terminating proxy — the API is HTTP
only. `--secrets-file <f|->` (dotenv), `--var KEY:VALUE`, `--name`,
and `--dry-run` work in both modes:

```sh
vrt deploy --root /data --dry-run ./my-worker      # pack + validate only
vrt deploy --url http://127.0.0.1:9090 --token-file ~/.vrt-mgmt-token ./my-worker
```

Other operator commands: `vrt logs --root <dir> <worker> [--follow]
[--since 5m] [--level error] [--json]` (read-only tail, never consumes
the drain cursor) and `vrt doctor [--root <dir>] [--json] [--strict]`
(workerd pin, root/key permissions, disk, ports, facade manifest,
service health, project config).

## Building and testing

Requirements: a recent stable Rust toolchain and Node.js 24+. The facade
build uses Node's built-in TypeScript type stripping.

```sh
npm run facade:build      # regenerate facade/dist + facade/manifest.json
npm test                  # facade + contract checks + cargo test --workspace
```

Some integration tests need a real `workerd` binary, or `workerd.capnp` for
the schema checks (`WORKERD_CAPNP=/path/to/workerd.capnp`). They skip
when it is missing. To run the whole `VRT_WORKERD_BIN` batch locally
against a binary you already verified against `deploy/docker/workerd.lock`:

```sh
scripts/ci-real-workerd.sh /path/to/workerd [/path/to/workerd.capnp]
```

CI runs the same script in the `real-workerd` job. Both CI and local runs
get the binary from `scripts/fetch-workerd.sh`, which is the only code that
turns `deploy/docker/workerd.lock` into bytes:

| `source` | Where the binary comes from | Digests checked |
| --- | --- | --- |
| `npm` (default) | `@cloudflare/workerd-linux-64@<version>` tarball | sha512 tarball `integrity`, `binarySha256` |
| `github-release` | a GitHub release asset of this project's workerd fork | `archiveSha256`, `binarySha256` |

Both sources also materialize `workerd.capnp` for the schema tests. Fetching
workerd there is a test-only exception to the "the image build never
downloads workerd" rule; Docker build and runtime paths still require an
operator-supplied binary.

A `github-release` pin is generated from the release manifest, never typed by
hand:

```sh
gh release download <vrt-tag> --pattern manifest.json --repo becoolme/workerd -O manifest.json
node scripts/update-workerd-lock.mjs --manifest manifest.json
node scripts/update-workerd-lock.mjs --manifest manifest.json --check  # what CI can verify
```

The lock records the workerd release the fork is based on (`upstream.version`), because the
compat-flag generator parses it as a workerd version. A release cut from an upstream commit that
carries no release tag yet has an empty `upstream.tag` in its manifest; name the version
explicitly then:

```sh
node scripts/update-workerd-lock.mjs --manifest manifest.json --upstream-version 1.20261120.1
```

`conformance/` holds the binding-layer conformance suite: declarative
golden-behavior cases (worker + HTTP steps + expectations citing
Cloudflare docs) executed against a running stack — see
[`conformance/README.md`](conformance/README.md). Its manifest/report
unit tests run as part of `npm test` and need no workerd.

If you keep your own deployment manifests, point `VRT_DEPLOY_TOML_DIRS` at
them (one or more directories, separated like `PATH`). The `deploy_toml`
tests then load every `vrt-host.toml`, `vrt-provider.toml`, `vrt-sql.toml`
and `cell-controller.toml` below those directories through the real config
loaders:

```sh
VRT_DEPLOY_TOML_DIRS=/path/to/your/k8s cargo test --workspace --offline --test deploy_toml
```

## Compatibility notes

OpenFlare accepts standard wrangler configuration. Cloudflare-specific
resource IDs, such as a KV namespace `id`, are accepted and ignored: bindings
resolve to the namespace local to the deployment. A field that cannot be
honoured is reported as a typed warning or a deploy-time error. It is never
silently dropped. Per-binding support tiers and known differences are listed in
[`docs/bindings/`](docs/bindings/README.md), every wrangler key's behavior in
[`docs/wrangler-config.md`](docs/wrangler-config.md), and the platform-level
summary in [`docs/compatibility.md`](docs/compatibility.md). Platform error
codes returned by the ingress are listed in
[`docs/log-codes.md`](docs/log-codes.md). WebSocket upgrade passthrough is
described in [`docs/ingress-websocket.md`](docs/ingress-websocket.md).
Per-worker CPU/memory/subrequest quotas (`limits`) and their cell capability
bit are described in [`docs/quotas.md`](docs/quotas.md).

## License

[MIT](LICENSE). Code under `third_party/open-compute` keeps its upstream
Apache-2.0 license.
