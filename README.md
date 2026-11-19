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

## Components

| crate / dir | role |
| --- | --- |
| `snapshot-protocol` | signed desired-state snapshot protocol (Ed25519, revision fencing) |
| `vrt-config` | deploy manifest → workerd capnp config + content-addressed version bundles; wrangler config mapping |
| `vrt-host` | per-tenant workerd supervisor, HTTP ingress, scale-to-zero wake, logs, cron/queue delivery |
| `vrt-provider` | binding provider: KV, R2, Queues, rate limiting (memory / filesystem / Kvrocks / S3 backends) |
| `vrt-sql` | D1 and Durable Object SQL storage service |
| `cell-controller` | consumes signed desired state and orchestrates hosts (cron, queues, leases, assignment) |
| `vrt-local` | standalone management plane and `vrt` CLI (init, deploy, rollback, secrets, hostnames) |
| `vrt-securefs` | hardened shared-filesystem helpers (0700 dirs, atomic writes, safe path segments) |
| `vrt-objstore`, `vrt-metrics` | shared object-store and metrics helpers |
| `facade/` | TypeScript system workers that expose bindings to user code inside workerd |
| `contract/` | cross-language contract fixtures (TS ⇄ Rust) and drift checks |
| `third_party/open-compute` | vendored upstream code (Apache-2.0, see its `LICENSE` and `UPSTREAM.md`) |
| `deploy/` | Dockerfiles and the standalone Docker Compose stack |
| `examples/` | example workers: hello, KV + assets, D1, R2, queues, cron, Durable Objects, workflows, wrangler projects |

## Quick start (Docker, standalone)

workerd is never downloaded by this repo. You supply a pinned binary and
its digest is checked against `deploy/docker/workerd.lock`:

```sh
cp /path/to/workerd deploy/standalone/workerd
WORKERD_SHA256=$(node -p "JSON.parse(require('fs').readFileSync('deploy/docker/workerd.lock','utf8')).artifacts['linux-x64'].binarySha256")
[ "$(shasum -a256 deploy/standalone/workerd | cut -d' ' -f1)" = "$WORKERD_SHA256" ] || echo "workerd does not match workerd.lock"

WORKERD_SOURCE=file WORKERD_SHA256=$WORKERD_SHA256 \
  docker compose -f deploy/standalone/docker-compose.yml up --build --wait
```

Deploy the hello example. The management API reads a path local to the
server, so copy the project into the container first. The example declares
a secret binding, so seed the secret before deploying:

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

CI runs the same script in the `real-workerd` job: it fetches the
`@cloudflare/workerd-linux-64` npm package at the version pinned in
`deploy/docker/workerd.lock` and verifies both the tarball integrity
and the binary SHA-256 against the lock before testing. Fetching
workerd in that job is a test-only exception to the "workerd is never
downloaded by this repo" rule — Docker build and runtime paths still
require an operator-supplied binary.

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
silently dropped. Platform error codes returned by the ingress are listed in
[`docs/log-codes.md`](docs/log-codes.md). WebSocket upgrade passthrough is
described in [`docs/ingress-websocket.md`](docs/ingress-websocket.md).

## License

[MIT](LICENSE). Code under `third_party/open-compute` keeps its upstream
Apache-2.0 license.
