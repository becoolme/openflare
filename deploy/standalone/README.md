# VRT standalone — one-compose quick start

One `docker compose up` runs the whole standalone runtime:

```
vrt-init ──▶ vrt (mgmt API + renewal on unix socket)
         └─▶ vrt-host (workerd supervisor + ingress :8080)
         └─▶ vrt-provider (KV/R2 bindings on local FS)
```

All four services share the `vrt-data` named volume (`0700`, same
non-root uid — Docker seeds it from the image's `/data`, no manual
chown). Only the ingress port is published — bound to `127.0.0.1`
by default; the management socket never leaves the container.

## Bring-up with diagnostics: `up.sh`

`deploy/standalone/up.sh` runs the same bring-up as the conformance
gate and makes a failed boot diagnosable:

```sh
WORKERD_SOURCE=file WORKERD_SHA256=$WORKERD_SHA256 deploy/standalone/up.sh
```

It calls `docker compose up --build --wait`; on failure it prints
`docker compose ps -a` plus the last 200 log lines of every unhealthy
or non-zero-exit service. Compose itself only reports
`container ... is unhealthy` — the real error (for example a rejected
`vrt-sql.toml`) is in that service's logs. Knobs: `COMPOSE_FILE`,
`COMPOSE_PROJECT`, `COMPOSE_EXTRA` (overlays), `LOG_TAIL`.

## Durability in this stack (dev/CI only)

`vrt-sql` fails closed when the fs persistence root (`/data/objstore`)
shares a device with `dbRoot` (`/data/sql/data`): one disk loss would
destroy the db and its backup copy together. All Docker named volumes
live on one device, so this quick-start stack passes
`of init --dev-allow-same-device-durability`, which writes
`persistence.allowSameDeviceDurability = true` into the generated
`vrt-sql.toml`.

- Both `of init` and `vrt-sql` print a warning (“durability is NOT
  guaranteed”); the generated file carries a `DEV ONLY` comment.
- **Production must not pass this flag.** Put the persistence root on a
  genuinely separate device, or use `backend = "s3"`. Nothing else in
  the repo injects the key (enforced by
  `vrt-sql/tests/startup_config_rv66.rs`).
- Upgrading a pre-existing volume: the old `vrt-sql.toml` has no
  exemption, so `of init` now reports `config differs on: …` and
  asks for `--reconfigure`. Add `--reconfigure` to the `vrt-init`
  service `command:` once (or recreate the volume) — otherwise
  `vrt-sql` exits with `VRT_SQL_DURABILITY_SAME_DEVICE`.

## Acceptance entry point: `e2e.sh`

`deploy/standalone/e2e.sh` is the real-machine acceptance gate —
run it after building with a REAL workerd:

```sh
# Either fetch the pinned binary (verifies digests from workerd.lock):
scripts/fetch-workerd.sh --out .workerd && cp .workerd/workerd deploy/standalone/workerd
# or bring your own build of the pinned version:
cp /path/to/workerd deploy/standalone/workerd            # pinned binary
# RV-R25: take the expected value from the in-repo pin, never from the same file.
WORKERD_SHA256=$(node -p "JSON.parse(require('fs').readFileSync('deploy/docker/workerd.lock','utf8')).artifacts['linux-x64'].binarySha256")
[ "$(shasum -a256 deploy/standalone/workerd | cut -d' ' -f1)" = "$WORKERD_SHA256" ] || { echo "workerd does not match workerd.lock"; exit 1; }
WORKERD_SOURCE=file WORKERD_SHA256=$WORKERD_SHA256 \
  deploy/standalone/e2e.sh
```

It brings the stack up exactly as below (no overlay), deploys
`examples/standalone-hello` + `examples/standalone-kv-assets`,
asserts Host-header 200s, KV read/write, assets ETag/304 + SPA
fallback, `of status`, and restart recovery — then `down -v`.
Failures dump every service's logs.

## Smoke (stub runtime — no workerd)

```sh
WORKERD_SOURCE=stub docker compose \
  -f deploy/standalone/docker-compose.yml \
  -f deploy/standalone/docker-compose.stub.yml \
  up --build --wait
# `of init` publishes a SIGNED EMPTY envelope, so all services go
# healthy before the first deploy — deploy whenever ready:
docker compose -f deploy/standalone/docker-compose.yml \
  -f deploy/standalone/docker-compose.stub.yml exec vrt \
  of status --root /data
```

The stub overlay pins `WORKERD_SOURCE=stub` + `--mode stub`; `of
init` verifies the binary is really `vrt-stub-workerd` (`--version`
marker) — a mode/binary mismatch fails closed instead of silently
running the wrong runtime.

To deploy the hello worker, copy `examples/standalone-hello/` into
the container volume first (the management API takes a server-local
path), or mount it: add `- ../examples:/examples:ro` under the
`vrt` service volumes, then `of deploy --root /data
/examples/standalone-hello`. You can also deploy straight from the
host — see "Remote deploy over TCP" below.

## Real workerd

workerd is **never** downloaded or built by this repo — bring a
pinned artifact. The image builds from the single canonical
`deploy/docker/Dockerfile.vrt-local` (`deploy/standalone/Dockerfile`
is a pointer to it; one build shared by all four services via
`vrt-standalone:local`, `pull_policy: never` on consumers):

1. `cp /path/to/fixed-version-workerd deploy/standalone/workerd`
   (git-ignored — never commit it)
2. `sha256sum deploy/standalone/workerd`
3. `.env`: `WORKERD_SOURCE=file`, `WORKERD_SHA256=<hex>`
4. `docker compose -f deploy/standalone/docker-compose.yml up --build`
   — the build fails on digest mismatch or a missing binary/arg.

Or keep your own pinned image with `/usr/local/bin/workerd` and set
`WORKERD_SOURCE=image` + `WORKERD_IMAGE=ref@sha256:...`.

The image ships `facade/dist` + `facade/manifest.json` under
`/usr/share/vrt/facade` — `of init` seeds system modules from
there automatically (manifest digests are verified at seed time;
bare-metal installs can pass `--facade-dist`).

It also co-installs the `of-bundler` helper at
`/usr/local/bin/of-bundler`, beside `of`. `of deploy` bundles each
worker project by shelling out to that helper (rolldown); release
binaries embed it, but a source-built `of` does not, so the image — not
the host — provides it. Nothing here needs an `OF_BUNDLER` override or
a host `of-bundler/target/` build directory, and the image build fails
closed if the helper is ever missing from that path.

With the real-workerd stack up, the binding-layer conformance suite
can run against it — it deploys each case worker through `of deploy`
inside the `vrt` container and checks responses at the ingress:

```sh
node conformance/run.mjs \
  --url http://127.0.0.1:8080 \
  --deploy-cmd ./conformance/deploy-standalone.sh
```

See [`conformance/README.md`](../../conformance/README.md) for
options (`--stub` against the stub compose, `--only`, `--no-deploy`)
and report format.

## Day-2 ops

```sh
compose="docker compose -f deploy/standalone/docker-compose.yml exec vrt"
$compose of status --root /data
$compose of deploy --root /data <dir>
$compose of deploy --root /data --env staging <dir>
$compose of deploy --root /data --preview <dir>
$compose of promote --root /data <worker> <version_id>
$compose of rollback --root /data <worker> <version_id>
$compose of hostnames add --root /data <worker> <host>
printf 's3cret' | $compose of secret put --root /data <ref>
$compose of versions list --root /data <worker>
$compose of logs --root /data <worker> --follow --since 10m --level error
$compose of doctor --root /data          # add --json or --strict
```

`deploy --preview` stores an immutable version without moving traffic;
`promote` switches the worker's active version through the same signed
desired revision path as `rollback`. With `[preview] domain` AND
`tokenSecretFile` set in `/data/config/vrt-host.toml`, the preview is
reachable at
`https://<versionId>--<worker>.<domain>/?__vrt_preview_token=…` (off by
default → `previewUrl` is `null`; the returned URL is signed and expires
after an hour; `of versions share` mints a longer one). See
[deploy versions](../../docs/versioning.md) for lifecycle, preview
access, retention, concurrency and current limits.

`of logs` is a read-only tail of the host log hub (console.log,
exceptions, platform codes; secrets are already redacted at ingest).
It never consumes the cell-controller drain cursor — `--follow`
polls the `controlListen` endpoint, `--since`/`--level` filter
client-side, `--json` prints one record per line.

`vrt-init` is idempotent: rerunning `up` with unchanged settings is
a no-op. If settings drifted (e.g. you changed `VRT_CELL`), `of
init` exits non-zero and names the differing config fields — apply
the change with `--reconfigure` (rewrites `config/` only; keys and
state are never regenerated). `vrt-init` compose service: edit its
`command:` to add `--reconfigure`, or exec it manually.

## Remote deploy over TCP

The management API normally lives on the container's unix socket.
`of serve` can additionally bind a TCP listener so a host-side `of`
CLI can deploy a local directory without copying it in:

```sh
printf '%s' "$(openssl rand -hex 32)" > /etc/vrt-mgmt-token && chmod 600 /etc/vrt-mgmt-token
docker compose -f deploy/standalone/docker-compose.yml exec vrt   of serve --root /data --supervise     --mgmt-tcp-listen 127.0.0.1:9090 --mgmt-tcp-token-file /etc/vrt-mgmt-token

of deploy --url http://127.0.0.1:9090 --token-file /etc/vrt-mgmt-token --env staging ./my-worker
```

The client packs the bundle locally (same validation, digests and
size caps as `--root` deploys) and uploads it; the server re-verifies
everything through the signed-envelope path, so a tampered bundle is
rejected exactly like a bad local deploy.

Security notes — the listener is **off by default** and only enabled
by the flags above:

- Bearer-token auth on every request. The token file must be `0600`
  and contain the same 64-hex string on both sides; comparison is
  constant-time. This token is independent of the unix-socket
  `mgmt/token`.
- Request bodies are capped (64 KiB default; uploads allow up to the
  packed-bundle cap) and per-source-IP rate limited.
- The transport is plain HTTP — bind `127.0.0.1` and reach it over an
  SSH tunnel or a TLS-terminating proxy when the CLI runs elsewhere.
- Secret values travel only inside the upload body
  (`--secrets-file <f|->`, dotenv format) or stdin — never on argv,
  in logs, or in error text. `--var KEY:VALUE` is for non-secret
  vars.

`of deploy --dry-run` (both modes) prints the pack summary — worker
name, module count, bundle bytes/digest, binding list, rejected keys —
without calling the API.

## Bare-metal dev without compose: `of dev`

On a machine with the `vrt-*` binaries on PATH (or `--vrt-*-bin`
flags):

```sh
of dev ./my-worker                    # 127.0.0.1:8787, watch+redeploy, live logs
of dev ./my-worker --env staging     # select wrangler env.staging
of dev ./my-worker --stub --no-watch  # stub runtime, redeploy by hand
```

State lands in `./my-worker/.vrt` (add it to `.gitignore`), ingress
is loopback-only, and any `Host` routes to the one deployed worker —
`of dev` writes `ingress.devCatchAll` into the generated config for
this. The flag exists only to make `of dev` work; production
`vrt-host.toml` files must not set it (ingress keeps strict
exact-Host routing). Ctrl+C SIGTERMs every child
(vrt-host/provider/sql) before exiting.

## Backup

Back up the whole `vrt-data` volume — it contains the signing seed
and secrets (`0600`, owner-only). Restoring the volume restores the
full control plane; revision fencing makes replayed envelopes
harmless.

## Host-directory storage (optional)

Prefer a host dir over a named volume? Docker will NOT chown it —
create and own it yourself, then use the overlay:

```sh
sudo mkdir -p /srv/vrt-data && sudo chown 10001:10001 /srv/vrt-data
docker compose -f deploy/standalone/docker-compose.yml \
  -f deploy/standalone/docker-compose.host-dir.yml up --build
```

## Upgrades

Rebuild the image, `up -d` — the same volume carries all state; the
first `vrt-init` pass is a no-op. Roll back by restoring a volume
backup.
