# Deploy versions (MVP)

`vrt` stores every deploy as an immutable version. The MVP separates
"upload a version" from "serve a version": `deploy --preview` writes a
version without moving traffic, `promote` switches the active pointer
through the existing signed-desired path, and `versions list` shows
the lifecycle of every stored version.

A preview version is also reachable over its own **preview URL** while
the production hostname keeps serving the active version. This page
describes the MVP boundary. It is intentionally smaller than
Cloudflare's full version and preview-routing model.

## Commands

```text
vrt deploy --preview --root <dir> [--name <n>] <worker_dir>
vrt promote --root <dir> [--worker <name|workerId>] <version_id>
vrt versions list --root <dir> <worker>
```

The legacy `vrt versions <worker>` form is an alias for
`vrt versions list`. Remote deploys (`vrt deploy --url ...`) accept
`--preview`; the management API also exposes `POST /v1/promote` with
`{ "versionId": "...", "worker": "..." }` (`worker` optional; omit it
only when the id is unique across workers).

`vrt deploy --preview` returns the same JSON shape as a normal deploy,
including `versionId`, plus `previewUrl` (`null` when the host has no
preview domain configured). No id is printed unless the version record
was persisted; a failure after the object write but before the state
write leaves only a harmless orphan object, never a reported version.

`vrt versions list` prints `WorkerVersions` JSON; every
`versions[*]` row carries `previewUrl` (same rule: `null` for archived
versions and for every version when no preview domain is configured).

## Data model

Versions live inside `state/state.json` (`schemaVersion` 2). Each
worker has `versions: [VersionRecord]` plus `activeVersionId`.

| Field | Meaning |
| --- | --- |
| `versionId` | `vrt_<sha256-prefix>` derived from the canonical behavior record. Same input, same id. |
| `artifactDigest` | Content-addressed bundle under `objects/<digest>`. |
| `configHash` | `cfg_<sha256-prefix>` over the same canonical behavior record. Diagnostic only; identity lives in `versionId`. |
| `status` | `preview`, `active`, or `archived`. |
| `createdAtUnixMs` | Creation time. `0` for records written before this MVP. |
| `createdBy` | Creator when the caller has an identity. Standalone `vrt` has no authenticated principal, so this is `null` today. |
| bindings / vars / crons / consumers / observability / DO classes / workflows / quotas | The complete behavior of this version, replayed by `to_desired` and rollback. |

Version identity deliberately excludes `versionId`, `createdAtUnixMs`,
`createdBy`, `status`, and `configHash`. Previewing a version and later
promoting it therefore keeps one id, and ids minted before this MVP
are byte-identical.

Lifecycle invariant: at most one record per worker is `active`, and it
is exactly the record selected by `activeVersionId`. `state::read`
normalizes state files written before this MVP: a missing `status`
decodes as `archived`, then the active pointer marks its record
`active`. The normalized form is persisted by the next mutating op.

There is no SQL migration in this feature: standalone version metadata
is part of the existing `state.json` document, not a vrt-sql table.
The new record fields are additive and optional, so an older build
(which ignores unknown fields on `VersionRecord`) can still read and
rewrite the state; it will drop the lifecycle fields, and the next
new-build read restores them from `activeVersionId`.

### Preview instances in the signed desired document

Each projected preview version becomes an extra worker entry in the
tenant fragment of `compute.desired`:

- `workerId` = `<producerWorkerId>.pv<sha256-prefix>` (deterministic, so
  a converge never churns sockets; short, because the id ends up in a
  `<gen>-N/sock/<id>.sock` path);
- `activeVersion` = the preview version id + bundle digest + compat;
- `bindings` = the version's bindings **minus every `secret` binding**;
- `routes` = empty (the host derives the preview hostname);
- `crons`, `queueConsumers`, `workflows`, `tails`, `streamingTails`,
  `durableObjectClasses` = empty;
- `preview = { workerId: <producer>, versionId: <preview version> }`.

The `preview` marker is a typed protocol field (not a naming
convention). `compute.desired` validation refuses any snapshot where a
preview instance carries routes, side effects, secret bindings, a
version id different from its `activeVersion`, a target outside the
tenant, a chain of previews, or more than
`MAX_PREVIEWS_PER_WORKER` (4) instances per producer. Older cells reject
the new field via `deny_unknown_fields` — fail closed, never served as
production.

The renderer, supervisor, provider and metrics all see an ordinary
worker entry: the preview gets its own socket and version pin, so
production bindings, routes and counters are untouched. The provider's
authorization view includes it, so data bindings keep working exactly
like production (see the security boundary below).

## Preview URLs

### Scheme

```text
https://<versionId>--<workerName>.<previewDomain>/
```

`<versionId>` is the immutable `vrt_…` id, `<workerName>` is the
production worker name, and `--` is the same separator the platform uses
for `<worker>--<team>`. Worker names can never contain `--`
(`valid_worker_name`) and version ids always carry a `<prefix>_<hex>`
shape, so the two halves are unambiguous. Version ids therefore contain
an underscore, which is technically outside the DNS host-character set;
`vrt-host` accepts `_` in Host normalization for exactly this reason —
production hostnames are validated without `_`, so this can only ever
match a preview route.

Examples:

```text
vrt_9f2c1e…--edge-api.preview.example.com      ← deploy --preview
vrt_71aa03…--edge-api.preview.example.com      ← now the ACTIVE version
```

### Configuration

Preview routing is **host configuration and off by default**. In
`examples/standalone-vrt-host.toml` / `root/config/vrt-host.toml`:

```toml
[preview]
# Public suffix for preview hostnames. Absent or empty = previews are
# not routable at all (every preview request is a 404).
domain = "preview.example.com"
```

- `domain` must be a lowercase DNS name: no scheme, port, path,
  wildcard, leading/trailing dot, no `_`. An unusable value fails host
  startup (`preview.domain must be a lowercase DNS name …`) instead of
  silently serving nothing.
- The host derives every preview hostname itself; the suffix is never
  read from a snapshot, a worker config or a request.
- `vrt-local` reads the **same file** (`<root>/config/vrt-host.toml`,
  `[preview] domain`) only to report `previewUrl` in the API/CLI. That
  value is display-only: routing authority stays with `vrt-host`, and an
  unreadable/invalid value simply reports `null`.

### Which requests serve which version

| Request | Serves | Response markers |
| --- | --- | --- |
| production hostname (worker `routes`) | active version | none |
| `<versionId>--<worker>.<domain>`, version `preview` | that preview instance | `x-vrt-preview: 1`, `x-vrt-preview-version: <versionId>` |
| `<versionId>--<worker>.<domain>`, version `active` | the production instance (same code as production) | same markers |
| any other `<…>.<domain>`, or any preview host when no domain is configured | **404** (`unknown_host`) | — |
| archived version | **404** | — |

There is no fallback: an unknown, archived or mistyped version id never
resolves to production, and a preview request never mutates
`activeVersionId`, worker hostnames or bindings. Derived routes never
overwrite an existing route in the table (first-wins), so a snapshot can
never shadow a production hostname with a derived one.

Up to `MAX_PREVIEWS_PER_WORKER` (4) newest preview versions per worker
are projected as instances (and are therefore routable). Older preview
versions stay stored and promotable; they are just not served over HTTP
until re-previewed (`vrt deploy --preview` again).

`x-vrt-preview` / `x-vrt-preview-version` are also forwarded to the
worker on the request path, so the code can branch its own side effects
without parsing its Host header. The ingress strips worker-supplied
`x-vrt-*` response headers and re-adds them itself, so a production
worker cannot forge preview status.

## Transaction boundaries

All mutating operations hold the single writer `flock` on
`state/lock`, then follow the existing write order:

1. materialize content-addressed assets / bundle objects;
2. write `vars.json` (union of every stored version's var values);
3. write `state.json` atomically;
4. sign and write `desired/envelope.json`;
5. append `state/oplog.jsonl`.

`deploy --preview` writes a `preview` record, keeps `activeVersionId`
and worker hostnames unchanged, and re-publishes the worker set
(production worker + preview instances) at a new revision. A worker
that has only preview versions has no active pointer and is skipped by
`to_desired`, so it cannot be served accidentally. Because the preview
instance is part of the rendered tenant config, a preview deploy runs
the same workerd load preflight as a normal deploy: a bundle the host
could not load is rejected before anything is published. `promote` runs
the same preflight before it moves the active pointer.

`promote` validates that the version belongs to the resolved worker
and that `objects/<artifactDigest>` still exists. It then switches the
pointer, validates the desired document, runs the workerd preflight,
writes state, and publishes. Any failure before the publish leaves the
old active version and the old envelope intact.

Because a preview changes the tenant fragment, the host installs a new
generation (rolling switch): the old generation keeps serving until the
new one is ready, and its content/bindings are byte-identical. A
preview that fails to assemble (e.g. a removed bundle object) leaves the
previous generation serving — production is never taken down by a
preview.

## Security boundaries (MVP)

- **Secrets.** Secret values are tenant-scoped, not version-scoped, so a
  preview never receives them: `to_desired` strips every `secret`
  binding from the preview instance and the snapshot validator refuses a
  preview that carries one. Code that requires a secret in preview must
  handle it being absent. (Tenant secrets are still available to the
  production version only.)
- **Data bindings are shared.** KV, R2, D1, Hyperdrive/PG, queues,
  Vectorize and cross-worker Durable Object bindings resolve to the same
  tenant resources for a preview as for production. There is no
  per-version data isolation yet: a preview that writes to KV/R2/D1
  writes production data, and only Cloudflare-style "preview
  namespaces" would change that. This is deliberate and *not silent*:
  every preview response carries `x-vrt-preview-version`, and the
  request carries `x-vrt-preview: 1`.
- **No side effects.** A preview instance never runs crons, never
  consumes queues, and never registers workflows or tail consumers
  (single-consumer and engine relations stay production-scoped).
- **Durable Objects and Workflows are refused** for previews
  (`preview: durableObjects` / `preview: workflows`) until storage can
  be version-isolated; a preview may therefore not declare its own DO
  classes or Workflows. Binding to *another* worker's DO class is still
  allowed (and shares that worker's state, same as the data-binding
  rule above).
- **Same process, separate instance.** A preview runs as another worker
  instance in the tenant's workerd process. It has its own socket,
  version pin, bindings and metrics label
  (`worker_id="<producer>.pv<digest>"`), but it shares the process and
  its resource budget; a preview isolate that exhausts memory/CPU can
  still hurt the tenant. Per-preview processes are out of scope.
- **Metrics.** Production counters (`vrt_worker_requests_total` etc.)
  keep the production `worker_id` label; preview traffic is attributed
  to the preview instance id and cannot inflate production numbers.
- **Platform assignment.** `assignment.json` lists production workers;
  a preview instance inherits its producer's assignment (it is never an
  independent assignment entry, and it never pulls a tenant into a pod
  on its own).

## Production integration (not in this MVP)

Local/HTTP and explicit `Host` headers are what the tests cover. Serving
real browsers needs, in addition:

1. **DNS**: a wildcard record `*.<previewDomain>` (the preview label is
   one DNS label, so a single wildcard covers every version), or
   per-version records from the control plane.
2. **TLS**: a certificate covering `*.<previewDomain>` (wildcards cover
   exactly one label — matching the scheme above), terminated at the
   shared entrypoint (Traefik/edge) and forwarded with the original Host.
3. **Edge routing**: the shared entrypoint must forward preview hosts to
   `vrt-host` instead of treating them as unknown. The existing
   constraints on the shared entrypoint (Traefik/ServiceLB, hostPort,
   certificates) apply unchanged.
4. **Abuse bounds**: preview hostnames are public once DNS exists; there
   is no per-preview access control yet (no signed preview tokens), so
   treat a preview as a public URL.

## Concurrency and idempotency

- The writer lock serializes deploys, previews, promotes, and
  rollbacks. Two concurrent promotes cannot both commit: the second
  observes the first's state, so the final state has exactly one
  `activeVersionId`.
- Promoting the current active version is a no-op and returns the
  current revision (`changed: false`). It is not an error.
- If a previous promote committed state but crashed before writing the
  envelope, the replay detects the stale envelope revision, re-runs
  validation/preflight, and republishes the already-active version.
  This repairs the crash window instead of returning a false success.
- A version id is content-derived and does not include the worker
  name, so two workers shipping identical behavior can share an id.
  `promote <version_id>` therefore requires the id to resolve to one
  worker; pass `--worker` when it is ambiguous. Promoting a version
  against a different worker fails with `not found: version`.

## Fail-closed behavior

- Preview refuses `--secrets-file`/upload secret values. Secret values
  are tenant-level today and applying them on preview would change the
  currently serving version.
- Preview refuses versions that declare durable objects or workflows
  (no version-isolated storage yet).
- Preview never receives tenant secrets, and a snapshot whose preview
  instance carries a secret binding is rejected.
- Promote refuses a version whose bundle object was removed.
- A worker with a dangling `activeVersionId` still fails desired
  validation; it is never silently skipped.
- Failed preview/promote operations never return a version id or a
  success revision.
- With no configured preview domain every preview hostname is a 404 —
  there is no "serve the newest preview anyway" fallback.

## Explicitly not implemented

- Wildcard DNS, certificates and edge integration for preview URLs
  (see "Production integration").
- Per-version data isolation (KV/R2/D1/PG/queue namespaces).
- Preview access control (signed preview tokens / passwords).
- Per-preview processes (previews share the tenant workerd process).
- Durable Object and Workflow previews.
- Automatic GC and retention policy for versions. Normal deploys keep
  the existing `maxVersionsPerWorker` eviction behavior; the preview
  path does not evict anything. This MVP does not delete versions on
  its own.
- Code/config diffs and per-version metrics beyond the preview
  instance's own worker label.
- Versioned routes/hostnames. Production hostnames remain worker-level;
  a preview never changes them.

## `isPreview` for `cloudflare.config.ts`

`vrt deploy --preview` passes `--preview` to the config loader, so
`defineConfig(({ isPreview }) => …)` takes its preview branch. The flag
is derived from the deploy mode (never from a caller-supplied override):
a normal `vrt deploy` can only evaluate with `isPreview = false`. See
[wrangler-config.md](./wrangler-config.md#ispreview) for the exact
semantics.

Because config evaluation happens at pack time, the branch is baked into
the version: `--preview` changes what gets stored, `promote` later
serves exactly what the preview stored. A production deploy never
changes behaviour because of this flag, and a preview deploy cannot
write the active pointer.

## Tests

- Unit tests in `vrt-local/src/state.rs`: preview lifecycle, single
  active version, idempotent promote, identity excluding lifecycle
  metadata, legacy state normalization, preview projection (own id/socket,
  no routes/secrets/side effects, newest-first cap) and promote leaving
  the preview instance set.
- Unit tests in `vrt-host/src/route.rs`: preview host parsing
  (case/port/`_`), feature-off 404s, version selection, active-version
  preview host, derived routes never overwriting production.
- Unit tests in `vrt-host/src/config.rs`: `[preview] domain` default-off
  and fail-closed validation.
- Protocol tests in `snapshot-protocol/src/compute_desired.rs`: preview
  instance round-trip and every rejection (routes, side effects,
  secrets, version mismatch, unknown/self/chained target, capacity).
- API tests in `vrt-local/tests/versioning.rs`: same input same id,
  preview not moving the desired active version, preview-first worker
  absent from desired, promote switching traffic and being idempotent,
  cross-worker rejection, missing artifact rejection, concurrent
  promote invariant, `previewUrl` from the host config, DO/workflow
  preview refusal.
- Control-plane HTTP tests in `vrt-host/tests/versioning_http.rs`: a
  real management listener (loopback TCP, bearer auth) drives deploy /
  preview / promote / versions, and the same root is converged through
  `SupervisorHost` + ingress so "traffic did/didn't move" is asserted
  at the edge, not just in 200s. Covers preview isolation (including
  the served instance identity and the active bundle the edge reports),
  promote +
  idempotent replay, preview-with-secret-values refusal (no version,
  no secret write), unknown/ambiguous/cross-worker promote errors,
  concurrent promotes (one active, highest-revision success must be
  the final pointer), and management/host restarts.
- End-to-end in `vrt-host/tests/preview_url_e2e.rs` (real mgmt router +
  `vrt-host` supervisor + ingress, `vrt-stub-workerd`): deploy v1 →
  deploy v2 preview (`previewUrl` in the API response) → preview host
  serves the preview instance while production still serves v1 →
  preview traffic does not touch production counters → unknown/archived
  versions 404 → promote v2 → production serves v2 and the v2 preview
  URL follows → no-domain mode returns `null` and 404s → a missing
  preview artifact leaves production serving.

## Changelog

- 2026-11-22: versioned preview URLs
  (`https://<versionId>--<worker>.<previewDomain>/`), host
  `[preview] domain` config (default off), preview instances in the
  signed desired document, `previewUrl` on `/v1/deploy` and
  `/v1/versions`, `x-vrt-preview`/`x-vrt-preview-version` markers,
  secret stripping for previews, DO/Workflow preview refusal, and
  `--preview` → `isPreview` passthrough for `cloudflare.config.*`.
- 2026-11-22: initial versioning MVP (`deploy --preview`, `promote`,
  `versions list`), lifecycle fields on `VersionRecord`, and
  preview-only workers skipped by `to_desired`.
- 2026-11-22: control-plane HTTP integration coverage for the MVP
  (`vrt-host/tests/versioning_http.rs`); no behavior change.
- 2026-11-22: preview instances carry the `versionMetadata` projection
  (`tag`/`created_at`) of their own version like production; the
  control-plane HTTP suite asserts preview isolation by served instance
  (`x-stub-worker`) because the stub runs one process — and therefore
  one env — per tenant.
