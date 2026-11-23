# Deploy versions (MVP)

`of` stores every deploy as an immutable version. The MVP separates
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
of deploy --preview --root <dir> [--name <n>] <worker_dir>
of promote --root <dir> [--worker <name|workerId>] <version_id>
of versions list --root <dir> <worker>
of versions prune --root <dir> [<worker>] [--dry-run] [--max-age-days <n>] [--max-count <n>]
of versions retention --root <dir> [--auto|--no-auto] [--max-age-days <n>] [--max-count <n>]
of versions share --root <dir> [--worker <name|workerId>] [--ttl-seconds <n>] <version_id>
```

The legacy `of versions <worker>` form is an alias for
`of versions list`. Remote deploys (`of deploy --url ...`) accept
`--preview`; the management API also exposes `POST /v1/promote` with
`{ "versionId": "...", "worker": "..." }` (`worker` optional; omit it
only when the id is unique across workers).

`of deploy --preview` returns the same JSON shape as a normal deploy,
including `versionId`, plus `previewUrl` and
`previewUrlExpiresAtUnixS`. The URL is **signed and expiring** (one hour
by default): previews require a token, so a bare URL would open nothing.
`previewUrl` is `null` when previews are not fully configured (no
`[preview] domain`, no `tokenSecretFile`, or a key file that does not
match this root's signing seed). No id is printed unless the version
record was persisted; a failure after the object write but before the
state write leaves only a harmless orphan object, never a reported
version.

`of versions list` prints `WorkerVersions` JSON; every
`versions[*]` row carries `previewUrl` / `previewUrlExpiresAtUnixS` under
the same rule (`null` for archived versions, and for every version while
previews are not fully configured). The listing mints tokens read-only —
it never writes the key file; a deploy or `of versions share` does.

`of versions share <version_id>` mints a fresh URL for any stored
version (60 s … 30 days; default 24 h) and prints
`{worker, workerId, versionId, url, expiresAtUnixS}`. It fails loudly
(`preview: previewAccess`) when previews are not configured.

## Data model

Versions live inside `state/state.json` (`schemaVersion` 2). Each
worker has `versions: [VersionRecord]` plus `activeVersionId`.

| Field | Meaning |
| --- | --- |
| `versionId` | `vrt_<sha256-prefix>` derived from the canonical behavior record. Same input, same id. |
| `artifactDigest` | Content-addressed bundle under `objects/<digest>`. |
| `configHash` | `cfg_<sha256-prefix>` over the same canonical behavior record. Diagnostic only; identity lives in `versionId`. |
| `status` | `preview`, `active`, or `archived`. |
| `createdAtUnixMs` | Creation time. Refreshed when an existing record is re-previewed, so "newest preview" means the last preview a client asked for. `0` for records written before this MVP. |
| `createdBy` | Creator when the caller has an identity. Standalone `of` has no authenticated principal, so this is `null` today. |
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
https://<versionId>--<workerName>.<previewDomain>/?__vrt_preview_token=v1.<exp>.<hmac>
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
vrt_9f2c1e…--edge-api.preview.example.com/?__vrt_preview_token=v1.…  ← deploy --preview
vrt_71aa03…--edge-api.preview.example.com/?__vrt_preview_token=v1.…  ← now the ACTIVE version
```

The hostname alone is not a capability: without a valid token every
preview request is a 403.

### Configuration

Preview routing is **host configuration and off by default**. In
`examples/standalone-vrt-host.toml` / `root/config/vrt-host.toml`:

```toml
[preview]
# Public suffix for preview hostnames. Absent or empty = previews are
# not routable at all (every preview request is a 404).
domain = "preview.example.com"
# Mandatory HMAC key. Written by `of init` (0600, absolute path).
# Preview URLs are token-gated: a domain WITHOUT this file is closed.
tokenSecretFile = "/data/keys/preview-token.secret"
```

- `domain` must be a lowercase DNS name: no scheme, port, path,
  wildcard, leading/trailing dot, no `_`. An unusable value fails host
  startup (`preview.domain must be a lowercase DNS name …`) instead of
  silently serving nothing.
- `tokenSecretFile` must be an absolute path to a `0600` file holding 64
  hex chars. A configured-but-unreadable/malformed/world-readable file
  fails host startup; a domain with NO `tokenSecretFile` leaves preview
  serving **off** (a warning is logged). There is no anonymous mode.
- The host derives every preview hostname itself; the suffix is never
  read from a snapshot, a worker config or a request.
- `vrt-local` reads the **same file** only to report `previewUrl` in the
  API/CLI. It additionally requires the configured key file to match the
  key derived from `<root>/keys/signing.seed`, so a URL is never
  advertised unless the host can verify it. That value is display-only:
  routing authority stays with `vrt-host`.
- **Upgrade/downgrade**: adding `tokenSecretFile` turns previews from
  open to token-gated (the intended default). `[preview]` uses
  `deny_unknown_fields` like every host section, so an OLDER `vrt-host`
  refuses a config that carries the new key — remove the line before
  rolling the host back, or it fails startup (fail closed, never open).

### Which requests serve which version

| Request | Serves | Response markers |
| --- | --- | --- |
| production hostname (worker `routes`) | active version | none |
| `<versionId>--<worker>.<domain>` + valid token, version `preview` | that preview instance | `x-vrt-preview: 1`, `x-vrt-preview-version: <versionId>` |
| `<versionId>--<worker>.<domain>` + valid token, version `active` | the production instance (same code as production) | same markers |
| `<versionId>--<worker>.<domain>` with no/expired/wrong token | **403** (`preview_forbidden`) | — |
| any other `<…>.<domain>`, any preview host when the domain or token secret is not configured, or an archived version | **404** | — |

There is no fallback: an unknown, archived or mistyped version id never
resolves to production, a missing token never causes a production
response, and a preview request never mutates `activeVersionId`, worker
hostnames or bindings. Derived routes never
overwrite an existing route in the table (first-wins), so a snapshot can
never shadow a production hostname with a derived one.

Up to `MAX_PREVIEWS_PER_WORKER` (4) newest preview versions per worker
are projected as instances (and are therefore routable). Older preview
versions stay stored and promotable; they are just not served over HTTP
until re-previewed (`of deploy --preview` again).

`x-vrt-preview` / `x-vrt-preview-version` are also forwarded to the
worker on the request path, so the code can branch its own side effects
without parsing its Host header. The ingress strips worker-supplied
`x-vrt-*` response headers and re-adds them itself, so a production
worker cannot forge preview status.

### Access control (signed preview tokens)

Preview access is token-gated by default. The token is a bearer
capability bound to exactly one `(versionId, workerId)` pair:

```text
v1.<expiresAtUnixSeconds>.<hex hmac-sha256>

MAC input: "openflare.preview-token.v1\n<versionId>\n<workerId>\n<expiresAt>"
```

- The **key** is derived from the cell signing seed with a distinct
  label (`HMAC-SHA256(seed, "openflare.preview-token.v1")`) and persisted
  as `<root>/keys/preview-token.secret` (`0600`) so `vrt-host` can verify
  without holding the Ed25519 seed. Rotating the signing key invalidates
  every outstanding token — the only revocation mechanism (tokens are
  stateless and cannot be revoked individually).
- **Binding**: the host recomputes the MAC from the route it already
  resolved and the expiry field inside the token. Neither the version id
  nor the worker id is parsed out of attacker input, so a token cannot
  name a hostname of its own, cannot be replayed on another version or
  worker, and an edited expiry breaks the MAC.
- **Verification** runs before any tenant work: length-bounded parse,
  constant-time HMAC compare, then expiry. Missing → 403
  `preview_token_missing`, tampered/wrong pair → `preview_token_invalid`,
  past expiry → `preview_token_expired`, over-long lifetime →
  `preview_token_too_long`. Every failure is a hard 403; production is
  never used as a fallback.
- **Delivery**: the token rides in the `__vrt_preview_token` query
  parameter (so a link is self-contained) or in a host-scoped bootstrap
  cookie. The first authenticated navigation from a query token gets
  `Set-Cookie: __vrt_preview_token=…; Path=/; HttpOnly; Secure;
  SameSite=Lax; Max-Age=<remaining>`, so the page's subresources load
  without re-attaching the parameter. Each preview hostname is its own
  origin, so the cookie never crosses versions or workers. Both the query
  parameter and the cookie are stripped from the request forwarded to the
  worker.
- **Lifetime**: URLs printed by `of deploy --preview` and
  `of versions list` live one hour. `of versions share` accepts
  60 s…30 days (default 24 h). Verification refuses a correctly signed
  token whose remaining lifetime exceeds 30 days + 5 minutes skew.
- **No account model**: a token grants access to that preview only. It
  does not authenticate to the management API, production hostnames or
  another worker's preview.
- **Only routable versions get URLs.** Preview instances are projected
  for a SERVING worker only, so a preview-first worker (no active
  version yet) reports `previewUrl: null` until it is promoted once.
  `of versions share` refuses an archived or non-serving version
  instead of printing a dead link.
- **Referrer hygiene.** Preview responses set
  `Referrer-Policy: strict-origin-when-cross-origin` (the token is in the
  query string and must not leak cross-origin) and
  `X-Robots-Tag: noindex, nofollow`. The URL itself is a bearer
  credential: do not paste it into public logs, issues or analytics.

## Preview retention and GC

`deploy --preview` never evicts on its own, so the preview history of a
worker would grow forever. Retention is an explicit, stored policy — not
a hidden default buried in the deploy path.

### Policy

Stored in `state/retention.json` (a separate file, not inside
`state.json`: a build that predates retention must still be able to read
and rewrite the state document):

```json
{ "enabled": true, "maxAgeDays": 7, "maxCount": 4 }
```

| Field | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Run the automatic pass at the end of every deploy. A manual `of versions prune` always runs. |
| `maxAgeDays` | `7` | Remove preview versions older than this many days. `0` = no age limit. |
| `maxCount` | `4` | Keep at most this many previews per worker, newest first. `0` = no count limit. Matches the `MAX_PREVIEWS_PER_WORKER` render cap, so a stored preview is normally also a servable one. |
| | | An ENABLED policy must set at least one of the two limits; a policy that can never remove anything is refused. |

An absent file means the defaults. A malformed or out-of-bounds file
fails closed: deploys and `of versions prune` refuse to run instead of
guessing limits. `maxAgeDays` is capped at 3650 and `maxCount` at 256.

The policy applies to `preview` records only. Active and archived
versions are unaffected; archived history keeps following
`maxVersionsPerWorker` eviction on normal deploys.

### What is protected

- The worker's ACTIVE version, always. The mutation re-checks this, so
  a stale plan can never delete a version a concurrent promote just
  activated.
- The NEWEST preview of each worker, even when it is past `maxAgeDays`:
  the build an operator just created stays inspectable. Create a newer
  preview or promote it to retire it. Re-previewing a stored version
  refreshes its `createdAtUnixMs`, so it becomes the newest preview
  again (kept by retention and projected by the renderer). The version
  produced by the deploy that is running the automatic pass is never a
  candidate, even in a same-millisecond tie.
- Every artifact digest still referenced by ANY stored version of ANY
  worker, or by an init-seeded system module. Bundles are
  content-addressed and shared: pruning a version never deletes an
  object another version still needs.

"Newest" is ordered by `createdAtUnixMs` and then by insertion order in
`state.json` (versions are append-only). This is the SAME order
`to_desired` uses to pick the previews it renders, so GC can never drop a
preview the renderer would serve, and a deploy that created its version
in the same millisecond as an older one still counts as newer.

### When it runs

- **Automatic**: after every successful deploy/preview, BEFORE the
  desired document is validated and published. The signed snapshot
  therefore never references a pruned record, and a failure in the pass
  leaves the old version serving.
- **Manual**: `of versions prune [<worker>] [--dry-run]
  [--max-age-days <n>] [--max-count <n>]`. Worker is optional (all
  workers by default); CLI overrides are one-off and never persisted.
  `--dry-run` lists candidates and writes nothing.
- **Disable**: `of versions retention --no-auto` (or
  `"enabled": false`). With automatic pruning off nothing is deleted
  automatically; run `of versions prune` or re-enable the policy.

Policy edits are not versioned state: `of versions retention` writes the
policy file and appends a `versions-retention` oplog event, but does not
bump the signed-desired revision (nothing in the snapshot changes).

### Concurrency, idempotency and audit

- Prune takes the same `state/lock` writer flock as deploy, preview,
  promote and rollback. A prune and a promote of the same version are
  serialized: either promote commits first (the record becomes `active`
  and is protected) or prune commits first (promote then fails closed
  with `not found: version`). Neither order can produce a dangling
  `activeVersionId` or a stored version whose `objects/<digest>` is
  missing.
- A second pass over unchanged state removes nothing, does not bump the
  revision and does not republish the envelope (idempotent replay).
- Every non-empty pass appends a `versions-prune` event to
  `state/oplog.jsonl` with the trigger (`deploy`/`manual`), the policy
  used, and each removed version's worker, version id, artifact digest,
  creation time and reason (`age` or `count`). Dry runs write nothing.
- After the state write and publish, the existing object GC removes only
  digests no version references any more. A failed delete leaves an
  orphan file (harmless), never a broken reference.

Example:

```text
$ of versions retention --max-age-days 3 --max-count 2
{ "enabled": true, "maxAgeDays": 3, "maxCount": 2 }

$ of versions prune --dry-run
{
  "policy": { "enabled": true, "maxAgeDays": 3, "maxCount": 2 },
  "dryRun": true,
  "removed": [
    { "worker": "edge-api", "versionId": "vrt_9f2c…", "artifactDigest": "ab12…",
      "createdAtUnixMs": 1763…, "reason": "count" }
  ],
  "revision": 41,
  "changed": false
}
```

## Transaction boundaries

All mutating operations hold the single writer `flock` on
`state/lock`, then follow the existing write order:

1. materialize content-addressed assets / bundle objects;
2. write `vars.json` (union of every stored version's var values);
3. write `state.json` atomically;
4. sign and write `desired/envelope.json`;
5. append `state/oplog.jsonl`;
6. collect artifact objects no remaining version references (after the
   publish, still under the writer lock).

The preview retention pass runs after the deploy record is applied and
before the desired document is validated, so the state that gets
persisted already has the pruned records removed; object GC then runs at
step 6 for every deploy, preview included.

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
4. **Abuse bounds**: preview hostnames become resolvable once DNS exists,
   but content access is token-gated (see "Access control"). Add
   per-token rate limiting at the shared entrypoint if previews are
   reachable from the public internet; the host verifies every request
   but does not throttle token guesses.

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
- With no configured preview domain (or no `tokenSecretFile`) every
  preview hostname is a 404 — there is no "serve the newest preview
  anyway" fallback.
- A malformed/unreadable/world-readable token secret refuses host
  startup; a key file that does not match the control plane's derived
  key makes `vrt-local` report no URL (the host would 403).
- A preview request without a valid token is a 403; it is never served
  from the production instance.

## Explicitly not implemented

- Wildcard DNS, certificates and edge integration for preview URLs
  (see "Production integration").
- Per-version data isolation (KV/R2/D1/PG/queue namespaces).
- Per-preview passwords/basic auth, per-user authorization and
  individual token revocation (tokens expire; rotating the signing key
  revokes the whole set).
- Per-preview processes (previews share the tenant workerd process).
- Durable Object and Workflow previews.
- Pinning individual preview versions against retention. Promote the
  version to make it active, or create a newer preview; there is no
  per-version "keep" flag yet.
- Code/config diffs and per-version metrics beyond the preview
  instance's own worker label.
- Versioned routes/hostnames. Production hostnames remain worker-level;
  a preview never changes them.

## `isPreview` for `cloudflare.config.ts`

`of deploy --preview` passes `--preview` to the config loader, so
`defineConfig(({ isPreview }) => …)` takes its preview branch. The flag
is derived from the deploy mode (never from a caller-supplied override):
a normal `of deploy` can only evaluate with `isPreview = false`. See
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
  and fail-closed validation, and `tokenSecretFile` resolution (domain
  without a secret is closed; relative/world-readable/malformed secret
  fails startup; a valid pair mints and verifies).
- Protocol tests in `snapshot-protocol/src/compute_desired.rs`: preview
  instance round-trip and every rejection (routes, side effects,
  secrets, version mismatch, unknown/self/chained target, capacity).
- Token tests in `snapshot-protocol/src/preview_token.rs`: derivation is
  deterministic/domain-separated, version+worker binding, signed expiry,
  tamper/overlong/malformed rejection, query and cookie extraction, and
  the hardened bootstrap cookie.
- API tests in `vrt-local/tests/versioning.rs`: same input same id,
  preview not moving the desired active version, preview-first worker
  absent from desired, promote switching traffic and being idempotent,
  cross-worker rejection, missing artifact rejection, concurrent
  promote invariant, signed `previewUrl` from the host config (closed
  without a token secret), token binding/expiry, `share_version` scoping
  and TTL bounds, DO/workflow preview refusal.
- Retention tests in `vrt-local/src/retention.rs` (policy bounds,
  newest/active protection, count and age boundaries, idempotent
  mutation) and `vrt-local/tests/retention.rs` (aged preview pruned
  with its artifact, protected active/newest, dry run writes nothing,
  shared content-addressed artifact survives, automatic pass at deploy,
  disabled policy, policy file round-trip/fail-closed, concurrent
  prune+promote invariant, CLI `versions prune` / `versions retention`).
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
- Access-control end-to-end in `vrt-host/tests/preview_access_e2e.rs`:
  anonymous/expired/tampered/wrong-version/wrong-worker tokens are 403;
  a valid token serves the preview instance and sets the bootstrap cookie
  (cookie-only subresource request succeeds); production needs no token;
  a domain without a token secret stays closed (404, `previewUrl: null`)
  even with a valid token; the active version's preview host is gated
  too.
- Real-machine compose acceptance in `scripts/e2e-versioning.sh`: brings
  up `deploy/standalone` with a pinned real workerd, enables
  `[preview] domain` + `tokenSecretFile`, then drives one worker through
  deploy v1 → preview v2 (production stays v1, the signed URL serves v2)
  → promote v2 (production moves, the v2 URL follows) → rollback v1 (v2
  archived, its preview host 404s) → `versions prune --dry-run` (state
  hash and oplog unchanged) → real prune (only the aged preview is
  removed; active and newest preview survive) → `versions share` (no
  token and expired token are 403, the correct token is 200). The
  script redacts bearer tokens in every diagnostic and tears the stack
  down on failure.

## Changelog

- 2026-11-23: real-machine compose acceptance for the version
  lifecycle (`scripts/e2e-versioning.sh` plus its `node:test` guards).
  No runtime behavior change.
- 2026-11-22: preview access control. Signed, expiring preview tokens
  (HMAC bound to version + worker), mandatory by default; a
  `[preview] domain` without `tokenSecretFile` is closed; bootstrap
  cookie for subresources; `of versions share`; `previewUrl` is signed
  and reports its expiry; key derived from the signing seed and
  persisted as `keys/preview-token.secret`.
- 2026-11-22: re-previewing a stored version refreshes its recency
  (`createdAtUnixMs`) so it is the newest preview: it is projected again
  and survives the same deploy's retention pass. The deploy's own
  version is excluded from the automatic pass.
- 2026-11-22: preview retention/GC. Explicit policy in
  `state/retention.json` (defaults: enabled, 7 days, 4 previews per
  worker), automatic pass at the end of every deploy, `of versions
  prune` (with `--dry-run`) and `of versions retention`, protected
  active/newest previews, reference-counted artifact GC shared with
  normal deploy eviction, and `versions-prune` / `versions-retention`
  oplog events.
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
