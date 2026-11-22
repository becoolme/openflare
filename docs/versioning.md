# Deploy versions (MVP)

`vrt` stores every deploy as an immutable version. The MVP separates
"upload a version" from "serve a version": `deploy --preview` writes a
version without moving traffic, `promote` switches the active pointer
through the existing signed-desired path, and `versions list` shows
the lifecycle of every stored version.

This page describes the MVP boundary. It is intentionally smaller
than Cloudflare's full version and preview-routing model.

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
including `versionId`. No id is printed unless the version record was
persisted; a failure after the object write but before the state write
leaves only a harmless orphan object, never a reported version.

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

## Transaction boundaries

All mutating operations hold the single writer `flock` on
`state/lock`, then follow the existing write order:

1. materialize content-addressed assets / bundle objects;
2. write `vars.json` (union of every stored version's var values);
3. write `state.json` atomically;
4. sign and write `desired/envelope.json`;
5. append `state/oplog.jsonl`.

`deploy --preview` writes a `preview` record, keeps `activeVersionId`
and worker hostnames unchanged, and re-publishes the same desired
worker set at a new revision. A worker that has only preview versions
has no active pointer and is skipped by `to_desired`, so it cannot be
served accidentally. Preview also skips the workerd load preflight
(no traffic moves); `promote` runs the same preflight used by deploy
and rollback.

`promote` validates that the version belongs to the resolved worker
and that `objects/<artifactDigest>` still exists. It then switches the
pointer, validates the desired document, runs the workerd preflight,
writes state, and publishes. Any failure before the publish leaves the
old active version and the old envelope intact.

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
  currently serving version. The preview bundle may still reference
  existing secret refs.
- Promote refuses a version whose bundle object was removed.
- A worker with a dangling `activeVersionId` still fails desired
  validation; it is never silently skipped.
- Failed preview/promote operations never return a version id or a
  success revision.

## Explicitly not implemented

- Serving multiple versions at the same time.
- Preview URLs, wildcard DNS, or certificates.
- Automatic GC and retention policy for versions. Normal deploys keep
  the existing `maxVersionsPerWorker` eviction behavior; the preview
  path does not evict anything. This MVP does not delete versions on
  its own.
- Per-version metrics, code/config diffs, and access control.
- Evaluating and passing `isPreview` through worker config. `isPreview`
  is understood by the wrangler config helper but `vrt` does not pass
  it until preview routing exists.
- Versioned secrets and versioned routes/hostnames. Hostnames remain
  worker-level; a preview does not change them.

## Tests

- Unit tests in `vrt-local/src/state.rs`: preview lifecycle, single
  active version, idempotent promote, identity excluding lifecycle
  metadata, legacy state normalization.
- API tests in `vrt-local/tests/versioning.rs`: same input same id,
  preview not moving the desired active version, preview-first worker
  absent from desired, promote switching traffic and being idempotent,
  cross-worker rejection, missing artifact rejection, concurrent
  promote invariant.

## Changelog

- 2026-11-22: initial versioning MVP (`deploy --preview`, `promote`,
  `versions list`), lifecycle fields on `VersionRecord`, and
  preview-only workers skipped by `to_desired`.
