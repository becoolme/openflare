# D1 read-only follower

`vrt-sql follower` materializes a read-only copy of a D1 database from the
object-store chain that the writer already publishes. It does not take the
writer lease, does not write to the object store, and never accepts SQL writes.

The follower optionally serves a read-only D1 wire (`/dbs/{id}/query`) and the
provider routes eligible reads to it using the tenant's session bookmark. `execute`
and `dump` always stay on the writer; the follower never executes a write.

## What it reuses

The writer already uploads, before acknowledging a commit:

- a checkpoint snapshot (`snap-{lsn}.snap`),
- one `vrt-seg` segment per commit (page images + `prev_lsn` chain +
  `sha256`),
- a manifest with the epoch chain (`{epoch, first_lsn}`) and the latest
  snapshot.

The follower verifies that chain and replays it into a local SQLite image. It
does not need a new replication protocol, a new format, or another database
engine.

## Configuration

```toml
dbRoot = "/var/lib/vrt-sql/replica"

[follower]
databaseId = "01j2m8x4q9z7v3k6w5n1p0r8ta"
pollIntervalMs = 500      # default 1000; range [50, 3600000]
retainGenerations = 2     # default 2; range [1, 16]

# Optional read-only D1 wire. Exactly one of unixDir / listenTcp.
listenTcp = "0.0.0.0:8091"
authSecretFile = "/etc/openflare/secrets/vrt-sql-auth"
bookmarkSecretFile = "/etc/openflare/secrets/bookmark"

[persistence]
backend = "s3"            # follower requires fs or s3, never none

[persistence.s3]
endpoint = "https://account.r2.cloudflarestorage.com"
region = "auto"
bucket = "openflare-d1"
accessKeyFile = "/etc/openflare/secrets/r2-access-key"
secretKeyFile = "/etc/openflare/secrets/r2-secret-key"
prefix = "prod"
```

JSON is accepted with the same camelCase keys. The follower does **not** read
the bookmark or auth secrets — it only reads objects.

Run it as a separate process next to the writer (or on another host that can
reach the same bucket):

```bash
vrt-sql follower --config /etc/openflare/d1-follower.toml
# or: VRT_SQL_FOLLOWER_CONFIG=/etc/openflare/d1-follower.toml vrt-sql follower
```

One process replicates one `databaseId`. Start several processes for several
databases.

## Promotion

Before a manual writer promotion, run the read-only preflight:

```bash
vrt-sql promote --check --config /etc/openflare/d1-follower.toml
```

It prints `Follower::promotion_readiness()` JSON and exits non-zero unless the
follower is caught up, has a non-empty readable generation, and is not ahead of
the manifest. It never CASes the manifest or changes local state.

Promote for real with the same config:

```bash
# Stop the old writer first (or accept that it will be fenced).
vrt-sql promote --config /etc/openflare/d1-follower.toml \
  --writer-db-root /var/lib/vrt-sql/dbs [--json]
```

Promotion:
1. re-verifies the follower is exactly at the manifest head;
2. uploads the generation as the next epoch's baseline snapshot;
3. CASes the manifest to `epoch + 1` (this fences the old writer on its next
   commit);
4. adopts the generation as `{writerDbRoot}/{shard}/{id}/db.sqlite` and writes
   the `.vrt-lsn` sidecar;
5. releases the new lease so `vrt-sql serve` can claim it immediately.

If the process crashes between the manifest CAS and local adoption, rerunning
the same command resumes only the adoption (the manifest stores the generation
sha256). A follower that is behind, ahead, or whose manifest moved fails closed
with a non-zero exit.

## Serving reads and routing

A follower with a listener exposes:

| Endpoint | Behaviour |
| --- | --- |
| `POST /dbs/{id}/query` | Runs the batch on the current immutable generation (read-only connection). |
| `POST /dbs/{id}/execute` | Always HTTP 409 `VRT_REPLICA_READ_ONLY`. |
| `POST /dbs/{id}/dump` | Always HTTP 409 `VRT_REPLICA_PRIMARY_ONLY`. |
| `GET /internal/replica/status` | Authenticated watermark/status JSON. |
| `GET /healthz`, `/readyz` | Liveness; readiness requires a non-empty generation. |

The provider opts in per cell:

```toml
[vrtSql]
addr = "http://vrt-sql:8082"
authSecretFile = "/etc/openflare/secrets/vrt-sql-auth"
timeoutMs = 30000
replicas = [
  "http://vrt-sql-replica-0:8091",
  "http://vrt-sql-replica-1:8091",
]
```

Routing rules (round-robin over `replicas`):

1. `execute` and `dump` always go to `addr` (the writer).
2. `query` goes to a replica only when the session is not `first-primary`.
3. The replica checks the inbound `x-cf-d1-session-commit-token`:
   - `first-primary` -> `VRT_REPLICA_PRIMARY_REQUIRED`;
   - a signed bookmark -> served only if `(appliedEpoch, appliedLsn) >= bookmark`;
   - otherwise `VRT_REPLICA_STALE`;
   - `first-unconstrained`, empty, or absent -> no constraint.
4. A statement that would write is detected by SQLite's readonly error and
   answered with `VRT_REPLICA_READ_ONLY`; it is never executed on the replica.
5. The provider retries the read **once on the writer** for any of these 409
   signals, a 429/5xx, or a transport error. A replica SQL error (HTTP 200
   `{success:false}`) is returned as-is and is not retried.
6. A replica response carries its own signed bookmark at
   `(appliedEpoch, appliedLsn)`; workerd keeps the greatest token it has seen,
   so a lagging replica can never move a session backwards.

Because the replica decides with its exact watermark, the provider needs no
status poller and no eventually-consistent health cache. A stale replica costs
one extra request; a wrong answer is never served.

## On-disk layout

```text
{dbRoot}/
  follower-state.json                       watermark (published last)
  gen-{epoch:016x}-{lsn:016x}.sqlite        immutable generations
```

A sync builds a **new** generation:

1. bootstrap: download the latest snapshot (or start from an empty image) and
   apply segments up to the manifest head;
2. incremental: copy the current generation and apply only the new segments;
3. fsync, run `PRAGMA integrity_check`, clear SQLite sidecars;
4. atomically rename the file to `gen-{epoch}-{lsn}.sqlite`;
5. fsync the directory;
6. atomically replace `follower-state.json` (temp file + rename + dir fsync).

A crash before step 6 leaves an unreferenced generation, which is pruned on the
next successful sync. The previous generation and its watermark remain readable
throughout. Generations are never modified after publication, so a reader can
hold one open while newer generations are published.

## Reader contract

`Follower::open_readonly` (Rust API, `vrt-sql/src/follower.rs`) opens the
generation named in `follower-state.json` with SQLite `immutable=1`:

- the connection always sees one complete commit point;
- it may still see the previous commit after a sync — reopen to observe newer
  data (the same semantics as a read replica that has not been re-queried);
- `immutable=1` is sound because generation files are never written after
  publication; it also avoids needing a writable `-shm`.

`open_readonly` returns `NotReady` until the first commit exists.

## Watermark

`follower-state.json`:

```json
{
  "v": 1,
  "databaseId": "01j2m8x4q9z7v3k6w5n1p0r8ta",
  "epoch": 2,
  "lsn": 42,
  "snapshotLsn": 40,
  "generation": "gen-0000000000000002-000000000000002a.sqlite",
  "headEpoch": 2,
  "headLsn": 44,
  "updatedAtMs": 1799999999000
}
```

`Follower::status` combines it with a fresh manifest read:

| Phase | Meaning |
| --- | --- |
| `empty` | No manifest yet (database not created) or no local state |
| `catchingUp` | `appliedLsn < headLsn` |
| `caughtUp` | `appliedLsn == headLsn` |
| `stale` | `appliedLsn > headLsn` (should not happen; triggers a rebuild) |

`lagCommits = headLsn - appliedLsn`. There is no segment timestamp yet, so the
status reports commit lag, not wall-clock lag.

## Failure handling

The follower is fail-closed:

- snapshot size or SHA-256 mismatch, segment checksum mismatch, database-id
  mismatch, page-size drift, chain break, or a failed `integrity_check` aborts
  the batch — no generation is published;
- a tombstones manifest (database deleted) stops the follower with
  `Tombstoned`;
- a segment missing because the writer's GC advanced past this follower (or a
  snapshot newer than the local watermark) triggers **one** bounded re-bootstrap
  from the current snapshot; a second failure is returned as `Corrupt`;
- a writer takeover across epochs is followed normally: segments are looked up
  by the manifest's epoch chain, so `prev_lsn` stays contiguous across epochs;
- a corrupt/unreadable `follower-state.json` is treated as "no state" and rebuilt
  from the object store.

The follower never deletes or mutates writer objects, so a broken follower
cannot damage the primary.

## Limits

- **Full-file copy per applied batch.** Safe and simple; reflink/incremental
  page application is a later optimization. A follower polling once per second
  copies once per poll (batches coalesce to the manifest head), not once per
  commit.
- **One database per process config.** Run multiple processes for several
  databases.
- **Read-only routing only.** `execute`, `batch()` writes, migrations and
  `dump()` stay on the writer. A write sent through `/query` is rejected by the
  replica and retried on the writer (no double execution: the replica is
  read-only).
- **Manual failover only.** `vrt-sql promote` performs the lease/epoch CAS and
  local adoption; automatic promotion is still design-only. The follower is not
  a consensus member, and the old writer must be stopped (it is fenced on its
  next commit, not killed).
- **Bounded GC retention, no per-replica watermark.** GC keeps the newest two
  snapshots and the segments above the older one, so a follower that is one
  checkpoint behind can continue incrementally. A follower further behind (or
  one that misses the window) re-bootstraps from the current snapshot. Per-replica
  watermark reporting (retain exactly what live followers still need) is next.
- **Full snapshot download on re-bootstrap.** A large database re-downloads the
  snapshot when it cannot tail from its watermark.
- **Per-request engine open.** Each replica query opens the immutable generation
  and configures the SQLite authorizer; there is no connection pool yet.
- **No automatic replica discovery.** Replicas are static provider config.
- **Metrics** are per process; there is no cross-process lag dashboard yet.
  Use `/internal/replica/status` and logs.

## Tests

`vrt-sql/tests/follower_tests.rs`:

- bootstrap from the segment chain and from the latest snapshot;
- incremental tail, pinned-reader isolation, steady-state no-write;
- restart resumption from the local generation;
- GC-past-watermark re-bootstrap;
- corrupt segment fail-closed with the previous generation still readable;
- writer takeover across epochs;
- empty source, generation pruning, and status lag reporting.

`vrt-sql/tests/replica_tests.rs`:

- authenticated query serving and bookmark watermark;
- newer commits visible after the next sync;
- stale / invalid / `first-primary` session constraints return the routing code;
- writes on `/query` and all of `/execute` / `/dump` are rejected;
- unknown database, auth-required status endpoint.

`vrt-provider/src/d1.rs` unit tests cover routing: query to a replica, execute
and `first-primary` to the writer, stale/read-only/transport fallback, and
no-retry for a replica SQL error. One end-to-end test runs the real writer and
replica wire handlers over TCP and asserts read-your-writes: the new bookmark is
served by the writer before the next sync and by the replica after it.

Unit tests in `vrt-sql/src/follower.rs` cover config parsing/validation, URI
escaping, and generation naming.
