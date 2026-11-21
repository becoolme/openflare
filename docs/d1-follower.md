# D1 read-only follower

`vrt-sql follower` materializes a read-only copy of a D1 database from the
object-store chain that the writer already publishes. It does not take the
writer lease, does not write to the object store, and never accepts SQL writes.

This is the P0 replication core. Provider-side bookmark routing is a follow-up;
today the follower publishes an immutable local generation plus a watermark that
a co-located read path can open.

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

## Limits (P0)

- **Full-file copy per applied batch.** Safe and simple; reflink/incremental
  page application is a later optimization. A follower polling once per second
  copies once per poll (batches coalesce to the manifest head), not once per
  commit.
- **One database per process config.** Run multiple processes or extend the
  config to a list later.
- **No provider routing yet.** D1 reads still go to the writer. Bookmark-based
  selection (`servable(bookmark) = appliedEpoch > bm.epoch || (== && appliedLsn
  >= bm.lsn)`) and a read-only query surface are the next step.
- **No failover.** Promotion of a follower to writer still requires the
  existing lease/epoch takeover; the follower is not a consensus member.
- **GC watermark**: if the writer checkpoints and GCs faster than the follower
  polls, the follower re-bootstraps from the snapshot. Keep
  `pollIntervalMs` well below the writer's effective snapshot interval, or
  increase snapshot retention when that changes.
- **Full snapshot download on re-bootstrap.** A large database re-downloads the
  snapshot when it cannot tail from its watermark.

## Tests

`vrt-sql/tests/follower_tests.rs`:

- bootstrap from the segment chain and from the latest snapshot;
- incremental tail, pinned-reader isolation, steady-state no-write;
- restart resumption from the local generation;
- GC-past-watermark re-bootstrap;
- corrupt segment fail-closed with the previous generation still readable;
- writer takeover across epochs;
- empty source, generation pruning, and status lag reporting.

Unit tests in `vrt-sql/src/follower.rs` cover config parsing/validation, URI
escaping, and generation naming.
