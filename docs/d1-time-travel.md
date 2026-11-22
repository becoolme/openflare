# D1 Time Travel (offline P0)

`vrt-sql time-travel` restores a database to a historical commit. It reuses the existing
`snapshot + vrt-seg + manifest` chain and adds a small commit timeline; it does not maintain
per-minute snapshots.

This is an **offline operator command**: stop `vrt-sql serve` for the database, run the restore,
then restart the writer. An online API, platform cell-op and UI are follow-ups.

## Prerequisites

- `[persistence]` is `fs` or `s3` (the same store as the writer).
- The writer is stopped. The command takes the same local `db.lock` as the engine and fails with
  `writer appears to be running` if it cannot acquire it.
- History retention is enabled:

```toml
[persistence]
backend = "s3"
timeTravelRetentionMs = 604800000   # 7 days; default 0 = disabled
# ... [persistence.s3] ...
```

- `checkpointEveryCommits` is non-zero for cheap restores (default 64). Without snapshots the
  restore replays the segment chain from the initial image, which is correct but slower and
  depends on older segments still being retained.

## Command

```bash
# Resolve a wall-clock target (unix milliseconds) to the newest indexed commit at or before it.
vrt-sql time-travel \
  --config /etc/openflare/vrt-sql.toml \
  --db 01j2m8x4q9z7v3k6w5n1p0r8ta \
  --at 1799999999000

# Restore a signed bookmark (undo path).
vrt-sql time-travel ... --db <id> --bookmark 0000000000000002-000000000000002a-0123456789abcdef

# Inspect without changing anything; --json is machine-readable.
vrt-sql time-travel ... --db <id> --at <ms> --dry-run --json
```

The config is the same shape as the follower, minus `[follower]`:

```toml
dbRoot = "/var/lib/vrt-sql/dbs"

bookmarkSecretFile = "/etc/openflare/secrets/bookmark"  # for --bookmark / returned bookmarks

[persistence]
backend = "fs"
timeTravelRetentionMs = 604800000

[persistence.fs]
root = "/var/lib/vrt-sql/persistence"
```

`--at` is unix milliseconds. The resolved commit is the newest commit whose timeline timestamp is
`<=` the target. It is never rounded forward.

## Semantics

- The restored image is published as a **new commit** on a new writer epoch:
  `newLsn = previousHead + 1`, with a fresh snapshot at `newLsn`. The manifest lsn never rolls back.
- The command prints `previousBookmark` and `newBookmark`. The previous head stays addressable as
  long as its snapshot/segments are inside the retention window (undo is a normal bookmark restore).
- `--dry-run` reports the resolved commit, baseline snapshot and segment count without claiming
  the lease or rewriting the local database.
- Restore is crash-safe:
  1. the historical image is rebuilt in `.restore.tmp` and integrity-checked;
  2. it is renamed over `db.sqlite`;
  3. the new snapshot is uploaded;
  4. the manifest CAS publishes `newLsn`.
  A crash before the CAS leaves the local sidecar behind the manifest lsn, so the next writer
  restores the old head (the failed attempt is discarded); a crash after the CAS leaves the
  published state.

## Timeline and retention

The writer records `(lsn, epoch, committedAtMs)` entries and flushes a small JSON chunk at every
checkpoint and every 16 commits. Consequences:

- time resolution is commit-level, not minute-rounded;
- the last few commits before a crash may be absent from the timeline; a target in that tail
  resolves to the newest indexed commit at or before it (never a future state);
- `--bookmark` does not need the timeline; it restores the exact signed point.

`timeTravelRetentionMs > 0` makes GC keep every snapshot inside the window plus one baseline
snapshot before it, and the segments above that baseline. Older snapshots/metadata are deleted.
With `0`, GC keeps only the bounded two-snapshot window used for follower catch-up, so Time Travel
is effectively limited to that window.

Storage cost: one snapshot per `checkpointEveryCommits` commits inside the retention window. Size the
window and checkpoint interval together.

## Limits

- **Offline only** (P0): no HTTP admin endpoint, no platform API, no UI, no scheduled restores.
- Single database per invocation; the operator must know the derived `databaseId`.
- `--at` accepts unix milliseconds only (no RFC3339 parser in the data-plane binary).
- No timeline entry is created for commits that were never flushed before a crash.
- The restore itself is a full image rebuild (snapshot download + segment replay); large databases
  take time and temporary disk.
- No cross-cell orchestration: stop/restart the writer yourself.
- Time Travel does not change D1 wire semantics; `dump()`, sessions and bookmarks are unchanged.

## Tests

`vrt-sql/tests/time_travel_tests.rs`:

- timeline flushes independently of checkpoints;
- restore by time, by signed bookmark, and re-restore of the produced bookmark;
- continuation after restore uses `newLsn + 1` and the new epoch;
- dry run leaves the manifest and local image untouched;
- a live writer lock is refused;
- retention keeps the in-window snapshots plus one baseline and replays from the right segment.

`vrt-sql/tests/time_travel_cli.rs` runs the real binary against an fs store and asserts the JSON
output and the restored row count.
