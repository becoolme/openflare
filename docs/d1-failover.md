# D1 failover: staged design

Status: stages 1–2 implemented (manual promotion + provider routing). Stage 3
(automatic failover) is design only.

## Goal

Promote a verified follower to writer after the current writer is fenced or
unreachable, without split-brain writes, and let the provider route reads back
to the new writer.

## Invariants

- One writer epoch at a time. Every commit carries `(epoch, lsn)`; readers and
  writers reject stale epochs.
- Promotion is a CAS on the object-store manifest (`epoch + 1`), never a local
  decision. A disconnected writer cannot commit because its publish CAS fails.
- A promoted writer starts from the newest follower generation whose manifest
  chain was verified, and publishes a baseline snapshot before accepting
  writes.
- Failover is explicit until quorum/liveness is well understood. Automatic
  promotion must be opt-in per cell.

## Stages

1. **Manual promotion — implemented**
   - `vrt-sql promote --config follower.toml` verifies the local generation,
     CASes the manifest epoch, publishes it as the new epoch's baseline
     snapshot, and adopts it as the local writer image.
   - The promote process releases the new lease; start `vrt-sql serve` with the
     same `[persistence]` and `dbRoot` (`--writer-db-root`) to serve.
   - A crash between CAS and adoption is resumable: the manifest carries the
     generation sha256, so a rerun only completes the local adoption.
   - Fencing test: old writer's next commit fails; new writer accepts.
   - Tests: `vrt-sql/tests/follower_tests.rs` (`promote_*`).
2. **Provider routing — implemented**
   - The signed D1 binding carries `epoch` and an optional `writerAddr`.
     The provider uses `writerAddr` for the writer attempt and falls back to
     `[vrtSql].addr` when absent; replicas keep the existing bounded fallback.
   - The promoted epoch and address must be published together (one signed
     desired revision) after the new writer is serving.
   - The data plane rejects a request whose signed epoch differs from the
     current writer lease epoch (`VRT_FENCED`), so an old provider snapshot
     cannot write against the promoted writer.
3. **Automatic failover**
   - Cell-controller health probe + lease-aware promotion with a cooldown.
   - Requires a witness (object-store CAS is enough for single-writer; no
     quorum replication) and must fail closed if manifest freshness is unknown.

## Non-goals

Strongly consistent multi-replica D1, read-your-writes across replicas, and
zero-RPO failover. The current follower is asynchronous.

## Tests

- Promotion CAS vs stale writer (`vrt-sql/tests/follower_tests.rs`).
- Crash between CAS and baseline publish (resume idempotently).
- Stale follower refused.
- Provider writer-address override and replica fallback
  (`vrt-provider/src/d1.rs` unit tests).
- Stale writer epoch rejected at the wire (`vrt-sql/src/wire.rs`).
