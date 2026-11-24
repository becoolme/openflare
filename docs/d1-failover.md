# D1 failover: staged design

Status: design only — not implemented.

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

1. **Manual promotion**
   - `vrt-sql promote --config follower.toml` verifies the local generation,
     CASes the manifest epoch, rewrites local writer state, and starts the
     writer.
   - Fencing test: old writer's next commit fails; new writer accepts.
2. **Provider routing**
   - Follower status exposes generation/epoch.
   - Provider re-resolves writer endpoint after epoch change; stale-read
     fallback stays bounded.
3. **Automatic failover**
   - Cell-controller health probe + lease-aware promotion with a cooldown.
   - Requires a witness (object-store CAS is enough for single-writer; no
     quorum replication) and must fail closed if manifest freshness is unknown.

## Non-goals

Strongly consistent multi-replica D1, read-your-writes across replicas, and
zero-RPO failover. The current follower is asynchronous.

## Tests

- Promotion CAS vs stale writer.
- Crash between CAS and baseline publish (resume idempotently).
- Stale follower refused.
- Provider endpoint switch under concurrent reads/writes.
