//! vrt-sql: D1/Queues data-plane SQL service (Phase 3 / P3a).
//!
//! Scope of this round (T33): single-process local rusqlite engine + D1 `/query`/`/execute`/`/dump`
//! wire layer + single writer (in-process map + file lock) + `_vrt_dedup` same-transaction dedup ledger.
//! Durability level: **local fsync only, no replication**; object-store LTX replication, lease and
//! epoch fencing are left to later phases (TxnCtx already reserves the epoch field).

pub mod bookmark;
pub mod clock;
pub mod do_snap;
pub mod engine;
pub mod follower;
pub mod k2;
pub mod metrics;
pub mod paths;
pub mod persist;
pub mod queue;
pub mod replica;
pub mod security;
pub mod seg;
pub mod service;
pub mod timeline;
pub mod time_travel;
pub mod wal;
pub mod wire;
