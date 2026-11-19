//! vrt-sql：D1/Queues 数据面 SQL 服务（Phase 3 / P3a）。
//!
//! 本轮范围（T33）：单进程本地 rusqlite 引擎 + D1 `/query`/`/execute`
//! wire 层 + 单写者（进程内 map + 文件锁）+ `_vrt_dedup` 同事务去重账。
//! 持久化等级：**仅本地 fsync，无复制**；对象存储 LTX 复制、lease 与
//! epoch fencing 留给后续阶段（TxnCtx 已预留 epoch 字段）。

pub mod bookmark;
pub mod clock;
pub mod do_snap;
pub mod engine;
pub mod metrics;
pub mod paths;
pub mod persist;
pub mod queue;
pub mod security;
pub mod seg;
pub mod service;
pub mod wal;
pub mod wire;
