//! Kvrocks (Redis-protocol) [`KvProvider`].
//!
//! Layout per `(tenantId, namespaceId)` — both segment-encoded so `:`/`/`
//! cannot be injected:
//! - `vrt:kv:{t}:{ns}:{key}` — HASH `{v: value bytes, e: expires_i64
//!   ascii, m: canonical metadata json}` (`e` absent → persistent, `m`
//!   absent → no metadata). One hash per key makes reads atomic: a single
//!   `HGETALL`/`HMGET` can never pair the value of one put with the
//!   metadata of another (the two-key layout could, since a pipeline is
//!   not atomic against other connections' `MULTI`s).
//! - `vrt:kx:{t}:{ns}` — ZSET ordered index of live keys (score 0).
//!
//! `put` rewrites the whole hash inside `MULTI/EXEC` (`DEL` first so a
//! stale `m`/`e` field or TTL never survives an overwrite); `delete`
//! removes hash + index atomically. `list` walks the index with
//! `ZRANGEBYLEX` chunks, resolves `e`/`m` via `HMGET`, filters expiries
//! client-side (index members don't expire), and `ZREM`s dead entries
//! opportunistically.
//!
//! Consistency: list is a best-effort snapshot — concurrent puts may or
//! may not appear; keyset semantics (`key > after`) guarantee a stable
//! resume point so pagination never skips committed keys on a quiescent
//! namespace.

use crate::context::BindingContext;
use crate::encoding::segment_encode;
use crate::error::ProviderError;
use crate::limits::{KV_MAX_KEY_BYTES, KV_MAX_LIST_LIMIT, KV_MAX_VALUE_BYTES};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, ErrorKind, RedisError};
use std::time::Duration;

use super::{KvEntry, KvListPage, KvListRow, KvProvider, KvPutOptions};

const LIST_CHUNK: usize = 128;
/// get_many phase-2 value fetch chunk (RV-R8 #4) — bounds both the
/// per-pipeline reply size and the worst-case fetch amplification.
const VALUE_CHUNK: usize = 16;
const META_NO_EXPIRY: i64 = -1;

const FIELD_VALUE: &[u8] = b"v";
const FIELD_EXPIRES: &[u8] = b"e";
const FIELD_META: &[u8] = b"m";

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn ns_parts(ctx: &BindingContext) -> (String, String) {
    match ctx.resource() {
        crate::context::BindingResource::KvNamespace { id } => {
            (segment_encode(ctx.tenant_id()), segment_encode(id))
        }
        _ => unreachable!("KvProvider invoked with non-KV resource"),
    }
}

fn entry_key(t: &str, n: &str, key: &[u8]) -> Vec<u8> {
    let mut out = format!("vrt:kv:{t}:{n}:").into_bytes();
    out.extend_from_slice(key);
    out
}

fn index_key(t: &str, n: &str) -> String {
    format!("vrt:kx:{t}:{n}")
}

/// Validate stored canonical metadata bytes exactly like upstream's
/// `validate_stored_metadata` — bytes must be canonical JSON already.
fn validate_stored_metadata(metadata: &[u8]) -> Result<(), ProviderError> {
    let parsed: serde_json::Value =
        serde_json::from_slice(metadata).map_err(|_| ProviderError::KvCorrupt)?;
    if super::frame::canonical_metadata(&parsed).map_err(|_| ProviderError::KvCorrupt)? != metadata
    {
        return Err(ProviderError::KvCorrupt);
    }
    Ok(())
}

/// Fold one hash's `e`/`m`/`v` fields into a `KvEntry`. `v` missing →
/// corrupt; expired → `Ok(None)` by the caller's now-check.
fn entry_from_fields(
    value: Option<Vec<u8>>,
    expires_raw: Option<Vec<u8>>,
    meta: Option<Vec<u8>>,
) -> Result<KvEntry, ProviderError> {
    let Some(value) = value else {
        return Err(ProviderError::KvCorrupt);
    };
    let expires_at_ms = match expires_raw {
        Some(raw) => {
            let text = std::str::from_utf8(&raw).map_err(|_| ProviderError::KvCorrupt)?;
            let parsed: i64 = text.parse().map_err(|_| ProviderError::KvCorrupt)?;
            if parsed == META_NO_EXPIRY {
                None
            } else {
                Some(parsed)
            }
        }
        None => None,
    };
    if let Some(metadata) = &meta {
        validate_stored_metadata(metadata)?;
    }
    Ok(KvEntry {
        value,
        metadata_json: meta,
        expires_at_ms,
    })
}

fn map_redis(write: bool, error: &RedisError) -> ProviderError {
    // A connection-level failure on a mutation is ambiguous — the command
    // may have committed before the link dropped. Deterministic server
    // errors (the command was parsed and rejected) are known failures.
    if write && matches!(error.kind(), ErrorKind::IoError) {
        ProviderError::KvResultUnknown
    } else {
        ProviderError::KvUnavailable
    }
}

/// RV-R71: minimum gap between two connect attempts after a failure, so
/// a down kvrocks costs one bounded dial per window, not one per request.
const RECONNECT_COOLDOWN: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct ManagedState {
    /// `(generation, connection)`; `None` = needs (re)connect.
    conn: Option<(u64, MultiplexedConnection)>,
    next_gen: u64,
    last_failure: Option<tokio::time::Instant>,
}

/// RV-R71: lazily connecting, self-healing connection source. A
/// `MultiplexedConnection` never reconnects by itself: after an IO error
/// every later command fails forever. Here an IO error / timeout drops
/// the slot (by generation, so a stale failure can't evict a fresh
/// connection) and the next call dials again, single-flight, bounded by
/// `connect_timeout`, and rate-limited by [`RECONNECT_COOLDOWN`].
#[derive(Debug)]
struct Managed {
    client: redis::Client,
    connect_timeout: Duration,
    state: tokio::sync::Mutex<ManagedState>,
}

#[derive(Debug)]
enum ConnSource {
    /// Caller-owned connection (tests, one-shot purge executor).
    Fixed(MultiplexedConnection),
    Managed(Managed),
}

impl Managed {
    async fn acquire(&self) -> Result<(u64, MultiplexedConnection), ProviderError> {
        let mut st = self.state.lock().await;
        if let Some((g, c)) = &st.conn {
            return Ok((*g, c.clone()));
        }
        if st
            .last_failure
            .is_some_and(|t| t.elapsed() < RECONNECT_COOLDOWN)
        {
            return Err(ProviderError::KvUnavailable);
        }
        let dial = tokio::time::timeout(
            self.connect_timeout,
            self.client.get_multiplexed_tokio_connection(),
        )
        .await;
        match dial {
            Ok(Ok(c)) => {
                st.next_gen += 1;
                let g = st.next_gen;
                st.conn = Some((g, c.clone()));
                st.last_failure = None;
                Ok((g, c))
            }
            Ok(Err(_)) | Err(_) => {
                st.last_failure = Some(tokio::time::Instant::now());
                tracing::warn!("kvrocks connect failed; will retry after cooldown");
                Err(ProviderError::KvUnavailable)
            }
        }
    }

    async fn invalidate(&self, generation: u64) {
        let mut st = self.state.lock().await;
        if st.conn.as_ref().is_some_and(|(g, _)| *g == generation) {
            st.conn = None;
            // No cooldown here: the first redial after a broken link is
            // immediate (so the single read retry can succeed); only a
            // FAILED dial starts the cooldown.
        }
    }
}

/// Redis-protocol KV provider for Kvrocks.
#[derive(Debug)]
pub struct KvrocksKvProvider {
    src: ConnSource,
    /// Per-command timeout.
    timeout: Duration,
    /// One bounded retry on idempotent reads only.
    retry_reads: bool,
}

impl KvrocksKvProvider {
    /// Wrap an existing multiplexed connection.
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self {
            src: ConnSource::Fixed(conn),
            timeout: Duration::from_secs(10),
            retry_reads: true,
        }
    }

    /// RV-R71: lazy, reconnecting provider. Never dials at construction,
    /// so a kvrocks outage at startup does not take the whole provider
    /// (D1/R2/queues) down; KV calls fail closed with the retryable
    /// `KV_UNAVAILABLE` until the first successful dial.
    pub fn lazy(url: &str, connect_timeout: Duration) -> Result<Self, ProviderError> {
        let client = redis::Client::open(url).map_err(|_| ProviderError::KvUnavailable)?;
        Ok(Self {
            src: ConnSource::Managed(Managed {
                client,
                connect_timeout,
                state: tokio::sync::Mutex::new(ManagedState {
                    conn: None,
                    next_gen: 0,
                    last_failure: None,
                }),
            }),
            timeout: Duration::from_secs(10),
            retry_reads: true,
        })
    }

    /// RV-R71: real readiness probe — `PING` bounded by the command
    /// timeout (dials first if disconnected). The error carries only the
    /// fixed code; never the address or credentials.
    pub async fn probe(&self) -> Result<(), ProviderError> {
        self.run(false, |mut conn| async move {
            redis::cmd("PING").query_async::<String>(&mut conn).await
        })
        .await
        .map(|_| ())
    }

    async fn acquire(&self) -> Result<(u64, MultiplexedConnection), ProviderError> {
        match &self.src {
            ConnSource::Fixed(c) => Ok((0, c.clone())),
            ConnSource::Managed(m) => m.acquire().await,
        }
    }

    async fn invalidate(&self, generation: u64) {
        if let ConnSource::Managed(m) = &self.src {
            m.invalidate(generation).await;
        }
    }

    /// One attempt: acquire, run under the command timeout, and drop the
    /// connection on link-level failure so the next call redials.
    async fn attempt<T, F, Fut>(&self, write: bool, op: &F) -> Result<T, ProviderError>
    where
        F: Fn(MultiplexedConnection) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, RedisError>> + Send,
    {
        // Connect failure: nothing was sent, so even for writes the
        // outcome is known (KvUnavailable, safe to retry).
        let (generation, conn) = self.acquire().await?;
        match tokio::time::timeout(self.timeout, op(conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                if e.is_connection_dropped() || matches!(e.kind(), ErrorKind::IoError) {
                    self.invalidate(generation).await;
                }
                Err(map_redis(write, &e))
            }
            Err(_) => {
                self.invalidate(generation).await;
                Err(if write {
                    ProviderError::KvResultUnknown
                } else {
                    ProviderError::KvUnavailable
                })
            }
        }
    }

    /// Override the per-command timeout (default 10 s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Disable read retries (tests, strict single-flight deployments).
    pub fn without_read_retry(mut self) -> Self {
        self.retry_reads = false;
        self
    }

    async fn run<T, F, Fut>(&self, write: bool, op: F) -> Result<T, ProviderError>
    where
        F: Fn(MultiplexedConnection) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, RedisError>> + Send,
    {
        let outcome = self.attempt(write, &op).await;
        match outcome {
            Err(e) if !write && self.retry_reads => {
                tracing::warn!(code = e.code(), "kvrocks read failed; retrying once");
                self.attempt(write, &op).await
            }
            other => other,
        }
    }
}

impl KvrocksKvProvider {
    /// Connect to a kvrocks/redis URL with bounded connect + per-command
    /// timeouts — the constructor cell-controller's STORAGE_PURGE
    /// executor uses (redis stays inside this crate).
    pub async fn connect(
        url: &str,
        connect_timeout: Duration,
        command_timeout: Duration,
    ) -> Result<Self, ProviderError> {
        let client = redis::Client::open(url).map_err(|_| ProviderError::KvUnavailable)?;
        let conn = tokio::time::timeout(connect_timeout, client.get_multiplexed_tokio_connection())
            .await
            .map_err(|_| ProviderError::KvUnavailable)?
            .map_err(|_| ProviderError::KvUnavailable)?;
        Ok(Self::new(conn).with_timeout(command_timeout))
    }

    /// RV-R70: bounded purge of EVERY key under the namespace's physical
    /// prefix — `vrt:kv:{enc(t)}:{enc(ns)}:*` plus the `vrt:kx:{t}:{ns}`
    /// index.
    ///
    /// Drain order is index-first: `ZPOPMIN` pops bounded batches (the
    /// index doubles as a resumable work queue — no cursor state needed
    /// across ops), then one `SCAN MATCH` pass deletes orphan entries
    /// never indexed (drift / restore artifacts). `complete` is true
    /// ONLY when both the index and the entry pattern are fully drained
    /// — a budget cut always reports `complete:false`.
    ///
    /// Boundary safety: `segment_encode` cannot emit `:`, so
    /// `vrt:kv:{t}:{ns}:` never prefixes a sibling namespace's keys
    /// (`…:a:` ≠ `…:ab:`). `ZPOPMIN`/`DEL`/`SCAN` are each bounded by
    /// `max_keys`; every command goes through the per-command timeout
    /// in `run`.
    pub async fn purge_namespace(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        max_keys: usize,
    ) -> Result<crate::purge::PurgeOutcome, ProviderError> {
        if !crate::purge::valid_kv_purge_id(namespace_id)
            || !crate::purge::valid_purge_tenant(tenant_id)
            || max_keys == 0
        {
            return Err(ProviderError::KvInvalidOptions);
        }
        let t = segment_encode(tenant_id);
        let n = segment_encode(namespace_id);
        let ikey = index_key(&t, &n);
        let mut deleted = 0u64;
        // Phase 1 — index-driven drain. Each popped member's entry hash
        // is deleted in the same bounded chunk.
        loop {
            if deleted as usize >= max_keys {
                return Ok(crate::purge::PurgeOutcome {
                    deleted,
                    complete: false,
                });
            }
            let want = (max_keys - deleted as usize).min(PURGE_CHUNK) as isize;
            let popped: Vec<(Vec<u8>, f64)> = self
                .run(true, {
                    let ikey = ikey.clone();
                    move |mut conn| {
                        let ikey = ikey.clone();
                        async move { conn.zpopmin(&ikey, want).await }
                    }
                })
                .await?;
            if popped.is_empty() {
                break;
            }
            let ekeys: Vec<Vec<u8>> = popped
                .iter()
                .map(|(member, _)| entry_key(&t, &n, member))
                .collect();
            deleted += popped.len() as u64;
            self.run(true, move |mut conn| {
                let ekeys = ekeys.clone();
                async move { conn.del::<_, usize>(ekeys).await }
            })
            .await?;
        }
        // Phase 2 — index drained; verify no orphan entry keys remain
        // via a bounded SCAN MATCH pass (deleting any found).
        let pattern = format!("vrt:kv:{t}:{n}:*");
        let mut cursor = 0u64;
        loop {
            let pattern = pattern.clone();
            let (next, keys): (u64, Vec<Vec<u8>>) = self
                .run(true, move |mut conn| {
                    let pattern = pattern.clone();
                    async move {
                        redis::cmd("SCAN")
                            .arg(cursor)
                            .arg("MATCH")
                            .arg(&pattern)
                            .arg("COUNT")
                            .arg(PURGE_CHUNK)
                            .query_async(&mut conn)
                            .await
                    }
                })
                .await?;
            if !keys.is_empty() {
                let batch: Vec<String> = keys
                    .iter()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .collect();
                deleted += batch.len() as u64;
                self.run(true, move |mut conn| {
                    let batch = batch.clone();
                    async move { conn.del::<_, usize>(batch).await }
                })
                .await?;
            }
            if next == 0 {
                return Ok(crate::purge::PurgeOutcome {
                    deleted,
                    complete: true,
                });
            }
            if deleted as usize >= max_keys {
                return Ok(crate::purge::PurgeOutcome {
                    deleted,
                    complete: false,
                });
            }
            cursor = next;
        }
    }
}

/// Purge chunk — ZPOPMIN members / SCAN COUNT / DEL batch size.
const PURGE_CHUNK: usize = 256;

#[async_trait]
impl KvProvider for KvrocksKvProvider {
    async fn get(
        &self,
        ctx: &BindingContext,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError> {
        let (t, n) = ns_parts(ctx);
        self.get_scoped(&t, &n, key).await
    }

    async fn get_many(
        &self,
        ctx: &BindingContext,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError> {
        let (t, n) = ns_parts(ctx);
        self.get_many_scoped(&t, &n, keys).await
    }

    async fn put(
        &self,
        ctx: &BindingContext,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError> {
        let (t, n) = ns_parts(ctx);
        self.put_scoped(&t, &n, key, value, options).await
    }

    async fn delete(&self, ctx: &BindingContext, key: &[u8]) -> Result<(), ProviderError> {
        let (t, n) = ns_parts(ctx);
        self.delete_scoped(&t, &n, key).await
    }

    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError> {
        let (t, n) = ns_parts(ctx);
        self.list_scoped(&t, &n, prefix, after, limit).await
    }
}

impl KvrocksKvProvider {
    /// KV1: `(tenant, namespace)`-scoped data-plane methods behind
    /// [`KvDataBackend`]. The op channel carries namespace-scope (no
    /// binding context exists for an unbound namespace) — callers
    /// validate ids via `purge::valid_*` and we segment-encode both
    /// before they ever reach a physical key.
    async fn get_scoped(
        &self,
        t: &str,
        n: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError>{
                let ekey = entry_key(t, n, key);
        // HMGET v e m — one command, atomic snapshot of all three fields.
        let fields: Vec<Option<Vec<u8>>> = self
            .run(false, move |mut conn| {
                let ekey = ekey.clone();
                async move {
                    conn.hget(&ekey, (FIELD_VALUE, FIELD_EXPIRES, FIELD_META))
                        .await
                }
            })
            .await?;
        if fields.iter().all(Option::is_none) {
            return Ok(None);
        }
        let entry = entry_from_fields(
            fields.first().cloned().flatten(),
            fields.get(1).cloned().flatten(),
            fields.get(2).cloned().flatten(),
        )?;
        if entry.expires_at_ms.is_some_and(|e| e <= now_ms()) {
            return Ok(None);
        }
        Ok(Some(entry))
    }

    async fn get_many_scoped(
        &self,
        t: &str,
        n: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError>{
                let ekeys: Vec<Vec<u8>> = keys.iter().map(|k| entry_key(t, n, k)).collect();
        // RV-R8 #4 phase 1: metadata + DECLARED value size only —
        // `HMGET e m` + `HSTRLEN v` per key. The aggregate budget is
        // charged here, before a single value byte moves: an oversized
        // batch dies on the length probe, not after ~GiB of HGETs.
        // (HSTRLEN on a missing hash/field returns 0 — presence is
        // decided by the HMGET row.)
        let metas: Vec<Vec<Option<Vec<u8>>>> = self
            .run(false, {
                let ekeys = ekeys.clone();
                move |mut conn| {
                    let ekeys = ekeys.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for key in &ekeys {
                            pipe.hget(key, (FIELD_EXPIRES, FIELD_META));
                        }
                        pipe.query_async(&mut conn).await
                    }
                }
            })
            .await?;
        let vlens: Vec<u64> = self
            .run(false, {
                let ekeys = ekeys.clone();
                move |mut conn| {
                    let ekeys = ekeys.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for key in &ekeys {
                            pipe.cmd("HSTRLEN").arg(key).arg(FIELD_VALUE);
                        }
                        pipe.query_async(&mut conn).await
                    }
                }
            })
            .await?;
        if metas.len() != keys.len() || vlens.len() != keys.len() {
            return Err(ProviderError::KvUnavailable);
        }
        let now = now_ms();
        let mut aggregate = 0usize;
        let mut entries: Vec<Option<KvEntry>> = vec![None; keys.len()];
        let mut plan: Vec<usize> = Vec::with_capacity(keys.len());
        for (i, fields) in metas.iter().enumerate() {
            if fields.iter().all(Option::is_none) {
                continue;
            }
            let meta = fields.get(1).cloned().flatten();
            // Validates expires + metadata without the value (list()
            // uses the same empty-value probe).
            let probe = entry_from_fields(
                Some(Vec::new()),
                fields.first().cloned().flatten(),
                meta.clone(),
            )?;
            if probe.expires_at_ms.is_some_and(|e| e <= now) {
                continue;
            }
            let declared = usize::try_from(vlens[i]).unwrap_or(usize::MAX);
            aggregate = super::multi_get_budget_add(
                aggregate,
                declared,
                meta.as_ref().map_or(0, Vec::len),
            )?;
            entries[i] = Some(KvEntry {
                value: Vec::new(),
                metadata_json: meta,
                expires_at_ms: probe.expires_at_ms,
            });
            plan.push(i);
        }
        // Phase 2: pull values only for keys that fit the DECLARED
        // budget, in bounded chunks; actual bytes are re-charged as
        // they arrive — a value that grew between HSTRLEN and HGET
        // still fails closed.
        let mut actual = 0usize;
        for chunk in plan.chunks(VALUE_CHUNK) {
            let wanted: Vec<Vec<u8>> = chunk.iter().map(|i| ekeys[*i].clone()).collect();
            let values: Vec<Option<Vec<u8>>> = self
                .run(false, move |mut conn| {
                    let wanted = wanted.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for key in &wanted {
                            pipe.hget(key, FIELD_VALUE);
                        }
                        pipe.query_async(&mut conn).await
                    }
                })
                .await?;
            if values.len() != chunk.len() {
                return Err(ProviderError::KvUnavailable);
            }
            for (idx, value) in chunk.iter().zip(values) {
                match value {
                    // Vanished between phase 1 and 2 — a racing
                    // delete is an absence, not corruption.
                    None => entries[*idx] = None,
                    Some(value) => {
                        if value.len() > KV_MAX_VALUE_BYTES {
                            return Err(ProviderError::KvCorrupt);
                        }
                        let meta_len = entries[*idx]
                            .as_ref()
                            .and_then(|e| e.metadata_json.as_ref().map(Vec::len))
                            .unwrap_or(0);
                        actual = super::multi_get_budget_add(actual, value.len(), meta_len)?;
                        if let Some(e) = entries[*idx].as_mut() {
                            e.value = value;
                        }
                    }
                }
            }
        }
        Ok(entries)
    }

    async fn put_scoped(
        &self,
        t: &str,
        n: &str,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError>{
        if value.len() > KV_MAX_VALUE_BYTES {
            return Err(ProviderError::KvValueTooLarge);
        }
        if let Some(metadata) = &options.metadata_json {
            validate_stored_metadata(metadata)?;
        }
                let ekey = entry_key(t, n, key);
        let ikey = index_key(t, n);
        let mut items: Vec<(&[u8], Vec<u8>)> = vec![
            (FIELD_VALUE, value.to_vec()),
            (
                FIELD_EXPIRES,
                options
                    .expires_at_ms
                    .unwrap_or(META_NO_EXPIRY)
                    .to_string()
                    .into_bytes(),
            ),
        ];
        if let Some(metadata) = &options.metadata_json {
            items.push((FIELD_META, metadata.clone()));
        }
        let expires = options.expires_at_ms;
        self.run(true, move |mut conn| {
            let ekey = ekey.clone();
            let ikey = ikey.clone();
            let items = items.clone();
            let key = key.to_vec();
            async move {
                let mut pipe = redis::pipe();
                pipe.atomic();
                // DEL first so a stale `m`/`e` field or TTL never
                // survives an overwrite inside the same MULTI.
                pipe.del(&ekey);
                pipe.hset_multiple(&ekey, &items);
                if let Some(pxat) = expires {
                    pipe.pexpire_at(&ekey, pxat);
                }
                pipe.zadd(&ikey, key, 0.0);
                pipe.query_async(&mut conn).await
            }
        })
        .await
    }

    async fn delete_scoped(&self, t: &str, n: &str, key: &[u8]) -> Result<(), ProviderError>{
                let ekey = entry_key(t, n, key);
        let ikey = index_key(t, n);
        self.run(true, move |mut conn| {
            let ekey = ekey.clone();
            let ikey = ikey.clone();
            let key = key.to_vec();
            async move {
                redis::pipe()
                    .atomic()
                    .del(&ekey)
                    .zrem(&ikey, key)
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
    }

    async fn list_scoped(
        &self,
        t: &str,
        n: &str,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError>{
        if limit == 0 || limit > KV_MAX_LIST_LIMIT {
            return Err(ProviderError::KvInvalidOptions);
        }
        if prefix.len() > KV_MAX_KEY_BYTES {
            return Err(ProviderError::KvKeyTooLarge);
        }
        if let Some(after) = after {
            if after.len() > KV_MAX_KEY_BYTES || std::str::from_utf8(after).is_err() {
                return Err(ProviderError::KvCursorInvalid);
            }
        }
                let ikey = index_key(t, n);
        let now = now_ms();
        let wanted = usize::from(limit) + 1;
        let mut rows: Vec<KvListRow> = Vec::with_capacity(wanted);
        let mut cursor: Vec<u8> = after
            .map(|a| a.to_vec())
            .unwrap_or_else(|| prefix.as_bytes().to_vec());
        let mut inclusive = after.is_none();
        let mut dead: Vec<Vec<u8>> = Vec::new();
        // Walk the index in bounded ZRANGEBYLEX chunks until the page is
        // full or the index is exhausted. The first chunk is inclusive on
        // `prefix` (or exclusive on `after`); every later chunk resumes
        // exclusive on the last member returned.
        loop {
            let chunk: Vec<Vec<u8>> = self
                .run(false, {
                    let ikey = ikey.clone();
                    let cursor = cursor.clone();
                    let inclusive = inclusive;
                    move |mut conn| {
                        let ikey = ikey.clone();
                        let cursor = cursor.clone();
                        async move {
                            let min = if inclusive {
                                format!("[{}", String::from_utf8_lossy(&cursor))
                            } else {
                                format!("({}", String::from_utf8_lossy(&cursor))
                            };
                            conn.zrangebylex_limit(&ikey, min, "+", 0isize, LIST_CHUNK as isize)
                                .await
                        }
                    }
                })
                .await?;
            inclusive = false;
            if chunk.is_empty() {
                break;
            }
            let exhausted = chunk.len() < LIST_CHUNK;
            // Resolve `e`/`m` fields for the chunk in one pipelined HMGET
            // per member — value bytes are never transferred for listing.
            let metas: Vec<Vec<Option<Vec<u8>>>> = self
                .run(false, {
                    let ekeys: Vec<Vec<u8>> = chunk.iter().map(|k| entry_key(t, n, k)).collect();
                    move |mut conn| {
                        let ekeys = ekeys.clone();
                        async move {
                            let mut pipe = redis::pipe();
                            for key in &ekeys {
                                pipe.hget(key, (FIELD_EXPIRES, FIELD_META));
                            }
                            pipe.query_async(&mut conn).await
                        }
                    }
                })
                .await?;
            if metas.len() != chunk.len() {
                return Err(ProviderError::KvUnavailable);
            }
            for (member, fields) in chunk.iter().zip(metas.iter()) {
                cursor = member.clone();
                if !member.starts_with(prefix.as_bytes()) {
                    // Sorted walk has moved past the prefix range.
                    return finish_page(rows, limit, dead, self, &ikey).await;
                }
                if fields.iter().all(Option::is_none) {
                    // Index member whose hash vanished (mid-delete or
                    // expired by TTL between index write and field
                    // write) — collect for opportunistic cleanup.
                    dead.push(member.clone());
                    continue;
                }
                let entry = entry_from_fields(
                    Some(Vec::new()),
                    fields.first().cloned().flatten(),
                    fields.get(1).cloned().flatten(),
                )?;
                if entry.expires_at_ms.is_some_and(|e| e <= now) {
                    dead.push(member.clone());
                    continue;
                }
                rows.push(KvListRow {
                    key: member.clone(),
                    metadata_json: entry.metadata_json,
                    expires_at_ms: entry.expires_at_ms,
                });
                if rows.len() >= wanted {
                    break;
                }
            }
            if rows.len() >= wanted || exhausted {
                break;
            }
        }
        finish_page(rows, limit, dead, self, &ikey).await
    }
}

#[async_trait]
impl super::KvDataBackend for KvrocksKvProvider {
    async fn data_get(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError> {
        let (t, n) = (segment_encode(tenant_id), segment_encode(namespace_id));
        self.get_scoped(&t, &n, key).await
    }

    async fn data_put(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError> {
        let (t, n) = (segment_encode(tenant_id), segment_encode(namespace_id));
        self.put_scoped(&t, &n, key, value, options).await
    }

    async fn data_delete(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<(), ProviderError> {
        let (t, n) = (segment_encode(tenant_id), segment_encode(namespace_id));
        self.delete_scoped(&t, &n, key).await
    }

    async fn data_list(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError> {
        let (t, n) = (segment_encode(tenant_id), segment_encode(namespace_id));
        self.list_scoped(&t, &n, prefix, after, limit).await
    }
}


async fn finish_page(
    mut rows: Vec<KvListRow>,
    limit: u16,
    dead: Vec<Vec<u8>>,
    provider: &KvrocksKvProvider,
    ikey: &str,
) -> Result<KvListPage, ProviderError> {
    let complete = rows.len() <= usize::from(limit);
    rows.truncate(usize::from(limit));
    // Opportunistically drop dead index members; failure is non-fatal.
    if !dead.is_empty() {
        let ikey = ikey.to_string();
        let _: Result<(), ProviderError> = provider
            .run(false, move |mut conn| {
                let ikey = ikey.clone();
                let dead = dead.clone();
                async move { conn.zrem(&ikey, dead).await }
            })
            .await;
    }
    Ok(KvListPage { rows, complete })
}
