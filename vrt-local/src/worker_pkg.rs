//! Worker-directory packing — `vrt.toml` metadata + module files →
//! pack-format JSON → [`VersionBundle::from_pack`]. The stored
//! document/digest are produced by the SAME vrt-config code path the
//! host verifies, and vrt-config's `ts_contract` test pins them
//! byte-for-byte against the TypeScript builder.
//!
//! ```toml
//! name = "edge-api"                # worker name (shared DNS-label rule)
//! mainModule = "main.js"
//! compatibilityDate = "2024-09-25"
//! compatibilityFlags = ["nodejs_compat"]
//!
//! [modules]                        # explicit type overrides:
//! "assets/logo.bin" = "data"       #   esModule|commonJs|text|json|data|wasm
//!
//! [vars]                           # plaintext bindings → vars.json
//! LOG_LEVEL = "info"               # var_ref = var:{name}:{vhash8}:{binding}
//!                                  # — values are part of VERSION identity (F2)
//!
//! [secrets]                        # binding name → secretRef (values
//! API_KEY = "api-key-ref"          #   via `vrt secret put`, never here)
//!
//! [bindings]                       # resource bindings
//! kv = { CACHE = "ns-1" }
//! r2 = { BUCKET = "my-bucket" }
//! pg = { DB = "db-01" }
//! service = { SVC = "other-worker" }   # by worker NAME → resolved to workerId
//! assets = { SITE = "<64-hex>" }
//!
//! hostnames = ["api.example.com"]
//! [[crons]]
//! scheduleId = "cron:edge-api:0"
//! expression = "*/15 * * * *"
//!
//! [quotas]
//! cpuMs = 50
//! memoryMb = 128
//! subrequests = 50
//! outboundConnections = 16
//! ```
//!
//! Module discovery is a recursive scan: `.js/.mjs`→esModule,
//! `.cjs`→commonJs, `.json`→json (canonicalized — safe-integer rule
//! applies exactly as the TypeScript side), `.txt`→text,
//! `.wasm`→wasm; anything else requires an explicit `[modules]`
//! entry or the deploy fails closed. `vrt.toml`, dotfiles and
//! `node_modules/` are skipped; symlinks are rejected.

use crate::LocalError;
use serde::Deserialize;
use sha2::Digest;
use snapshot_protocol::compute_desired::{
    AssetsBinding, BindingDesired, BindingPermissions, CronDesired, DoBindingDesired,
    DoClassDesired, KvBinding, PgBinding, R2Binding, RunWorkerFirst, SecretBinding, ServiceBinding,
    VarBinding, WorkerQuotas,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// vrt.toml document (deny_unknown_fields — a typo must fail closed).
/// pub(crate) — `project_cfg` synthesizes this shape from wrangler
/// sources through the identical deserialize path.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerToml {
    pub(crate) name: String,
    pub(crate) main_module: String,
    pub(crate) compatibility_date: String,
    #[serde(default)]
    pub(crate) compatibility_flags: Vec<String>,
    /// Explicit `path → type` overrides/additions.
    #[serde(default)]
    pub(crate) modules: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) vars: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) secrets: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) bindings: Option<BindingsToml>,
    #[serde(default)]
    pub(crate) hostnames: Vec<String>,
    #[serde(default)]
    pub(crate) crons: Vec<CronDesired>,
    /// T37b：`[[queueConsumers]]` —— 本 worker 消费的队列（queue
    /// 为显示名，queueId 打包期按 `vrt-q:{tenant}:{name}` 派生）。
    #[serde(default)]
    pub(crate) queue_consumers: Vec<QueueConsumerToml>,
    /// P3c-1：`durableObjectBindings` —— DO namespace bindings
    /// （wrangler `durable_objects.bindings[]` 归一后同一形状）。
    #[serde(default)]
    pub(crate) durable_object_bindings: Vec<DoBindingToml>,
    /// P3c-1：`durableObjectMigrations` —— wrangler `[[migrations]]`
    /// 归一后的 shape；deploy 时按序求值为最终 class 集合。
    #[serde(default)]
    pub(crate) durable_object_migrations: Vec<DoMigrationToml>,
    /// W1：`[[workflows]]` 归一后同一形状。
    #[serde(default)]
    pub(crate) workflows: Vec<WorkflowToml>,
    #[serde(default)]
    pub(crate) quotas: Option<QuotasToml>,
    /// T21: `[assets]` directory declaration.
    #[serde(default)]
    pub(crate) assets: Option<AssetsToml>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BindingsToml {
    #[serde(default)]
    pub(crate) kv: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) r2: BTreeMap<String, String>,
    /// `name → 用户侧 database_id`（服务端在打包期派生真实 id）。
    #[serde(default)]
    pub(crate) d1: BTreeMap<String, String>,
    /// T37: `name → 用户侧队列名`（queueId 在打包期由
    /// `vrt-q:{tenant}:{name}` 派生——standalone 无控制面 provision）。
    #[serde(default)]
    pub(crate) queue: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) pg: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) service: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) assets: BTreeMap<String, String>,
}

/// T37b `[[queueConsumers]]`：字段与 wrangler `queues.consumers[]`
/// 对齐（snake→camel 由 project_cfg 归一；数值边界在 pack 时查，
/// 与 [`QueueConsumerDesired`] 校验一致）。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueueConsumerToml {
    /// 队列显示名（id 打包期派生）。
    pub(crate) queue: String,
    /// 死信队列显示名。
    pub(crate) dead_letter_queue: Option<String>,
    /// 1..=100。
    pub(crate) max_batch_size: Option<u32>,
    /// 毫秒，0..=60_000（归一化 schema `maxBatchTimeoutMs`）。
    pub(crate) max_batch_timeout_ms: Option<u32>,
    /// 0..=100。
    pub(crate) max_retries: Option<u32>,
    /// 秒，0..=86_400（归一化 `retryDelaySeconds`）。
    pub(crate) retry_delay_seconds: Option<u32>,
    /// 1..=1000（平台派发并发上限）。
    pub(crate) max_concurrency: Option<u32>,
}

/// P3c-1 `durableObjectBindings[]` —— 字段与 wrangler
/// `durable_objects.bindings[]` 对齐（`script_name` → `scriptName`
/// 归一由 project_cfg 完成；vrt.toml 原生直接用 camelCase）。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DoBindingToml {
    /// `env.<name>` 上的 namespace stub 句柄。
    pub(crate) name: String,
    /// 目标 worker 导出的 JS class 名。
    pub(crate) class_name: String,
    /// wrangler `script_name` —— 同租户内另一 worker 名；省略 = 本
    /// worker 声明该类。
    pub(crate) script_name: Option<String>,
}

/// P3c-1 `durableObjectMigrations[]` —— wrangler `[[migrations]]` 的
/// 归一 shape。按数组序求值：`newClasses`/`newSqliteClasses` 声明、
/// `renamedClasses` 继承存储身份、`deletedClasses` 移除。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DoMigrationToml {
    /// 人类可读 tag（CF 语义——仅标识用途，不参与求值）。
    pub(crate) tag: Option<String>,
    /// KV 后端 class —— localDisk 下与 SQLite 同一存储，`enableSql=false`。
    #[serde(default)]
    pub(crate) new_classes: Vec<String>,
    /// SQLite 后端 class —— `enableSql=true`，暴露 `storage.sql`。
    #[serde(default)]
    pub(crate) new_sqlite_classes: Vec<String>,
    /// `{from,to}` —— `to` 继承 `from` 的 uniqueKey（数据保留）。
    #[serde(default)]
    pub(crate) renamed_classes: Vec<DoRenameToml>,
    /// 移除 class 声明；数据目录在 generation 切换完成后清理。
    #[serde(default)]
    pub(crate) deleted_classes: Vec<String>,
}

/// P3c-1 `renamedClasses[]` 的 `{from,to}` 形状。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DoRenameToml {
    /// 既有 class 名（当前版本或本 deploy 先前 migration step 声明）。
    pub(crate) from: String,
    /// 新 class 名——继承 `from` 的 uniqueKey 与 enableSql。
    pub(crate) to: String,
}

/// W1 `[[workflows]]` —— 字段与 wrangler 对齐（`class_name` →
/// `className`、`script_name` → `scriptName` 归一由 project_cfg 完
/// 成）。MVP：只允许同 worker 的 class；`script_name` 指向其他
/// worker 时 pack fail closed。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowToml {
    /// 工作流名（`^[A-Za-z0-9_-]{1,64}$`），worker 内唯一。
    pub(crate) name: String,
    /// `env.<binding>` 绑定名（JS identifier）。
    pub(crate) binding: String,
    /// 同 worker 导出的 `WorkflowEntrypoint` class 名。
    pub(crate) class_name: String,
    /// wrangler `script_name` —— 仅允许等于本 worker 名（same-
    /// worker）；指向其他 worker 在 pack 拒绝（W1 范围限制）。
    pub(crate) script_name: Option<String>,
    /// `limits.steps`（1..=100_000）。
    pub(crate) limits_steps: Option<u32>,
}

/// T21: `[assets]` — static-assets directory packed at deploy.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssetsToml {
    /// Directory relative to the worker dir (canonicalized inside).
    pub(crate) dir: String,
    /// Binding name (`env.<name>.fetch()`) — defaults to `ASSETS`.
    #[serde(default = "default_assets_binding")]
    pub(crate) binding: String,
    /// Cloudflare-compatible subset: `auto-trailing-slash` | `none`.
    #[serde(default)]
    pub(crate) html_handling: Option<String>,
    /// `none` | `404-page` | `single-page-application`.
    #[serde(default)]
    pub(crate) not_found_handling: Option<String>,
    /// T31c: opt into Cloudflare assets-first routing. `vrt.toml`
    /// native default is `false` — worker-first, assets reachable only
    /// via `env.<binding>.fetch()` — preserving the behavior every
    /// pre-T31c standalone deployment was built and tested against.
    /// Set `assetsFirst = true` for the Cloudflare default.
    #[serde(default)]
    pub(crate) assets_first: bool,
    /// T31c: `run_worker_first` — `true`/array-of-globs overrides per
    /// Cloudflare semantics; `!`-prefixed rules route assets-first.
    #[serde(default)]
    pub(crate) run_worker_first: Option<RunWorkerFirstToml>,
}

/// T31c: `runWorkerFirst` value on `[assets]` — Wrangler wire shape.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RunWorkerFirstToml {
    /// `runWorkerFirst = <bool>`.
    Bool(bool),
    /// `runWorkerFirst = ["/path", "!/asset-path", …]`.
    Rules(Vec<String>),
}

fn default_assets_binding() -> String {
    "ASSETS".into()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QuotasToml {
    pub(crate) cpu_ms: u32,
    pub(crate) memory_mb: u32,
    pub(crate) subrequests: u32,
    pub(crate) outbound_connections: u32,
}

/// P3c-1：DO 打包上下文——部署密钥（uniqueKey HMAC 派生）与
/// 当前激活版本的 class 集合（`renamed_classes` lineage 来源）。
/// `None` ⇒ 任何 DO 声明都拒绝（`pack`/`pack_with_resolver` 无
/// 密钥/状态上下文，产出的 uniqueKey 无法证明由服务端派生）。
pub struct DoPackCtx<'a> {
    /// uniqueKey 派生密钥（standalone：signing seed；控制面：快照
    /// 签名密钥材料）。只作 HMAC key，不持久化、不外发。
    pub pepper: [u8; 32],
    /// 当前激活版本声明的 class 集合（rename `from` 解析源）。
    pub prior_classes: &'a [DoClassDesired],
}

/// Caller-side augmentations applied on top of vrt.toml (API surface —
/// e.g. service bindings resolved against live state).
#[derive(Clone, Debug, Default)]
pub struct DeployOverrides {
    /// Override `vrt.toml` `name` (rare — for programmatic deploys).
    pub name: Option<String>,
    /// Extra desired bindings appended after vrt.toml's.
    pub bindings: Vec<BindingDesired>,
    /// Extra hostnames.
    pub hostnames: Vec<String>,
}

/// Fully packed worker ready for state application.
pub struct PackedWorker {
    /// Worker name.
    pub name: String,
    /// Bundle digest (content identity).
    pub digest: String,
    /// Stored bundle document bytes (`objects/<digest>`).
    pub stored_document: Vec<u8>,
    /// compat values for the version record.
    pub compatibility_date: String,
    /// compat flags.
    pub compatibility_flags: Vec<String>,
    /// Desired bindings (var/secret/resource).
    pub bindings: Vec<BindingDesired>,
    /// `varRef → value` entries for THIS version — stored on the
    /// version record and projected into `vars.json` as a union
    /// (content-hash-scoped refs keep versions disjoint, F2).
    pub var_values: BTreeMap<String, String>,
    /// Desired crons.
    pub crons: Vec<CronDesired>,
    /// T37b: desired queue consumers（queueId 已解析）。
    pub queue_consumers: Vec<snapshot_protocol::compute_desired::QueueConsumerDesired>,
    /// P3c-1: DO class 声明——migrations 求值结果，`uniqueKey`
    /// 已由部署密钥 HMAC 派生，rename lineage 已解析。
    pub durable_object_classes: Vec<DoClassDesired>,
    /// W1: workflow 声明——`uniqueKey` 已由部署密钥 HMAC 派生
    /// （`vrt-wf:` 域），same-worker 限制与名称/绑定校验已过。
    pub workflows: Vec<snapshot_protocol::compute_desired::WorkflowDesired>,
    /// Declared hostnames (replaces state list when non-empty).
    pub hostnames: Vec<String>,
    /// Quotas.
    pub quotas: WorkerQuotas,
    /// Deterministic first-deploy worker id material.
    pub first_deploy_id: String,
    /// T21: assets to materialize into the local asset store before
    /// the publish commits (`None` when no `[assets]` dir declared).
    pub pending_assets: Option<PendingAssets>,
}

/// T21: assets scanned at pack time — the deploy writes objects +
/// manifest through `AssetStore::put_*_verified`, so a digest that
/// does not match the bytes can never be committed.
pub struct PendingAssets {
    /// Canonical manifest JSON bytes.
    pub manifest_bytes: Vec<u8>,
    /// sha256 of `manifest_bytes` (the binding's manifestDigest).
    pub manifest_digest: String,
    /// `(sha256, bytes)` per file — written under `objects/<sha256>`.
    pub objects: Vec<(String, Vec<u8>)>,
}

/// Per-manifest caps — mirror the platform upload API.
const MAX_ASSET_FILES: usize = 512;
const MAX_ASSET_FILE_BYTES: u64 = 1024 * 1024;
const MAX_ASSET_TOTAL_BYTES: u64 = 8 * 1024 * 1024;

/// Extension→MIME whitelist (mirrors the platform table; unknown
/// extensions map to `application/octet-stream` — never client input).
pub fn mime_for_path(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "json" | "map" => "application/json",
        "txt" => "text/plain",
        "xml" => "application/xml",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "pdf" => "application/pdf",
        "wasm" => "application/wasm",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "csv" => "text/csv",
        "md" => "text/markdown",
        _ => "application/octet-stream",
    }
}

/// Scan `assets_dir` → canonical manifest + objects. Paths are
/// forward-slash `/`-prefixed request paths; `..`/absolute/backslash/
/// NUL names are impossible under `read_dir`, but symlinks and
/// over-cap inputs are still rejected.
fn scan_assets_dir(worker_dir: &Path, assets: &AssetsToml) -> Result<PendingAssets, LocalError> {
    let dir = worker_dir
        .join(&assets.dir)
        .canonicalize()
        .map_err(|_| LocalError::Worker("assets dir missing"))?;
    let base = worker_dir
        .canonicalize()
        .map_err(|_| LocalError::Worker("worker dir"))?;
    if !dir.is_dir() || !dir.starts_with(&base) {
        return Err(LocalError::Worker("assets dir escapes worker dir"));
    }
    let valid_handling = |v: &Option<String>, allowed: &[&str]| -> Result<String, LocalError> {
        match v.as_deref() {
            None => Ok(allowed[0].to_string()),
            Some(v) if allowed.contains(&v) => Ok(v.to_string()),
            Some(_) => Err(LocalError::Worker("assets handling")),
        }
    };
    let html_handling = valid_handling(&assets.html_handling, &["auto-trailing-slash", "none"])?;
    let not_found = valid_handling(
        &assets.not_found_handling,
        &["none", "404-page", "single-page-application"],
    )?;
    let mut files: Vec<(String, String, u64, String, Vec<u8>)> = Vec::new();
    let mut total: u64 = 0;
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).map_err(|_| LocalError::Worker("assets read"))? {
            let entry = entry.map_err(|_| LocalError::Worker("assets read"))?;
            let ft = entry
                .file_type()
                .map_err(|_| LocalError::Worker("assets stat"))?;
            if ft.is_symlink() {
                return Err(LocalError::Worker("assets symlink rejected"));
            }
            if ft.is_dir() {
                stack.push(entry.path());
                continue;
            }
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue; // dotfiles skipped, same as module scan
            }
            let entry_path = entry.path();
            let rel = entry_path
                .strip_prefix(&dir)
                .map_err(|_| LocalError::Worker("assets path"))?;
            let rel_str = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            let req_path = format!("/{rel_str}");
            let bytes =
                std::fs::read(&entry_path).map_err(|_| LocalError::Worker("assets read"))?;
            let size = bytes.len() as u64;
            if size > MAX_ASSET_FILE_BYTES {
                return Err(LocalError::Worker("asset file too large"));
            }
            total += size;
            if total > MAX_ASSET_TOTAL_BYTES {
                return Err(LocalError::Worker("assets total too large"));
            }
            if files.len() >= MAX_ASSET_FILES {
                return Err(LocalError::Worker("too many asset files"));
            }
            let digest = hex::encode(sha2::Sha256::digest(&bytes));
            let mime = mime_for_path(&rel_str).to_string();
            files.push((req_path, digest, size, mime, bytes));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let file_entries: Vec<serde_json::Value> = files
        .iter()
        .map(|(path, sha, size, mime, _)| {
            serde_json::json!({
                "mime": mime,
                "path": path,
                "sha256": sha,
                "size": size,
            })
        })
        .collect();
    let manifest = serde_json::json!({
        "files": file_entries,
        "htmlHandling": html_handling,
        "notFoundHandling": not_found,
        "schemaVersion": 1,
    });
    // Canonical JSON: serde_json emits object keys in insertion order
    // for Map (BTreeMap-backed by default feature `preserve_order`
    // off → keys sorted). Keys above are alphabetically ordered to
    // keep the digest stable across implementations.
    let manifest_bytes =
        serde_json::to_vec(&manifest).map_err(|_| LocalError::Worker("assets manifest"))?;
    let manifest_digest = hex::encode(sha2::Sha256::digest(&manifest_bytes));
    Ok(PendingAssets {
        manifest_bytes,
        manifest_digest,
        objects: files.into_iter().map(|(_, d, _, _, b)| (d, b)).collect(),
    })
}

const MAX_WORKER_DIR_BYTES: u64 = 96 * 1024 * 1024;

/// Pack `worker_dir`. `resolve_worker` maps a worker NAME (service
/// binding target) to a workerId — standalone names are unique, so a
/// name miss fails closed rather than guessing.
pub fn pack(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
) -> Result<PackedWorker, LocalError> {
    pack_inner(worker_dir, overrides, tenant_id, &|_| None, None)
}

/// Same as [`pack`] with a name→workerId resolver for `service`
/// bindings (standalone workers are looked up in state).
pub fn pack_with_resolver(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
) -> Result<PackedWorker, LocalError> {
    pack_inner(worker_dir, overrides, tenant_id, resolve_worker, None)
}

/// [`pack_with_resolver`] + DO 上下文——deploy 入口（唯一能给
/// uniqueKey 派生与 rename lineage 提供密钥/状态的调用方）。
pub fn pack_with_ctx(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
    do_ctx: Option<&DoPackCtx<'_>>,
) -> Result<PackedWorker, LocalError> {
    pack_inner(worker_dir, overrides, tenant_id, resolve_worker, do_ctx)
}

fn pack_inner(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
    do_ctx: Option<&DoPackCtx<'_>>,
) -> Result<PackedWorker, LocalError> {
    // T31/C4: `vrt.toml` keeps precedence; otherwise the wrangler family
    // (json > jsonc > toml) normalizes into the same meta shape.
    let (meta, meta_source) = crate::project_cfg::load_worker_meta(worker_dir)?;
    let _ = meta_source;
    let name = overrides.name.clone().unwrap_or_else(|| meta.name.clone());

    // --- module scan ---
    let mut modules: BTreeMap<String, String> = BTreeMap::new();
    let mut declared = meta.modules;
    let mut total: u64 = 0;
    // `[assets] dir` is content-addressed asset data, not worker
    // modules — exclude it from the module scan (an .html file there
    // must not fail "module type").
    // Canonicalize the worker dir ONCE up front — under a symlinked
    // tempdir (`/var` → `/private/var`) the canonical assets root and
    // the raw `worker_dir` disagree, which used to drop the exclusion.
    let worker_canonical = worker_dir
        .canonicalize()
        .unwrap_or_else(|_| worker_dir.to_path_buf());
    let assets_rel = meta
        .assets
        .as_ref()
        .map(|a| {
            worker_dir
                .join(&a.dir)
                .canonicalize()
                .ok()
                .filter(|p| p.starts_with(&worker_canonical))
        })
        .flatten();
    scan_dir(worker_dir, worker_dir, &mut |rel, path| {
        if let Some(root) = &assets_rel {
            let canon = path
                .canonicalize()
                .unwrap_or_else(|_| worker_canonical.join(rel));
            if canon.starts_with(root) {
                return Ok(());
            }
        }
        total += std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if total > MAX_WORKER_DIR_BYTES {
            return Err(LocalError::Worker("dir size"));
        }
        if let Some(ty) = declared.remove(rel) {
            modules.insert(rel.to_string(), ty);
        } else if let Some(ty) = type_for_extension(path) {
            modules.insert(rel.to_string(), ty.to_string());
        } else {
            return Err(LocalError::Worker("module type"));
        }
        Ok(())
    })?;
    // Declared modules that are NOT files on disk fail closed.
    if !declared.is_empty() {
        return Err(LocalError::Worker("modules missing"));
    }

    // --- pack-format JSON (same wire forms the TS builder emits) ---
    let mut pack_modules = serde_json::Map::new();
    // W7: esModule sources for the workflow class scan below.
    let mut esm_sources: Vec<String> = Vec::new();
    for (rel, ty) in &modules {
        let path = worker_dir.join(rel);
        let wire = match ty.as_str() {
            "esModule" | "commonJs" | "text" | "json" => {
                let text =
                    std::fs::read_to_string(&path).map_err(|_| LocalError::Worker("utf8"))?;
                if ty == "esModule" {
                    esm_sources.push(text.clone());
                }
                match ty.as_str() {
                    "esModule" => serde_json::Value::String(text),
                    "commonJs" => serde_json::json!({"cjs": text}),
                    "text" => serde_json::json!({"text": text}),
                    _ => {
                        serde_json::json!({"json": serde_json::from_str::<serde_json::Value>(&text).map_err(|_| LocalError::Worker("json module"))?})
                    }
                }
            }
            "wasm" | "data" => {
                use base64::Engine;
                let bytes = std::fs::read(&path).map_err(|_| LocalError::Worker("read"))?;
                let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
                if ty == "wasm" {
                    serde_json::json!({"wasm_b64": b64})
                } else {
                    serde_json::json!({"data_b64": b64})
                }
            }
            _ => return Err(LocalError::Worker("module type")),
        };
        pack_modules.insert(rel.clone(), wire);
    }
    let pack = serde_json::json!({
        "mainModule": meta.main_module,
        "modules": pack_modules,
        "compatibilityDate": meta.compatibility_date,
        "compatibilityFlags": meta.compatibility_flags,
    });
    // The shared validator/digester — identical code to the platform.
    let bundle = vrt_config::VersionBundle::from_pack(&pack)?;

    // --- bindings ---
    let read_write = BindingPermissions {
        read: true,
        write: true,
    };
    let read_only = BindingPermissions {
        read: true,
        write: false,
    };
    let mut bindings: Vec<BindingDesired> = Vec::new();
    let mut var_values = BTreeMap::new();
    // F2: var values are version-scoped — the ref embeds a content
    // hash of this deploy's var set so vars.json can hold every
    // version's entries as a disjoint union (rollback resolves the
    // old version's refs; a var-only edit still yields a new ref set).
    let vars_hash = {
        let canonical = serde_json::to_vec(&meta.vars).unwrap_or_default();
        hex::encode(sha2::Sha256::digest(&canonical))
    };
    for (binding, value) in &meta.vars {
        let var_ref = format!("var:{name}:{}:{binding}", &vars_hash[..8]);
        var_values.insert(var_ref.clone(), value.clone());
        bindings.push(BindingDesired::Var(VarBinding {
            name: binding.clone(),
            var_ref,
        }));
    }
    for (binding, secret_ref) in &meta.secrets {
        bindings.push(BindingDesired::Secret(SecretBinding {
            name: binding.clone(),
            secret_ref: secret_ref.clone(),
        }));
    }
    if let Some(b) = &meta.bindings {
        for (binding, ns) in &b.kv {
            bindings.push(BindingDesired::Kv(KvBinding {
                name: binding.clone(),
                namespace_id: ns.clone(),
                permissions: read_write.clone(),
            }));
        }
        for (binding, bucket) in &b.r2 {
            bindings.push(BindingDesired::R2(R2Binding {
                name: binding.clone(),
                bucket: bucket.clone(),
                permissions: read_write.clone(),
            }));
        }
        for (binding, user_id) in &b.d1 {
            // databaseId 由服务端派生（T34c/F2：含 tenant_id ——
            // T35 多租户控制面接入时无需格式迁移；standalone 单租户
            // 时等价 CF 账号级 database_id 语义）。同一实例内相同
            // databaseId 的多个 Worker 共享同一数据库（D1 语义）；
            // Worker 永远拿不到任意 id——签名快照覆盖派生值。
            if user_id.is_empty()
                || user_id.len() > 64
                || !user_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(LocalError::Worker(
                    "d1 user id must be 1-64 chars of [A-Za-z0-9_-]",
                ));
            }
            use sha2::Digest;
            let digest = hex::encode(sha2::Sha256::digest(
                format!("vrt-d1:{tenant_id}:{user_id}").as_bytes(),
            ));
            bindings.push(BindingDesired::D1(
                snapshot_protocol::compute_desired::D1Binding {
                    name: binding.clone(),
                    database_id: digest[..26].to_string(),
                    epoch: 1,
                    permissions: read_write.clone(),
                },
            ));
        }
        for (binding, queue_name) in &b.queue {
            // T37: queueId 派生——sha256("vrt-q:{tenant}:{name}") 的
            // 前 26 hex 字符（hex ⊂ [a-z0-9]，与 database_id 同一规则）。
            // 同实例同名的多个 producer 共享同一队列（CF 语义）；
            // worker 永远拿不到任意 id——签名快照覆盖派生值。
            let digest = derive_queue_id(tenant_id, queue_name)?;
            bindings.push(BindingDesired::Queue(
                snapshot_protocol::compute_desired::QueueBinding {
                    name: binding.clone(),
                    queue_id: digest,
                    queue_name: binding.clone(),
                    permissions: read_write.clone(),
                },
            ));
        }
        for (binding, db) in &b.pg {
            // P1: `name → logical tenant id` — the provider resolves
            // credentials from `secrets/pg-credentials.json` under
            // `vrtpgcred:<id>` (never from the bundle). Read+write
            // grant; a `[[pg]] readOnly` toml flag is not part of the
            // standalone shape — read-only local bindings mark the
            // credential payload's `readOnly` instead.
            bindings.push(BindingDesired::Pg(PgBinding {
                name: binding.clone(),
                database_id: format!("vrtpg:{db}"),
                credential_ref: format!("vrtpgcred:{db}"),
                permissions: read_write.clone(),
            }));
        }
        for (binding, target) in &b.service {
            let worker_id = resolve_worker(target).ok_or(LocalError::Worker("service target"))?;
            bindings.push(BindingDesired::Service(ServiceBinding {
                name: binding.clone(),
                worker_id,
            }));
        }
        for (binding, digest) in &b.assets {
            bindings.push(BindingDesired::Assets(AssetsBinding {
                name: binding.clone(),
                manifest_digest: digest.clone(),
                permissions: read_only.clone(),
                assets_first: None,
                run_worker_first: None,
            }));
        }
    }
    for b in &overrides.bindings {
        // P1: standalone PG is declared via `[bindings].pg` / `[[pg]]`
        // so `credential_ref` (`vrtpgcred:<id>`) is minted here, paired
        // with `secrets/pg-credentials.json`. An overrides-injected Pg
        // would smuggle in an arbitrary resource id — fail closed.
        if matches!(b, BindingDesired::Pg(_)) {
            return Err(LocalError::Worker(
                "pg bindings are declared via [bindings].pg or [[pg]], not overrides",
            ));
        }
    }
    bindings.extend(overrides.bindings.iter().cloned());

    // T37b: `[[queueConsumers]]` —— 队列名 → queueId 派生（与
    // producer 同一哈希规则）；DLQ 名同样派生，自指被拒；
    // 数值边界与 snapshot-protocol 校验一致（双层 fail closed）。
    let mut queue_consumers = Vec::with_capacity(meta.queue_consumers.len());
    for c in &meta.queue_consumers {
        let queue_id = derive_queue_id(tenant_id, &c.queue)?;
        let dead_letter_queue_id = c
            .dead_letter_queue
            .as_deref()
            .map(|dlq| derive_queue_id(tenant_id, dlq))
            .transpose()?;
        if dead_letter_queue_id.as_deref() == Some(queue_id.as_str()) {
            return Err(LocalError::Worker(
                "queueConsumers dead_letter_queue must differ from queue",
            ));
        }
        queue_consumers.push(snapshot_protocol::compute_desired::QueueConsumerDesired {
            queue_id,
            queue_name: c.queue.clone(),
            dead_letter_queue_id,
            max_batch_size: c.max_batch_size,
            max_batch_timeout_secs: c.max_batch_timeout_ms.map(|ms| ms / 1000),
            max_retries: c.max_retries,
            retry_delay_secs: c.retry_delay_seconds,
            max_concurrency: c.max_concurrency,
        });
    }

    // Deterministic worker-id material: hash the worker name so a
    // re-deployed directory keeps its identity without stored state.
    // （DO uniqueKey 派生也需要 —— 提到 assets 之前。）
    let id_material = hex::encode(sha2::Sha256::digest(name.as_bytes()));

    // P3c-1: Durable Objects —— migrations 按序求值为最终 class
    // 集合；uniqueKey = HMAC(pepper, "vrt-do:{tenant}:{worker}:{class}")
    // （ring hmac；pepper = 部署签名密钥材料 —— worker 无法预测，
    // 服务端稳定；rename 继承 `from` 的 key 使目录跨改名存活）。
    let worker_id = format!("w_{}", &id_material[..16]);
    let mut durable_object_classes: Vec<DoClassDesired> = Vec::new();
    let has_do_cfg =
        !meta.durable_object_bindings.is_empty() || !meta.durable_object_migrations.is_empty();
    if has_do_cfg {
        let ctx = do_ctx.ok_or(LocalError::Worker("durable objects require deploy context"))?;
        let mut declared: BTreeMap<String, DoClassDesired> = BTreeMap::new();
        macro_rules! mint {
            ($class:expr, $sql:expr) => {{
                let class: &str = $class;
                if declared.contains_key(class) {
                    return Err(LocalError::Worker("do class redeclared"));
                }
                declared.insert(
                    class.to_string(),
                    DoClassDesired {
                        class_name: class.to_string(),
                        unique_key: derive_do_unique_key(&ctx.pepper, tenant_id, &worker_id, class),
                        enable_sql: $sql,
                        from_worker_id: None,
                    },
                );
            }};
        }
        for mig in &meta.durable_object_migrations {
            if mig.tag.as_deref().is_some_and(|t| t.len() > 128) {
                return Err(LocalError::Worker("do migration tag too long"));
            }
            for class in &mig.new_classes {
                valid_do_name(class)?;
                mint!(class, false);
            }
            for class in &mig.new_sqlite_classes {
                valid_do_name(class)?;
                mint!(class, true);
            }
            for rename in &mig.renamed_classes {
                valid_do_name(&rename.from)?;
                valid_do_name(&rename.to)?;
                if declared.contains_key(&rename.to) {
                    return Err(LocalError::Worker("do rename target declared"));
                }
                // `from` 的存储身份 = 本 deploy 已声明值，否则查当前
                // 激活版本；两者都没有 → rename 指向不存在的 class，
                // 静默丢弃数据的风险 → fail closed。
                let entry = declared.remove(&rename.from).or_else(|| {
                    ctx.prior_classes
                        .iter()
                        .find(|c| c.class_name == rename.from)
                        .cloned()
                });
                let Some(entry) = entry else {
                    return Err(LocalError::Worker("do rename from unknown class"));
                };
                declared.insert(
                    rename.to.clone(),
                    DoClassDesired {
                        class_name: rename.to.clone(),
                        unique_key: entry.unique_key,
                        enable_sql: entry.enable_sql,
                        from_worker_id: None,
                    },
                );
            }
            for class in &mig.deleted_classes {
                valid_do_name(class)?;
                let known = declared.remove(class).is_some()
                    || ctx.prior_classes.iter().any(|c| c.class_name == *class);
                if !known {
                    return Err(LocalError::Worker("do delete unknown class"));
                }
            }
        }
        durable_object_classes = declared.into_values().collect();
        // 声明了 bindings 但无 class 声明：本 worker 至少有一个
        // 非 script_name 绑定 → fail closed（与快照校验同向）。
        for b in &meta.durable_object_bindings {
            valid_do_name(&b.name)?;
            valid_do_name(&b.class_name)?;
            let service_worker_id = match &b.script_name {
                None => None,
                Some(svc) => {
                    let target =
                        resolve_worker(svc).ok_or(LocalError::Worker("do script_name target"))?;
                    (target != worker_id).then_some(target)
                }
            };
            if service_worker_id.is_none()
                && !durable_object_classes
                    .iter()
                    .any(|c| c.class_name == b.class_name)
            {
                return Err(LocalError::Worker("do binding class not declared"));
            }
            bindings.push(BindingDesired::DurableObject(DoBindingDesired {
                name: b.name.clone(),
                class_name: b.class_name.clone(),
                service_worker_id,
            }));
        }
    }

    // W1 `[[workflows]]`；W7 起 `script_name` 可指向同租户另一
    // worker —— uniqueKey = HMAC(pepper,
    // "vrt-wf:{tenant}:{definingWorker}:{wfName}")（与 DO class 同一
    // 身份域不同前缀；定义者归属 → 存储/op 都在目标 worker 名下，
    // 与 CF 一致）。目标必须是已部署 worker（resolve_worker 404
    // → fail closed，与 DO script_name 相同）。
    let mut workflows: Vec<snapshot_protocol::compute_desired::WorkflowDesired> = Vec::new();
    if !meta.workflows.is_empty() {
        let ctx = do_ctx.ok_or(LocalError::Worker("workflows require deploy context"))?;
        let mut seen_names = std::collections::BTreeSet::new();
        for w in &meta.workflows {
            valid_workflow_name(&w.name)?;
            valid_do_name(&w.binding)?;
            valid_do_name(&w.class_name)?;
            if !seen_names.insert(w.name.clone()) {
                return Err(LocalError::Worker("workflow name redeclared"));
            }
            // wrangler: `script_name` defaults to the enclosing
            // script — an explicit self-reference is legal even on
            // first deploy (the worker isn't in the registry yet, so
            // resolve_worker would 404). Any other value must resolve
            // to an already-deployed worker of this tenant (deploy
            // order: the target must exist first — fail closed).
            let service_worker_id = match &w.script_name {
                None => None,
                Some(script) if *script == name => None,
                Some(script) => {
                    let target = resolve_worker(script)
                        .ok_or(LocalError::Worker("workflow script_name target"))?;
                    (target != worker_id).then_some(target)
                }
            };
            if let Some(steps) = w.limits_steps {
                if steps == 0 || steps > 100_000 {
                    return Err(LocalError::Worker("workflow limits.steps bounds"));
                }
            }
            // uniqueKey 派生域含 DEFINING worker —— 引擎存储归属
            // script_name 目标；目标侧字段在 desired 层另行校验。
            let defining = service_worker_id.clone().unwrap_or(worker_id.clone());
            // W7 deploy-gate: same-worker class_name must be a named
            // export AND a WorkflowEntrypoint subclass — missing
            // classes die at workerd load anyway (preflight) but with
            // a config-level error; a non-subclass passes workerd's
            // load yet dies at engine init — both rejected HERE with a
            // typed reason instead. `script_name` targets: the target
            // bundle isn't in this pack — existence is proven by the
            // workerd preflight (entrypoint designator fails the load
            // → apply rejected, old generation keeps serving).
            if service_worker_id.is_none() {
                match crate::wf_scan::workflow_class_problems(
                    &esm_sources.join("\n"),
                    std::slice::from_ref(&w.class_name),
                )
                .first()
                {
                    Some(crate::wf_scan::WfClassProblem::NotExported(_)) => {
                        return Err(LocalError::Worker("workflow class not exported by bundle"));
                    }
                    Some(crate::wf_scan::WfClassProblem::NotAClass(_)) => {
                        return Err(LocalError::Worker(
                            "workflow class_name export is not a class",
                        ));
                    }
                    Some(crate::wf_scan::WfClassProblem::NotEntrypoint(_)) => {
                        return Err(LocalError::Worker(
                            "workflow class does not extend WorkflowEntrypoint",
                        ));
                    }
                    None => {}
                }
            }
            workflows.push(snapshot_protocol::compute_desired::WorkflowDesired {
                name: w.name.clone(),
                binding: w.binding.clone(),
                class_name: w.class_name.clone(),
                service_worker_id,
                unique_key: derive_wf_unique_key(&ctx.pepper, tenant_id, &defining, &w.name),
                limits_steps: w.limits_steps,
            });
        }
    }

    // T21: `[assets] dir` — pack the directory into the local
    // content-addressed store (materialized by `Local::deploy`; the
    // binding references the manifest digest it produced).
    let pending_assets = match &meta.assets {
        Some(decl) => {
            let pending = scan_assets_dir(worker_dir, decl)?;
            bindings.push(BindingDesired::Assets(AssetsBinding {
                name: decl.binding.clone(),
                manifest_digest: pending.manifest_digest.clone(),
                permissions: read_only.clone(),
                assets_first: decl.assets_first.then_some(true),
                run_worker_first: decl.run_worker_first.as_ref().map(|r| match r {
                    RunWorkerFirstToml::Bool(b) => RunWorkerFirst::Bool(*b),
                    RunWorkerFirstToml::Rules(rules) => RunWorkerFirst::Rules(rules.clone()),
                }),
            }));
            Some(pending)
        }
        None => None,
    };
    // `[bindings].assets` precomputed digests stay declarable — the
    // deploy verifies the manifest exists in the local store (fail
    // closed when it does not).

    let mut hostnames = meta.hostnames.clone();
    hostnames.extend(overrides.hostnames.iter().cloned());

    Ok(PackedWorker {
        name,
        digest: bundle.digest().to_string(),
        stored_document: bundle.stored_document().to_vec(),
        compatibility_date: meta.compatibility_date,
        compatibility_flags: meta.compatibility_flags,
        bindings,
        var_values,
        crons: meta.crons,
        queue_consumers,
        durable_object_classes,
        workflows,
        hostnames,
        quotas: meta
            .quotas
            .map(|q| WorkerQuotas {
                cpu_ms: q.cpu_ms,
                memory_mb: q.memory_mb,
                subrequests: q.subrequests,
                outbound_connections: q.outbound_connections,
            })
            .unwrap_or(WorkerQuotas {
                cpu_ms: 50,
                memory_mb: 128,
                subrequests: 50,
                outbound_connections: 16,
            }),
        first_deploy_id: id_material[..16].to_string(),
        pending_assets,
    })
}

fn type_for_extension(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("js" | "mjs") => Some("esModule"),
        Some("cjs") => Some("commonJs"),
        Some("json") => Some("json"),
        Some("txt" | "text") => Some("text"),
        Some("wasm") => Some("wasm"),
        _ => None,
    }
}

/// Recursive scan; skips `vrt.toml`, dotfiles, `node_modules`.
/// Symlinks are rejected — a worker dir must be self-contained.
fn scan_dir(
    root: &Path,
    dir: &Path,
    emit: &mut dyn FnMut(&str, &Path) -> Result<(), LocalError>,
) -> Result<(), LocalError> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|_| LocalError::Worker("dir read"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if file_name.starts_with('.')
            || file_name == "node_modules"
            || crate::project_cfg::is_config_basename(file_name)
        {
            continue;
        }
        let meta = std::fs::symlink_metadata(&path).map_err(|_| LocalError::Worker("stat"))?;
        if meta.file_type().is_symlink() {
            return Err(LocalError::Worker("symlink"));
        }
        if meta.is_dir() {
            scan_dir(root, &path, emit)?;
        } else if meta.is_file() {
            let rel = path
                .strip_prefix(root)
                .map_err(|_| LocalError::Worker("path"))?
                .to_string_lossy()
                .replace('\\', "/");
            emit(&rel, &path)?;
        }
    }
    Ok(())
}

/// P3c-1：JS 标识符检查（class 名与 binding 名同规则——
/// `^[A-Za-z_$][A-Za-z0-9_$]{0,63}$`，与 binding name 校验一致；
/// class 名上限放宽到 128 与 CF 一致）。
fn valid_do_name(value: &str) -> Result<(), LocalError> {
    let ok = !value.is_empty()
        && value.len() <= 128
        && (value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'$'))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$');
    if ok {
        Ok(())
    } else {
        Err(LocalError::Worker(
            "durable object names must be JS identifiers (<=128)",
        ))
    }
}

/// P3c-1：`uniqueKey = HMAC-SHA256(pepper,
/// "vrt-do:{tenant}:{worker}:{class}")` —— 64 hex。派生输入不含
/// 用户可控的存储身份：tenant/worker/class 是部署身份，pepper 是
/// 部署密钥（standalone signing seed / 控制面快照密钥）——worker
/// 既无法预测也无法伪造另一个 class 的存储目录。
/// W1：`vrt-wf:{tenant}:{worker}:{wfName}` → 64-hex 存储身份，
/// 与 DO uniqueKey 同域不同前缀（engine class 目录独立于用户类）。
pub fn derive_wf_unique_key(
    pepper: &[u8; 32],
    tenant_id: &str,
    worker_id: &str,
    wf_name: &str,
) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, pepper);
    let tag = ring::hmac::sign(
        &key,
        format!("vrt-wf:{tenant_id}:{worker_id}:{wf_name}").as_bytes(),
    );
    hex::encode(tag.as_ref())
}

/// W1：workflow name 校验（`^[A-Za-z0-9_-]{1,64}$`）。
fn valid_workflow_name(name: &str) -> Result<(), LocalError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(LocalError::Worker(
            "workflow name must be 1-64 chars of [A-Za-z0-9_-]",
        ));
    }
    Ok(())
}

pub fn derive_do_unique_key(
    pepper: &[u8; 32],
    tenant_id: &str,
    worker_id: &str,
    class_name: &str,
) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, pepper);
    let tag = ring::hmac::sign(
        &key,
        format!("vrt-do:{tenant_id}:{worker_id}:{class_name}").as_bytes(),
    );
    hex::encode(tag.as_ref())
}

/// T37/T37b：`vrt-q:{tenant}:{name}` → sha256 前 26 hex 字符
/// （hex ⊂ `[a-z0-9]{26}`，vrt-sql 路径规则）。standalone 无控制面
/// provision——同名队列在实例内唯一。
pub fn derive_queue_id(tenant_id: &str, queue_name: &str) -> Result<String, LocalError> {
    if queue_name.is_empty()
        || queue_name.len() > 64
        || !queue_name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(LocalError::Worker(
            "queue name must be 1-64 chars of [A-Za-z0-9_-]",
        ));
    }
    let digest = hex::encode(sha2::Sha256::digest(
        format!("vrt-q:{tenant_id}:{queue_name}").as_bytes(),
    ));
    Ok(digest[..26].to_string())
}
