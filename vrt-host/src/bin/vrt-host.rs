//! `vrt-host` — per-tenant workerd supervisor + local HTTP ingress.
//!
//! Startup sequence (fail closed at every step):
//! 1. `--config <toml>` → validated [`HostConfig`].
//! 2. Secure-directory checks on `work_dir` / `cache_dir` (0700, owned).
//! 3. `FileDesiredStore` loads + verifies the `compute.desired` snapshot
//!    (signature/revision/scope/expiry — verification lives in
//!    `snapshot-protocol`, never re-implemented here). A host with no
//!    valid snapshot refuses to start.
//! 4. Assembler + `SupervisorHost` converge tenants; ingress route
//!    table swaps atomically.
//! 5. Poll loop: snapshot reload → converge → route swap. Reload
//!    failures keep the last good snapshot (logged, never fatal).
//! 6. SIGTERM/SIGINT → stop accepting ingress requests → per-tenant
//!    drain (30 s cap, SIGTERM then SIGKILL per child) → exit.
//!
//! Logs are structured (JSON, env-filter) and never contain secrets —
//! `SecretValue` has no `Display` and a redacted `Debug`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use vrt_host::artifact::{BundleCache, BundleSource, DirBundleSource};
use vrt_host::assemble::Assembler;
use vrt_host::config::HostConfig;
use vrt_host::desired::{DesiredSource, HostDesiredStore};
use vrt_host::ingress::{self, IngressState};
use vrt_host::route::RouteTable;
use vrt_host::supervisor::{SupervisorDeps, SupervisorHost, TenantRegistry};
use vrt_provider::FileDesiredStore;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    // T16/B3: `vrt-host render-params` — derive the capnp render-params
    // JSON from facade/manifest.json (debug/export tool; the running
    // path derives the same document from `[render] facadeManifest`).
    if args.get(1).map(|a| a.as_str()) == Some("render-params") {
        return render_params(&args[2..]);
    }
    let mut config_path: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                config_path = args.get(i + 1).map(PathBuf::from);
                i += 2;
            }
            other if !other.starts_with('-') => {
                config_path = Some(PathBuf::from(other));
                i += 1;
            }
            _ => i += 1,
        }
    }
    let Some(config_path) = config_path else {
        eprintln!("usage: vrt-host --config <config.toml>");
        return ExitCode::from(2);
    };
    let config = match HostConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vrt-host: {e}");
            return ExitCode::from(2);
        }
    };
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    // RV-R66: DO/workflow state must have an off-host copy unless the dev
    // waiver is explicit (never a silent single-copy production run).
    if let Some(msg) = config.durability_posture_error() {
        eprintln!("vrt-host: {msg}");
        return ExitCode::from(2);
    }
    if config.allow_local_durability && config.do_storage_dir.is_some() {
        tracing::warn!("allowLocalDurability=true: DO/workflow state is single-copy (dev only)");
    }
    // T13/M2: `[isolation]` needs euid 0 (launch drops uid/gid via
    // setgroups/setgid/setuid). Refuse NOW rather than latching
    // every tenant SwitchFailed — a configured-but-unusable mode is
    // a startup failure, never a slow per-tenant crash loop.
    // SAFETY: geteuid(2) has no preconditions and cannot fail.
    if config.isolation.is_some() && unsafe { libc::geteuid() } != 0 {
        eprintln!("vrt-host: [isolation] requires euid 0 (setuid/setgid at launch)");
        return ExitCode::from(2);
    }
    if let Err(code) = run(config) {
        return code;
    }
    ExitCode::SUCCESS
}

/// `render-params --facade-manifest <p> --provider-socket <unix path>
///   --system-compat-date <YYYY-MM-DD> [--system-compat-flag <f>]...
///   [--egress-policy <json>] --out <file>`
///
/// Emits the params-file document `emit_mode` would derive. Fails
/// closed on any malformed input (manifest schema, socket path,
/// compat values) — the output is never half-written (atomic write).
fn render_params(args: &[String]) -> ExitCode {
    let mut manifest: Option<PathBuf> = None;
    let mut socket: Option<String> = None;
    let mut compat_date: Option<String> = None;
    let mut compat_flags: Vec<String> = Vec::new();
    let mut egress_policy: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut i = 0;
    while i < args.len() {
        let next = |i: &mut usize| -> Option<String> {
            *i += 1;
            args.get(*i).cloned()
        };
        match args[i].as_str() {
            "--facade-manifest" => manifest = next(&mut i).map(PathBuf::from),
            "--provider-socket" => socket = next(&mut i),
            "--system-compat-date" => compat_date = next(&mut i),
            "--system-compat-flag" => {
                if let Some(f) = next(&mut i) {
                    compat_flags.push(f);
                }
            }
            "--egress-policy" => egress_policy = next(&mut i).map(PathBuf::from),
            "--out" => out = next(&mut i).map(PathBuf::from),
            other => {
                eprintln!("vrt-host render-params: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
        i += 1;
    }
    let (Some(manifest), Some(socket), Some(compat_date), Some(out)) =
        (manifest, socket, compat_date, out)
    else {
        eprintln!(
            "usage: vrt-host render-params --facade-manifest <p> --provider-socket <path> \
             --system-compat-date <YYYY-MM-DD> [--system-compat-flag <f>]... \
             [--egress-policy <json>] --out <file>"
        );
        return ExitCode::from(2);
    };
    match vrt_host::config::params_json_from_manifest(
        &manifest,
        &socket,
        &compat_date,
        &compat_flags,
        egress_policy.as_ref(),
    ) {
        Ok(doc) => {
            let text = serde_json::to_string_pretty(&doc).unwrap_or_default();
            if vrt_securefs::atomic_write(&out, text.as_bytes(), 0o644).is_err() {
                eprintln!("vrt-host render-params: write failed");
                return ExitCode::from(2);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("vrt-host render-params: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(config: HostConfig) -> Result<(), ExitCode> {
    for dir in [&config.work_dir, &config.cache_dir] {
        vrt_securefs::ensure_secure_dir(dir).map_err(|e| {
            // io::Error only contains errno/cause and safety-check info (path, owner/mode),
            // not secrets or config content.
            tracing::error!(dir = %dir.display(), error = %e, "secure directory check failed");
            ExitCode::from(2)
        })?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| {
            tracing::error!("tokio runtime init failed");
            ExitCode::from(2)
        })?;
    runtime.block_on(run_async(config))
}

async fn run_async(config: HostConfig) -> Result<(), ExitCode> {
    let store = FileDesiredStore::new(
        &config.desired.path,
        &config.cell_id,
        config.desired.trusted_keys.clone(),
        config.desired.grace_versions,
    );
    // Absent file = the cell-controller has not delivered the first
    // snapshot yet (start race / cell without a publication): wait
    // in-process, bounded, NOT ready. Present-but-untrusted = refuse.
    if let Err(e) = vrt_host::initial_snapshot::wait_for_initial_snapshot(
        || store.reload(chrono::Utc::now()),
        vrt_host::initial_snapshot::INITIAL_SNAPSHOT_WAIT,
        vrt_host::initial_snapshot::INITIAL_SNAPSHOT_POLL_MIN,
        vrt_host::initial_snapshot::INITIAL_SNAPSHOT_POLL_MAX,
    )
    .await
    {
        tracing::error!(error = %e, "initial snapshot load failed; refusing to start");
        return Err(ExitCode::from(2));
    }
    let desired_source: Arc<dyn DesiredSource> = Arc::new(HostDesiredStore::new(store));

    let emit_mode = config.emit_mode().map_err(|e| {
        tracing::error!(error = %e, "render config invalid");
        ExitCode::from(2)
    })?;
    let cache = Arc::new(BundleCache::new(&config.cache_dir));
    // System (facade) modules are image-baked, not published artifacts
    // (the CP bundle route will never serve them): verify each against
    // the manifest and seed the object cache BEFORE any tenant
    // assembles. Any failure refuses startup (fail closed).
    if let Some(manifest_path) = &config.render.facade_manifest {
        match vrt_host::system_seed::seed_system_modules(&cache, manifest_path) {
            Ok(n) => tracing::info!(seeded = n, "system modules verified and seeded"),
            Err(e) => {
                tracing::error!(error = %e, manifest = %manifest_path.display(), "system module seeding failed; refusing to start");
                return Err(ExitCode::from(2));
            }
        }
    }
    // T15/B2: `[bundles]` configured → pull from the control-plane
    // route (verified digest, bounded retries); absent → local dir
    // source (dev fixtures only).
    let source: Arc<dyn BundleSource> = match &config.bundles {
        // T17: `[bundles] dir` — local directory source (standalone
        // mode reads vrt-local's objects/ directly, same contract).
        Some(b) if b.dir.is_some() => {
            Arc::new(DirBundleSource::new(b.dir.clone().expect("dir checked")))
        }
        Some(b) => {
            let http_config = match b.to_http_config() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "bundles config invalid; refusing to start");
                    return Err(ExitCode::from(2));
                }
            };
            match vrt_host::bundle_http::HttpBundleSource::new(http_config) {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    tracing::error!(error = %e, "bundle source init failed; refusing to start");
                    return Err(ExitCode::from(2));
                }
            }
        }
        None => Arc::new(DirBundleSource::new(config.cache_dir.join("source"))),
    };
    // T21: `[assets]` configured → materializer bound to the shared
    // store; pull source only when baseUrl+tokenFile are set
    // (standalone relies on `of deploy` pre-materialization). Absent
    // config → ASSETS bindings fail closed at assemble.
    let asset_sync = match &config.assets {
        Some(a) => {
            let store = match vrt_provider::assets::AssetStore::open(&a.dir) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "assets store init failed; refusing to start");
                    return Err(ExitCode::from(2));
                }
            };
            let source: Arc<dyn vrt_host::asset_sync::AssetSource> = match a.pull_source() {
                Ok(Some(s)) => Arc::new(s),
                Ok(None) => Arc::new(vrt_host::asset_sync::AbsentAssetSource),
                Err(e) => {
                    tracing::error!(error = %e, "assets config invalid; refusing to start");
                    return Err(ExitCode::from(2));
                }
            };
            Some(Arc::new(vrt_host::asset_sync::AssetSync::new(
                store, source,
            )))
        }
        None => None,
    };
    // P3c-1: DO storage root — declared by config; the assembler
    // creates per-tenant/worker dirs under it before launch. Absent
    // → DO tenants fail closed at assemble (never silently in-memory).
    let mut assembler = Assembler::new_with_assets(
        config.workerd_binary.clone(),
        config.work_dir.clone(),
        cache,
        source,
        emit_mode,
        config.base_env.clone(),
        config.limits.resource_limits(),
        config.render.extra_args.clone(),
        // T17/F1: vars/secrets re-read per converge (0600 re-verified).
        config.render_inputs_provider(),
        asset_sync,
    );
    if let Some(root) = &config.do_storage_dir {
        assembler = assembler.with_do_storage(root.clone());
    }
    // Per-worker quotas: `render.quotas` states that this host's
    // workerd enforces the VRT `limits` block. Off ⇒ a tenant declaring
    // a limit fails at assemble (never silently unlimited).
    assembler = assembler.with_quotas(config.render.quotas);
    // H5-r1: platform mode derives every tenant's binding-backend
    // socket under the provider listenerDir (config validate already
    // rejected verbatim-socket+assignmentFile and socket+dir combos).
    if let Some(dir) = &config.render.binding_backend_dir {
        assembler = assembler.with_binding_backend_dir(dir.clone());
    }
    // T13/M2: `[isolation]` — per-tenant uid/gid; requires euid 0 at
    // launch (the launcher fails closed otherwise — see launch.rs).
    if let Some(iso) = config.isolation {
        assembler = assembler.with_isolation(vrt_host::assemble::IsolationPolicy {
            base: iso.base,
            span: iso.span,
        });
    }
    // W6: workflow-storage deletion endpoint lives on this same
    // control listener — absent → the capnp `vrt-wfdel` external is
    // not emitted and the engine loopback shim answers 501 explicitly.
    // Worker logs: the hub shares the control listener (tail workers
    // reach it through the `vrt-logsink` external; cell-controller
    // drains it). No control listener → no collection (fail closed).
    let log_hub = config
        .control_listen
        .as_ref()
        .map(|_| vrt_host::logs::LogHub::new(vrt_host::logs::HubLimits::default()));
    if let Some(control) = &config.control_listen {
        assembler = assembler.with_wf_delete_addr(control.clone());
        if let Some(hub) = &log_hub {
            assembler = assembler.with_log_hub(hub.clone());
        }
    }
    // C1: `[cache]` — per-tenant Cache API unix sockets under
    // `<workDir>/cache/`. `CacheService::new` validates the limits
    // and the 0711 root contract. C1-r2 fault-domain review: an init
    // failure DEGRADES the capability instead of failing the host —
    // cache is an additive feature (absent ⇒ workerd already throws
    // `No Cache was configured`), so downgrading to "unconfigured"
    // keeps every other tenant function serving, exactly the AGENTS
    // partial-failure rule (old service stays up, new capability
    // explicitly off, observable via metric+log, never a fake store).
    // `with_cache_root` is skipped ⇒ no `cacheApiOutbound` emit.
    let cache_service = match &config.cache {
        Some(c) => {
            let root = config.work_dir.join("cache");
            let built = vrt_host::cache::CacheService::new(Some(root.clone()), c.limits());
            // Warn log + error counter + `vrt_host_cache_degraded` gauge.
            vrt_host::cache::record_cache_init(&built);
            match built {
                Ok(svc) => {
                    assembler = assembler.with_cache_root(root);
                    Some(Arc::new(svc))
                }
                Err(_) => None,
            }
        }
        None => None,
    };
    // [analyticsEngine] — per-tenant AE ingest sockets under
    // `<workDir>/ae/sock/`; spool + Parquet output under
    // `<workDir>/ae/`. Same degrade rule as cache: an init failure
    // downgrades the capability (tenants declaring AE bindings then
    // fail the render closed — never a fake ingest), observable via
    // log + `vrt_host_ae_degraded`.
    let ae_service = {
        let ae_dir = config.work_dir.join("ae");
        let sock_root = ae_dir.join("sock");
        let built = vrt_host::ae::AeService::new(
            Some(sock_root.clone()),
            ae_dir,
            config.analytics_engine.limits(),
        );
        vrt_host::ae::record_ae_init(&built);
        match built {
            Ok(svc) => {
                assembler = assembler.with_ae_root(sock_root);
                Some(Arc::new(svc))
            }
            Err(_) => None,
        }
    };
    let assembler: Arc<dyn vrt_host::supervisor::TenantAssembler> = Arc::new(assembler);

    // P3c-2: DO placement authority — object-store backends build a
    // CAS client + run a write/read/delete probe NOW (a configured
    // but unreachable store refuses startup, exactly like vrt-sql's
    // persistence self-check; NEVER a silent LocalPlacement
    // fallback). `none`/absent → LocalPlacement (standalone).
    let (placement, do_pair) = match build_placement(&config) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::error!(error = %e, "placement init failed (fail closed)");
            return Err(ExitCode::from(2));
        }
    };
    let mut deps = SupervisorDeps::default();
    deps.cache = cache_service;
    deps.ae = ae_service.clone();
    deps.placement = placement.clone();
    // P3c-2: snapshot surface (same store + concrete manifest impl)
    // + migration coordinator shared with the control listener.
    let (do_snap_ctx, do_migrator) = match &do_pair {
        Some((store, manifest)) => {
            let snap_cfg =
                config
                    .do_snapshots
                    .clone()
                    .unwrap_or(vrt_host::config::DoSnapshotsConfig {
                        interval_ms: 300_000,
                        jitter_ms: 30_000,
                        departing_max_ms: 15 * 60_000,
                        xfer_quarantine_ms: 7 * 24 * 3600 * 1000,
                        departing_retry_base_ms: 2_000,
                        departing_retry_cap_ms: 60_000,
                    });
            // WAL1: [doWal] — resolve clamps; out-of-range values
            // fall back to defaults with a warning (fail closed).
            let (wal_cfg, wal_warns) = config
                .do_wal
                .clone()
                .unwrap_or(vrt_host::config::DoWalConfig {
                    enabled: false,
                    tick_ms: 2_000,
                    max_dbs: 32,
                    max_pending_bytes: 256 << 20,
                    max_segment_bytes: 8 << 20,
                    upload_attempts: 3,
                    idle_stop_ms: 300_000,
                    flush_max_ms: 5_000,
                    tick_upload_budget_ms: 500,
                    replay_max_bytes: 1 << 30,
                    rebaseline_chain_bytes: None,
                    baseline_backoff_max_ms: 1_800_000,
                })
                .resolve();
            for w in &wal_warns {
                tracing::warn!("config: {w}");
            }
            let wal = wal_cfg.enabled.then(|| {
                std::sync::Arc::new(vrt_host::do_wal::DoWalReplicator::new(
                    wal_cfg,
                    store.clone(),
                    config.work_dir.join("do-wal-tmp"),
                    std::sync::Arc::new(|| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0)
                    }),
                ))
            });
            let ctx = std::sync::Arc::new(vrt_host::supervisor::DoSnapCtx {
                store: store.clone(),
                placement: manifest.clone(),
                tmp_root: config.work_dir.join("do-snap-tmp"),
                wal,
                interval_ms: snap_cfg.interval_ms(),
                jitter_ms: snap_cfg.jitter_ms(),
                departing_max_ms: snap_cfg.departing_max_ms(),
                xfer_quarantine_ms: snap_cfg.xfer_quarantine_ms(),
                departing_retry_base_ms: snap_cfg.departing_retry_base_ms(),
                departing_retry_cap_ms: snap_cfg.departing_retry_cap_ms(),
            });
            // P3c-2-r1: stage timeout is DERIVED from the lease
            // params (the fenced writer's worst-case stop-write
            // bound = renew cadence + kill margin + skew), never a
            // parallel constant — 60s was shorter than a 2-minute
            // lease and would time every migration out.
            let stage_ms = manifest.stop_write_bound_ms();
            let mig = std::sync::Arc::new(vrt_host::do_migration::Migrator {
                store: store.clone(),
                placement: manifest.clone(),
                tmp_root: config.work_dir.join("do-mig-tmp"),
                stage_timeout: std::time::Duration::from_millis(stage_ms),
                // Reservation must outlive the whole staged drive
                // (fence → dead → snap → commit → resume); 4 stages
                // of the same bound.
                reserve_ms: stage_ms.saturating_mul(4),
                poll: std::time::Duration::from_millis(100),
            });
            (Some(ctx), Some(mig))
        }
        None => (None, None),
    };
    // W6-r2: workflow-delete tombstones live under the host-owned
    // work_dir (0700) so they survive vrt-host restarts; the
    // supervisor reclaims them at serialized-DO-switch safe points.
    deps.wf_tomb_dir = config.work_dir.join("wf-del");
    deps.do_snap = do_snap_ctx;
    // RV-R53: host-level tombstone purges need the DO storage root;
    // RV-R53.4: the durable purge ledger lives beside it and resumes
    // object-store deletions across restarts.
    deps.do_storage_root = config.do_storage_dir.clone();
    deps.purge = match (&do_pair, &config.do_storage_dir) {
        (Some((store, manifest)), Some(root)) => Some(std::sync::Arc::new(
            vrt_host::do_purge::DoPurge::open(root, store.clone(), manifest.clone()),
        )),
        _ => None,
    };

    let registry = TenantRegistry::new();
    let mut sup_params = config.supervisor.params();
    // Scale to zero: ONE shared coordinator for every tenant actor;
    // `None` (default) keeps the supervisor byte-identical.
    sup_params.scale_to_zero = config.scale_to_zero.build();
    // MIG-13-r2/L2: cap the whole prewarm episode at 2x the ingress
    // switch-hold window — a swap that could not land inside one hold
    // deadline was never going to; the parked process dies and the
    // switch falls back to the serialized path.
    // MIG-13-r3: the cap must also cover a worst-case pre-gate
    // episode — `2*hold` alone could fire before the drain bound
    // elapsed and expire inside the gate window, pointing live
    // early-flip routes at a killed warm process (the r2 502). The
    // drain+grace+kill+margin floor therefore lives in
    // `SupervisorConfig::params()` (every caller gets it); here only
    // the hold-derived value can RAISE it further. `on_tick`
    // additionally never fires the cap while the gate is open —
    // sizing is the floor, the gate check the guard.
    if config.ingress.switch_hold_ms > 0 {
        sup_params.prewarm_ttl = sup_params
            .prewarm_ttl
            .max(Duration::from_millis(config.ingress.switch_hold_ms) * 2);
    }
    let mut host = SupervisorHost::new(registry.clone(), deps, sup_params);
    // [geoip] (default on): open the MMDB if one is already on disk;
    // the sync task below fills/refreshes it. A missing or unreadable
    // database simply yields no geo fields — never a startup error.
    let geoip = config
        .geoip
        .enabled
        .then(|| vrt_host::geoip::GeoIp::open(config.geoip.mmdb_path_or(&config.work_dir)));
    let ingress_state = IngressState::new(
        registry.clone(),
        Duration::from_millis(config.ingress.request_timeout_ms),
        Duration::from_millis(config.ingress.connect_timeout_ms),
        config.ingress.max_body_bytes,
    )
    .with_switch_hold(
        Duration::from_millis(config.ingress.switch_hold_ms),
        config.ingress.switch_hold_max_reqs,
        config.ingress.switch_hold_max_bytes,
    )
    .with_forwarded_proto(config.ingress.forwarded_policy())
    // MIG-9: typed upstream client-IP trust — same rebuild chain.
    .with_client_ip(config.ingress.client_ip_policy())
    // [geoip]: local MMDB fills `request.cf` geo/ASN fields the
    // verified upstream chain did not provide.
    .with_geoip(geoip.clone());
    // WebSocket tunnel bounds (`[ingress.websocket]`, validated at load).
    let ws = config.ingress.websocket_limits();
    let ingress_state = ingress_state.with_ws_limits(
        ws.idle_timeout,
        ws.max_duration,
        ws.max_sessions,
        ws.max_sessions_per_tenant,
    );
    if let Some(hub) = &log_hub {
        ingress_state.set_log_hub(hub.clone());
    }
    // `of dev` only: unmatched-Host fallback to a single-worker
    // route table (`ingress.devCatchAll`, default off).
    if config.ingress.dev_catch_all {
        ingress_state.set_dev_catch_all(true);
        tracing::warn!("ingress.devCatchAll enabled — any Host routes to the single dev worker");
    }

    let status = vrt_host::status::new_status();
    // MIG-10: probe the CA trust store tenant-egress TLS relies on
    // (workerd `trustBrowserCas` reads the system bundle). A missing or
    // empty bundle is logged as a structured ERROR and surfaced via
    // `vrt_host_egress_trust_store_ok` + host-status.json — it does NOT
    // block startup: inbound traffic never depends on the bundle and
    // refusing to serve would widen a degraded capability into a full
    // outage. The gauge keeps the regression alertable.
    {
        let ts = vrt_host::trust_store::check();
        vrt_host::trust_store::record(ts);
        if ts.ok() {
            tracing::info!(
                path = vrt_host::trust_store::CA_BUNDLE_PATH,
                "egress CA trust store present"
            );
        } else {
            tracing::error!(
                path = vrt_host::trust_store::CA_BUNDLE_PATH,
                reason = ts.code(),
                "egress CA trust store unavailable — tenant HTTPS fetch() will fail TLS verification (install ca-certificates in the runtime image)"
            );
        }
        status.write().unwrap().egress_trust_store_ok = ts.ok();
    }
    let ctx = ConvergeCtx {
        assignment_file: config.desired.assignment_file.as_deref(),
        pod_id: config.desired.pod_id.as_deref(),
        status: status.clone(),
    };
    // T34f2/R1: startup-only orphan sweep — at this point the process
    // provably owns NO children, so a verified workerd whose owning
    // host is dead is definitively a crashed predecessor's leftover.
    // Never inside launch/apply paths: a live previous generation
    // matches the orphan shape and must keep serving through switches.
    vrt_host::supervisor::reap_orphaned_children(&config.work_dir);
    converge_once(
        &mut host,
        desired_source.as_ref(),
        &assembler,
        &ingress_state,
        &ctx,
    );
    vrt_host::status::write_status_file(
        &config.work_dir,
        &ctx.status,
        &registry,
        config.render.quotas,
    );

    let listener = TcpListener::bind(&config.listen_addr).await.map_err(|e| {
        tracing::error!(error = %e, "ingress listen failed");
        ExitCode::from(2)
    })?;
    // T14/B1: control listener (/readyz + /healthz) — separate from
    // tenant ingress so a tenant hostname can never shadow it.
    if let Some(control) = &config.control_listen {
        let addr: std::net::SocketAddr = control.parse().map_err(|_| {
            tracing::error!("controlListen parse failed");
            ExitCode::from(2)
        })?;
        let control_listener = TcpListener::bind(addr).await.map_err(|e| {
            tracing::error!(error = %e, "control listen failed");
            ExitCode::from(2)
        })?;
        // MIG-11: control-plane dispatches (cron/queue/wfops) share the
        // ingress serialized-switch hold ceilings — same bounds, same
        // gate, same typed switching reject.
        let mut router = vrt_host::status::control_router(
            status.clone(),
            registry.clone(),
            vrt_host::supervisor::SwitchHoldParams {
                budget: Duration::from_millis(config.ingress.switch_hold_ms),
                max_reqs: config.ingress.switch_hold_max_reqs,
                max_bytes: config.ingress.switch_hold_max_bytes,
            },
        );
        // P3c-2: frozen-migration admin trigger — same loopback
        // control-listener trust model as /v1/cron/trigger, gated on
        // an object-store placement backend existing at all.
        if let Some(mig) = do_migrator {
            router = router.merge(vrt_host::do_migration::admin::router(
                vrt_host::do_migration::admin::AdminState {
                    migrator: mig,
                    registry: registry.clone(),
                    seen: std::sync::Arc::new(std::sync::Mutex::new(
                        std::collections::HashMap::new(),
                    )),
                },
            ));
        }
        // W6-r2: workflow-instance delete tombstones — reached ONLY
        // via the capnp `vrt-wfdel` external service (engine-internal
        // shim `vrt-wf-loopback-<workerId>`), never by tenant workers.
        // Physical unlink happens later at supervisor safe points.
        router = router.merge(vrt_host::wf_delete::router(
            vrt_host::wf_delete::WfDelState::new(registry.clone(), config.work_dir.join("wf-del")),
        ));
        if let Some(hub) = &log_hub {
            router = router.merge(vrt_host::logs::router(hub.clone()));
        }
        tokio::spawn(async move {
            let _ = axum::serve(control_listener, router).await;
        });
    }
    // B12: dedicated metrics listener (`metricsListen`) — bound only
    // when configured; k8s exposes it via the named `metrics` port +
    // NetworkPolicy restricted to vmagent.
    if let Some(metrics_addr) = &config.metrics_listen {
        let addr: std::net::SocketAddr = metrics_addr.parse().map_err(|_| {
            tracing::error!("metricsListen is not a socket addr");
            ExitCode::from(2)
        })?;
        let metrics_listener = TcpListener::bind(addr).await.map_err(|e| {
            tracing::error!(error = %e, "metrics listen failed");
            ExitCode::from(2)
        })?;
        let metrics_router = vrt_host::metrics::router(registry.clone());
        tokio::spawn(async move {
            let _ = axum::serve(metrics_listener, metrics_router).await;
        });
        tracing::info!(%metrics_addr, "metrics listener bound");
    }
    // B12: build-info gauge.
    // T28/H2: cell identity — process config is the single source;
    // exported as a const label on every emitted series so scraped
    // data self-identifies (no pod-label/scrape relabel second source).
    vrt_host::metrics::registry().set_const_label("cell_id", config.effective_cell_id());
    vrt_host::metrics::registry().set(
        "vrt_host_build_info",
        &[("version", env!("CARGO_PKG_VERSION"))],
        1.0,
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    // [geoip]: background refresh of the MMDB — bounded retries,
    // atomic install, exits on the shutdown watch like ingress.
    if let Some(geo) = geoip.clone() {
        let sync_cfg = config.geoip.sync_config(&config.work_dir);
        let geoip_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            vrt_host::geoip::sync::run(sync_cfg, geo, geoip_shutdown).await;
        });
    }
    // [analyticsEngine]: spool→Parquet flush — exits on the shutdown
    // watch (one final flush before exit).
    if let Some(svc) = ae_service.clone() {
        svc.spawn_flush(shutdown_tx.subscribe());
    }
    let ingress_task = {
        let ingress_state = ingress_state.clone();
        tokio::spawn(ingress::serve(listener, ingress_state, async move {
            let mut rx = shutdown_rx;
            let _ = rx.changed().await;
        }))
    };

    // Reload + converge loop.
    let reload_ms = config.desired.reload_ms.max(100);
    let drain_timeout = Duration::from_millis(config.supervisor.drain_timeout_ms);
    let kill_grace = Duration::from_millis(config.supervisor.kill_grace_ms);
    // MIG-12: post-idle waitUntil grace — part of the per-generation
    // SIGTERM budget, so the process-level deadline covers it.
    let wait_until_grace = Duration::from_millis(config.supervisor.wait_until_grace_ms);
    let desired_source2 = desired_source.clone();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| ExitCode::from(2))?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| ExitCode::from(2))?;
    let mut ticks = tokio::time::interval(Duration::from_millis(reload_ms));
    loop {
        tokio::select! {
            _ = ticks.tick() => {
                if let Err(e) = desired_source2.reload().await {
                    tracing::warn!(error = %e, "snapshot reload failed; keeping previous");
                }
                converge_once(&mut host, desired_source2.as_ref(), &assembler, &ingress_state, &ctx);
                host.reap_finished();
                // T25/M2: snapshot the live tenant states to
                // <workDir>/host-status.json so `of status` can see
                // supervisor state, last failure reasons and circuit
                // state without reaching the control listener.
                vrt_host::status::write_status_file(
                    &config.work_dir,
                    &ctx.status,
                    &registry,
                    config.render.quotas,
                );
            }
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }

    tracing::info!("shutdown: draining tenants and ingress");
    let _ = shutdown_tx.send(true);
    // Bound the whole teardown: per-tenant drain cap + waitUntil
    // grace + kill grace, plus a fixed margin for ingress in-flight
    // HTTP requests.
    let budget = drain_timeout + wait_until_grace + kill_grace + Duration::from_secs(5);
    let _ = tokio::time::timeout(budget, async {
        let _ = ingress_task.await;
        host.shutdown_all(drain_timeout + wait_until_grace + kill_grace)
            .await;
    })
    .await;
    tracing::info!("vrt-host stopped");
    Ok(())
}

/// Convergence context for the T14/B1 assignment contract — when
/// `assignment` is configured the pod runs ONLY the workers named for
/// its pod_id; a missing/stale/mismatched assignment blocks
/// convergence (fail closed, old generations keep serving).
struct ConvergeCtx<'a> {
    assignment_file: Option<&'a std::path::Path>,
    pod_id: Option<&'a str>,
    status: vrt_host::status::HostStatus,
}

/// Reload-visible convergence: verified snapshot → supervisor specs +
/// atomic route-table swap.
fn converge_once(
    host: &mut SupervisorHost,
    source: &dyn DesiredSource,
    assembler: &Arc<dyn vrt_host::supervisor::TenantAssembler>,
    ingress_state: &Arc<IngressState>,
    ctx: &ConvergeCtx,
) {
    match source.snapshot() {
        Ok(desired) => {
            // T14/B1: adoption gate — the verified snapshot must pair
            // with a valid assignment before any tenant spec is built.
            let (effective, assigned, blocked) = match ctx.assignment_file {
                Some(path) => {
                    let digest = source.payload_digest().unwrap_or_default();
                    match vrt_host::assignment::adopt(
                        path,
                        ctx.pod_id.unwrap_or(""),
                        &desired,
                        &digest,
                    ) {
                        Ok((filtered, assigned)) => (filtered, assigned, None),
                        Err(e) => {
                            tracing::warn!(
                                reason = vrt_host::status::assignment_reason(e).as_str(),
                                "assignment rejected — keeping previous generation"
                            );
                            (
                                (*desired).clone(),
                                Vec::new(),
                                Some(vrt_host::status::assignment_reason(e)),
                            )
                        }
                    }
                }
                // No assignment mode: every (tenant, worker) pair in the
                // verified snapshot is this pod's responsibility —
                // otherwise readyz would be vacuously true (T15/R3).
                None => (
                    (*desired).clone(),
                    desired
                        .tenants
                        .iter()
                        .flat_map(|t| {
                            t.workers
                                .iter()
                                .map(|w| (t.tenant_id.clone(), w.worker_id.clone()))
                        })
                        .collect(),
                    None,
                ),
            };
            // T15/R2: a rejected adoption reports ONLY the blocked
            // reason — `revision`/`assigned` keep the last adopted
            // values so readyz never claims the new revision as loaded.
            if let Some(b) = blocked {
                let mut st = ctx.status.write().unwrap_or_else(|e| e.into_inner());
                st.blocked_reason = Some(b);
                return; // fail closed — previous generation keeps serving
            }
            // Converge BEFORE publishing the adopted revision: per-tenant
            // expected digests are updated synchronously inside converge,
            // so status.assigned never outruns the supervisor (T15/R2).
            let outcome = host.converge(&effective, assembler);
            {
                let mut st = ctx.status.write().unwrap_or_else(|e| e.into_inner());
                st.revision = Some(desired.revision);
                st.payload_digest = source.payload_digest();
                st.assigned = assigned;
                st.blocked_reason = None;
            }
            if !outcome.applied.is_empty()
                || !outcome.removed.is_empty()
                || !outcome.failed.is_empty()
            {
                tracing::info!(
                    applied = outcome.applied.len(),
                    removed = outcome.removed.len(),
                    failed = outcome.failed.len(),
                    "converged"
                );
            }
            ingress_state.set_routes(RouteTable::from_desired(&effective));
        }
        Err(e) => {
            tracing::warn!(error = %e, "no usable snapshot; keeping previous routes");
        }
    }
}

/// P3c-2: construct the placement impl from `[placement]`.
/// Returns the placement trait object plus the object store itself
/// (shared by the DO snapshot/restore paths — one store, one
/// namespace family `do/`).
fn build_placement(
    config: &vrt_host::config::HostConfig,
) -> Result<
    (
        std::sync::Arc<dyn vrt_host::do_placement::DoPlacement>,
        Option<(
            std::sync::Arc<dyn vrt_objstore::ObjectStore>,
            std::sync::Arc<vrt_host::do_placement_store::ManifestDoPlacement>,
        )>,
    ),
    String,
> {
    use std::sync::Arc;
    let Some(p) = &config.placement else {
        return Ok((Arc::new(vrt_host::do_placement::LocalPlacement), None));
    };
    match p.backend.as_str() {
        "none" | "local" => Ok((Arc::new(vrt_host::do_placement::LocalPlacement), None)),
        "fs" | "s3" => {
            let store: Arc<dyn vrt_objstore::ObjectStore> = match p.backend.as_str() {
                "fs" => {
                    let fs = p.fs.as_ref().ok_or("placement.fs missing")?;
                    match vrt_objstore::FsStore::new(fs.root.clone()) {
                        Ok(s) => {
                            tracing::info!(root = %fs.root.display(), "placement backend = fs");
                            Arc::new(s)
                        }
                        Err(e) => return Err(format!("placement fs store init: {e}")),
                    }
                }
                "s3" => {
                    let s3 = p.s3.as_ref().ok_or("placement.s3 missing")?;
                    let access = vrt_sql::service::load_secret_0600(
                        &s3.access_key_file,
                        vrt_securefs::SecretPurpose::Credential,
                    )
                    .map_err(|e| format!("placement s3 access key file: {e}"))?;
                    let secret = vrt_sql::service::load_secret_0600(
                        &s3.secret_key_file,
                        vrt_securefs::SecretPurpose::Credential,
                    )
                    .map_err(|e| format!("placement s3 secret key file: {e}"))?;
                    let cfg = vrt_objstore::S3StoreConfig {
                        endpoint: s3.endpoint.clone(),
                        region: s3.region.clone(),
                        bucket: s3.bucket.clone(),
                        access_key: String::from_utf8_lossy(&access).trim().to_string(),
                        secret_key: vrt_objstore::sigv4::SecretString::new(
                            String::from_utf8_lossy(&secret).trim().to_string(),
                        ),
                        timeout: std::time::Duration::from_millis(s3.timeout_ms),
                        conditional_writes: s3.conditional_writes,
                        retries: 2,
                        prefix: s3.prefix.clone(),
                    };
                    match vrt_objstore::S3Store::new(cfg) {
                        Ok(s) => {
                            tracing::info!(
                                endpoint = %s3.endpoint,
                                bucket = %s3.bucket,
                                "placement backend = s3"
                            );
                            Arc::new(s)
                        }
                        Err(e) => return Err(format!("placement s3 store init: {e}")),
                    }
                }
                _ => unreachable!("matched above"),
            };
            // MIG-10: startup self-check — conditional write + read +
            // delete on a placement-namespaced probe key, retried
            // through the SHARED bounded primitive
            // (`vrt-objstore::self_check`, the same one vrt-sql's
            // MIG-9 persistence check uses — not a copy). Real-cluster
            // evidence 2026-11-17: a transient pod-network/TLS stall
            // (3-6 s handshake) crashed the single 5 s attempt and
            // CrashLooped the pod 6× before recovering.
            //   - only `Unavailable{retryable:true}` retries (transient
            //     network/5xx/timeout); 401/403/authz and every other
            //     error fails closed IMMEDIATELY — a misconfig never
            //     heals inside the budget;
            //   - budget/backoff come from `[placement] selfCheck*`
            //     (default 60 s, same fields vrt-sql exposes — the
            //     placement table reuses PersistenceConfig);
            //   - the per-request timeout is `placement.s3.timeoutMs`
            //     (store-level, already configurable — not a hardcoded
            //     5 s);
            //   - while retrying no listener is bound → readyz stays
            //     not-ready by construction; the startupProbe (70 ×
            //     10 s = 700 s) covers the budget so liveness never
            //     kills the pod mid-check;
            //   - logged errors pass sanitize_self_check_error so a
            //     backend URL (path/query) never reaches logs.
            let retry = vrt_objstore::self_check::SelfCheckRetry {
                budget: std::time::Duration::from_millis(p.self_check_budget_ms),
                initial_backoff: std::time::Duration::from_millis(p.self_check_initial_backoff_ms),
                max_backoff: std::time::Duration::from_millis(p.self_check_max_backoff_ms),
            };
            vrt_host::placement_self_check::run(
                &store,
                &retry,
                |attempt, elapsed, next_backoff, e| {
                    tracing::warn!(
                        attempt,
                        elapsed_ms = elapsed.as_millis() as u64,
                        next_backoff_ms = next_backoff.as_millis() as u64,
                        error = %vrt_objstore::self_check::sanitize_self_check_error(
                            &e.to_string()
                        ),
                        "placement self-check retryable failure, backing off"
                    );
                },
            )
            .map_err(|e| {
                format!(
                    "placement self-check: {}",
                    vrt_objstore::self_check::sanitize_self_check_error(&e.to_string())
                )
            })?;
            let holder = std::env::var("HOSTNAME").unwrap_or_else(|_| "vrt-host".into());
            let holder = format!(
                "{}:{}:{}:{:x}",
                config.cell_id,
                holder,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            );
            let manifest =
                std::sync::Arc::new(vrt_host::do_placement_store::ManifestDoPlacement::new(
                    store.clone(),
                    holder,
                    p.lease_ms,
                    p.clock_skew_margin_ms,
                ));
            Ok((manifest.clone(), Some((store, manifest))))
        }
        other => Err(format!("placement.backend {other}")),
    }
}
