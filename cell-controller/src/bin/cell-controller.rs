//! `cell-controller` binary — wires the orchestrator over HTTP.
//!
//! Config (TOML):
//! ```toml
//! cell_id = "cluster-abc"
//! control_plane_url = "http://vibeploy-control.internal"
//! token_file = "/var/run/secrets/vrt/cell-token"
//! health_listen = "0.0.0.0:9090"
//! poll_interval_secs = 10
//! cron_holder = "cell-controller-0"
//! fence_file = "/var/lib/vibeploy-cell/revision-fence.json"
//! # allow_insecure_http = true   # local/test ONLY — production must use https
//! [[trusted_keys]]
//! keyId = "..."
//! publicKey = "..."
//! ```
//!
//! The token file carries the scoped cluster ingest token
//! (`vrt.cell-controller` scope — `deriveClusterEventIngestScopedToken`).
//! Production pod transport + k8s Lease are unverified here -- the binary uses
//! `UnavailablePodChannel` (fails loud → PARTIAL, never fake APPLIED) and
//! an in-memory lease (single replica).

use cell_controller::controller::ReconcileOutcome;
use cell_controller::cron::CronRunner;
use cell_controller::health;
use chrono::Utc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/etc/vibeploy/cell-controller.toml".into());
    // All construction goes through `prod::` — the SAME path tests
    // exercise (T13/H1); nothing here may re-wire deps by hand.
    let config = cell_controller::prod::load_config(&config_path).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let (source, ack) = cell_controller::prod::http_deps(&config).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    // T14/B1: `[[pods]]` config → FileEnvelopePodChannel; absent →
    // Unavailable channel (fails loud → PARTIAL, never fake APPLIED).
    let channel = cell_controller::prod::pod_channel(&config).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    // T21: cron capability is decided by cron_deps (pods + replicas);
    // assets capability is reported statically — the T21 host/provider
    // binaries implement the asset fetch contract.
    let (cron_log, cron_invoker, cron_lease, cron_lease_ttl, cron_capability) =
        cell_controller::prod::cron_deps(&config).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        });
    // T35: `d1` only when `[d1sql]` configured — ops executor below.
    let d1_capability = config.d1sql.is_some();
    // RV-R70: `storagePurge` claims a WIRED executor, not merely a
    // config section — a present-but-broken `[storage_purge]` must
    // never advertise the bit (fail closed: ops would sit PENDING
    // forever since the kind never expires).
    let storage_purge_exec = match &config.storage_purge {
        Some(cfg) => {
            match cell_controller::storagepurge::StoragePurgeExecutor::connect(cfg).await {
                Ok(e) => Some(std::sync::Arc::new(e)),
                Err(e) => {
                    tracing::error!(%e, "storage_purge executor init failed — capability off");
                    None
                }
            }
        }
        None => None,
    };
    // CACHE_PURGE: fan-out executor over the `[[pods]]` loopback
    // bases — same wired-not-just-configured rule as storage_purge:
    // a present-but-broken `[cache_purge]` (missing token, non-loopback
    // base) must never advertise the bit.
    let cache_purge_exec = match &config.cache_purge {
        Some(cfg) => {
            let pod_bases: Vec<(String, String)> = config
                .pods
                .iter()
                .map(|p| (p.id.clone(), p.readyz.clone()))
                .collect();
            match cell_controller::cachepurge::CachePurgeExecutor::connect(cfg, &pod_bases) {
                Ok(e) => Some(std::sync::Arc::new(e)),
                Err(e) => {
                    tracing::error!(%e, "cache_purge executor init failed — capability off");
                    None
                }
            }
        }
        None => None,
    };
    let capabilities = cell_controller::ack::AckCapabilities {
        cron: cron_capability,
        assets: true,
        d1: d1_capability,
        // T37b: queue dispatcher shares the `[d1sql]` vrt-sql authority;
        // deps construction below fails closed (exit 2) when present-but-
        // broken, so `d1sql.is_some()` ⇔ dispatcher runs.
        queue: d1_capability,
        // W2: workflow render support ships with vrt-config/vrt-local —
        // unconditionally on for this binary generation.
        workflows: true,
        // W3: wfops executor requires `[[pods]]` loopback bases (the
        // op path posts to the pod's vrt-host `/v1/wfops`). No pods ⇒
        // ops can never be delivered ⇒ report not-capable (fail closed).
        workflows_ops: !config.pods.is_empty(),
        // W4: D1 ops ride the generic `/vrt/cell-ops` channel when the
        // data plane is wired — the cell-ops pull loop below runs
        // whenever `[d1sql]` OR pods are configured, so claiming this
        // bit only requires d1sql. Absent ⇒ the control plane keeps
        // writing to the legacy `vrt_d1_ops` table (fail closed).
        d1_cell_ops: d1_capability,
        // W7: `service_worker_id` render ships with this generation —
        // unconditionally on; pre-W7 cells keep `deny_unknown_fields`
        // rejecting the field, and the control plane gates on this bit.
        workflows_cross_worker: true,
        storage_purge: storage_purge_exec.is_some(),
        cache_purge: cache_purge_exec.is_some(),
        vectorize_purge: storage_purge_exec
            .as_ref()
            .is_some_and(|e| e.has_vectorize()),
        // KV bindings are admitted only when a KV backend was actually built.
        kv: storage_purge_exec.as_ref().is_some_and(|e| e.has_kv()),
        // R2 object data ops: only when an R2 backend was actually built.
        r2: storage_purge_exec.as_ref().is_some_and(|e| e.has_r2()),
        // Named service entrypoints render with this generation --
        // unconditionally on (same reasoning as `workflows_cross_worker`).
        service_entrypoint: true,
        // Service `props` and queue-level `deliveryDelaySeconds` render with
        // this generation; the control plane gates the optional wire fields
        // on these bits so an older cell never rejects the whole snapshot.
        service_props: true,
        queue_delivery_delay: true,
        // `workerLoader` bindings render with this generation (the
        // workerd pin carries the fork's `maxWorkers` field) --
        // unconditionally on.
        worker_loader: true,
        // Per-worker quotas: the workerd pin carries the fork's `limits`
        // (cpuMs/memoryMb/subRequests) and this vrt-host generation
        // renders it. Opt-in (`quotas = true` in the controller config)
        // so the ACK key is off the wire until the control plane's
        // strict schema knows it; until then declared quota overrides
        // are refused by the control plane (409, fail closed).
        quotas: config.quotas,
        // `analyticsEngine` bindings render AND vrt-host's AeService
        // ships in this release -- unconditionally on (same reasoning
        // as `worker_loader`).
        analytics_engine: true,
        // `{"type":"unsafeEval"}` bindings (unsafe_eval) render ships
        // with this generation -- unconditionally on.
        unsafe_eval: true,
        // Workers Cache serving (`cache` worker field) renders with
        // this generation -- unconditionally on (same reasoning).
        cache: true,
        // `[observability]` worker fields render with this
        // generation -- unconditionally on (same reasoning).
        observability: true,
        // `streamingTailConsumers` renders with this generation --
        // unconditionally on (same reasoning).
        streaming_tail_consumers: true,
        // `flagship` bindings + `flagshipApps` render with this
        // generation (the pinned workerd ships
        // `cloudflare-internal:flagship`) -- unconditionally on.
        flagship: true,
        // `k2` bindings render with this generation (the pinned
        // workerd ships `cloudflare-internal:k2` and the sysmanifest
        // requires `facade-k2`) -- unconditionally on.
        k2: true,
        email: true,
        // `images` bindings render with this generation (the pinned
        // workerd ships `cloudflare-internal:images-api` and the
        // sysmanifest requires `facade-images`) -- unconditionally on.
        images: true,
        // `secretsStore` bindings render with this generation (the
        // bundled `/workerd/secrets-store.js` worker ships in every
        // pinned binary — upstream, no fork shim) -- unconditionally on.
        secrets_store: true,
        version_metadata: true,
        // `vectorize` bindings render with this generation (the pinned
        // workerd ships `cloudflare-internal:vectorize-api` and the
        // sysmanifest requires `facade-vectorize`) -- unconditionally on.
        vectorize: true,
        // `browser` bindings render with this generation (the
        // sysmanifest requires `facade-browser` and the kitesurf engine
        // payload ships in every pinned image) -- unconditionally on.
        browser: true,
    };
    let mut controller = cell_controller::prod::build_controller(
        &config,
        source,
        channel,
        ack,
        // Unverified: production NetworkPolicy applier (k8s client) -- not
        // wired. With `egress_required` (default true) this fails
        // closed: PARTIAL `egress-policy-unavailable` acks and
        // /healthz degradation until a real applier lands.
        None,
        capabilities,
    )
    .unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });

    let shared = controller.health();
    // T28/H2: cell identity as a const label on every emitted
    // series — process config is the single source of truth.
    cell_controller::metrics::set_cell_label(&config.cell_id);
    // B12: internal metrics listener (vmagent-scraped `metrics` port).
    if let Some(metrics_addr) = &config.metrics_listen {
        let l = tokio::net::TcpListener::bind(metrics_addr).await?;
        tokio::spawn(async move {
            let _ = axum::serve(l, cell_controller::metrics::router()).await;
        });
        tracing::info!(%metrics_addr, "metrics listener bound");
    }
    let listener = tokio::net::TcpListener::bind(&config.health_listen).await?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, health::router(shared)).await;
    });

    // B9: lease chosen by cron_deps — `[cron_lease] type="kubernetes"`
    // → KubernetesLeaseStore (multi-replica legal); otherwise the
    // memory lease ≡ always-leader for single-replica deployments.
    let mut cron = CronRunner::new(
        cron_lease,
        config.cron_holder.clone(),
        cron_lease_ttl,
        cron_log,
        cron_invoker,
    );

    // B7: render-inputs puller — same token/base as desired. Failure
    // never tears down what the host already has (stale inputs keep
    // old generations serving).
    let render_inputs = config.render_inputs_dir.as_ref().map(|dir| {
        let token = std::fs::read_to_string(&config.token_file)
            .expect("token_file unreadable")
            .trim()
            .to_string();
        (
            cell_controller::render_inputs::HttpRenderInputsSource::new(
                &config.control_plane_url,
                &config.cell_id,
                token,
                std::time::Duration::from_secs(5),
                cell_controller::retry::RetryPlan::default(),
            )
            .expect("render-inputs source"),
            dir.clone(),
        )
    });
    // T35: D1 ops executor — own task + own poll cadence (2s): op
    // latency is user-visible (CLI/API waits on the result) while the
    // reconcile loop stays slow. Cursor lives in memory; un-acked ops
    // re-deliver (dedup on seq makes re-execution safe).
    let d1cfg_cell = config.d1sql.clone();
    // W4: persisted execution journal for the legacy channel — same
    // persist-before-ack rule as the generic channel.
    let d1_order_shared = cell_controller::d1ops::D1OrderGuard::default();
    let d1_dedup_path = config
        .fence_file
        .as_ref()
        .and_then(|f| std::path::Path::new(f).parent().map(|d| d.to_path_buf()))
        .or_else(|| config.cron_state_dir.clone())
        .map(|d| d.join("d1ops-executed.json"));
    if let Some(d1cfg) = config.d1sql.clone() {
        let d1_order = d1_order_shared.clone();
        let cp = config.control_plane_url.clone();
        let cell = config.cell_id.clone();
        let token = std::fs::read_to_string(&config.token_file)
            .expect("token_file unreadable")
            .trim()
            .to_string();
        tokio::spawn(async move {
            let timeout = std::time::Duration::from_secs(d1cfg.timeout_secs.max(1));
            let exec = match cell_controller::d1ops::D1OpsExecutor::new(&d1cfg) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!(%e, "d1ops executor init failed — ops channel disabled");
                    return;
                }
            };
            let src = match cell_controller::d1ops::HttpD1OpsSource::new(
                &cp,
                &cell,
                token.clone(),
                timeout,
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(%e, "d1ops source init failed");
                    return;
                }
            };
            let sink =
                match cell_controller::d1ops::HttpD1OpsAckSink::new(&cp, &cell, token, timeout) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(%e, "d1ops ack sink init failed");
                        return;
                    }
                };
            // T35b: pull returns ALL pending ops (seq is SERIAL —
            // allocation order ≠ commit order, so a seq cursor would
            // permanently lose a late-committing op). Dedup by op id.
            let mut seen: cell_controller::d1ops::SeenSet = Default::default();
            let mut executed = match &d1_dedup_path {
                Some(p) => cell_controller::execset::ExecutedSet::load(p.clone()),
                None => {
                    tracing::warn!(
                        "no cell state dir — d1ops dedup is in-memory only (at-least-once)"
                    );
                    cell_controller::execset::ExecutedSet::memory()
                }
            };
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                tick.tick().await;
                match cell_controller::d1ops::ops_tick(
                    &src,
                    &sink,
                    &exec,
                    &mut seen,
                    &mut executed,
                    &d1_order,
                    64,
                )
                .await
                {
                    Ok(n) => {
                        if n > 0 {
                            tracing::info!(n, "d1 ops processed");
                        }
                    }
                    Err(e) => tracing::warn!(%e, "d1 ops tick failed"),
                }
            }
        });
        tracing::info!("d1 ops executor enabled");
    }
    // D1 stage 3: opt-in automatic failover. One probe loop per configured
    // database; the follower CAS stays the single-writer authority and the
    // control plane republishes the signed desired after the report.
    if let Some(d1cfg) = config.d1sql.as_ref() {
        if !d1cfg.auto_failover.is_empty() {
            let token = std::fs::read_to_string(&config.token_file)
                .expect("token_file unreadable")
                .trim()
                .to_string();
            let secret = match vrt_securefs::load_service_secret(
                std::path::Path::new(&d1cfg.auth_secret_file),
                vrt_securefs::SecretPurpose::ServiceHmac,
            ) {
                Ok(s) => s.into_bytes(),
                Err(e) => {
                    tracing::error!(%e, "d1 auto-failover auth secret unusable — disabled");
                    return Err(e.into());
                }
            };
            for cfg in d1cfg.auto_failover.clone() {
                let deps = match cell_controller::d1_auto::HttpAutoDeps::new(
                    &config.control_plane_url,
                    &config.cell_id,
                    token.clone(),
                    secret.clone(),
                ) {
                    Ok(d) => std::sync::Arc::new(d),
                    Err(e) => {
                        tracing::error!(%e, "d1 auto-failover disabled — init failed");
                        continue;
                    }
                };
                let probe = cfg.probe_interval();
                let db = cfg.database_id.clone();
                let db_for_task = db.clone();
                let probe_secs = cfg.probe_interval_secs;
                tokio::spawn(async move {
                    let mut auto = cell_controller::d1_auto::AutoFailover::new(cfg.clone());
                    let mut tick = tokio::time::interval(probe);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        match auto
                            .tick(&*deps, std::time::Instant::now())
                            .await
                        {
                            cell_controller::d1_auto::AutoOutcome::Healthy => {}
                            outcome => tracing::warn!(
                                db = %db_for_task,
                                ?outcome,
                                "d1 auto-failover tick"
                            ),
                        }
                    }
                });
                tracing::warn!(
                    db = %db,
                    probe_secs,
                    "d1 automatic failover enabled"
                );
            }
        }
    }
    // W3/W4: generic cell-ops channel — workflow instance ops AND
    // (W4) D1 ops. Enabled whenever the cell can reach a pod (same
    // `[[pods]]` bases) OR has a `[d1sql]` data plane; each kind is
    // routed inside `wfops_tick` to whichever executor is wired.
    // Disabled cells simply never pull `/vrt/cell-ops` — ops expire
    // server-side (fail closed).
    if !config.pods.is_empty() || config.d1sql.is_some() || storage_purge_exec.is_some() {
        let cp = config.control_plane_url.clone();
        let cell = config.cell_id.clone();
        let token = std::fs::read_to_string(&config.token_file)
            .expect("token_file unreadable")
            .trim()
            .to_string();
        let pods: Vec<(String, String)> = config
            .pods
            .iter()
            .map(|p| (p.id.clone(), p.readyz.clone()))
            .collect();
        // W3-r2 P0-3: persisted executed-op dedup lives in the cell
        // state dir (fence_file's parent, else cron_state_dir). Absent
        // both ⇒ in-memory only — the documented at-least-once
        // fallback for unconfigured cells.
        let d1_order = d1_order_shared.clone();
        let dedup_path = config
            .fence_file
            .as_ref()
            .and_then(|f| std::path::Path::new(f).parent().map(|d| d.to_path_buf()))
            .or_else(|| config.cron_state_dir.clone())
            .map(|d| d.join("wfops-executed.json"));
        let ctl = controller.dispatch_handle();
        tokio::spawn(async move {
            let mut executed = match &dedup_path {
                Some(p) => cell_controller::wfops::ExecutedSet::load(p.clone()),
                None => {
                    tracing::warn!(
                        "no cell state dir — wfops dedup is in-memory only (at-least-once)"
                    );
                    cell_controller::wfops::ExecutedSet::memory()
                }
            };
            let timeout = std::time::Duration::from_secs(15);
            let wf_exec = if pods.is_empty() {
                None
            } else {
                match cell_controller::wfops::WfOpsExecutor::new(&pods, timeout) {
                    Ok(e) => Some(e),
                    Err(e) => {
                        tracing::error!(%e, "wfops executor init failed — wf kinds disabled");
                        None
                    }
                }
            };
            // W4: D1 executor for the generic channel — same `[d1sql]`
            // config as the legacy task, independent wiring so one
            // failing executor never disables the other's kinds.
            let d1_exec = match &d1cfg_cell {
                Some(c) => match cell_controller::d1ops::D1OpsExecutor::new(c) {
                    Ok(e) => Some(e),
                    Err(e) => {
                        tracing::error!(%e, "d1 executor init failed — d1 kinds disabled");
                        None
                    }
                },
                None => None,
            };
            // KV1: the data-ops executor shares the STORAGE_PURGE KV
            // backend — `kv` capability is reported only when a backend
            // actually connected (has_kv), so a missing executor here
            // can only mean "no KV backend", which fails closed.
            let kv_exec = storage_purge_exec
                .as_ref()
                .and_then(|s| s.kv_backend())
                .map(|b| {
                    cell_controller::kvops::KvOpsExecutor::new(
                        b as std::sync::Arc<dyn vrt_provider::kv::KvDataBackend>,
                    )
                });
            let r2_exec = storage_purge_exec
                .as_ref()
                .and_then(|s| s.r2_backend())
                .map(|b| {
                    cell_controller::r2ops::R2OpsExecutor::new(
                        b as std::sync::Arc<dyn vrt_provider::r2::R2DataBackend>,
                    )
                });
            if wf_exec.is_none()
                && d1_exec.is_none()
                && storage_purge_exec.is_none()
                && kv_exec.is_none()
                && r2_exec.is_none()
            {
                tracing::error!("no executor wired — cell-ops channel disabled");
                return;
            }
            let src = match cell_controller::wfops::HttpCellOpsSource::new(
                &cp,
                &cell,
                token.clone(),
                timeout,
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(%e, "wfops source init failed");
                    return;
                }
            };
            let sink =
                match cell_controller::wfops::HttpCellOpsAckSink::new(&cp, &cell, token, timeout) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(%e, "wfops ack sink init failed");
                        return;
                    }
                };
            let mut seen: cell_controller::d1ops::SeenSet = Default::default();
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                tick.tick().await;
                // Signed-desired snapshot per tick — the authority for
                // worker→tenant resolution + workflow declaration.
                let (desired, _, _) = ctl.snapshot();
                match cell_controller::wfops::wfops_tick(
                    &src,
                    &sink,
                    wf_exec.as_ref(),
                    d1_exec.as_ref(),
                    storage_purge_exec.as_deref(),
                    cache_purge_exec.as_deref(),
                    kv_exec.as_ref(),
                    r2_exec.as_ref(),
                    &mut seen,
                    &mut executed,
                    desired.as_ref(),
                    &d1_order,
                    64,
                )
                .await
                {
                    Ok(n) => {
                        if n > 0 {
                            tracing::info!(n, "wf ops processed");
                        }
                    }
                    Err(e) => tracing::warn!(%e, "wf ops tick failed"),
                }
            }
        });
        tracing::info!("wf ops executor enabled");
    }
    // Worker logs: drain every pod's bounded log ring and push to the
    // control plane. Needs `[[pods]]` (loopback bases); absent pods =>
    // collection stays in the pods (bounded rings) and is never read.
    if !config.pods.is_empty() {
        let cp = config.control_plane_url.clone();
        let cell = config.cell_id.clone();
        let token = std::fs::read_to_string(&config.token_file)
            .expect("token_file unreadable")
            .trim()
            .to_string();
        let pods: Vec<(String, String)> = config
            .pods
            .iter()
            .map(|p| (p.id.clone(), p.readyz.clone()))
            .collect();
        let timeout = std::time::Duration::from_secs(10);
        let tick_every = std::time::Duration::from_secs(2);
        match (
            cell_controller::logs::HttpLogSource::new(&pods, timeout),
            cell_controller::logs::HttpLogSink::new(&cp, &cell, token, timeout),
        ) {
            (Ok(src), Ok(sink)) => {
                let ids = src.pod_ids();
                tokio::spawn(async move {
                    let mut shipper = cell_controller::logs::LogShipper::new(src, sink, ids, tick_every);
                    let mut tick = tokio::time::interval(tick_every);
                    loop {
                        tick.tick().await;
                        let r = shipper.tick().await;
                        if r.shipped > 0 {
                            tracing::debug!(shipped = r.shipped, "worker logs shipped");
                        }
                    }
                });
                tracing::info!("worker log shipper enabled");
            }
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!(%e, "worker log shipper init failed — logs stay in pod rings");
            }
        }
    }
    // Usage meter: scrape each pod's trusted counters and push per-window
    // deltas to the control plane. Same scoped token + `[[pods]]` bases as
    // the log shipper; commit-on-push keeps retries idempotent.
    if !config.pods.is_empty() {
        let cp = config.control_plane_url.clone();
        let cell = config.cell_id.clone();
        let token = std::fs::read_to_string(&config.token_file)
            .expect("token_file unreadable")
            .trim()
            .to_string();
        let pods: Vec<(String, String)> = config
            .pods
            .iter()
            .map(|p| (p.id.clone(), p.readyz.clone()))
            .collect();
        let timeout = std::time::Duration::from_secs(10);
        let tick_every = std::time::Duration::from_secs(10);
        match (
            cell_controller::meter::HttpMeterSource::new(&pods, timeout),
            cell_controller::meter::HttpMeterSink::new(&cp, &cell, token, timeout),
        ) {
            (Ok(src), Ok(sink)) => {
                let ids = src.pod_ids();
                let boot_id = format!("{}-{}", cell, std::process::id());
                tokio::spawn(async move {
                    let mut shipper =
                        cell_controller::meter::MeterShipper::new(src, sink, ids, boot_id);
                    let mut tick = tokio::time::interval(tick_every);
                    loop {
                        tick.tick().await;
                        let end = chrono::Utc::now().timestamp_millis();
                        let start = end - tick_every.as_millis() as i64;
                        let r = shipper.tick(start, end).await;
                        if r.pushed {
                            tracing::debug!(entries = r.entries, scraped = r.scraped, "usage meter batch pushed");
                        } else if r.scrape_failed > 0 {
                            tracing::debug!(failed = r.scrape_failed, "usage meter scrape failed");
                        }
                    }
                });
                tracing::info!("usage meter shipper enabled");
            }
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!(%e, "usage meter shipper init failed — metering disabled");
            }
        }
    }
    // Periodic cell report: pod metrics + D1/queue stats + Workflow/DO
    // instance listings. Enabled by default when there is anything to
    // report on; `[report] enabled = false` opts out.
    let report_cfg = config.report.clone().unwrap_or_default();
    if report_cfg.enabled && (!config.pods.is_empty() || config.d1sql.is_some()) {
        let token = std::fs::read_to_string(&config.token_file)
            .expect("token_file unreadable")
            .trim()
            .to_string();
        let pods: Vec<(String, String)> = config
            .pods
            .iter()
            .map(|p| (p.id.clone(), p.readyz.clone()))
            .collect();
        let stats = config.d1sql.as_ref().and_then(|cfg| {
            match cell_controller::d1ops::D1OpsExecutor::new(cfg) {
                Ok(e) => Some(e),
                Err(e) => {
                    tracing::warn!(%e, "report stats disabled — d1sql executor init failed");
                    None
                }
            }
        });
        // DO alarm index: persisted in the cell state dir so it survives
        // controller restarts. Without a state dir it is memory-only and
        // every class is re-observed on the next report tick.
        let alarm_path = config
            .fence_file
            .as_ref()
            .and_then(|f| std::path::Path::new(f).parent().map(|d| d.to_path_buf()))
            .or_else(|| config.cron_state_dir.clone())
            .map(|d| cell_controller::alarm_index::index_path(&d));
        if alarm_path.is_none() {
            tracing::warn!(
                "no cell state dir — DO alarm index is in-memory only (rebuilt each tick)"
            );
        }
        let alarm_index =
            std::sync::Arc::new(cell_controller::alarm_index::AlarmIndex::open(alarm_path));
        match cell_controller::report::HttpReportIo::new(
            &pods,
            &config.control_plane_url,
            &config.cell_id,
            token,
            stats,
            std::time::Duration::from_secs(report_cfg.timeout_secs),
        ) {
            Ok(io) => {
                let ctl = controller.dispatch_handle();
                let cell = config.cell_id.clone();
                let every = std::time::Duration::from_secs(report_cfg.interval_secs);
                let report_index = alarm_index.clone();
                tokio::spawn(async move {
                    let mut reporter = cell_controller::report::Reporter::new(io, cell)
                        .with_alarm_index(report_index);
                    let mut tick = tokio::time::interval(every);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let (desired, _, assignments) = ctl.snapshot();
                        let Some(desired) = desired else { continue };
                        let now = Utc::now().timestamp_millis();
                        match reporter.tick(&desired, &assignments, now).await {
                            Ok(out) if out.errors.is_empty() => tracing::debug!(
                                pods = out.pods,
                                d1 = out.d1,
                                queues = out.queues,
                                workflows = out.workflows,
                                do_classes = out.do_classes,
                                "cell report pushed"
                            ),
                            Ok(out) => tracing::warn!(
                                pods = out.pods,
                                errors = ?out.errors,
                                "cell report pushed with collection errors"
                            ),
                            Err(e) => tracing::warn!(%e, "cell report push failed"),
                        }
                    }
                });
                // Stage 2: consume the alarm index and wake due tenants. The
                // host owns workerd's alarm scheduler; this task only starts
                // the generation, then the next class scan clears the entry.
                let wake_pods = pods.clone();
                let wake_index = alarm_index.clone();
                let ctl_wake = controller.dispatch_handle();
                tokio::spawn(async move {
                    let transport = match cell_controller::alarm_wake::HttpAlarmWake::new(
                        &wake_pods,
                        std::time::Duration::from_secs(15),
                    ) {
                        Ok(t) => t,
                        Err(e) => {
                            tracing::error!(%e, "do alarm wake transport init failed");
                            return;
                        }
                    };
                    let waker = cell_controller::alarm_wake::AlarmWaker::new(wake_index);
                    let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let (_, _, assignments) = ctl_wake.snapshot();
                        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
                        let out = waker
                            .tick(&assignments, &transport, std::time::Instant::now(), now_ms)
                            .await;
                        if out.sent > 0 || out.failed > 0 || out.unassigned > 0 {
                            tracing::info!(
                                due = out.due,
                                sent = out.sent,
                                ok = out.ok,
                                failed = out.failed,
                                unassigned = out.unassigned,
                                "do alarm wake tick"
                            );
                        }
                    }
                });
                tracing::info!(
                    interval_secs = report_cfg.interval_secs,
                    "cell report enabled"
                );
            }
            Err(e) => tracing::error!(%e, "cell report disabled — init failed"),
        }
    }
    // T37b: queue consumer dispatcher — same data-plane authority as
    // d1ops (`[d1sql]` endpoint+secret); absent config ⇒ disabled.
    // Tick rides the main loop cadence (1s sub-tick below).
    if let Some(dispatcher) =
        cell_controller::prod::queue_dispatch_deps(&config).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        })
    {
        tracing::info!("queue dispatcher enabled");
        // Independent 1s task: the semantics of consumer maxBatchTimeout >=1s require
        // a claim cadence finer than the reconcile period (T37b section 8).
        let ctl = controller.dispatch_handle();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let (desired, expires_at, assignments) = ctl.snapshot();
                let out = dispatcher
                    .tick(desired.as_ref(), expires_at, &assignments, Utc::now())
                    .await;
                if out.lease_lost > 0 || out.claim_errors > 0 {
                    tracing::debug!(
                        lease_lost = out.lease_lost,
                        claim_errors = out.claim_errors,
                        "queue dispatch tick"
                    );
                }
            }
        });
    }
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(
        config.poll_interval_secs.max(1),
    ));
    loop {
        interval.tick().await;
        let now = Utc::now();
        match controller.reconcile_once(now).await {
            ReconcileOutcome::Applied { revision, .. } => {
                tracing::info!(revision, "desired applied")
            }
            ReconcileOutcome::Partial {
                revision,
                ref failed_pods,
                ..
            } => {
                tracing::warn!(revision, ?failed_pods, "desired partially applied")
            }
            ReconcileOutcome::Rejected { reason, .. } => {
                tracing::warn!(reason = reason.code(), "desired rejected")
            }
            ReconcileOutcome::PullFailed(ref e) => {
                cell_controller::metrics::poll_error("desired");
                tracing::warn!(%e, "desired pull failed")
            }
            ReconcileOutcome::NoPublication => tracing::debug!("no publication yet"),
            ReconcileOutcome::EgressUnavailable { revision } => {
                tracing::warn!(revision, "egress policy unconfirmed — tenants held back")
            }
            ReconcileOutcome::AlreadyApplied { .. } => {}
        }
        if let Some((source, dir)) = &render_inputs {
            match source.fetch().await {
                Ok(Some(fetched)) => {
                    // B1: commit ONLY after the write lands — a failed
                    // write must re-fetch next tick, never skip forever.
                    match cell_controller::render_inputs::write_render_inputs(dir, &fetched.payload)
                    {
                        Ok(()) => source.commit(&fetched),
                        Err(e) => tracing::warn!(
                            error = %e,
                            "render-inputs write failed — previous file kept, will retry"
                        ),
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    cell_controller::metrics::poll_error("render_inputs");
                    tracing::warn!(error = %e, "render-inputs pull failed — previous file kept")
                }
            }
        }
        controller.probe_tick(now).await;
        let cron_outcome = controller.cron_tick(&mut cron, now).await;
        if cron_outcome.not_leader {
            tracing::debug!("cron: not leader");
        }
    }
}
