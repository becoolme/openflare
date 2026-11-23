//! `vrt-provider` — per-tenant binding-backend provider daemon.
//!
//! Loads a signed `compute.desired` snapshot + trusted keys, binds one
//! unix listener per tenant in the snapshot, and serves the KV/R2
//! binding-backend protocol subset against the configured backends.
//! All credentials come from environment variables — never argv/logs.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use vrt_provider::config::ProviderConfig;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::desired_file::FileDesiredStore;
use vrt_provider::kv::KvrocksKvProvider;
use vrt_provider::r2::s3::{S3Config, SecretString};
use vrt_provider::r2::S3R2Provider;
use vrt_provider::server::{self, TenantService};
use vrt_provider::unixsock;
use vrt_provider::Authorizer;

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("vrt-provider: failed to build runtime");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run())
}

async fn run() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vrt_provider=info".into()),
        )
        .init();

    // T25/L7: `--config <path>` (the compose command form) OR a
    // positional path, with `VRT_PROVIDER_CONFIG` taking precedence.
    // Unknown flags fail closed — a mis-parsed path used to mean the
    // daemon silently never started.
    let config_path = std::env::var_os("VRT_PROVIDER_CONFIG")
        .map(PathBuf::from)
        .or_else(|| {
            match vrt_provider::cli::resolve_config_path(
                &std::env::args().skip(1).collect::<Vec<_>>(),
            ) {
                Ok(path) => path,
                Err(error) => {
                    tracing::error!(%error, "invalid invocation");
                    std::process::exit(1);
                }
            }
        });
    let Some(config_path) = config_path else {
        tracing::error!("missing config path (--config <path>, argv[1], or VRT_PROVIDER_CONFIG)");
        return ExitCode::FAILURE;
    };
    tracing::info!(path = %config_path.display(), "loading provider config");
    let raw = match std::fs::read(&config_path) {
        Ok(raw) => raw,
        Err(error) => {
            tracing::error!(%error, "failed to read config file");
            return ExitCode::FAILURE;
        }
    };
    let config: ProviderConfig =
        match serde_json::from_slice::<ProviderConfig>(&raw).or_else(|_| {
            toml_from_slice(&raw).map_err(|_| {
                serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "config is neither JSON nor TOML",
                ))
            })
        }) {
            Ok(config) => config,
            Err(_) => {
                tracing::error!("failed to parse config (JSON or TOML expected)");
                return ExitCode::FAILURE;
            }
        };
    if let Err(error) = config.validate() {
        tracing::error!(%error, "invalid provider config");
        return ExitCode::FAILURE;
    }

    // T25/L8: `cursorSecretFile` (of init's keys/cursor.secret, 0600)
    // is the only source when configured; otherwise fall back to the
    // legacy env var (platform mode). Never both, never plaintext.
    let cursor_secret = match &config.cursor_secret_file {
        Some(path) => match vrt_provider::cli::load_cursor_secret(path) {
            Ok(secret) => secret,
            Err(error) => {
                tracing::error!(%error, path = %path.display(),
                    "cursor secret file unusable (missing, not 0600, or empty)");
                return ExitCode::FAILURE;
            }
        },
        None => match std::env::var(&config.cursor_secret_env) {
            Ok(secret) => match vrt_provider::cli::parse_cursor_secret_env(&secret) {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::error!(%error, "cursor secret env var invalid");
                    return ExitCode::FAILURE;
                }
            },
            Err(_) => {
                tracing::error!(
                    "cursor secret unavailable (cursorSecretFile unset and env var missing)"
                );
                return ExitCode::FAILURE;
            }
        },
    };
    let signer = Arc::new(CursorSigner::new(&cursor_secret));
    drop(cursor_secret);

    let desired = Arc::new(FileDesiredStore::new(
        &config.desired.path,
        &config.cell_id,
        config.desired.trusted_keys.clone(),
        config.desired.grace_versions,
    ));
    // The cell-controller (sibling container) writes the envelope only after
    // it pulled + verified it, so a missing file at start is a normal race:
    // wait in-process (bounded), NOT ready. A present-but-untrusted envelope
    // (bad signature / wrong cell / expired / malformed) still exits at once.
    if let Err(error) = vrt_provider::initial_snapshot::wait_for_initial_snapshot(
        || desired.reload(chrono::Utc::now()),
        vrt_provider::initial_snapshot::INITIAL_SNAPSHOT_WAIT,
        vrt_provider::initial_snapshot::INITIAL_SNAPSHOT_POLL_MIN,
        vrt_provider::initial_snapshot::INITIAL_SNAPSHOT_POLL_MAX,
    )
    .await
    {
        tracing::error!(%error, "initial desired snapshot unavailable or failed verification");
        return ExitCode::FAILURE;
    }
    let revision = desired.revision().unwrap_or(0);
    tracing::info!(revision, "desired snapshot loaded");

    let tenants = desired.tenant_ids().unwrap_or_default();
    // An EMPTY verified snapshot is a legal state (fresh cell, or every
    // worker deleted — the control plane signs a zero-tenant
    // `compute.desired`). Exiting here made the pod crash-loop until the
    // first deploy. Standby instead: no listeners are bound, healthz /
    // readyz stay green (the process is alive and the snapshot VERIFIED),
    // and the reconcile loop below binds listeners as soon as a later
    // verified snapshot names tenants. Untrusted snapshots never reach
    // this point — `wait_for_initial_snapshot` already failed closed.
    if tenants.is_empty() {
        tracing::info!(
            revision,
            "verified snapshot has no tenants; standing by for the next snapshot"
        );
    }

    let kv_backend = match build_kv(&config).await {
        Ok(backend) => backend,
        Err(code) => return code,
    };
    let (r2_backend, fs_r2) = match build_r2(&config) {
        Ok(backend) => backend,
        Err(code) => return code,
    };
    let d1_client = match &config.vrt_sql {
        Some(sql) => {
            let secret = match vrt_provider::cli::load_cursor_secret(&sql.auth_secret_file) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, path = %sql.auth_secret_file.display(),
                        "vrt-sql auth secret file unusable");
                    return ExitCode::FAILURE;
                }
            };
            match vrt_provider::d1::D1Client::new(
                sql.addr.clone(),
                secret.clone(),
                std::time::Duration::from_millis(sql.timeout_ms),
            )
            .map(|d1| d1.with_replicas(sql.replicas.clone()))
            .map_err(|_| ())
            .and_then(|d1| {
                // T37: queue and d1 share the same vrt-sql downstream/secret.
                vrt_provider::queue::QueueClient::new(
                    sql.addr.clone(),
                    secret.clone(),
                    std::time::Duration::from_millis(sql.timeout_ms),
                )
                .map_err(|_| ())
                .and_then(|q| {
                    // K2 shares the same downstream/secret as well.
                    vrt_provider::k2::K2Client::new(
                        sql.addr.clone(),
                        secret,
                        std::time::Duration::from_millis(sql.timeout_ms),
                    )
                    .map(|k| (d1, q, k))
                    .map_err(|_| ())
                })
            }) {
                Ok((d1, q, k)) => (Some(Arc::new(d1)), Some(Arc::new(q)), Some(Arc::new(k))),
                Err(_) => {
                    tracing::error!("vrtSql.addr invalid");
                    return ExitCode::FAILURE;
                }
            }
        }
        None => (None, None, None),
    };
    let (d1_client, queue_client, k2_client) = d1_client;
    // send_email upstream adapter — `[email]` config, secrets resolve
    // from the named env vars. Absent config → route 503s honestly.
    let email_backend: Option<Arc<dyn vrt_provider::email::EmailBackend>> = match &config.email {
        Some(cfg) => match vrt_provider::email::adapter_from_config(cfg) {
            Ok(b) => Some(b),
            Err(e) => {
                tracing::error!(error = %e, "email backend init failed");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    // P1: PG backend is a pure credential-file reader — constructing it
    // never touches the network (lazy egress per request).
    let pg_backend = config.pg.as_ref().map(|pg| {
        let creds = std::sync::Arc::new(vrt_provider::pg::PgCredentialStore::new(
            pg.credentials_file.clone(),
        ));
        std::sync::Arc::new(vrt_provider::pg::PgBackend::new(creds))
    });
    // MIG-15: pg-wire proxy shares the credential store — the `tcp`
    // payload per credentialRef is its upstream identity source.
    let pg_proxy = match (&config.pg_proxy, &pg_backend) {
        (Some(px), Some(_)) => {
            if !px.listen_is_loopback() {
                tracing::error!("pgProxy.listen must be a loopback socket address");
                return ExitCode::FAILURE;
            }
            Some(px.clone())
        }
        (Some(_), None) => {
            // Proxy configured without the credential file = a silently
            // dead listener — fail closed at startup instead.
            tracing::error!("pgProxy requires the pg credentials file to be configured");
            return ExitCode::FAILURE;
        }
        _ => None,
    };
    let assets = match &config.assets {
        Some(fs) => match vrt_provider::assets::AssetStore::open(&fs.dir) {
            Ok(store) => Some(store),
            Err(e) => {
                tracing::error!(error = %e, "assets store init failed");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let vectorize = match &config.vectorize {
        Some(fs) => match vrt_provider::vectorize::VectorizeStore::new(fs.dir.clone()) {
            Ok(store) => Some(std::sync::Arc::new(store)),
            Err(e) => {
                tracing::error!(error = %e, "vectorize store init failed");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let authorizer = Arc::new(Authorizer::new(desired.clone(), config.operation_timeout()));
    // RV-R8 #2: `tenant_concurrency` is PER TENANT — each listener's
    // TenantService gets its own semaphore (the Arc dies with the
    // listener, so the count is bounded by live tenants); only the
    // global cap is shared.
    let tenant_concurrency = config.tenant_concurrency;
    let tenant_qps = config.tenant_qps;
    let tenant_bytes_per_sec = config.tenant_bytes_per_sec;
    let global_permits = Arc::new(tokio::sync::Semaphore::new(config.global_concurrency));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // MIG-15: pg-wire proxy listener + revocation sweeper — loopback
    // only (validated above); sessions are killed within one sweep of
    // their binding leaving desired state.
    if let Some(px) = pg_proxy {
        match tokio::net::TcpListener::bind(&px.listen).await {
            Ok(l) => {
                let creds = std::sync::Arc::new(vrt_provider::pg::PgCredentialStore::new(
                    config.pg.as_ref().unwrap().credentials_file.clone(),
                ));
                let proxy = vrt_provider::pgproxy::PgProxy::new(
                    px,
                    config.hyperdrive.map(|hd| hd.pool),
                    creds,
                    desired.clone(),
                );
                tokio::spawn({
                    let proxy = proxy.clone();
                    async move { proxy.serve(l).await }
                });
                tokio::spawn({
                    let proxy = proxy.clone();
                    let shutdown_rx = shutdown_rx.clone();
                    async move { proxy.sweep(shutdown_rx).await }
                });
                if let Some(pool) = proxy.pool() {
                    tokio::spawn({
                        let pool = pool.clone();
                        let shutdown_rx = shutdown_rx.clone();
                        async move { pool.sweep(shutdown_rx).await }
                    });
                    tracing::info!(
                        max_connections = pool.config().max_connections,
                        "pg-proxy session pool enabled"
                    );
                }
                tracing::info!("pg-proxy listener bound");
            }
            Err(e) => {
                tracing::error!(error = %e, "pg-proxy listen failed");
                return ExitCode::FAILURE;
            }
        }
    }

    // B12: internal metrics listener (never on the public facade).
    if let Some(metrics_addr) = &config.metrics_listen {
        match metrics_addr.parse::<std::net::SocketAddr>() {
            Ok(addr) => match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => {
                    let router = vrt_provider::metrics::router()
                        .route("/readyz/kv", axum::routing::get(kv_readyz));
                    tokio::spawn(async move {
                        let _ = axum::serve(l, router).await;
                    });
                    tracing::info!(%metrics_addr, "metrics listener bound");
                }
                Err(e) => {
                    tracing::error!(error = %e, %metrics_addr, "metrics listen failed");
                    return ExitCode::FAILURE;
                }
            },
            Err(_) => {
                tracing::error!("metricsListen is not a socket addr");
                return ExitCode::FAILURE;
            }
        }
    }
    // W5: bounded background staging GC for the fs R2 backend — sweeps
    // `.multipart` dirs whose TTL expired, refreshes the per-tenant
    // staging gauge, and counts reclaimed uploads/bytes.
    if let Some(fs_provider) = fs_r2 {
        let interval = std::time::Duration::from_millis(
            config
                .r2
                .as_ref()
                .map(|f| f.staging_gc_interval_ms)
                .unwrap_or(600_000),
        );
        let mut gc_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            // P2: pairs seen last round but gone now (tenant/bucket
            // removed or fully drained) must be zeroed — a set-only
            // gauge leaks stale bytes forever.
            let mut last_pairs: std::collections::HashSet<(String, String)> =
                std::collections::HashSet::new();
            loop {
                // fs scans + dir removals are blocking IO — keep them
                // off the runtime worker (spawn_blocking, same
                // discipline as the provider's own ops).
                let p = fs_provider.clone();
                match tokio::task::spawn_blocking(move || p.sweep_staging()).await {
                    Ok(Ok(report)) => {
                        for (reason, n) in [("expired", report.expired), ("orphan", report.orphans)]
                        {
                            if n > 0 {
                                vrt_provider::metrics::registry().inc(
                                    "vrt_provider_r2_staging_gc_uploads_reclaimed_total",
                                    &[("reason", reason)],
                                    n as f64,
                                );
                            }
                        }
                        if report.bytes_freed > 0 {
                            vrt_provider::metrics::registry().inc(
                                "vrt_provider_r2_staging_gc_bytes_freed_total",
                                &[],
                                report.bytes_freed as f64,
                            );
                        }
                    }
                    Ok(Err(_)) | Err(_) => tracing::warn!("r2 staging gc sweep failed"),
                }
                let p = fs_provider.clone();
                if let Ok(Ok(rows)) = tokio::task::spawn_blocking(move || p.staging_bytes()).await {
                    let mut current = std::collections::HashSet::new();
                    for (tenant, bucket, bytes) in rows {
                        current.insert((tenant.clone(), bucket.clone()));
                        vrt_provider::metrics::registry().set(
                            "vrt_provider_r2_staging_bytes",
                            &[("tenant", &tenant), ("bucket", &bucket)],
                            bytes as f64,
                        );
                    }
                    for (tenant, bucket) in
                        vrt_provider::metrics::vanished_series(&last_pairs, &current)
                    {
                        vrt_provider::metrics::registry().set(
                            "vrt_provider_r2_staging_bytes",
                            &[("tenant", &tenant), ("bucket", &bucket)],
                            0.0,
                        );
                    }
                    last_pairs = current;
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = gc_shutdown.changed() => break,
                }
            }
        });
    }
    // T13/M5: listeners follow the desired tenant SET — reload diffs
    // H5-r2: uid-collision verdict — a SET property of the current
    // desired set, derived through the same `vrt_securefs::colliding_tenants`
    // the host uses. Colliding tenants never reach `spawn_listener`
    // (no listener → no reachable socket); a tenant that becomes
    // colliding later is drained + unlinked by reconcile. `None`
    // isolation ⇒ no uid semantics, nothing collides.
    let iso_range = config.isolation.map(|iso| (iso.base, iso.span));
    let filter_colliding = |wanted: &[String]| -> Vec<String> {
        let (serveable, collided) = vrt_provider::listeners::partition_colliding(iso_range, wanted);
        if !collided.is_empty() {
            vrt_provider::metrics::registry().inc(
                "vrt_provider_isolation_uid_collisions",
                &[],
                collided.len() as f64,
            );
            let (base, span) = iso_range.expect("collided implies isolation");
            let mut by_uid: std::collections::BTreeMap<u32, Vec<&String>> =
                std::collections::BTreeMap::new();
            for t in &collided {
                by_uid
                    .entry(vrt_securefs::tenant_uid(base, span, t))
                    .or_default()
                    .push(t);
            }
            for (uid, group) in &by_uid {
                tracing::error!(
                    uid,
                    tenants = ?group,
                    "isolation uid collision — tenant listeners not bound (order-independent)"
                );
            }
        }
        serveable
    };

    // bind new tenants and drain+unlink removed ones (see listeners.rs).
    let mut listeners = vrt_provider::listeners::ListenerSet::new();
    let listener_dir = config.listener_dir.clone();
    let spawn_listener = {
        let authorizer = authorizer.clone();
        let kv = kv_backend.clone();
        let r2 = r2_backend.clone();
        let d1 = d1_client.clone();
        let queue_client = queue_client.clone();
        let pg = pg_backend.clone();
        let vectorize = vectorize.clone();
        let signer = signer.clone();
        let assets = assets.clone();
        let r2_max_object_bytes = config
            .s3
            .as_ref()
            .map(|s| s.max_object_bytes)
            .or_else(|| config.r2.as_ref().map(|f| f.max_object_bytes))
            .unwrap_or(256 * 1024 * 1024);
        let global_permits = global_permits.clone();
        let body_read_timeout = std::time::Duration::from_millis(config.body_read_timeout_ms);
        let dir = listener_dir.clone();
        // T13/M2: `[isolation]` derives each tenant's expected peer
        // uid via the SAME `vrt_securefs::tenant_uid` the host uses
        // to setuid the tenant's workerd — socket-name identity plus
        // kernel-verified peer uid closes the cross-tenant dial path.
        let isolation = config.isolation;
        move |tenant_id: &str| {
            let expected_uid =
                isolation.map(|iso| vrt_securefs::tenant_uid(iso.base, iso.span, tenant_id));
            let listener =
                unixsock::bind_listener_guarded(&dir, tenant_id, expected_uid).map_err(|e| {
                    // T34e: the reconcile only carries the tenant id — the
                    // io error (kind + message) and the socket path length
                    // live here, at the one place that still has them.
                    let sock = vrt_securefs::socket_path(&dir, tenant_id);
                    tracing::error!(
                        tenant = %tenant_id,
                        socket_path = %sock.display(),
                        socket_len = sock.as_os_str().len(),
                        error = %e,
                        "tenant listener bind failed"
                    );
                    e
                })?;
            let service = Arc::new(TenantService {
                tenant_id: tenant_id.to_string(),
                authorizer: authorizer.clone(),
                kv: kv.clone(),
                r2: r2.clone(),
                assets: assets.clone(),
                signer: signer.clone(),
                r2_max_object_bytes,
                permits: Arc::new(tokio::sync::Semaphore::new(tenant_concurrency)),
                global_permits: global_permits.clone(),
                body_read_timeout,
                d1: d1.clone(),
                queue: queue_client.clone(),
                k2: k2_client.clone(),
                email: email_backend.clone(),
                pg: pg.clone(),
                vectorize: vectorize.clone(),
            });
            // RV-R67: one quota per listener (bounded by live tenants).
            let quota = Arc::new(vrt_provider::quota::TenantQuota::new(
                tenant_qps,
                tenant_bytes_per_sec,
                std::time::Instant::now(),
            ));
            let router = server::tenant_router_with_quota(service, quota);
            let (tx, rx) = watch::channel(false);
            let tenant = tenant_id.to_string();
            let mut rx = rx;
            let task = tokio::spawn(async move {
                let result = unixsock::serve(listener, router, async move {
                    let _ = rx.changed().await;
                })
                .await;
                if let Err(error) = &result {
                    tracing::error!(%error, tenant = %tenant, "tenant listener terminated");
                }
                result
            });
            Ok((tx, task))
        }
    };
    let tenants = filter_colliding(&tenants);
    let (added, _removed, failed) = listeners.reconcile(&tenants, &spawn_listener, |t| {
        vrt_provider::listeners::cleanup_socket(&listener_dir, t)
    });
    for t in &added {
        tracing::info!(tenant = %t, "tenant listener bound");
    }
    if !failed.is_empty() {
        tracing::error!(tenants = ?failed, "refusing to start: tenant listener bind failed");
        return ExitCode::FAILURE;
    }

    // Snapshot reload loop — observes the same shutdown signal so the
    // drain join is never blocked by it.
    let reload_task = vrt_provider::desired_file::spawn_reload_task(
        desired.clone(),
        std::time::Duration::from_millis(config.desired.reload_ms),
        shutdown_rx.clone(),
    );

    // SIGTERM → graceful drain with a hard cap.
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(sig) => sig,
        Err(_) => {
            tracing::error!("failed to install SIGTERM handler");
            return ExitCode::FAILURE;
        }
    };
    // Reload-reconcile loop: diff the desired tenant set every reload
    // tick — new tenants get listeners, removed tenants drain + unlink.
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(
        config.desired.reload_ms.max(1_000),
    ));
    loop {
        tokio::select! {
            _ = sigterm.recv() => break,
            _ = tick.tick() => {
                let wanted = desired.tenant_ids().unwrap_or_default();
                let wanted = filter_colliding(&wanted);
                let (added, removed, failed) = listeners.reconcile(
                    &wanted,
                    &spawn_listener,
                    |t| vrt_provider::listeners::cleanup_socket(&listener_dir, t),
                );
                for t in &added {
                    tracing::info!(tenant = %t, "tenant listener bound (reload)");
                }
                for t in &removed {
                    tracing::info!(tenant = %t, "tenant listener drained (reload)");
                }
                for t in &failed {
                    tracing::error!(tenant = %t, "tenant listener bind failed (reload)");
                }
            }
        }
    }
    tracing::info!("SIGTERM received; draining tenant listeners");
    let _ = shutdown_tx.send(true);

    let drain = config.drain_timeout();
    let drained = tokio::time::timeout(drain, async {
        listeners.shutdown_all().await;
        let _ = reload_task.await;
    })
    .await;
    if drained.is_err() {
        tracing::error!("drain deadline exceeded; forcing shutdown");
        return ExitCode::FAILURE;
    }
    tracing::info!("drained cleanly");
    ExitCode::SUCCESS
}

async fn build_kv(
    config: &ProviderConfig,
) -> Result<Option<Arc<dyn vrt_provider::kv::KvProvider>>, ExitCode> {
    if let Some(fs) = &config.kv {
        let provider = vrt_provider::kv::fs::FsKvProvider::new(fs.dir.clone()).map_err(|_| {
            tracing::error!("kv.dir is not a usable 0700 directory");
            ExitCode::FAILURE
        })?;
        return Ok(Some(Arc::new(provider)));
    }
    let Some(kv) = &config.kvrocks else {
        tracing::warn!("no kv backend configured; KV ops will fail closed");
        return Ok(None);
    };
    let url = match kv
        .url_env
        .as_deref()
        .and_then(|name| std::env::var(name).ok())
        .or_else(|| kv.url.clone())
    {
        Some(url) => url,
        None => {
            tracing::error!("kvrocks url missing (url or urlEnv)");
            return Err(ExitCode::FAILURE);
        }
    };
    // RV-R71: lazy + self-healing. Startup no longer dials (or exits) on
    // a kvrocks outage; a malformed URL is still a config error.
    let mut provider = match KvrocksKvProvider::lazy(
        url.as_str(),
        std::time::Duration::from_millis(kv.connect_timeout_ms),
    ) {
        Ok(p) => p.with_timeout(std::time::Duration::from_millis(kv.timeout_ms)),
        Err(_) => {
            tracing::error!("kvrocks url is malformed");
            return Err(ExitCode::FAILURE);
        }
    };
    if kv.disable_read_retry {
        provider = provider.without_read_retry();
    }
    let provider = Arc::new(provider);
    if provider.probe().await.is_err() {
        tracing::warn!("kvrocks not reachable at startup; KV ops fail closed until it recovers");
    }
    KVROCKS_PROBE.get_or_init(|| provider.clone());
    Ok(Some(provider))
}

/// RV-R71: dependency readiness for KV only. `/readyz` stays a liveness-
/// style 200 so a kvrocks outage never pulls D1/R2/queue traffic out of
/// rotation; operators and alerts watch `/readyz/kv` (200 ok / 503 with
/// a fixed body, never an address or error text; 404 when no kvrocks).
async fn kv_readyz() -> (axum::http::StatusCode, &'static str) {
    use axum::http::StatusCode;
    match KVROCKS_PROBE.get() {
        None => (StatusCode::NOT_FOUND, "kvrocks not configured"),
        Some(p) => match p.probe().await {
            Ok(()) => (StatusCode::OK, "ok"),
            Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "KV_UNAVAILABLE"),
        },
    }
}

/// RV-R71: handle used by the metrics listener's `/readyz/kv` probe.
static KVROCKS_PROBE: std::sync::OnceLock<Arc<KvrocksKvProvider>> = std::sync::OnceLock::new();

fn build_r2(
    config: &ProviderConfig,
) -> Result<
    (
        Option<Arc<dyn vrt_provider::r2::R2Provider>>,
        Option<Arc<vrt_provider::r2::fs::FsR2Provider>>,
    ),
    ExitCode,
> {
    if let Some(fs) = &config.r2 {
        let provider = vrt_provider::r2::fs::FsR2Provider::new(fs.dir.clone(), fs.max_object_bytes)
            .map_err(|_| {
                tracing::error!("r2.dir is not a usable 0700 directory");
                ExitCode::FAILURE
            })?
            .with_upload_ttl_ms(fs.staging_ttl_ms)
            .with_staging_max_bytes_per_tenant(
                fs.staging_max_bytes_per_tenant
                    .unwrap_or_else(|| fs.max_object_bytes.saturating_mul(4)),
            )
            .with_max_active_uploads(fs.max_active_uploads);
        let provider = Arc::new(provider);
        let dyn_provider: Arc<dyn vrt_provider::r2::R2Provider> = provider.clone();
        return Ok((Some(dyn_provider), Some(provider)));
    }
    let Some(s3) = &config.s3 else {
        tracing::warn!("no s3 backend configured; R2 ops will fail closed");
        return Ok((None, None));
    };
    let access_key = match std::env::var(&s3.access_key_env) {
        Ok(value) if !value.is_empty() => value,
        _ => {
            tracing::error!("s3 access key env var missing or empty");
            return Err(ExitCode::FAILURE);
        }
    };
    let secret_key = match std::env::var(&s3.secret_key_env) {
        Ok(value) if !value.is_empty() => value,
        _ => {
            tracing::error!("s3 secret key env var missing or empty");
            return Err(ExitCode::FAILURE);
        }
    };
    let provider = S3R2Provider::new(S3Config {
        endpoint: s3.endpoint.clone(),
        region: s3.region.clone(),
        bucket: s3.bucket.clone(),
        access_key,
        secret_key: SecretString::new(secret_key),
        timeout: std::time::Duration::from_millis(s3.timeout_ms),
        max_object_bytes: s3.max_object_bytes,
        conditional_writes: s3.conditional_writes,
        multipart_ttl_ms: s3.multipart_ttl_ms,
    })
    .map_err(|_| {
        tracing::error!("failed to build s3 client");
        ExitCode::FAILURE
    })?;
    Ok((Some(Arc::new(provider)), None))
}

fn toml_from_slice(raw: &[u8]) -> Result<ProviderConfig, toml::de::Error> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| toml::from_str::<toml::Value>("invalid").unwrap_err())?;
    toml::from_str(text)
}
