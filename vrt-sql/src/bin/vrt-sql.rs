//! `vrt-sql serve` -- D1 data-plane SQL service.
//!
//! Auth: only the provider may call (shared 0600 secret signed token).
//! Listens on a unix socket or TCP, exactly one of them; /healthz needs no auth;
//! SIGTERM -> stop accepting -> drain in-flight transactions -> exit.

use std::path::PathBuf;
use std::process::ExitCode;
use tokio::signal::unix::{signal, SignalKind};
use vrt_securefs::SecretPurpose;
use vrt_sql::service::{load_secret_0600, ServiceAuth, ServiceConfig};
use vrt_sql::wire::AppState;

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(_) => {
            eprintln!("vrt-sql: failed to build runtime");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run())
}

fn usage() -> ExitCode {
    eprintln!("usage: vrt-sql serve --config <path>");
    eprintln!("       vrt-sql follower --config <path>");
    ExitCode::FAILURE
}

async fn run() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vrt_sql=info".into()),
        )
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("follower") {
        let path = match args.as_slice() {
            [_, flag, path] if flag == "--config" => Some(PathBuf::from(path)),
            [_, path] if !path.starts_with('-') => Some(PathBuf::from(path)),
            _ => std::env::var_os("VRT_SQL_FOLLOWER_CONFIG").map(PathBuf::from),
        };
        return match path {
            Some(path) => run_follower(path).await,
            None => usage(),
        };
    }
    let config_path = std::env::var_os("VRT_SQL_CONFIG")
        .map(PathBuf::from)
        .or_else(|| match args.as_slice() {
            [cmd, flag, path] if cmd == "serve" && flag == "--config" => Some(PathBuf::from(path)),
            [flag, path] if flag == "--config" => Some(PathBuf::from(path)),
            [path] if !path.starts_with('-') => Some(PathBuf::from(path)),
            _ => None,
        });
    let Some(config_path) = config_path else {
        return usage();
    };
    let raw = match std::fs::read(&config_path) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "failed to read config");
            return ExitCode::FAILURE;
        }
    };
    let config: ServiceConfig = match serde_json::from_slice(&raw)
        .or_else(|_| toml::from_str::<ServiceConfig>(std::str::from_utf8(&raw).unwrap_or("")))
    {
        Ok(c) => c,
        Err(_) => {
            tracing::error!("config parse failed (JSON or TOML expected)");
            return ExitCode::FAILURE;
        }
    };
    if let Err(field) = config.validate() {
        tracing::error!(field, "invalid config");
        return ExitCode::FAILURE;
    }
    // Secret file validation (0600) -- missing/insecure fails closed immediately;
    // contents never enter logs.
    let bookmark_key = match load_secret_0600(&config.bookmark_secret_file, SecretPurpose::ServiceHmac) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!(error = %e, path = %config.bookmark_secret_file.display(),
                "bookmark secret file unusable");
            return ExitCode::FAILURE;
        }
    };
    let auth_secret = match load_secret_0600(&config.auth_secret_file, SecretPurpose::ServiceHmac) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!(error = %e, path = %config.auth_secret_file.display(),
                "auth secret file unusable");
            return ExitCode::FAILURE;
        }
    };
    let mut key = [0u8; 32];
    key.copy_from_slice(&bookmark_key[..32]);
    drop(bookmark_key);
    let mut state = AppState::new(config.db_root.clone(), key)
        .with_auth(ServiceAuth::new(auth_secret))
        .with_permits(config.global_concurrency)
        .with_body_max(config.body_max_bytes);
    state.txn_budget = std::time::Duration::from_millis(config.operation_timeout_ms);
    // T36: object-store persistence (section 4, fail closed -- config is already blocked in validate;
    // none is test/dev only, with a startup WARN).
    match build_persister(&config) {
        Ok(Some(pair)) => {
            let registry = (*state.registry).clone().with_persisters(pair);
            state.registry = std::sync::Arc::new(registry);
        }
        Ok(None) => {
            tracing::warn!(
                "persistence backend = none: local fsync only, NO replication; test/dev only"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "persistence init failed (fail closed)");
            return ExitCode::FAILURE;
        }
    }
    let state = std::sync::Arc::new(state);

    // RV-R53.3: periodic bounded tombstone GC — every GC_SWEEP_INTERVAL
    // reclaims object-store data for `.dropped` markers past retention.
    // Runs on spawn_blocking (sync store IO), failures isolated per
    // database, and absent persistence the round is a no-op.
    {
        let reg = state.registry.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(vrt_sql::engine::GC_SWEEP_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let r = reg.clone();
                let report = tokio::task::spawn_blocking(move || r.gc_tombstoned_round()).await;
                match report {
                    Ok(rep) if rep.failures > 0 => tracing::warn!(
                        scanned = rep.scanned,
                        reclaimed = rep.reclaimed,
                        failures = rep.failures,
                        "tombstone gc round finished with isolated failures"
                    ),
                    Ok(rep) if rep.reclaimed > 0 => tracing::info!(
                        scanned = rep.scanned,
                        reclaimed = rep.reclaimed,
                        objects_freed = rep.objects_freed,
                        "tombstone gc round reclaimed databases"
                    ),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(%e, "tombstone gc round panicked/join failed"),
                }
            }
        });
    }

    // metrics internal listener.
    if let Some(addr) = &config.metrics_listen {
        let addr: std::net::SocketAddr = addr.parse().expect("validated");
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => {
                let router = vrt_sql::metrics::router();
                tokio::spawn(async move {
                    let _ = axum::serve(l, router).await;
                });
                tracing::info!(%addr, "metrics listener bound");
            }
            Err(e) => {
                tracing::error!(error = %e, %addr, "metrics listen failed");
                return ExitCode::FAILURE;
            }
        }
    }

    let app = vrt_sql::wire::app((*state).clone());
    let drain = std::time::Duration::from_millis(config.drain_timeout_ms);
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            tracing::error!("failed to install SIGTERM handler");
            return ExitCode::FAILURE;
        }
    };

    if let Some(dir) = &config.unix_dir {
        // UDS: 0700 dir + 0600 socket (same convention as vrt-provider unixsock).
        if let Err(e) = vrt_securefs::ensure_secure_dir(dir) {
            tracing::error!(error = %e, "unixDir unusable");
            return ExitCode::FAILURE;
        }
        let path = dir.join("vrt-sql.sock");
        // F1: 0600 from birth — staged bind + rename (dir is 0700, so
        // the old bind-then-chmod window was already sealed).
        let _ = std::fs::remove_file(&path); // stale socket
        let std_listener = match vrt_securefs::bind_unix_private(&path, 0o600) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "unix bind failed");
                return ExitCode::FAILURE;
            }
        };
        std_listener.set_nonblocking(true).ok();
        let listener = match tokio::net::UnixListener::from_std(std_listener) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "unix listener conversion failed");
                return ExitCode::FAILURE;
            }
        };
        tracing::info!(path = %path.display(), "vrt-sql listening (unix)");
        let state2 = state.clone();
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                sigterm.recv().await;
                shutdown_sequence(&state2, drain).await;
            })
            .await;
        return ExitCode::SUCCESS;
    }

    let addr: std::net::SocketAddr = config
        .listen_tcp
        .as_ref()
        .expect("validated")
        .parse()
        .expect("validated");
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, %addr, "tcp bind failed");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(%addr, "vrt-sql listening (tcp)");
    let state2 = state.clone();
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            sigterm.recv().await;
            shutdown_sequence(&state2, drain).await;
        })
        .await;
    ExitCode::SUCCESS
}

/// `vrt-sql follower`: tail the object-store chain into immutable local generations.
///
/// The follower never takes the writer lease and never writes to the object store. It publishes
/// `{dbRoot}/{databaseId}/follower-state.json` as its watermark; readers open the generation named
/// there with `Follower::open_readonly` (`immutable=1`).
async fn run_follower(config_path: PathBuf) -> ExitCode {
    use std::sync::Arc;
    use vrt_sql::follower::{Follower, FollowerServiceConfig};
    use vrt_sql::paths::ResourceKind;

    let raw = match std::fs::read(&config_path) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, path = %config_path.display(), "failed to read follower config");
            return ExitCode::FAILURE;
        }
    };
    let config: FollowerServiceConfig = match serde_json::from_slice(&raw)
        .or_else(|_| toml::from_str::<FollowerServiceConfig>(std::str::from_utf8(&raw).unwrap_or("")))
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "follower config parse failed (JSON or TOML expected)");
            return ExitCode::FAILURE;
        }
    };
    if let Err(field) = config.validate() {
        tracing::error!(field, "invalid follower config");
        return ExitCode::FAILURE;
    }
    let store = match vrt_sql::service::open_object_store(&config.persistence) {
        Ok(Some(s)) => s,
        Ok(None) => {
            tracing::error!("follower requires persistence.backend fs|s3");
            return ExitCode::FAILURE;
        }
        Err(e) => {
            tracing::error!(error = %e, "follower persistence init failed (fail closed)");
            return ExitCode::FAILURE;
        }
    };
    let clock: Arc<dyn vrt_sql::clock::Clock> = Arc::new(vrt_sql::clock::SystemClock);
    let follower = match Follower::new(
        store,
        ResourceKind::D1,
        config.follower.database_id.clone(),
        config.db_root.clone(),
        clock,
    ) {
        Ok(f) => Arc::new(f.with_retain_generations(config.follower.retain_generations)),
        Err(e) => {
            tracing::error!(error = %e, "follower init failed");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(
        db = %config.follower.database_id,
        root = %config.db_root.display(),
        poll_ms = config.follower.poll_interval_ms,
        "vrt-sql follower starting"
    );

    // Optional read-only D1 wire listener. Reads are served from the current immutable
    // generation; the provider routes eligible queries here when `vrtSql.replicas` is set.
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let mut server: Option<tokio::task::JoinHandle<()>> = None;
    if config.unix_dir.is_some() || config.listen_tcp.is_some() {
        let auth_path = config
            .auth_secret_file
            .as_ref()
            .expect("validated: authSecretFile required when serving");
        let auth_secret = match load_secret_0600(auth_path, SecretPurpose::ServiceHmac) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, path = %auth_path.display(),
                    "follower auth secret file unusable");
                return ExitCode::FAILURE;
            }
        };
        let mut state = vrt_sql::replica::ReplicaState::new(follower.clone())
            .with_auth(vrt_sql::service::ServiceAuth::new(auth_secret));
        if let Some(path) = &config.bookmark_secret_file {
            let raw = match load_secret_0600(path, SecretPurpose::ServiceHmac) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, path = %path.display(),
                        "follower bookmark secret file unusable");
                    return ExitCode::FAILURE;
                }
            };
            if raw.len() < 32 {
                tracing::error!(path = %path.display(), "bookmark secret shorter than 32 bytes");
                return ExitCode::FAILURE;
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&raw[..32]);
            state = state.with_signer(vrt_sql::bookmark::BookmarkSigner::new(key));
        }
        let app = vrt_sql::replica::app(state);
        let stop = shutdown.clone();
        if let Some(dir) = &config.unix_dir {
            if let Err(e) = vrt_securefs::ensure_secure_dir(dir) {
                tracing::error!(error = %e, "follower unixDir unusable");
                return ExitCode::FAILURE;
            }
            let path = dir.join("vrt-sql-replica.sock");
            let _ = std::fs::remove_file(&path);
            let std_listener = match vrt_securefs::bind_unix_private(&path, 0o600) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(error = %e, "follower unix bind failed");
                    return ExitCode::FAILURE;
                }
            };
            std_listener.set_nonblocking(true).ok();
            let listener = match tokio::net::UnixListener::from_std(std_listener) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(error = %e, "follower unix listener conversion failed");
                    return ExitCode::FAILURE;
                }
            };
            tracing::info!(path = %path.display(), "follower replica listening (unix)");
            server = Some(tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async move { stop.notified().await })
                    .await;
            }));
        } else {
            let addr: std::net::SocketAddr = config
                .listen_tcp
                .as_ref()
                .expect("validated")
                .parse()
                .expect("validated");
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(error = %e, %addr, "follower replica bind failed");
                    return ExitCode::FAILURE;
                }
            };
            tracing::info!(%addr, "follower replica listening (tcp)");
            server = Some(tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async move { stop.notified().await })
                    .await;
            }));
        }
    }

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            tracing::error!("failed to install SIGTERM handler");
            return ExitCode::FAILURE;
        }
    };
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(
        config.follower.poll_interval_ms,
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // consume the immediate first tick; an initial sync runs below

    loop {
        let f = follower.clone();
        let result = tokio::task::spawn_blocking(move || f.sync_once()).await;
        match result {
            Ok(Ok(report)) => {
                if report.progressed() {
                    tracing::info!(
                        db = %follower.database_id(),
                        bootstrapped = report.bootstrapped,
                        applied = report.segments_applied,
                        from_lsn = report.from_lsn,
                        to_lsn = report.to_lsn,
                        head_lsn = report.head_lsn,
                        caught_up = report.caught_up(),
                        "follower sync"
                    );
                } else if !report.caught_up() {
                    tracing::debug!(
                        db = %follower.database_id(),
                        head_lsn = report.head_lsn,
                        "follower idle (no progress)"
                    );
                }
            }
            Ok(Err(e)) => tracing::warn!(
                db = %follower.database_id(),
                error = %e,
                "follower sync failed; retrying on the next poll"
            ),
            Err(e) => tracing::warn!(
                db = %follower.database_id(),
                error = %e,
                "follower sync task panicked/join failed"
            ),
        }
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!(db = %follower.database_id(), "SIGTERM; follower stopping");
                break;
            }
            _ = tick.tick() => {}
        }
    }
    shutdown.notify_waiters();
    if let Some(task) = server {
        if tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .is_err()
        {
            tracing::warn!("follower replica shutdown timed out");
        }
    }
    ExitCode::SUCCESS
}

/// T36b: shutdown sequence -- closing rejects new requests -> drain in-flight -> CAS-release all
/// leases (the next holder takes over immediately, without waiting for the lease to expire naturally). The total
/// lease-release budget is 5s; a stuck store also does not delay shutdown.
const RELEASE_LEASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn shutdown_sequence(state: &std::sync::Arc<AppState>, drain: std::time::Duration) {
    tracing::info!("SIGTERM; closing to new requests, draining in-flight transactions");
    state.registry.begin_shutdown();
    tokio::time::timeout(drain, state.drain()).await.ok();
    let reg = state.registry.clone();
    let released = tokio::time::timeout(RELEASE_LEASE_TIMEOUT, async move {
        tokio::task::spawn_blocking(move || {
            reg.release_leases(std::time::Instant::now() + RELEASE_LEASE_TIMEOUT);
        })
        .await
        .ok();
    })
    .await;
    if released.is_err() {
        tracing::warn!("lease release timed out; remaining leases expire naturally");
    }
}

/// T36: build the persistence surface from `[persistence]`.
/// `none` -> Ok(None) (the caller WARNs); `fs`/`s3` -> Persister.
/// Clock is SystemClock; holder defaults to `host:pid:start_nanos`.
fn build_persister(
    config: &ServiceConfig,
) -> Result<Option<vrt_sql::engine::PersisterPair>, String> {
    use std::sync::Arc;
    let p = &config.persistence;
    // RV-R66: persistence layer on the same device as the local db = a single copy; refuse to start unless dev.
    if p.backend == "fs" {
        let root = p
            .fs
            .as_ref()
            .ok_or("persistence.fs missing")?
            .root
            .clone();
        match vrt_sql::service::check_fs_durability_distinct(
            &config.db_root,
            &root,
            p.allow_same_device_durability,
        ) {
            Ok(false) => {}
            Ok(true) => tracing::warn!(
                root = %root.display(),
                "persistence.allowSameDeviceDurability is enabled: persistence fs root shares a device with dbRoot; this is dev mode and durability is NOT guaranteed (no disaster recovery)"
            ),
            Err(e) => return Err(e.to_string()),
        }
    }
    let cfg = p.persist_config();
    let clock: Arc<dyn vrt_sql::clock::Clock> = Arc::new(vrt_sql::clock::SystemClock);
    let Some(store) = vrt_sql::service::open_object_store(p)? else {
        return Ok(None);
    };
    // T37r: one Persister per resource -- D1 writes `db/{id}`/`dbs/`,
    // Queue writes `q/{id}`/`queues/` (structural namespace separation).
    let persister = Arc::new(vrt_sql::persist::Persister::new(
        store.clone(),
        cfg.clone(),
        clock.clone(),
        config.db_root.clone(),
        vrt_sql::paths::ResourceKind::D1,
    ));
    let queue_persister = Arc::new(vrt_sql::persist::Persister::new(
        store,
        cfg,
        clock,
        vrt_sql::paths::queue_root_for(&config.db_root),
        vrt_sql::paths::ResourceKind::Queue,
    ));
    // T36b/P0-B: startup self-check -- a write/read/delete probe on the object store; failure refuses startup
    // (container volume ownership/permission errors surface here as a clear startup error, rather than every
    // runtime write returning 503 "backend unavailable"). One probe per store
    // covers both kinds (the probe writes a self-check key with no resource prefix).
    // MIG-9: in-process bounded retry -- on real hardware R2 cold-start `send: error` was observed
    // to self-heal after 1-3 pod CrashLoops; now exponential backoff inside the startup thread
    // (0.5s start, x2, capped at 8s, 60s budget, configurable). During retries the port is unbound
    // -> readiness is naturally not-ready; startupProbe decides, so liveness does not declare it dead.
    // Auth-class (403) / non-retryable errors return immediately without spending the budget. Error text in logs goes through
    // sanitize to strip embedded URLs, so credentials/signatures never enter logs (SigV4 uses headers).
    let retry = vrt_sql::persist::SelfCheckRetry {
        budget: std::time::Duration::from_millis(config.persistence.self_check_budget_ms),
        initial_backoff: std::time::Duration::from_millis(
            config.persistence.self_check_initial_backoff_ms,
        ),
        max_backoff: std::time::Duration::from_millis(
            config.persistence.self_check_max_backoff_ms,
        ),
    };
    persister
        .self_check_with_retry(&retry, |attempt, elapsed, next_backoff, e| {
            tracing::warn!(
                attempt,
                elapsed_ms = elapsed.as_millis() as u64,
                next_backoff_ms = next_backoff.as_millis() as u64,
                error = %vrt_sql::persist::sanitize_self_check_error(&e.to_string()),
                "persistence self-check retryable failure, backing off"
            );
        })
        .map_err(|e| {
            format!(
                "persistence store self-check failed: {}",
                vrt_sql::persist::sanitize_self_check_error(&e.to_string())
            )
        })?;
    Ok(Some(vrt_sql::engine::PersisterPair {
        d1: persister,
        queue: queue_persister,
    }))
}
