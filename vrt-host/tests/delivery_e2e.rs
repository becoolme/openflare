//! T14/B1 in-process end-to-end: `prod::build_controller` +
//! `prod::pod_channel` (the SAME constructors the binary runs) →
//! FileEnvelopePodChannel writes `envelope.json`+`assignment.json` →
//! `FileDesiredStore` re-verifies the signed bytes →
//! `assignment::adopt` filters → real `SupervisorHost` launches
//! `vrt-stub-workerd` → `/readyz` reports → the controller probes and
//! only then ACKs APPLIED.
//!
//! Everything runs inside one tokio test — no shell, no external
//! processes beyond the stub children, which `shutdown_all` reaps.

mod common;
use cell_controller::ack::{AckSink, MemoryAckSink};
use cell_controller::pod::PodChannel;
use cell_controller::source::{DesiredSource as CcDesiredSource, MemoryDesiredSource};
use cell_controller::CellController;
use chrono::{Duration, Utc};
use common::*;
use serde_json::json;
use snapshot_protocol::envelope::{sign, SignParams, SignedSnapshot, SigningKey};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration as StdDur;
use vrt_host::artifact::{BundleCache, MemoryBundleSource};
use vrt_host::assemble::{Assembler, EmitMode};
use vrt_host::desired::{DesiredSource, HostDesiredStore};
use vrt_host::status::{new_status, readyz_view, HostStatus};
use vrt_host::supervisor::{SupervisorDeps, SupervisorHost, TenantAssembler, TenantRegistry};
use vrt_provider::FileDesiredStore;

fn key() -> SigningKey {
    SigningKey::from_seed(&[0x42; 32]).unwrap()
}
fn now() -> chrono::DateTime<Utc> {
    // Real clock — FileDesiredStore expiry compares against Utc::now.
    Utc::now()
}
/// Real `VersionBundle` — stub assembly (T15/B2) fetches every
/// worker's declared digest, so fixtures need verifiable bytes.
fn test_bundle() -> (String, Vec<u8>) {
    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    (
        bundle.digest().to_string(),
        bundle.stored_document().to_vec(),
    )
}

fn worker(worker_id: &str) -> serde_json::Value {
    worker_with_digest(worker_id, &test_bundle().0)
}

fn worker_with_digest(worker_id: &str, digest: &str) -> serde_json::Value {
    json!({
        "workerId": worker_id,
        "name": worker_id,
        "activeVersion": {
            "versionId": format!("vrt_{worker_id}"),
            "artifactDigest": digest,
            "compatibilityDate": "2024-01-01",
            "compatibilityFlags": []
        },
        "bindings": [], "crons": [], "routes": [],
        "quotas": {"cpuMs": 50, "memoryMb": 128, "outboundConnections": 16, "subrequests": 50}
    })
}
fn publish(cell: &str, revision: u64, workers: &[serde_json::Value]) -> SignedSnapshot {
    let payload = json!({
        "schemaVersion": 1, "cellId": cell, "revision": revision,
        "tenants": [{"tenantId": "team-a", "workers": workers}]
    });
    sign(
        &SignParams {
            kind: "compute.desired",
            scope: &format!("cell:{cell}"),
            revision,
            issued_at: now(),
            expires_at: now() + Duration::seconds(3600),
            payload: &payload,
        },
        &key(),
    )
    .unwrap()
}

struct Rig {
    pod_dir: PathBuf,
    controller: CellController,
    source: Arc<MemoryDesiredSource>,
    ack: Arc<MemoryAckSink>,
    host_store: Arc<HostDesiredStore>,
    host: SupervisorHost,
    assembler: Arc<dyn TenantAssembler>,
    status: HostStatus,
    registry: Arc<TenantRegistry>,
    pod_id: String,
}

/// One host reload+converge tick — same call order as `bin/vrt-host`:
/// snapshot reload → assignment adopt-or-block → converge → status.
fn host_tick(rig: &mut Rig) {
    let _ = rig.host_store.inner().reload(Utc::now());
    let Ok(desired) = rig.host_store.snapshot() else {
        return;
    };
    let digest = rig.host_store.inner().payload_digest().unwrap_or_default();
    let assignment_path = rig.pod_dir.join("assignment.json");
    let (effective, assigned, blocked) =
        match vrt_host::assignment::adopt(&assignment_path, &rig.pod_id, &desired, &digest) {
            Ok((f, a)) => (f, a, None),
            Err(e) => (
                desired.as_ref().clone(),
                Vec::<(String, String)>::new(),
                Some(vrt_host::status::assignment_reason(e)),
            ),
        };
    // Mirror bin: blocked adoption reports ONLY the reason and keeps
    // the last adopted revision/assigned (T15/R2).
    if let Some(b) = blocked {
        let mut st = rig.status.write().unwrap_or_else(|e| e.into_inner());
        st.blocked_reason = Some(b);
        return;
    }
    rig.host.converge(&effective, &rig.assembler);
    {
        let mut st = rig.status.write().unwrap_or_else(|e| e.into_inner());
        st.revision = Some(desired.revision);
        st.payload_digest = Some(digest);
        st.assigned = assigned;
        st.blocked_reason = None;
    }
    rig.host.reap_finished();
}

async fn rig(workers: &[serde_json::Value], pod_id: &str) -> (Rig, tempfile::TempDir) {
    rig_with_opts(workers, pod_id, Default::default(), None).await
}

async fn rig_with_env(
    workers: &[serde_json::Value],
    pod_id: &str,
    base_env: std::collections::BTreeMap<String, String>,
) -> (Rig, tempfile::TempDir) {
    rig_with_opts(workers, pod_id, base_env, None).await
}

async fn rig_with_opts(
    workers: &[serde_json::Value],
    pod_id: &str,
    base_env: std::collections::BTreeMap<String, String>,
    source_override: Option<Arc<dyn vrt_host::artifact::BundleSource>>,
) -> (Rig, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let pod_dir = dir.path().join("delivery");
    let work = dir.path().join("work");
    let cache_dir = dir.path().join("cache");
    for p in [&pod_dir, &work, &cache_dir] {
        std::fs::create_dir_all(p).unwrap();
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    // Host side: verified-snapshot store + real supervisor + readyz.
    let host_store = Arc::new(HostDesiredStore::new(FileDesiredStore::new(
        pod_dir.join("envelope.json"),
        "cell-1",
        vec![key().trusted_key()],
        1,
    )));
    let registry = TenantRegistry::new();
    let status = new_status();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let readyz = format!("http://{}", listener.local_addr().unwrap());
    {
        let router = vrt_host::status::control_router(
            status.clone(),
            registry.clone(),
            vrt_host::supervisor::SwitchHoldParams::default(),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
    }
    let assembler: Arc<dyn TenantAssembler> = Arc::new(Assembler::new(
        stub_binary(),
        work,
        Arc::new(BundleCache::new(cache_dir)),
        source_override.unwrap_or_else(|| {
            let seeded = Arc::new(MemoryBundleSource::default());
            let (digest, bytes) = test_bundle();
            seeded.insert_aliased(&digest, &bytes);
            seeded
        }),
        EmitMode::StubJson,
        base_env,
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    ));
    let host = SupervisorHost::new(registry.clone(), SupervisorDeps::default(), test_params());

    // Controller side — production construction path end to end.
    let config = cell_controller::prod::ProdConfig {
        cell_id: "cell-1".into(),
        control_plane_url: "https://cp.internal".into(),
        render_inputs_dir: None,
        token_file: "/nonexistent".into(),
        health_listen: "0.0.0.0:9090".into(),
        metrics_listen: None,
        poll_interval_secs: 10,
        cron_holder: "test".into(),
        fence_file: None,
        allow_insecure_http: false,
        egress_required: false,
        netpolicy: None,
        trusted_keys: vec![key().trusted_key()],
        pods: vec![cell_controller::FilePodSpec {
            id: pod_id.into(),
            dir: pod_dir.to_string_lossy().to_string(),
            readyz,
        }],
        replicas: 1,
        cron_state_dir: None,
        cron_lease: None,
        d1sql: None,
        queue_dispatch: None,
        storage_purge: None,
        cache_purge: None,
        quotas: true,
        report: None,
    };
    let channel: Arc<dyn PodChannel> = cell_controller::prod::pod_channel(&config).unwrap();
    let snap = publish("cell-1", 1, workers);
    let source = Arc::new(MemoryDesiredSource::new(Ok(Some(snap.bytes.clone()))));
    let ack = Arc::new(MemoryAckSink::default());
    let controller = cell_controller::prod::build_controller(
        &config,
        source.clone() as Arc<dyn CcDesiredSource>,
        channel,
        ack.clone() as Arc<dyn AckSink>,
        None,
        Default::default(),
    )
    .unwrap();
    let rig = Rig {
        pod_dir,
        controller,
        source,
        ack,
        host_store,
        host,
        assembler,
        status,
        registry,
        pod_id: pod_id.into(),
    };
    (rig, dir)
}

/// Drive host+controller ticks until `pred` holds or ~15 s elapse.
async fn drive(rig: &mut Rig, pred: impl Fn(&Rig) -> bool) -> bool {
    for _ in 0..300 {
        host_tick(rig);
        rig.controller.reconcile_once(now()).await;
        if pred(rig) {
            return true;
        }
        tokio::time::sleep(StdDur::from_millis(25)).await;
    }
    false
}

/// `drive` variant that re-publishes `bytes` every tick — the memory
/// source is a FIFO (one push = one fetch), so a PARTIAL verdict that
/// needs another converge pass must be re-offered explicitly.
async fn drive_published(
    rig: &mut Rig,
    bytes: Vec<u8>,
    pred: impl Fn(&Rig) -> bool,
) -> bool {
    for _ in 0..300 {
        host_tick(rig);
        rig.source.push(Ok(Some(bytes.clone())));
        rig.controller.reconcile_once(now()).await;
        if pred(rig) {
            return true;
        }
        tokio::time::sleep(StdDur::from_millis(25)).await;
    }
    false
}

fn ack_statuses(rig: &Rig) -> Vec<String> {
    rig.ack
        .sent()
        .iter()
        .map(|r| format!("{:?}:{}", r.status, r.reason.clone().unwrap_or_default()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_end_to_end_applied_switch_and_rollback() {
    let (mut rig, _dir) = rig(&[worker("w1")], "pod-0").await;

    // First publish → files → stub launch → readyz ready → APPLIED.
    let applied = drive(&mut rig, |r| {
        r.ack
            .sent()
            .iter()
            .any(|a| matches!(a.status, cell_controller::AckStatus::Applied))
    })
    .await;
    assert!(applied, "acks: {:?}", ack_statuses(&rig));
    let view = readyz_view(&rig.status, &rig.registry);
    assert!(view.ready, "{view:?}");

    // Idempotent delivery: same revision must not rewrite the files.
    let env_path = rig.pod_dir.join("envelope.json");
    let asg_path = rig.pod_dir.join("assignment.json");
    let env_mtime = std::fs::metadata(&env_path).unwrap().modified().unwrap();
    let asg_mtime = std::fs::metadata(&asg_path).unwrap().modified().unwrap();
    rig.controller.reconcile_once(now()).await;
    assert_eq!(
        std::fs::metadata(&env_path).unwrap().modified().unwrap(),
        env_mtime
    );
    assert_eq!(
        std::fs::metadata(&asg_path).unwrap().modified().unwrap(),
        asg_mtime
    );

    // Revision 2 → switch → APPLIED again. The first converge races
    // the pod's load (PARTIAL); the same publication is re-offered
    // until the pod reports rev-2 loaded and the APPLIED ack lands.
    let snap2 = publish("cell-1", 2, &[worker("w1")]);
    let applied2 = drive_published(&mut rig, snap2.bytes.clone(), |r| {
        r.ack.sent().iter().any(|a| {
            a.revision == 2 && matches!(a.status, cell_controller::AckStatus::Applied)
        })
    })
    .await;
    assert!(applied2, "acks: {:?}", ack_statuses(&rig));

    // Rollback: re-publish rev-1 bytes → controller fence rejects →
    // REJECTED ack, revision-rollback reason; rev-2 keeps serving.
    let snap1 = publish("cell-1", 1, &[worker("w1")]);
    rig.source.push(Ok(Some(snap1.bytes)));
    let rejected = drive(&mut rig, |r| {
        r.ack.sent().iter().any(|a| {
            matches!(a.status, cell_controller::AckStatus::Rejected)
                && a.reason.as_deref() == Some("revision-rollback")
        })
    })
    .await;
    assert!(rejected, "acks: {:?}", ack_statuses(&rig));
    let view = readyz_view(&rig.status, &rig.registry);
    assert!(view.ready && view.revision == Some(2), "{view:?}");

    rig.host.shutdown_all(StdDur::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_or_mismatched_assignment_blocks_convergence() {
    let (mut rig, _dir) = rig(&[worker("w1")], "pod-0").await;

    // Mid-write gap: envelope delivered, assignment absent → blocked.
    let snap = publish("cell-1", 1, &[worker("w1")]);
    std::fs::write(rig.pod_dir.join("envelope.json"), &snap.bytes).unwrap();
    host_tick(&mut rig);
    let view = readyz_view(&rig.status, &rig.registry);
    assert!(!view.ready);
    assert_eq!(view.reason.as_deref(), Some("assignment-incomplete"));
    // No tenant was converged.
    assert!(rig.registry.get("team-a").is_none());

    // Mismatched digest: assignment binds a different payloadDigest.
    let bad = json!({
        "podId": "pod-0", "revision": 1,
        "payloadDigest": "0".repeat(64),
        "envelopeDigest": "0".repeat(64),
        "generatedAt": "2026-11-08T00:00:00Z",
        "workers": [{"tenantId":"team-a","workerId":"w1"}]
    });
    std::fs::write(rig.pod_dir.join("assignment.json"), bad.to_string()).unwrap();
    host_tick(&mut rig);
    let view = readyz_view(&rig.status, &rig.registry);
    assert_eq!(view.reason.as_deref(), Some("assignment-mismatch"));
    assert!(rig.registry.get("team-a").is_none());

    // Controller side: correct assignment → everything converges.
    let applied = drive(&mut rig, |r| {
        r.ack
            .sent()
            .iter()
            .any(|a| matches!(a.status, cell_controller::AckStatus::Applied))
    })
    .await;
    assert!(applied, "acks: {:?}", ack_statuses(&rig));

    // T15/R2: a rejected adoption must NOT bump the reported revision —
    // readyz keeps reporting the last ADOPTED revision plus the typed
    // blocked reason (previously the new revision leaked through).
    let snap2 = publish("cell-1", 2, &[worker("w1")]);
    std::fs::write(rig.pod_dir.join("envelope.json"), &snap2.bytes).unwrap();
    std::fs::write(
        rig.pod_dir.join("assignment.json"),
        json!({
            "podId": "pod-0", "revision": 1,
            "payloadDigest": "0".repeat(64),
            "envelopeDigest": "0".repeat(64),
            "generatedAt": "2026-11-08T00:00:00Z",
            "workers": [{"tenantId":"team-a","workerId":"w1"}]
        })
        .to_string(),
    )
    .unwrap();
    host_tick(&mut rig);
    let view = readyz_view(&rig.status, &rig.registry);
    assert_eq!(view.reason.as_deref(), Some("assignment-mismatch"));
    assert_eq!(
        view.revision,
        Some(1),
        "blocked must keep last adopted: {view:?}"
    );
    assert!(!view.ready);

    // T15/R3: an EXPLICITLY empty assignment (digest matched) is a
    // legitimate "zero workers" pod → ready, workers drained.
    let snap3 = publish("cell-1", 3, &[worker("w1")]);
    std::fs::write(rig.pod_dir.join("envelope.json"), &snap3.bytes).unwrap();
    std::fs::write(
        rig.pod_dir.join("assignment.json"),
        json!({
            "podId": "pod-0", "revision": 3,
            "payloadDigest": snap3.payload_digest.clone(),
            "envelopeDigest": "0".repeat(64),
            "generatedAt": "2026-11-08T00:00:00Z",
            "workers": []
        })
        .to_string(),
    )
    .unwrap();
    let ok = drive(&mut rig, |r| {
        let v = readyz_view(&r.status, &r.registry);
        v.ready && v.revision == Some(3)
    })
    .await;
    assert!(
        ok,
        "empty assignment should converge to ready: {:?}",
        readyz_view(&rig.status, &rig.registry)
    );
    rig.host.shutdown_all(StdDur::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_failure_never_acks_applied() {
    // Pod dir exists but readyz points at a dead port — delivery
    // succeeds, probe fails → PARTIAL only, never APPLIED.
    let dir = tempfile::TempDir::new().unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let pod_dir = dir.path().join("delivery");
    std::fs::create_dir_all(&pod_dir).unwrap();
    std::fs::set_permissions(&pod_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let dead = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        format!("http://{a}")
    };
    let config = cell_controller::prod::ProdConfig {
        cell_id: "cell-1".into(),
        control_plane_url: "https://cp.internal".into(),
        render_inputs_dir: None,
        token_file: "/nonexistent".into(),
        health_listen: "0.0.0.0:9090".into(),
        metrics_listen: None,
        poll_interval_secs: 10,
        cron_holder: "test".into(),
        fence_file: None,
        allow_insecure_http: false,
        egress_required: false,
        netpolicy: None,
        trusted_keys: vec![key().trusted_key()],
        pods: vec![cell_controller::FilePodSpec {
            id: "pod-0".into(),
            dir: pod_dir.to_string_lossy().to_string(),
            readyz: dead,
        }],
        replicas: 1,
        cron_state_dir: None,
        cron_lease: None,
        d1sql: None,
        queue_dispatch: None,
        storage_purge: None,
        cache_purge: None,
        quotas: true,
        report: None,
    };
    let channel = cell_controller::prod::pod_channel(&config).unwrap();
    let snap = publish("cell-1", 1, &[worker("w1")]);
    let source = Arc::new(MemoryDesiredSource::new(Ok(Some(snap.bytes.clone()))));
    let ack = Arc::new(MemoryAckSink::default());
    let mut controller = cell_controller::prod::build_controller(
        &config,
        source as Arc<dyn CcDesiredSource>,
        channel,
        ack.clone() as Arc<dyn AckSink>,
        None,
        Default::default(),
    )
    .unwrap();
    for _ in 0..5 {
        controller.reconcile_once(now()).await;
    }
    let sent = ack.sent();
    assert!(!sent.is_empty());
    assert!(
        sent.iter()
            .all(|a| !matches!(a.status, cell_controller::AckStatus::Applied)),
        "probe failure must never yield APPLIED: {sent:?}"
    );
    assert!(sent
        .iter()
        .any(|a| matches!(a.status, cell_controller::AckStatus::Partial)));
}

/// T15/R2: "received" must never read as "loaded". With the stub's
/// ready-report delayed, readyz reports not-ready (workers-not-ready)
/// while the adopted revision is already visible — and the controller
/// must not ACK APPLIED until the installed generation serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_launch_is_never_reported_as_loaded() {
    let mut env = std::collections::BTreeMap::new();
    env.insert("VRT_STUB_READY_DELAY_MS".to_string(), "2000".to_string());
    let (mut rig, _dir) = rig_with_env(&[worker("w1")], "pod-0", env).await;

    // Drive until the tenant exists — launch pending (stub still
    // sleeping before its ready report).
    let observed_pending = drive(&mut rig, |r| {
        r.registry.get("team-a").is_some() && !readyz_view(&r.status, &r.registry).ready
    })
    .await;
    assert!(observed_pending);
    let view = readyz_view(&rig.status, &rig.registry);
    assert!(!view.ready, "{view:?}");
    assert_eq!(view.revision, Some(1), "adopted revision is reported");
    let w = view.workers.iter().find(|w| w.worker == "w1").unwrap();
    assert_eq!(w.reason.as_deref(), Some("workers-not-ready"));
    // No APPLIED while the generation is still launching.
    assert!(
        !rig.ack
            .sent()
            .iter()
            .any(|a| matches!(a.status, cell_controller::AckStatus::Applied)),
        "pending launch must not APPLIED: {:?}",
        ack_statuses(&rig)
    );

    // Once the stub reports ready, install flips serving==expected →
    // ready → APPLIED.
    let applied = drive(&mut rig, |r| {
        readyz_view(&r.status, &r.registry).ready
            && r.ack
                .sent()
                .iter()
                .any(|a| matches!(a.status, cell_controller::AckStatus::Applied))
    })
    .await;
    assert!(applied, "acks: {:?}", ack_statuses(&rig));
    rig.host.shutdown_all(StdDur::from_secs(5)).await;
}

// ------------------------------------------------------------------
// T15/B2 — control-plane bundle pull
// ------------------------------------------------------------------

/// Scripted fake control-plane `GET /vrt/bundles/:digest`: counts
/// requests per digest, checks the bearer token, serves configured
/// bytes or status codes.
struct BundleServer {
    url: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    responses: Arc<std::sync::Mutex<std::collections::HashMap<String, Resp>>>,
}

enum Resp {
    Bytes(Vec<u8>),
    Status(u16),
}

async fn bundle_server() -> BundleServer {
    let requests: Arc<std::sync::Mutex<Vec<String>>> = Default::default();
    let responses: Arc<std::sync::Mutex<std::collections::HashMap<String, Resp>>> =
        Default::default();
    let rq = requests.clone();
    let rs = responses.clone();
    let app = axum::Router::new().route(
        "/vrt/bundles/{digest}",
        axum::routing::get(
            move |axum::extract::Path(digest): axum::extract::Path<String>,
                  headers: axum::http::HeaderMap| {
                let rq = rq.clone();
                let rs = rs.clone();
                async move {
                    rq.lock().unwrap().push(digest.clone());
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    if auth != "Bearer tok" {
                        return axum::http::Response::builder()
                            .status(403)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    let resp = rs.lock().unwrap().get(&digest).map(|r| match r {
                        Resp::Bytes(b) => Resp::Bytes(b.clone()),
                        Resp::Status(c) => Resp::Status(*c),
                    });
                    match resp {
                        Some(Resp::Bytes(b)) => axum::http::Response::builder()
                            .status(200)
                            .header("content-type", "application/octet-stream")
                            .body(axum::body::Body::from(b))
                            .unwrap(),
                        Some(Resp::Status(c)) => axum::http::Response::builder()
                            .status(c)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                        None => axum::http::Response::builder()
                            .status(404)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    }
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    BundleServer {
        url,
        requests,
        responses,
    }
}

impl BundleServer {
    fn http_source(&self) -> Arc<vrt_host::bundle_http::HttpBundleSource> {
        Arc::new(
            vrt_host::bundle_http::HttpBundleSource::new(vrt_host::bundle_http::HttpBundleConfig {
                base_url: self.url.clone(),
                token: "tok".into(),
                request_timeout: StdDur::from_secs(2),
                deadline: StdDur::from_secs(6),
                max_attempts: 3,
                max_bytes: 4 * 1024 * 1024,
            })
            .unwrap(),
        )
    }

    fn set(&self, digest: &str, resp: Resp) {
        self.responses
            .lock()
            .unwrap()
            .insert(digest.to_string(), resp);
    }

    fn hits(&self, digest: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|d| *d == digest)
            .count()
    }
}

/// B2: cold cache → the host pulls the bundle from the control-plane
/// route, verifies it, launches the stub, and only then reports ready
/// → APPLIED. A tampered bundle (digest mismatch) fails the tenant's
/// assembly: readyz reports `bundle-unavailable`, the old generation
/// keeps serving, and the controller never ACKs APPLIED for it.
/// 5xx is retried (bounded); 4xx is authoritative (single request).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bundle_pull_verify_and_fail_closed() {
    let server = bundle_server().await;
    let (digest, bytes) = test_bundle();
    server.set(&digest, Resp::Bytes(bytes));
    let (mut rig, _dir) = rig_with_opts(
        &[worker("w1")],
        "pod-0",
        Default::default(),
        Some(server.http_source()),
    )
    .await;

    // Cold cache → HTTP pull → verified → stub launch → APPLIED.
    let applied = drive(&mut rig, |r| {
        r.ack
            .sent()
            .iter()
            .any(|a| matches!(a.status, cell_controller::AckStatus::Applied))
    })
    .await;
    assert!(applied, "acks: {:?}", ack_statuses(&rig));
    assert_eq!(server.hits(&digest), 1, "exactly one pull (cache fill)");
    // Cache now holds the verified object.
    assert!(rig
        .pod_dir
        .parent()
        .unwrap()
        .join("cache/bundles")
        .join(&digest)
        .exists());

    // Rev-2 pins a bundle the server TAMPERS with — digest mismatch →
    // assembly fails closed, old gen keeps serving. MIG-1: this is a
    // TENANT-level failure — the pod stays ready (revision adopted)
    // and reports `tenant_failures`; the tamper never becomes pod-level
    // evidence and never drains the cell.
    let bad_digest = "b".repeat(64);
    server.set(&bad_digest, Resp::Bytes(b"forged bundle".to_vec()));
    let snap2 = publish("cell-1", 2, &[worker_with_digest("w1", &bad_digest)]);
    rig.source.push(Ok(Some(snap2.bytes.clone())));
    let degraded = drive(&mut rig, |r| {
        let v = readyz_view(&r.status, &r.registry);
        v.revision == Some(2)
            && v.tenant_failures
                .iter()
                .any(|f| f.code == "bundle-unavailable")
    })
    .await;
    assert!(degraded, "{:?}", readyz_view(&rig.status, &rig.registry));
    // The first converge raced the bundle fetch (pod still converging →
    // PARTIAL). Once the tenant verdict lands, re-delivering the same
    // publication reports the tenant failure — MIG-4: a non-empty
    // tenant_failures set can NEVER ride an APPLIED verdict; the ack
    // stays PARTIAL until the tenant recovers (it never does here:
    // the digest keeps mismatching). `bundle-unavailable` is a
    // whole-tenant assemble failure — `workerIds` stays absent so the
    // control plane applies its changed ∩ tenant rule.
    rig.source.push(Ok(Some(snap2.bytes.clone())));
    rig.controller.reconcile_once(now()).await;
    let sent_acks = rig.ack.sent();
    let partial_r2 = sent_acks
        .iter()
        .filter(|a| {
            a.revision == 2 && matches!(a.status, cell_controller::AckStatus::Partial)
        })
        .collect::<Vec<_>>();
    assert!(
        partial_r2
            .iter()
            .any(|a| a
                .tenant_failures
                // MIG-6: Option<Vec> — explicit-empty vs field-absent.
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .any(|f| f.code == "bundle-unavailable")),
        "rev-2 ack must carry the tenant failure: {:?}",
        ack_statuses(&rig)
    );
    assert!(
        sent_acks
            .iter()
            .all(|a| a.revision != 2
                || !matches!(a.status, cell_controller::AckStatus::Applied)),
        "a failed tenant may never be acked APPLIED: {:?}",
        ack_statuses(&rig)
    );
    assert_eq!(server.hits(&bad_digest), 1, "DigestMismatch needs no retry");

    // 5xx → bounded retries (max_attempts=3), then fail closed.
    let flaky = "c".repeat(64);
    server.set(&flaky, Resp::Status(503));
    let snap3 = publish("cell-1", 3, &[worker_with_digest("w1", &flaky)]);
    rig.source.push(Ok(Some(snap3.bytes.clone())));
    let failed = drive(&mut rig, |r| {
        readyz_view(&r.status, &r.registry)
            .tenant_failures
            .iter()
            .any(|f| f.code == "bundle-unavailable")
            && server.hits(&flaky) >= 3
    })
    .await;
    assert!(failed, "bounded 5xx retry expected");
    assert_eq!(server.hits(&flaky), 3, "3 attempts then give up");

    // 4xx → authoritative, no retry (single request).
    let denied = "d".repeat(64);
    server.set(&denied, Resp::Status(403));
    let snap4 = publish("cell-1", 4, &[worker_with_digest("w1", &denied)]);
    rig.source.push(Ok(Some(snap4.bytes.clone())));
    let hit = drive(&mut rig, |_r| server.hits(&denied) >= 1).await;
    assert!(hit);
    tokio::time::sleep(StdDur::from_millis(400)).await;
    assert_eq!(server.hits(&denied), 1, "4xx is never retried");
    rig.host.shutdown_all(StdDur::from_secs(5)).await;
}
