//! T37b-r5: production-path queue dispatch -- the real `QueueDispatcher`
//! (cell-controller) + the real `HttpQueueInvoker` + the real vrt-host
//! `/v1/queue/dispatch` route (production router/handler/response encoding).
//! workerd is replaced by a dev-stub TCP endpoint -- the protocol surface (hyper HTTP/1.1 POST
//! `/vrt/queue-dispatch`) matches the facade; vrt-sql is replaced by a minimal in-process
//! wire fake (claim/ack/release semantics are the same as the real wire -- this test
//! focuses on the dispatcher<->vrt-host hop; the real vrt-sql chain is already covered by
//! `real_vrt_sql_restart_mid_dispatch_recovers`).
//!
//! It is also a two-way contract test: the QueueOutcome JSON returned by the facade (the real encoded
//! bytes pass through vrt-host verbatim) must be parseable by the production dispatcher and settle as
//! `ackAll` -- drift on either side blows up in this test.
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::routing::post;
use axum::Json;
use cell_controller::assign::{AssignedWorker, PodAssignment};
use cell_controller::queue_dispatch::{
    DispatchConfig, HttpQueueInvoker, QueueDispatcher, QueueSqlClient,
};
use serde_json::{json, Value};
use snapshot_protocol::compute_desired::ComputeDesired;
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::supervisor::{TenantRegistry, TenantRuntime};

// ---------------- minimal vrt-sql wire fake ----------------
// Same shape as the production wire: claim sends {leaseId,dispatchEpoch,messages[]},
// message lease/attempts+1/visibility semantics are consistent; only validation unrelated to this test is cut.

#[derive(Default, Clone)]
struct FakeMsg {
    body_hex: String,
    content_type: String,
    timestamp_ms: i64,
    visible_at: i64,
    attempts: u32,
    lease_id: Option<String>,
    lease_expiry: i64,
}

#[derive(Default)]
struct FakeQueue {
    msgs: BTreeMap<String, FakeMsg>,
    acked: Vec<String>,
    holder: Option<(String, i64)>,
    epoch: u64,
}

type Shared = Arc<StdMutex<BTreeMap<String, FakeQueue>>>;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn fake_op(
    State(w): State<Shared>,
    Path((qid, op)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let mut world = w.lock().unwrap();
    let now = now_ms();
    let q = world.entry(qid).or_default();
    match op.as_str() {
        "claim" => {
            let holder = body["holderId"].as_str().unwrap_or("");
            let ttl = body["holderTtlMs"].as_i64().unwrap_or(0);
            let max = body["max"].as_u64().unwrap_or(0) as usize;
            let lttl = body["leaseTtlMs"].as_i64().unwrap_or(0);
            if let Some((h, exp)) = &q.holder {
                if h != holder && *exp > now {
                    return (
                        axum::http::StatusCode::CONFLICT,
                        Json(
                            json!({"success":false,"code":"VRT_QUEUE_DISPATCH_LEASE","retryable":false}),
                        ),
                    );
                }
                if h != holder && *exp <= now {
                    q.epoch += 1;
                }
            } else {
                q.epoch = 1;
            }
            q.holder = Some((holder.to_string(), now + ttl));
            let lease = "lease-fake".to_string();
            let mut msgs = Vec::new();
            for (id, m) in q.msgs.iter_mut() {
                if msgs.len() >= max {
                    break;
                }
                let leased = m.lease_id.is_some() && m.lease_expiry > now;
                if leased || m.visible_at > now {
                    continue;
                }
                m.attempts += 1;
                m.lease_id = Some(lease.clone());
                m.lease_expiry = now + lttl;
                msgs.push(json!({
                    "id": id, "leaseId": lease, "attempts": m.attempts,
                    "body": m.body_hex, "contentType": m.content_type,
                    "timestampMs": m.timestamp_ms,
                }));
            }
            (
                axum::http::StatusCode::OK,
                Json(
                    json!({"success":true,"leaseId":lease,"dispatchEpoch":q.epoch,"messages":msgs}),
                ),
            )
        }
        "ack" => {
            let lease = body["leaseId"].as_str().unwrap_or("");
            let ids: Vec<String> = body["ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|i| i.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            for id in &ids {
                match q.msgs.get(id) {
                    Some(m) if m.lease_id.as_deref() == Some(lease) && m.lease_expiry > now => {}
                    _ => {
                        return (
                            axum::http::StatusCode::CONFLICT,
                            Json(
                                json!({"success":false,"code":"VRT_QUEUE_LEASE_LOST","retryable":false}),
                            ),
                        )
                    }
                }
            }
            for id in &ids {
                q.msgs.remove(id);
                q.acked.push(id.clone());
            }
            (axum::http::StatusCode::OK, Json(json!({"success":true})))
        }
        _ => (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({"success":false,"code":"VRT_NOOP"})),
        ),
    }
}

// ---------------- production path tests ----------------

const QID: &str = "qqqqqqqqqqqqqqqqqqqqqqqqqq";

fn desired() -> ComputeDesired {
    serde_json::from_value(json!({
        "schemaVersion": 1,
        "cellId": "cell-1",
        "revision": 1,
        "tenants": [{
            "tenantId": "t1",
            "workers": [{
                "workerId": "w1",
                "name": "consumer",
                "activeVersion": {
                    "versionId": "v1",
                    "artifactDigest": "a".repeat(64),
                    "compatibilityDate": "2025-01-01",
                    "compatibilityFlags": []
                },
                "bindings": [],
                "routes": [],
                "crons": [],
                "queueConsumers": [{"queueId": QID, "queueName": "jobs"}],
                "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 0, "outboundConnections": 0}
            }]
        }]
    }))
    .unwrap()
}

fn assignments() -> BTreeMap<String, PodAssignment> {
    let mut m = BTreeMap::new();
    m.insert(
        "local".to_string(),
        PodAssignment {
            pod_id: "local".into(),
            revision: 1,
            payload_digest: "x".into(),
            envelope_digest: "y".into(),
            generated_at: String::new(),
            workers: vec![AssignedWorker {
                tenant_id: "t1".into(),
                worker_id: "w1".into(),
            }],
        },
    );
    m
}

async fn serve_on(app: axum::Router) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    addr
}

/// dispatcher -> vrt-host (production route) -> stub facade -> QueueOutcome ->
/// dispatcher parse -> ack settlement. Asserts the whole wire interoperates under production code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_through_real_vrt_host_acks() {
    // 1. stub facade: dev-stub TCP endpoint + **raw TCP byte assertions**.
    //    An axum/hyper server leniently accepts absolute-form and a missing Host --
    //    exactly the r6 production defect (the workerd socket service strictly rejects it) -- so
    //    the stub must assert the request line/Host against workerd's strict surface, and must no longer use
    //    a high-level framework that swallows the wire shape.
    let facade_calls = Arc::new(AtomicU64::new(0));
    let fc = facade_calls.clone();
    let facade_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let facade_addr: SocketAddr = facade_l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = facade_l.accept().await.unwrap();
            let fc = fc.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                fc.fetch_add(1, Ordering::Relaxed);
                let line = req.lines().next().unwrap_or("");
                // workerd accepts origin-form; absolute-form or no Host
                // -> bare-text "Bad Request" (actually observed in e2e).
                assert!(
                    line.starts_with("POST /vrt/queue-dispatch "),
                    "request line must be origin-form: {line:?}"
                );
                assert!(
                    req.to_ascii_lowercase().contains(
                        "
host:"
                    ),
                    "Host header required by workerd: {req:?}"
                );
                let body = serde_json::json!({"outcome":"ok","ackAll":true});
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    body.to_string().len(),
                    body
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });

    // 2. real vrt-host route: tenant ready + qconsumer slot -> stub.
    let rt = TenantRuntime::new("t1");
    let mut eps = BTreeMap::new();
    eps.insert("w1".to_string(), WorkerEndpoint::Tcp(facade_addr));
    rt.test_install_qconsumer(eps);
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt);
    let host_addr = serve_on(vrt_host::queue_dispatch::router(registry)).await;

    // 3. minimal vrt-sql fake + one message.
    let world: Shared = Arc::new(StdMutex::new(BTreeMap::new()));
    {
        let mut w = world.lock().unwrap();
        w.entry(QID.to_string()).or_default().msgs.insert(
            "m1".into(),
            FakeMsg {
                body_hex: "aa".into(),
                content_type: "text".into(),
                timestamp_ms: now_ms(),
                ..Default::default()
            },
        );
    }
    let sql_addr = serve_on(
        axum::Router::new()
            .route("/queues/{id}/{op}", post(fake_op))
            .with_state(world.clone()),
    )
    .await;

    // 4. production dispatcher + HttpQueueInvoker (pods.local=host_addr).
    let sql = Arc::new(
        QueueSqlClient::new(&sql_addr, b"test-secret".to_vec(), Duration::from_secs(5)).unwrap(),
    );
    let inv = Arc::new(
        HttpQueueInvoker::new(&[("local".into(), host_addr)], Duration::from_secs(5)).unwrap(),
    );
    let d = QueueDispatcher::new(
        sql,
        inv,
        DispatchConfig {
            holder_id: "test-disp".into(),
            dispatch_ttl_ms: 30_000,
            msg_lease_ms: 60_000,
            delivery_timeout: Duration::from_secs(5),
            global_inflight_max: 4,
            claim_max_bytes: 4 << 20,
        },
    )
    .unwrap();
    let desired = desired();
    let assigns = assignments();
    let exp = chrono::Utc::now() + chrono::Duration::minutes(5);

    // tick1 claim → batch_timeout(1s) → tick2 dispatch → facade
    // QueueOutcome -> ack settlement: the message is dequeued.
    let o1 = d
        .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
        .await;
    assert_eq!(o1.claimed, 1);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
        .await;
    for _ in 0..200 {
        if world
            .lock()
            .unwrap()
            .get(QID)
            .is_some_and(|q| q.msgs.is_empty() && q.acked == ["m1"])
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("message must be acked through the real vrt-host dispatch path");
}

/// Contract regression: a vrt-host typed rejection (NO_CONSUMER) -> the dispatcher parses it as a
/// platform-side error (attempts not consumed -- `release` semantics) rather than malformed.
/// When the facade endpoint is missing vrt-host returns
/// `{delivered:false,error:{code:VRT_QDISP_NO_CONSUMER}}`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_consumer_typed_error_is_platform_not_malformed() {
    // tenant ready but no qconsumer slot -> vrt-host 404 NO_CONSUMER.
    let rt = TenantRuntime::new("t1");
    rt.test_set_state(vrt_host::state::TenantState::Ready);
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt);
    let host_addr = serve_on(vrt_host::queue_dispatch::router(registry)).await;

    // wire fake + release support (attempts compensation assertion).
    let ops = Arc::new(StdMutex::new(Vec::<String>::new()));
    let ops2 = ops.clone();
    let world: Shared = Arc::new(StdMutex::new(BTreeMap::new()));
    {
        let mut w = world.lock().unwrap();
        w.entry(QID.to_string()).or_default().msgs.insert(
            "m1".into(),
            FakeMsg {
                body_hex: "aa".into(),
                timestamp_ms: now_ms(),
                ..Default::default()
            },
        );
    }
    let world2 = world.clone();
    let sql_app = axum::Router::new().route(
        "/queues/{id}/{op}",
        post(
            move |Path((qid, op)): Path<(String, String)>, Json(body): Json<Value>| {
                let ops = ops2.clone();
                let world = world2.clone();
                async move {
                    ops.lock().unwrap().push(op.clone());
                    if op == "release" {
                        // release semantics mirrored: clear lease + attempts-1.
                        let mut w = world.lock().unwrap();
                        let q = w.entry(qid).or_default();
                        let lease = body["leaseId"].as_str().unwrap_or("");
                        for item in body["items"].as_array().unwrap() {
                            let id = item["id"].as_str().unwrap();
                            let m = q.msgs.get_mut(id).unwrap();
                            assert_eq!(m.lease_id.as_deref(), Some(lease));
                            m.lease_id = None;
                            m.visible_at =
                                now_ms() + item["delaySecs"].as_i64().unwrap_or(0) * 1000;
                            m.attempts = m.attempts.saturating_sub(1);
                        }
                        return (
                            axum::http::StatusCode::OK,
                            Json(json!({"success":true,"released":1})),
                        );
                    }
                    fake_op(State(world), Path((qid, op)), Json(body)).await
                }
            },
        ),
    );
    let sql_addr = serve_on(sql_app).await;

    let sql = Arc::new(
        QueueSqlClient::new(&sql_addr, b"test-secret".to_vec(), Duration::from_secs(5)).unwrap(),
    );
    let inv = Arc::new(
        HttpQueueInvoker::new(&[("local".into(), host_addr)], Duration::from_secs(5)).unwrap(),
    );
    let d = QueueDispatcher::new(
        sql,
        inv,
        DispatchConfig {
            holder_id: "test-disp".into(),
            dispatch_ttl_ms: 30_000,
            msg_lease_ms: 60_000,
            delivery_timeout: Duration::from_secs(5),
            global_inflight_max: 4,
            claim_max_bytes: 4 << 20,
        },
    )
    .unwrap();
    let desired = desired();
    let assigns = assignments();
    let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
    d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
        .await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
        .await;
    // Wait for release to land: attempts compensated back to 0, message stays in the queue.
    for _ in 0..200 {
        if world
            .lock()
            .unwrap()
            .get(QID)
            .is_some_and(|q| q.msgs.get("m1").is_some_and(|m| m.attempts == 0))
        {
            let ops = ops.lock().unwrap();
            assert!(ops.iter().any(|o| o == "release"), "ops: {ops:?}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("NO_CONSUMER must settle as platform release (attempts back to 0)");
}
