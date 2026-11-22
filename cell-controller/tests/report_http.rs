//! Real HTTP e2e for the periodic cell report: a fake pod serves
//! `/metrics` + `/v1/introspect/instances`, a fake control plane captures
//! `/vrt/metrics`, and the reporter runs its production `HttpReportIo` against
//! both. No traits are mocked here.

use axum::extract::Json;
use axum::routing::{get, post};
use axum::Router;
use cell_controller::assign::{AssignedWorker, PodAssignment};
use cell_controller::report::{Reporter, REPORT_SCHEMA_VERSION};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct PodState {
    counter: AtomicU64,
}

async fn metrics(axum::extract::State(s): axum::extract::State<Arc<PodState>>) -> String {
    let c = s.counter.fetch_add(5, Ordering::SeqCst);
    format!(
        "process_start_time_seconds 1000\n\
         vrt_worker_requests_total{{worker_id=\"team:w\",status_class=\"2xx\"}} {c}\n\
         vrt_worker_request_duration_seconds_bucket{{worker_id=\"team:w\",le=\"0.005\"}} {c}\n\
         vrt_worker_request_duration_seconds_sum{{worker_id=\"team:w\"}} 0.1\n\
         vrt_worker_request_duration_seconds_count{{worker_id=\"team:w\"}} {c}\n\
         vrt_tenant_cpu_seconds_total{{tenant_id=\"team\"}} {c}\n\
         vrt_host_launches_total{{outcome=\"ready\",reason=\"none\"}} 1\n"
    )
}

async fn instances(Json(q): Json<Value>) -> Json<Value> {
    assert_eq!(q["tenantId"], "team");
    assert_eq!(q["workerId"], "team:w");
    Json(json!({
        "tenantId": "team",
        "workerId": "team:w",
        "classKey": q["classKey"],
        "declared": true,
        "instances": [{"id": "a".repeat(64), "bytes": 4096}],
        "storageBytes": 4096,
        "truncated": false,
        "sampledAtMs": 1000
    }))
}

async fn capture(
    axum::extract::State(pushes): axum::extract::State<Arc<Mutex<Vec<Value>>>>,
    Json(body): Json<Value>,
) -> axum::http::StatusCode {
    pushes.lock().unwrap().push(body);
    axum::http::StatusCode::OK
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

fn desired() -> Value {
    json!({
        "schemaVersion": 1,
        "cellId": "cell-1",
        "revision": 1,
        "tenants": [{
            "tenantId": "team",
            "workers": [{
                "workerId": "team:w",
                "name": "w",
                "activeVersion": {
                    "versionId": "v1",
                    "artifactDigest": "a".repeat(64),
                    "compatibilityDate": "2024-09-25",
                    "compatibilityFlags": []
                },
                "bindings": [{
                    "type": "d1", "name": "DB", "databaseId": "b".repeat(26),
                    "epoch": 1, "permissions": {"read": true, "write": true}
                }],
                "routes": [],
                "crons": [],
                "durableObjectClasses": [{
                    "className": "Counter", "uniqueKey": "c".repeat(64), "enableSql": true
                }],
                "workflows": [{
                    "name": "flow", "binding": "FLOW", "className": "Flow",
                    "uniqueKey": "d".repeat(64)
                }],
                "quotas": {"cpuMs": 50, "memoryMb": 128, "outboundConnections": 16, "subrequests": 50}
            }]
        }]
    })
}

#[tokio::test]
async fn reporter_pushes_real_http_metrics_and_instances() {
    let pod = serve(
        Router::new()
            .route("/metrics", get(metrics))
            .route("/v1/introspect/instances", post(instances))
            .with_state(Arc::new(PodState {
                counter: AtomicU64::new(10),
            })),
    )
    .await;
    let pushes = Arc::new(Mutex::new(Vec::new()));
    let plane = serve(
        Router::new()
            .route("/api/internal/clusters/{cell}/vrt/metrics", post(capture))
            .with_state(pushes.clone()),
    )
    .await;
    let io = cell_controller::report::HttpReportIo::new(
        &[("pod-0".to_string(), pod)],
        &plane,
        "cell-1",
        "token".into(),
        None,
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    let mut reporter = Reporter::new(io, "cell-1");
    let desired: snapshot_protocol::compute_desired::ComputeDesired =
        serde_json::from_value(desired()).unwrap();
    let assignments = BTreeMap::from([(
        "pod-0".to_string(),
        PodAssignment {
            pod_id: "pod-0".into(),
            revision: 1,
            payload_digest: "a".repeat(64),
            envelope_digest: String::new(),
            generated_at: String::new(),
            workers: vec![AssignedWorker {
                tenant_id: "team".into(),
                worker_id: "team:w".into(),
            }],
        },
    )]);
    let first = reporter
        .tick(&desired, &assignments, 1_060_000)
        .await
        .unwrap();
    assert_eq!(first.pods, 1);
    assert_eq!(first.workflows, 1);
    assert_eq!(first.do_classes, 1);
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let second = reporter
        .tick(&desired, &assignments, 1_090_000)
        .await
        .unwrap();
    assert_eq!(second.pods, 1);

    let captured = pushes.lock().unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0]["schemaVersion"], REPORT_SCHEMA_VERSION);
    assert_eq!(captured[0]["cellId"], "cell-1");
    assert_eq!(captured[0]["pods"][0]["workers"][0]["requests"], 10);
    assert_eq!(captured[0]["pods"][0]["windowMs"], 60_000);
    assert_eq!(captured[1]["pods"][0]["workers"][0]["requests"], 5);
    assert_eq!(captured[1]["pods"][0]["windowMs"], 30_000);
    let resources = &captured[1]["resources"];
    assert_eq!(
        resources["workflows"]["team:w"]["flow"]["instances"][0]["bytes"],
        4096
    );
    assert_eq!(
        resources["durableObjects"]["team:w"]["Counter"]["instances"][0]["id"],
        "a".repeat(64)
    );
}
