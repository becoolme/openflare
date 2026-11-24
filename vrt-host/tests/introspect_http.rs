//! HTTP e2e for `/v1/introspect/instances`: the real control router resolves
//! a declared class dir from the serving tenant and returns the sqlite files.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::collections::BTreeMap;
use tower::ServiceExt;
use vrt_host::introspect;
use vrt_host::supervisor::{DoStorageView, TenantRegistry, TenantRuntime};

#[tokio::test]
async fn introspect_lists_declared_class_instances_over_http() {
    let tmp = tempfile::tempdir().unwrap();
    let key = "a".repeat(64);
    let dir = tmp.path().join("team:w").join(&key);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{}.sqlite", "b".repeat(64))), b"12345").unwrap();
    std::fs::write(dir.join(format!("{}.sqlite", "c".repeat(64))), b"12").unwrap();
    std::fs::write(dir.join(format!("{}.sqlite-wal", "b".repeat(64))), b"wal").unwrap();

    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("team");
    rt.set_do_storage_view(Some(DoStorageView {
        tenant_dir: tmp.path().to_path_buf(),
        worker_classes: BTreeMap::from([("team:w".to_string(), vec![key.clone()])]),
    }));
    registry.insert("team".into(), rt);

    let app = introspect::router(registry);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/introspect/instances")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "tenantId": "team",
                "workerId": "team:w",
                "classKey": key,
            })
            .to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["declared"], true, "{body}");
    assert_eq!(body["instances"][0]["id"], "b".repeat(64));
    assert_eq!(body["instances"][0]["bytes"], 5);
    assert_eq!(body["instances"][1]["id"], "c".repeat(64));
    // main files + the wal sidecar
    assert_eq!(body["storageBytes"], 5 + 2 + 3);
    assert_eq!(body["truncated"], false);

    // Undeclared class: fast fail-closed answer, no filesystem probe.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/introspect/instances")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "tenantId": "team",
                "workerId": "team:w",
                "classKey": "d".repeat(64),
            })
            .to_string(),
        ))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let body: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["declared"], false, "{body}");
    assert_eq!(body["instances"].as_array().unwrap().len(), 0);

    #[cfg(unix)]
    {
        // A class dir symlinked outside the tenant root is refused, never listed.
        let tenant_root = tmp.path().join("tenant2");
        std::fs::create_dir_all(tenant_root.join("team2:w2")).unwrap();
        let outside = tmp.path().join("outside2");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join(format!("{}.sqlite", "f".repeat(64))), b"x").unwrap();
        let escape_key = "9".repeat(64);
        std::os::unix::fs::symlink(&outside, tenant_root.join("team2:w2").join(&escape_key))
            .unwrap();
        let registry2 = TenantRegistry::new();
        let rt2 = TenantRuntime::new("team2");
        rt2.set_do_storage_view(Some(DoStorageView {
            tenant_dir: tenant_root,
            worker_classes: BTreeMap::from([("team2:w2".to_string(), vec![escape_key.clone()])]),
        }));
        registry2.insert("team2".into(), rt2);
        let app2 = introspect::router(registry2.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/v1/introspect/instances")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "tenantId": "team2",
                    "workerId": "team2:w2",
                    "classKey": escape_key,
                })
                .to_string(),
            ))
            .unwrap();
        let res = app2.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    // Forged shapes never reach a path.
    for bad in [
        serde_json::json!({"tenantId": "team", "workerId": "../x", "classKey": "a".repeat(64)}),
        serde_json::json!({"tenantId": "team", "workerId": "team:w", "classKey": "A".repeat(64)}),
        serde_json::json!({"tenantId": "team", "workerId": "team:w", "classKey": "a".repeat(64), "path": "/etc/passwd"}),
    ] {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/introspect/instances")
            .header("content-type", "application/json")
            .body(Body::from(bad.to_string()))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert!(
            matches!(
                res.status(),
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
            ),
            "{bad}"
        );
    }
}

#[tokio::test]
async fn alarm_introspection_reads_the_device_alarm_over_http() {
    let tmp = tempfile::tempdir().unwrap();
    let key = "a".repeat(64);
    let dir = tmp.path().join("team:w").join(&key);
    std::fs::create_dir_all(&dir).unwrap();
    let id = "b".repeat(64);
    let conn = rusqlite::Connection::open(dir.join(format!("{id}.sqlite"))).unwrap();
    conn.execute_batch(
        "CREATE TABLE _cf_METADATA (key INTEGER PRIMARY KEY, value BLOB);\
         INSERT INTO _cf_METADATA VALUES (1, 1700000000123000000);",
    )
    .unwrap();
    drop(conn);

    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("team");
    rt.set_do_storage_view(Some(DoStorageView {
        tenant_dir: tmp.path().to_path_buf(),
        worker_classes: BTreeMap::from([("team:w".to_string(), vec![key.clone()])]),
    }));
    registry.insert("team".into(), rt);

    let app = introspect::router(registry);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/introspect/alarms")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "tenantId": "team",
                "workerId": "team:w",
                "classKey": key,
            })
            .to_string(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["instances"][0]["id"], id);
    assert_eq!(body["instances"][0]["alarmAtMs"], 1_700_000_000_123u64);
    assert_eq!(body["instances"][0]["alarmKnown"], true);
}
