//! Read-only instance introspection for the control-plane stats report.
//!
//! The cell controller knows which `(tenant, worker, classKey)` pairs the
//! signed desired declares; it asks each pod for the sqlite instance files in
//! that class directory. This is the runtime-side instance index the console
//! needs for Workflow/Durable Object listings — the host already owns the
//! directory layout (`<tenant_dir>/<workerId>/<uniqueKey>/<hex>.sqlite`), so no
//! new registry or index is required.
//!
//! Trust model: same loopback control listener as `/v1/wfops` and
//! `/v1/queue/dispatch`; identity is resolved through the serving tenant's
//! `DoStorageView`, never from a path in the request. A class that is not
//! declared for that worker answers `declared:false` (not a filesystem probe).

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

use crate::supervisor::TenantRegistry;

/// One sqlite file in a class directory.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InstanceFile {
    /// File stem — the engine's instance id (workerd `localDisk` layout).
    pub id: String,
    /// Main `.sqlite` file bytes.
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InstancesView {
    pub tenant_id: String,
    pub worker_id: String,
    pub class_key: String,
    /// `false` when the serving desired does not declare this class (no probe).
    pub declared: bool,
    pub instances: Vec<InstanceFile>,
    /// All regular-file bytes in the class dir (main + wal/shm sidecars).
    pub storage_bytes: u64,
    /// Set when the directory scan hit [`MAX_SCAN_ENTRIES`]/[`MAX_INSTANCES`].
    pub truncated: bool,
    pub sampled_at_ms: i64,
}

/// Request from the cell controller. Shapes are validated before any path is
/// derived; `deny_unknown_fields` keeps a forged `path`/`prefix` out.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstancesQuery {
    pub tenant_id: String,
    pub worker_id: String,
    pub class_key: String,
}

/// Bounds: a runaway class dir must never stall the control listener.
pub const MAX_SCAN_ENTRIES: usize = 50_000;
pub const MAX_INSTANCES: usize = 10_000;

fn valid_tenant(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn valid_worker(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 160
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn valid_class_key(key: &str) -> bool {
    key.len() == 64
        && key
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn router(registry: Arc<TenantRegistry>) -> axum::Router {
    axum::Router::new()
        .route("/v1/introspect/instances", post(instances))
        .with_state(registry)
}

async fn instances(
    State(registry): State<Arc<TenantRegistry>>,
    Json(query): Json<InstancesQuery>,
) -> Response {
    if !valid_tenant(&query.tenant_id)
        || !valid_worker(&query.worker_id)
        || !valid_class_key(&query.class_key)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": {"code": "invalid_request"}})),
        )
            .into_response();
    }
    let Some(runtime) = registry.get(&query.tenant_id) else {
        return Json(empty(&query, false)).into_response();
    };
    let Some(view) = runtime.do_storage_view() else {
        return Json(empty(&query, false)).into_response();
    };
    let Some(dir) = view.class_dir(&query.worker_id, &query.class_key) else {
        return Json(empty(&query, false)).into_response();
    };
    match scan_class_dir(&dir) {
        Ok((instances, storage_bytes, truncated)) => Json(InstancesView {
            tenant_id: query.tenant_id,
            worker_id: query.worker_id,
            class_key: query.class_key,
            declared: true,
            instances,
            storage_bytes,
            truncated,
            sampled_at_ms: now_ms(),
        })
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": {"code": "scan_failed", "error": error}})),
        )
            .into_response(),
    }
}

fn empty(query: &InstancesQuery, declared: bool) -> InstancesView {
    InstancesView {
        tenant_id: query.tenant_id.clone(),
        worker_id: query.worker_id.clone(),
        class_key: query.class_key.clone(),
        declared,
        instances: Vec::new(),
        storage_bytes: 0,
        truncated: false,
        sampled_at_ms: now_ms(),
    }
}

/// Scan one class dir. Hard bounds keep a runaway directory from holding the
/// listener; a missing dir is an empty class, not an error.
fn scan_class_dir(dir: &Path) -> Result<(Vec<InstanceFile>, u64, bool), String> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0, false)),
        Err(e) => return Err(e.to_string()),
    };
    let mut instances = Vec::new();
    let mut storage_bytes = 0u64;
    let mut seen = 0usize;
    let mut truncated = false;
    for entry in rd.flatten() {
        seen += 1;
        if seen > MAX_SCAN_ENTRIES {
            truncated = true;
            break;
        }
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_file() {
            continue;
        }
        storage_bytes = storage_bytes.saturating_add(meta.len());
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id) = name.strip_suffix(".sqlite") {
            if instances.len() >= MAX_INSTANCES {
                truncated = true;
                continue;
            }
            instances.push(InstanceFile {
                id: id.to_string(),
                bytes: meta.len(),
            });
        }
    }
    instances.sort_by(|a, b| a.id.cmp(&b.id));
    Ok((instances, storage_bytes, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::{DoStorageView, TenantRuntime};
    use std::collections::BTreeMap;

    fn view(tenant_dir: std::path::PathBuf, worker: &str, key: &str) -> DoStorageView {
        DoStorageView {
            tenant_dir,
            worker_classes: BTreeMap::from([(worker.to_string(), vec![key.to_string()])]),
        }
    }

    #[test]
    fn scans_files_and_bounds_instances() {
        let tmp = tempfile::tempdir().unwrap();
        let key = "a".repeat(64);
        let dir = tmp.path().join("team:w").join(&key);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("b".repeat(64) + ".sqlite"), b"12345").unwrap();
        std::fs::write(dir.join("a".repeat(64) + ".sqlite"), b"123").unwrap();
        std::fs::write(dir.join("a".repeat(64) + ".sqlite-wal"), b"wal").unwrap();
        std::fs::write(dir.join("notes.txt"), b"x").unwrap();

        let (instances, bytes, truncated) = scan_class_dir(&dir).unwrap();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].id, "a".repeat(64));
        assert_eq!(instances[0].bytes, 3);
        assert_eq!(instances[1].id, "b".repeat(64));
        assert!(!truncated);
        // main files + wal + notes
        assert_eq!(bytes, 3 + 5 + 3 + 1);
        // missing dir is an empty class, never an error
        let (none, zero, _) = scan_class_dir(&tmp.path().join("absent")).unwrap();
        assert!(none.is_empty());
        assert_eq!(zero, 0);
    }

    #[test]
    fn declared_lookup_is_authoritative() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = TenantRegistry::new();
        let rt = TenantRuntime::new("team");
        let key = "c".repeat(64);
        rt.set_do_storage_view(Some(view(tmp.path().to_path_buf(), "team:w", &key)));
        registry.insert("team".into(), rt);

        let got = registry.get("team").unwrap();
        let v = got.do_storage_view().unwrap();
        assert!(v.class_dir("team:w", &key).is_some());
        assert!(v.class_dir("team:w", &"d".repeat(64)).is_none());
        assert!(v.class_dir("other:w", &key).is_none());
    }
}
