//! Opt-in HTTP promotion endpoint for a vrt-sql follower.
//!
//! Mounted only when `follower.promoteWriterDbRoot` is configured. It exists so
//! a cell-controller (or operator) can request the same CAS/adoption as
//! `vrt-sql promote` without shelling out. The follower process is the only
//! component that already holds the verified generation and object-store
//! credentials.
//!
//! Auth is the shared vrt-sql service HMAC (`x-vrt-sql-auth`) signed over this
//! path with no `x-vrt-epoch`/`x-vrt-seq` headers.

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

use crate::follower::{Follower, FollowerError};
use crate::persist::{PersistError, Persister};

/// Hard bound on a promote request body.
pub const MAX_BODY: usize = 4 * 1024;

#[derive(Clone)]
struct PromoteState {
    follower: Arc<Follower>,
    persister: Arc<Persister>,
    auth: crate::service::ServiceAuth,
    /// One promotion at a time. The manifest CAS is already atomic, but the
    /// local adoption copy must not race another promote on this process.
    busy: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PromoteBody {
    #[serde(default)]
    expected_epoch: Option<u64>,
    #[serde(default)]
    expected_lsn: Option<u64>,
    #[serde(default)]
    generation: Option<String>,
}

/// Router to merge into the follower listener.
pub fn router(
    follower: Arc<Follower>,
    persister: Arc<Persister>,
    auth: crate::service::ServiceAuth,
) -> axum::Router {
    axum::Router::new()
        .route("/internal/dbs/{id}/promote", post(promote))
        .with_state(PromoteState {
            follower,
            persister,
            auth,
            busy: Arc::new(tokio::sync::Mutex::new(())),
        })
}

fn err(status: StatusCode, code: &'static str, msg: &str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "success": false,
            "error": code,
            "message": msg,
        })),
    )
        .into_response()
}

async fn promote(
    State(state): State<PromoteState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if body.len() > MAX_BODY {
        return err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "VRT_PROMOTE_OVERSIZED",
            "promote body exceeds the bound",
        );
    }
    if id != state.follower.database_id() {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_PROMOTE_UNKNOWN_DB",
            "database is not replicated by this follower",
        );
    }
    let path = format!("/internal/dbs/{id}/promote");
    if !state.auth.verify(&headers, &Method::POST, &path, &body) {
        return err(
            StatusCode::UNAUTHORIZED,
            "VRT_PROMOTE_UNAUTHORIZED",
            "invalid service auth",
        );
    }
    let request: PromoteBody = if body.is_empty() {
        PromoteBody::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => {
                return err(
                    StatusCode::BAD_REQUEST,
                    "VRT_PROMOTE_MALFORMED",
                    "promote body is not valid JSON",
                )
            }
        }
    };

    let _guard = match state.busy.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            return err(
                StatusCode::CONFLICT,
                "VRT_PROMOTE_BUSY",
                "another promotion is in progress on this follower",
            )
        }
    };
    let follower = state.follower.clone();
    let persister = state.persister.clone();
    let db = follower.database_id().to_string();
    let task_db = db.clone();
    let task = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, FollowerError> {
        let plan = follower.promotion_plan()?;
        if let Some(epoch) = request.expected_epoch {
            if request.expected_lsn != Some(plan.expected_lsn) || epoch != plan.expected_epoch {
                return Err(FollowerError::Corrupt(
                    "promote expected epoch/lsn does not match the follower plan".into(),
                ));
            }
        } else if request.expected_lsn.is_some() {
            return Err(FollowerError::Corrupt(
                "promote expected epoch/lsn must be supplied together".into(),
            ));
        }
        if let Some(generation) = &request.generation {
            if generation != &plan.generation {
                return Err(FollowerError::Corrupt(
                    "promote generation does not match the follower generation".into(),
                ));
            }
        }
        let outcome = follower.promote(&persister)?;
        let mut handle = outcome.handle.clone();
        let released = persister
            .release(&task_db, &mut handle, persister.clock().now_ms())
            .is_ok();
        Ok(serde_json::json!({
            "success": true,
            "previousEpoch": outcome.previous_epoch,
            "previousLsn": outcome.previous_lsn,
            "epoch": outcome.epoch,
            "lsn": outcome.lsn,
            "resumed": outcome.resumed,
            "released": released,
        }))
    })
    .await;

    match task {
        Ok(Ok(v)) => {
            tracing::info!(db = %db, result = %v, "follower promotion committed");
            Json(v).into_response()
        }
        Ok(Err(e)) => {
            tracing::warn!(db = %db, error = %e, "follower promotion failed");
            promote_error(e)
        }
        Err(e) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "VRT_PROMOTE_FAILED",
            &format!("promote task failed: {e}"),
        ),
    }
}

fn promote_error(e: FollowerError) -> Response {
    match e {
        FollowerError::NotReady => err(
            StatusCode::CONFLICT,
            "VRT_PROMOTE_NOT_READY",
            "follower is not caught up or has no generation",
        ),
        FollowerError::Tombstoned => err(
            StatusCode::CONFLICT,
            "VRT_PROMOTE_TOMBSTONED",
            "database is tombstoned",
        ),
        FollowerError::Promotion(PersistError::ManifestChanged { .. }) => err(
            StatusCode::CONFLICT,
            "VRT_PROMOTE_MANIFEST_CHANGED",
            "manifest moved; re-sync the follower and retry",
        ),
        FollowerError::Promotion(PersistError::Tombstoned) => err(
            StatusCode::CONFLICT,
            "VRT_PROMOTE_TOMBSTONED",
            "database is tombstoned",
        ),
        FollowerError::Promotion(PersistError::InvalidGeneration(_))
        | FollowerError::Corrupt(_) => err(
            StatusCode::CONFLICT,
            "VRT_PROMOTE_INVALID",
            "follower generation or request is invalid",
        ),
        FollowerError::Promotion(PersistError::Store(_)) | FollowerError::Store(_) => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_PROMOTE_UNAVAILABLE",
            "object store unavailable",
        ),
        other => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "VRT_PROMOTE_FAILED",
            &other.to_string(),
        ),
    }
}
