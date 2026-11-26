//! `of check` — pre-flight "can this worker deploy" validation.
//!
//! Runs the identical pipeline a real deploy runs — wrangler/vrt.toml
//! normalization, pack assembly (module table, binding projection, DO
//! context), bundle materialization — then wraps the packed worker in
//! a synthetic one-tenant `ComputeDesired` and calls the SAME
//! `ComputeDesired::validate` a cell applies to every snapshot. The
//! report therefore covers every deploy-blocking check that is
//! computable without live state.
//!
//! Honest limits (state-dependent, unverifiable offline):
//! - `service` binding targets resolve against deployed workers —
//!   `assemble` fails closed on a miss, so a worker dir that binds a
//!   sibling reports "unknown worker" rather than "probably fine".
//! - Secret *values* (`[secrets]` refs, secrets-store derived refs)
//!   are not provisioned here — declared refs are shape-checked but
//!   existence is verified at real deploy time.
//! - Cell capability admission (ACK bits) is server-side only.

use std::path::Path;

use serde::Serialize;
use snapshot_protocol::compute_desired::{
    BindingDesired, ComputeDesired, RouteDesired, TenantDesired, VersionDesired, WorkerDesired,
};

use crate::state::LocalState;
use crate::worker_pkg::{assemble, gather_dir_for_env_preview_sandbox, DeployOverrides};
use crate::LocalError;

/// Synthetic ids for the validation snapshot — identifier-charset
/// clean, never persisted.
const CHECK_CELL: &str = "check-cell";
const CHECK_TENANT: &str = "check-tenant";

/// Machine-readable `of check` report.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckReport {
    /// True when every stage the check can evaluate passed.
    pub ok: bool,
    /// Binding names + types that survived projection, e.g.
    /// `kv:KV`, `versionMetadata:CF_VERSION_METADATA`.
    pub bindings: Vec<String>,
    /// Files gathered into the pack (main + discovered modules,
    /// rules-typed text/data/wasm, assets excluded).
    pub files: usize,
    /// Content-addressed version id a real deploy of this exact
    /// content would mint — lets CI see whether a change would roll
    /// a new version.
    pub version_id: String,
}

/// Run the full offline-deployable validation for `worker_dir`.
/// `overrides` carries `--env`/`--var`/`--tag` the same way deploy
/// consumes them, so the checked artifact is byte-identical to what
/// a deploy would produce.
pub fn check_worker(
    worker_dir: &Path,
    overrides: &DeployOverrides,
) -> Result<CheckReport, LocalError> {
    let g = gather_dir_for_env_preview_sandbox(
        worker_dir,
        overrides.env_name.as_deref(),
        overrides.is_preview,
        overrides.config_sandbox,
    )?;
    let files = g.files.len();
    let pkg = assemble(g, overrides, CHECK_TENANT, &|_| None, None)?;

    // Synthetic snapshot around the packed worker — the same
    // validator a cell runs on every signed snapshot. The real
    // versionId/digest are reused so format checks exercise
    // production values, not placeholders.
    let desired = ComputeDesired {
        schema_version: snapshot_protocol::compute_desired::SCHEMA_VERSION,
        cell_id: CHECK_CELL.to_string(),
        revision: 1,
        tenants: vec![TenantDesired {
            tenant_id: CHECK_TENANT.to_string(),
            workers: vec![WorkerDesired {
                cache: None,
                worker_id: format!("check-{}", pkg.name),
                name: pkg.name.clone(),
                active_version: VersionDesired {
                    version_id: LocalState::version_id_for_check(&pkg),
                    artifact_digest: pkg.digest.clone(),
                    compatibility_date: pkg.compatibility_date.clone(),
                    compatibility_flags: pkg.compatibility_flags.clone(),
                    tag: overrides.tag.clone().unwrap_or_default(),
                    created_at: chrono::Utc::now().to_rfc3339(),
                },
                bindings: pkg.bindings.clone(),
                // `of check` is a validation snapshot, not a preview
                // deploy — `cloudflare.config.*` evaluates with
                // isPreview = false.
                preview: None,
                routes: overrides
                    .hostnames
                    .iter()
                    .map(|h| RouteDesired {
                        hostname: h.clone(),
                    })
                    .collect(),
                crons: pkg.crons.clone(),
                queue_consumers: pkg.queue_consumers.clone(),
                observability: pkg.observability.clone(),
                streaming_tail_consumers: pkg.streaming_tail_consumers.clone(),
                durable_object_classes: pkg.durable_object_classes.clone(),
                workflows: pkg.workflows.clone(),
                tail_consumers: pkg.tail_consumers.clone(),
                quotas: pkg.quotas.clone(),
            }],
            flagship_apps: Default::default(),
        }],
        deletions: Vec::new(),
    };
    desired
        .validate()
        .map_err(|e| LocalError::WorkerOwned(format!("desired: {e}")))?;

    let bindings = pkg.bindings.iter().map(binding_label).collect();
    Ok(CheckReport {
        ok: true,
        bindings,
        files,
        version_id: LocalState::version_id_for_check(&pkg),
    })
}

fn binding_label(b: &BindingDesired) -> String {
    let ty = serde_json::to_value(b)
        .ok()
        .and_then(|v| v.get("type").and_then(|t| t.as_str().map(String::from)))
        .unwrap_or_else(|| "unknown".to_string());
    format!("{ty}:{}", b.name())
}
