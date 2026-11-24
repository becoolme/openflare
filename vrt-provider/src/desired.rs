//! `DesiredStore` — read-side view of the currently authorized
//! `compute.desired` snapshot per tenant, plus an in-memory
//! implementation used by tests and as a reference for the file store.
//!
//! A snapshot is served until its `expiresAt` is past, then every
//! [`tenant_view`](DesiredStore::tenant_view) call fails closed with
//! [`DesiredUnavailable::Expired`].

use crate::context::{BindingResource, Permissions};
use chrono::{DateTime, Utc};
use snapshot_protocol::compute_desired::{BindingDesired, ComputeDesired};
use snapshot_protocol::envelope::Verified;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

/// A version pin: version id plus the artifact/descriptor digest the
/// facade must echo in `x-open-compute-descriptor-sha256`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionPin {
    /// `VersionDesired.versionId`.
    pub version_id: String,
    /// `VersionDesired.artifactDigest` (lowercase sha256 hex).
    pub artifact_digest: String,
}

/// Per-worker authorization view.
#[derive(Clone, Debug)]
pub struct WorkerView {
    /// Currently active version.
    pub active: VersionPin,
    /// Previously-active versions still inside their drain grace window,
    /// newest first.
    pub draining: Vec<VersionPin>,
    /// `binding name -> typed resource` table from the snapshot.
    pub bindings: HashMap<String, BindingResource>,
    /// `binding name -> permissions` table from the snapshot.
    pub permissions: HashMap<String, Permissions>,
}

/// Per-tenant authorization view.
#[derive(Clone, Debug)]
pub struct TenantView {
    /// Snapshot revision this view was built from.
    pub revision: u64,
    /// Snapshot expiry; checked again by the store on every lookup.
    pub expires_at: DateTime<Utc>,
    /// `workerId -> worker view`.
    pub workers: HashMap<String, WorkerView>,
}

/// Why desired state could not be served. Maps to fail-closed 503s.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DesiredUnavailable {
    /// No usable snapshot is loaded for this cell/tenant.
    #[error("desired snapshot missing")]
    Missing,
    /// The loaded snapshot's `expiresAt` is in the past.
    #[error("desired snapshot expired")]
    Expired,
}

/// Authorization-time view of the verified desired state.
pub trait DesiredStore: Send + Sync + fmt::Debug {
    /// Return the tenant view or fail closed. `now` is injected so tests
    /// control the expiry check deterministically.
    fn tenant_view(
        &self,
        tenant_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Arc<TenantView>, DesiredUnavailable>;
}

/// Build fresh [`TenantView`]s from one snapshot and roll the grace set:
/// versions active under the previous snapshot stay authorized until they
/// age out of `grace_cap` subsequent snapshots.
pub(crate) fn build_views(
    desired: &ComputeDesired,
    expires_at: DateTime<Utc>,
    previous: Option<&HashMap<String, Arc<TenantView>>>,
    grace_cap: usize,
) -> HashMap<String, Arc<TenantView>> {
    let mut views = HashMap::with_capacity(desired.tenants.len());
    for tenant in &desired.tenants {
        let old_tenant = previous.and_then(|map| map.get(&tenant.tenant_id));
        let mut workers = HashMap::with_capacity(tenant.workers.len());
        for worker in &tenant.workers {
            let old_worker = old_tenant.and_then(|t| t.workers.get(&worker.worker_id));
            let mut draining: VecDeque<VersionPin> = old_worker
                .map(|w| w.draining.iter().cloned().collect())
                .unwrap_or_default();
            let active = VersionPin {
                version_id: worker.active_version.version_id.clone(),
                artifact_digest: worker.active_version.artifact_digest.clone(),
            };
            if let Some(old) = old_worker.map(|w| &w.active) {
                if *old != active && !draining.iter().any(|p| p == old) {
                    draining.push_front(old.clone());
                }
            }
            draining.retain(|p| p != &active);
            while draining.len() > grace_cap {
                draining.pop_back();
            }
            let mut bindings = HashMap::with_capacity(worker.bindings.len());
            let mut permissions = HashMap::with_capacity(worker.bindings.len());
            for binding in &worker.bindings {
                bindings.insert(binding.name().to_string(), binding_resource(binding));
                // The grant comes verbatim from the signed snapshot — never
                // widened here. Bindings without a provider data plane
                // (var/secret/service) carry no row; a lookup miss defaults
                // to deny-all in the authorizer.
                if let Some(grant) = binding.permissions() {
                    permissions.insert(
                        binding.name().to_string(),
                        Permissions {
                            read: grant.read,
                            write: grant.write,
                        },
                    );
                }
            }
            workers.insert(
                worker.worker_id.clone(),
                WorkerView {
                    active,
                    draining: draining.into_iter().collect(),
                    bindings,
                    permissions,
                },
            );
        }
        views.insert(
            tenant.tenant_id.clone(),
            Arc::new(TenantView {
                revision: desired.revision,
                expires_at,
                workers,
            }),
        );
    }
    views
}

fn binding_resource(binding: &BindingDesired) -> BindingResource {
    match binding {
        BindingDesired::Kv(b) => BindingResource::KvNamespace {
            id: b.namespace_id.clone(),
        },
        BindingDesired::R2(b) => BindingResource::R2Bucket {
            name: b.bucket.clone(),
        },
        BindingDesired::Pg(b) => BindingResource::PgDatabase {
            id: b.database_id.clone(),
            credential_ref: b.credential_ref.clone(),
        },
        BindingDesired::Assets(b) => BindingResource::AssetsManifest {
            digest: b.manifest_digest.clone(),
        },
        BindingDesired::D1(b) => BindingResource::D1Database {
            id: b.database_id.clone(),
            epoch: b.epoch,
            writer_addr: b.writer_addr.clone(),
        },
        BindingDesired::Queue(b) => BindingResource::Queue {
            id: b.queue_id.clone(),
        },
        BindingDesired::K2(b) => BindingResource::K2Stream {
            id: b.stream_id.clone(),
        },
        BindingDesired::SendEmail(b) => BindingResource::SendEmail {
            name: b.name.clone(),
            destination_address: b.destination_address.clone(),
            allowed_destination_addresses: b.allowed_destination_addresses.clone(),
            allowed_sender_addresses: b.allowed_sender_addresses.clone(),
        },
        BindingDesired::Vectorize(b) => BindingResource::VectorizeIndex {
            id: b.index_id.clone(),
            dimensions: b.dimensions,
            metric: b.metric.clone(),
        },
        // Var/secret/service bindings have no provider-backed resource;
        // they never reach this protocol, so map them to a sentinel
        // that always mismatches on resource comparison.
        other => BindingResource::KvNamespace {
            id: format!("\0unbound:{}", other.name()),
        },
    }
}

/// In-memory desired store. Holds the latest applied snapshot plus the
/// derived per-tenant views and drain-grace bookkeeping.
#[derive(Debug)]
pub struct MemoryDesiredStore {
    state: Mutex<Option<StoreState>>,
    grace_cap: usize,
}

#[derive(Debug)]
pub(crate) struct StoreState {
    pub(crate) views: HashMap<String, Arc<TenantView>>,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) revision: u64,
    /// The verified snapshot payload — consumers that need the full
    /// desired state (vrt-host convergence, route table) read it through
    /// `snapshot()`; signature verification is never repeated.
    pub(crate) desired: Arc<ComputeDesired>,
    /// T14/B1: `payloadDigest` of the verified envelope — vrt-host binds
    /// its assignment file to this digest (fail closed on mismatch).
    pub(crate) payload_digest: String,
}

impl MemoryDesiredStore {
    /// Empty store — every lookup fails closed until [`apply`] is called.
    pub fn new(grace_cap: usize) -> Self {
        Self {
            state: Mutex::new(None),
            grace_cap,
        }
    }

    /// Install a verified snapshot: derive tenant views and roll grace
    /// sets. Rejects revisions that do not advance the current one.
    pub fn apply(&self, verified: &Verified<ComputeDesired>) -> Result<u64, DesiredApplyError> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let current = guard.as_ref().map(|s| s.revision).unwrap_or(0);
        if verified.payload.revision <= current {
            return Err(DesiredApplyError::NonMonotonic {
                current,
                attempted: verified.payload.revision,
            });
        }
        let views = build_views(
            &verified.payload,
            verified.expires_at,
            guard.as_ref().map(|s| &s.views),
            self.grace_cap,
        );
        *guard = Some(StoreState {
            views,
            expires_at: verified.expires_at,
            revision: verified.payload.revision,
            desired: Arc::new(verified.payload.clone()),
            payload_digest: verified.payload_digest.clone(),
        });
        Ok(verified.payload.revision)
    }

    /// Current snapshot revision, if any.
    pub fn revision(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.revision)
    }
}

impl Default for MemoryDesiredStore {
    fn default() -> Self {
        Self::new(1)
    }
}

impl DesiredStore for MemoryDesiredStore {
    fn tenant_view(
        &self,
        tenant_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Arc<TenantView>, DesiredUnavailable> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let state = guard.as_ref().ok_or(DesiredUnavailable::Missing)?;
        if now >= state.expires_at {
            return Err(DesiredUnavailable::Expired);
        }
        state
            .views
            .get(tenant_id)
            .cloned()
            .ok_or(DesiredUnavailable::Missing)
    }
}

/// [`MemoryDesiredStore::apply`] failure modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DesiredApplyError {
    /// Snapshot revision did not advance past the loaded one.
    #[error("non-monotonic revision: current {current}, attempted {attempted}")]
    NonMonotonic {
        /// Currently loaded revision.
        current: u64,
        /// Revision that was rejected.
        attempted: u64,
    },
}
