//! The authorizer: binds one request to the verified `compute.desired`
//! state and mints a sealed [`BindingContext`].
//!
//! Checks, in order (all denials → uniform `BINDING_PERMISSION_DENIED`
//! with a structured-log reason):
//! 1. desired snapshot present and unexpired (else fail closed 503)
//! 2. `worker-id` header equals the binding-id owner
//! 3. optional `binding-id` header equals the path binding id
//! 4. worker exists under the *listener* tenant
//! 5. version id + descriptor digest match active or a draining pin
//! 6. binding exists, kind matches the requested product
//! 7. `namespaceResourceId` equals the authorized resource id
//! 8. `resourceSpecGeneration <= snapshot revision`
//!
//! Tenant identity comes from the unix listener — never from headers.

use crate::context::{BindingContext, ResourceKind};
use crate::desired::{DesiredStore, DesiredUnavailable};
use crate::error::{ForbiddenReason, ProviderError};
use crate::identity::RequestIdentity;
use chrono::{DateTime, Utc};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Authorizes requests against the current desired state.
pub struct Authorizer {
    store: Arc<dyn DesiredStore>,
    operation_timeout: Duration,
    clock: fn() -> DateTime<Utc>,
}

impl fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authorizer")
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl Authorizer {
    /// Create an authorizer over `store` with a per-request operation
    /// timeout used as the [`BindingContext`] deadline.
    pub fn new(store: Arc<dyn DesiredStore>, operation_timeout: Duration) -> Self {
        Self {
            store,
            operation_timeout,
            clock: Utc::now,
        }
    }

    /// Inject a wall clock (tests).
    pub fn with_clock(mut self, clock: fn() -> DateTime<Utc>) -> Self {
        self.clock = clock;
        self
    }

    /// Authorize one parsed identity for `kind` on `tenant_id` (listener
    /// tenant). Returns the sealed context or a typed error.
    pub fn authorize(
        &self,
        tenant_id: &str,
        identity: &RequestIdentity,
        binding_id: &str,
        kind: ResourceKind,
    ) -> Result<BindingContext, ProviderError> {
        let now = (self.clock)();
        let view = match self.store.tenant_view(tenant_id, now) {
            Ok(view) => view,
            Err(DesiredUnavailable::Missing | DesiredUnavailable::Expired) => {
                tracing::warn!(
                    tenant = tenant_id,
                    "binding request denied: desired snapshot unavailable"
                );
                return Err(ProviderError::ResourceUnavailable);
            }
        };

        let deny = |reason: ForbiddenReason| {
            tracing::warn!(
                tenant = tenant_id,
                worker = %identity.binding_worker_id,
                binding = %identity.binding_name,
                reason = reason.as_str(),
                "binding request denied"
            );
            ProviderError::Forbidden(reason)
        };

        if identity.worker_id_header != identity.binding_worker_id {
            return Err(deny(ForbiddenReason::WorkerIdMismatch));
        }
        if let Some(header_id) = &identity.binding_id_header {
            if header_id != binding_id {
                return Err(deny(ForbiddenReason::BindingIdMismatch));
            }
        }
        let worker = view
            .workers
            .get(&identity.binding_worker_id)
            .ok_or_else(|| deny(ForbiddenReason::WorkerNotFound))?;
        let Some(pin) = std::iter::once(&worker.active)
            .chain(worker.draining.iter())
            .find(|p| p.version_id == identity.version_id)
        else {
            return Err(deny(ForbiddenReason::VersionMismatch));
        };
        if pin.artifact_digest != identity.descriptor_sha256 {
            return Err(deny(ForbiddenReason::DigestMismatch));
        }
        let resource = worker
            .bindings
            .get(&identity.binding_name)
            .ok_or_else(|| deny(ForbiddenReason::BindingNotFound))?;
        if resource.kind() != kind {
            return Err(deny(ForbiddenReason::BindingKindMismatch));
        }
        if resource.resource_id() != identity.namespace_resource_id {
            return Err(deny(ForbiddenReason::ResourceMismatch));
        }
        if identity.spec_generation > view.revision {
            return Err(deny(ForbiddenReason::SpecGenerationMismatch));
        }
        let permissions = worker
            .permissions
            .get(&identity.binding_name)
            .copied()
            .unwrap_or_default();
        Ok(BindingContext::grant(
            tenant_id.to_string(),
            identity.binding_worker_id.clone(),
            identity.version_id.clone(),
            binding_id.to_string(),
            identity.binding_name.clone(),
            resource.clone(),
            permissions,
            identity.spec_generation,
            view.revision,
            Instant::now() + self.operation_timeout,
        ))
    }
}
