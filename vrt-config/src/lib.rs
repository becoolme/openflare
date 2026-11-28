//! # vrt-config
//!
//! Deployment manifest → workerd static `config.capnp` generation plus
//! content-addressed version artifacts (Phase 1 T2 of the workers
//! runtime design, `doc/platform/workers-runtime-config.md`).
//!
//! - [`manifest`]: [`DeployManifest`](manifest::DeployManifest), the
//!   typed form of `packWranglerProject` output — normalized module list,
//!   validation, and the wire→typed conversion
//!   ([`DeployManifest::from_pack`](manifest::DeployManifest::from_pack)).
//! - [`bundle`]: [`VersionBundle`](bundle::VersionBundle), the
//!   immutable content-addressed artifact —
//!   `digest = sha256(canonical manifest)`, object key
//!   `workers/v1/bundles/<digest>`, self-verifying stored document.
//! - [`capnp`]: [`render_tenant_config`](capnp::render_tenant_config) —
//!   one tenant's `compute.desired` fragment + bundles + runtime params
//!   → deterministic workerd config text, its digest, and the
//!   materialization contract (`embeds`, `external_addrs`).

pub mod bundle;
pub mod cachectl;
mod canonical;
pub mod capnp;
pub mod check;
pub mod compat_flags;
pub mod egress;
pub mod error;
mod escape;
pub mod heartbeat;
pub mod launch_args;
pub mod logs;
pub mod manifest;
pub mod netpolicy;
pub mod sysmanifest;
pub mod waituntil;
pub mod workflows;

pub use bundle::{ModuleDigest, VersionBundle, BUNDLE_KEY_PREFIX, SCHEMA_VERSION};
pub use capnp::{
    ensure_quotas_enforceable, render_tenant_config, resolve_secret, resolve_tenant_env,
    resolve_var, secret_env_name, EmbedKind, EmbedRef, ExternalAddr, FacadeRefs, RenderedConfig,
    RuntimeParams, SecretValue, ServiceEndpoint, SystemBinding, SystemDiskService, SystemModuleRef,
    SystemOutbound, SystemWorkerRef, TenantRenderInput, BINDING_BACKEND_SERVICE, BUNDLE_DIR_PREFIX,
    CACHE_SERVICE, MAX_VALUE_LEN, OUTBOUND_NETWORK_SERVICE, SYS_DIR_PREFIX,
};
pub use compat_flags::{is_experimental_compat_flag, is_tenant_allowed_compat_flag};
pub use egress::{EgressAllowEntry, EgressNameRule, EgressPolicy, EGRESS_POLICY_BINDING};
pub use error::{BundleError, ConfigError};
pub use launch_args::{
    experimental_extra_args, runtime_experimental_extra_args, tenant_experimental_extra_args,
    ExperimentalNeeds,
};
pub use logs::{log_token, tail_service_name, LogTailParams, LOGSINK_SERVICE};
pub use manifest::{
    valid_compat_date, valid_compat_flag, DataModule, DeployManifest, Module, ModuleType,
    TextModule,
};
pub use netpolicy::{
    render_network_policy, Cidr, EgressBackend, NetPort, Protocol, RuntimeNetPolicy, SelectorPeer,
};
pub use sysmanifest::{
    parse_facade_manifest, resolve_disk_paths, FacadeManifest, FACADE_MANIFEST_SCHEMA_VERSION,
};
