//! `vrt-provider` — typed Workers binding provider implementing the
//! facade-facing private HTTP binding-backend protocol subset (KV + R2),
//! with authorization backed by verified `compute.desired` snapshots.
//!
//! Architecture:
//! - [`context`]: `BindingContext` — sealed, authorizer-only request scope.
//! - [`error`]: `ProviderError` — typed errors mapped to the upstream
//!   binding-backend error response shape and `x-open-compute-error-code`.
//! - [`desired`]: `DesiredStore` trait + in-memory/file implementations.
//! - [`authorize`]: snapshot-bound request authorization.
//! - [`kv`], [`r2`], [`pg`]: typed async provider traits + backends.
//! - [`server`]: axum dispatch of `/internal/bindings/v1/{kv,r2}/...`.
//! - [`unixsock`]: per-tenant unix listener hardening and serving.
//! - [`config`]: typed provider configuration for the binary.

pub mod assets;
pub mod assets_headers;
pub mod assets_redirects;
pub mod assets_routes;
pub mod authorize;
pub mod cli;
pub mod config;
pub mod context;
pub mod cursor;
pub mod d1;
pub mod desired;
pub mod desired_file;
pub mod encoding;
pub mod error;
pub mod identity;
pub mod initial_snapshot;
pub mod kv;
pub mod limits;
pub mod listeners;
pub mod metrics;
pub mod pg;
pub mod pgproxy;
pub mod purge;
pub mod queue;
pub mod k2;
pub mod quota;
pub mod r2;
pub mod server;
pub mod unixsock;
pub(crate) mod upstream_body;

pub use authorize::Authorizer;
pub use context::{BindingContext, BindingResource, Permissions, ResourceKind};
pub use cursor::CursorSigner;
pub use desired::{DesiredStore, MemoryDesiredStore, TenantView, VersionPin, WorkerView};
pub use desired_file::FileDesiredStore;
pub use error::ProviderError;
pub use server::TenantService;
