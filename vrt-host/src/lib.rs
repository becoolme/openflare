//! `vrt-host` — per-tenant workerd supervisor, version assembly, and local
//! HTTP ingress for the Vibeploy workers runtime (Phase 1 T5,
//! `doc/platform/workers-runtime-host.md`).
//!
//! Architecture:
//! - [`spec`]: [`TenantLaunchSpec`](spec::TenantLaunchSpec) — everything the
//!   launcher needs to start one tenant generation (binary, config path,
//!   materialized worker endpoints, env whitelist, limits). Secrets travel
//!   only through `env` (`SecretValue`) — never argv/logs.
//! - [`launch`]: [`WorkerdLauncher`](launch::WorkerdLauncher) trait +
//!   [`ProcessWorkerdLauncher`](launch::ProcessWorkerdLauncher) — spawns the
//!   child with a scrubbed environment, `control-fd` on fd 3
//!   (`UnixStream::pair` + `dup2`), and its own process group.
//! - [`control`]/[`ready`]: bounded control-fd line parser behind a
//!   replaceable [`ControlEventParser`](control::ControlEventParser) trait
//!   (the real workerd event format is marked unverified in the design doc)
//!   plus the readiness check: listen events for every expected worker
//!   socket AND a successful connect probe per endpoint.
//! - [`state`]/[`backoff`]/[`monitor`]: `TenantState` machine, bounded
//!   exponential restart backoff + failure-window circuit breaker, and a
//!   portable RSS/fd sampler.
//! - [`supervisor`]: the per-tenant actor — crash restart, version switch
//!   (new generation ready → atomic route swap → drain old), graceful
//!   drain and kill, SIGTERM plumbing.
//! - [`artifact`]: content-addressed bundle cache (`BundleSource` trait;
//!   object-store source is T7/T8 scope).
//! - [`assemble`]: `compute.desired` tenant fragment + bundles →
//!   `config.capnp` + embeds + `TenantLaunchSpec`.
//! - [`route`]/[`ingress`]: `Host → (tenant, worker)` table from verified
//!   desired state and the local HTTP entry proxying to worker sockets.
//! - [`desired`]: file-backed signed snapshot store (frozen-last-good).
//! - [`config`]: typed binary configuration.
//!
//! The crate never runs a real workerd in tests — `vrt-stub-workerd`
//! (dev/test only) stands in through the same `TenantLaunchSpec` contract.
//!
//! `dev-stub` cargo feature (off by default): compiles
//! `WorkerEndpoint::Tcp`, `EmitMode::StubJson`, and the
//! `vrt-stub-workerd` binary. Integration tests enable it via the
//! crate's own dev-dependency; production builds must never enable it —
//! loopback TCP worker endpoints are reachable by any Pod process.

pub mod ae;
pub mod artifact;
pub mod assemble;
pub mod asset_sync;
pub mod assignment;
pub mod backoff;
pub mod bundle_http;
pub mod cache;
pub mod config;
pub mod control;
pub mod cron_trigger;
pub mod desired;
pub mod do_migration;
pub mod do_placement;
pub mod do_placement_store;
pub mod do_purge;
pub mod do_snapshot;
pub mod do_switch_pipeline;
pub mod do_transfer;
pub mod do_wal;
pub mod endpoint;
pub mod exclusion;
pub mod hibernate;
pub mod ingress;
pub mod initial_snapshot;
pub mod launch;
pub mod logs;
pub mod metrics;
pub mod monitor;
pub mod placement_self_check;
pub mod queue_dispatch;
pub mod ready;
pub mod render_inputs;
pub mod route;
pub mod runaway;
pub mod spec;
pub mod state;
pub mod status;
pub mod supervisor;
pub mod system_seed;
pub mod trust_store;
pub mod wf_delete;
pub mod wfops;

pub use endpoint::WorkerEndpoint;
pub use launch::{LaunchedWorkerd, ProcessWorkerdLauncher, WorkerdLauncher};
pub use spec::{ResourceLimits, TenantLaunchSpec};
pub use state::TenantState;
