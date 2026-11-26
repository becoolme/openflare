//! `cell-controller` — compute.desired cell-side orchestrator (Phase 1 T8,
//! `doc/platform/workers-runtime-cell-controller.md`).
//!
//! The controller is a pure orchestrator: every external dependency is a
//! typed trait and all tests run on in-memory implementations.
//!
//! - [`source`]: pull the latest signed `compute.desired` envelope from the
//!   control plane (`DesiredSource` — HTTP impl with timeout + bounded,
//!   jittered retries; `MemoryDesiredSource` for tests).
//! - [`verify`]: `snapshot-protocol` verification behind [`CellVerifier`]
//!   (kind/scope/expiry/signature/revision fence) with typed rejections.
//! - [`assign`]: deterministic worker→Pod assignment (rendezvous hashing)
//!   and per-Pod filtered `ComputeDesired` documents — the
//!   `vrt-host` convergence contract consumes `&ComputeDesired` unchanged.
//! - [`pod`]: `PodChannel` — delivery + readiness probe boundary.
//! - [`ack`]: `AckSink` — typed `{revision, payloadDigest, status}` reports.
//! - [`lease`]/[`cron`]: Cron leader election (`LeaseStore`) and the
//!   deduplicated, lease-gated trigger runner.
//! - [`health`]: `/healthz` + `/readyz` surface (fail-closed readiness).
//! - [`manifest`]: `ManifestApplier` — typed sink for `vrt-config`
//!   generated NetworkPolicy YAML (interface + memory impl only).
//! - [`controller`]: the reconcile/cron/probe loops tying it together.

pub mod ack;
pub mod alarm_index;
pub mod alarm_wake;
pub mod assign;
pub mod controller;
pub mod cron;
pub mod d1_auto;
pub mod d1ops;
pub mod execset;
pub mod kvops;
pub mod r2ops;
pub mod fence;
pub mod file_channel;
pub mod health;
pub mod lease;
pub mod lease_k8s;
pub mod logs;
pub mod manifest;
pub mod meter;
pub mod metrics;
pub mod pod;
pub mod prod;
pub mod queue_dispatch;
pub mod report;
pub mod retry;
pub mod source;
pub mod storagepurge;
pub mod verify;
pub mod wfops;

pub use ack::{AckReport, AckSink, AckStatus};
pub use assign::{assign_workers, filter_desired, PodAssignment};
pub use controller::{CellController, CellControllerDeps, ReconcileOutcome};
pub use file_channel::{envelope_digest_of, FileEnvelopePodChannel, FilePodSpec};
pub use health::{CellHealth, ReadyState};
pub use lease::{LeaseClaim, LeaseOutcome, LeaseStore};
pub use pod::{PodChannel, PodProbeResult};
pub use verify::{CellVerifier, ClaimedEnvelope, RejectReason};

pub mod render_inputs;
