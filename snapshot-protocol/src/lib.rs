//! # snapshot-protocol
//!
//! Shared signed-snapshot protocol for Vibeploy control plane → cluster
//! delivery. The cryptographic envelope semantics are an equivalent
//! reimplementation of the production
//! `postgres-tenant-gateway/src/config_protocol.rs` protocol (Ed25519 over
//! canonical JSON, sha256 payload digest, key-id bound to the base64 SPKI
//! text, canonical `...Z` millisecond timestamps, bounded lifetime and
//! clock skew), generalized to a `kind`/`scope`-addressed envelope.
//!
//! - [`envelope`]: [`sign`](envelope::sign), [`verify`](envelope::verify),
//!   [`RevisionGuard`](envelope::RevisionGuard), key types — the generic
//!   signed-envelope layer.
//! - [`compute_desired`]: the `compute.desired` payload schema — the
//!   desired-state document a runtime cell consumes — with
//!   [`verify_compute_desired`](compute_desired::verify_compute_desired)
//!   as the full fail-closed entry point.
//! - [`error`]: [`SnapshotError`](error::SnapshotError), a typed enum whose
//!   stable `code()` strings never carry key material.
//!
//! Design doc: `doc/platform/workers-runtime-snapshot-protocol.md`.
//! Golden-vector equivalence with the gateway is asserted in tests using
//! `shared/postgres-tenant-gateway-hot-config-protocol-golden.json`.

pub mod compute_desired;
pub mod envelope;
pub mod error;
pub mod preview_token;
#[cfg(test)]
mod golden_fixture;
mod strict;

pub use envelope::{
    compute_envelope_digest, sign, verify, ExpectedScope, RevisionGuard, SignParams,
    SignedSnapshot, SigningKey, TrustedKey, Verified,
};
pub use error::SnapshotError;
