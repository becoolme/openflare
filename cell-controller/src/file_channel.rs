//! `FileEnvelopePodChannel` — T14/B1 production pod transport for the
//! MVP same-node/shared-volume layout: each `vrt-host` pod owns one
//! delivery directory; the controller writes `envelope.json` (raw
//! signed bytes) then `assignment.json` (typed [`PodAssignment`]) with
//! tmp+fsync+rename so the host never sees a torn pair. The host only
//! adopts the pair when `assignment.envelopeDigest` matches the
//! envelope file's sha256, so an intermediate state (envelope written,
//! assignment not yet) is ignored and retried on the next tick.
//!
//! Probes hit the pod's `GET /readyz` over loopback HTTP — bounded
//! body, hard timeout, typed JSON. Unix-socket probing is a follow-up
//! (reqwest lacks the transport here; noted as unverified in the doc).

use crate::assign::PodAssignment;
use crate::pod::{PodApplyResult, PodChannel, PodError, PodProbeResult, TenantFailure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Largest `/readyz` body accepted — bigger is a protocol violation.
const MAX_PROBE_BYTES: usize = 64 * 1024;
/// Largest signed envelope the channel will write (matches the
/// control-plane publication bound).
const MAX_ENVELOPE_BYTES: usize = 4 * 1024 * 1024;

/// One pod's delivery directory + probe endpoint (config `[[pods]]`).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePodSpec {
    /// Pod identity — must match `PodAssignment.pod_id` consumers.
    pub id: String,
    /// Delivery directory the pod's vrt-host polls. MUST be absolute,
    /// exist, be `0700`, and owned by the pod uid (or root in dev).
    pub dir: String,
    /// `GET /readyz` base, e.g. `http://127.0.0.1:9091` — loopback only.
    pub readyz: String,
}

/// File-based pod channel; construction validates every pod dir +
/// probe endpoint up front (fail closed — a bad spec aborts startup).
pub struct FileEnvelopePodChannel {
    pods: BTreeMap<String, PodTarget>,
    timeout: Duration,
    client: reqwest::Client,
}

struct PodTarget {
    dir: PathBuf,
    readyz: String,
}

/// Minimal typed surface of the pod's `/readyz` response — fields the
/// host may add in future versions are ignored (rolling-upgrade
/// compatible); every VALUE the controller consumes is re-validated.
#[derive(Debug, Deserialize)]
struct PodReadyz {
    ready: bool,
    revision: Option<u64>,
    #[serde(default, rename = "payloadDigest")]
    payload_digest: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    /// MIG-1: absent on pre-MIG-1 hosts — treated as `[]`, never as
    /// pod-level evidence.
    #[serde(default, rename = "tenantFailures")]
    tenant_failures: Vec<RawTenantFailure>,
}

/// Raw tenant-failure entry off the wire — sanitized before it can
/// reach an ACK (codes whitelisted, strings bounded).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTenantFailure {
    tenant: String,
    #[serde(default)]
    worker_ids: Vec<String>,
    code: String,
    #[serde(default)]
    detail: Option<String>,
}

/// Whitelisted typed reasons a pod may report — anything else is
/// collapsed to `pods-not-applied` so free-form pod strings can never
/// flow into an ACK reason field.
const POD_REASON_WHITELIST: &[&str] = &[
    "assignment-mismatch",
    "assignment-incomplete",
    "snapshot-unavailable",
    "workers-not-ready",
    "switch-failed",
    "egress-policy-unavailable",
    "bundle-unavailable",
    "render-inputs-unavailable",
    // Informational only (ready=true): scale-to-zero tenant hibernated,
    // wakes on demand. Never a failure — see `pod::is_informational_reason`.
    "hibernated",
];

fn sanitize_reason(reason: Option<String>) -> Option<String> {
    reason.filter(|r| POD_REASON_WHITELIST.contains(&r.as_str()))
}

#[cfg(test)]
mod reason_tests {
    use super::*;

    #[test]
    fn hibernated_passes_whitelist() {
        assert_eq!(
            sanitize_reason(Some("hibernated".into())).as_deref(),
            Some("hibernated")
        );
    }

    #[test]
    fn freeform_and_failure_reasons_unchanged() {
        assert_eq!(sanitize_reason(Some("boom: secret".into())), None);
        assert_eq!(sanitize_reason(None), None);
        assert_eq!(
            sanitize_reason(Some("switch-failed".into())).as_deref(),
            Some("switch-failed")
        );
    }

    #[test]
    fn readyz_ready_true_with_hibernated_parses() {
        let r: PodReadyz = serde_json::from_str(
            r#"{"ready":true,"revision":3,"payloadDigest":"d","reason":"hibernated"}"#,
        )
        .unwrap();
        assert!(r.ready);
        assert_eq!(sanitize_reason(r.reason).as_deref(), Some("hibernated"));
    }
}

/// MIG-1: tenant-failure codes the controller forwards verbatim —
/// anything else collapses to `unknown-worker-failure` (kept, never
/// dropped: a failure the cell cannot name is still a failure).
const TENANT_FAILURE_CODES: &[&str] = &[
    "switch-failed",
    "crashed",
    "circuit-open",
    "launch-failed",
    // MIG-4: attributed config-load failure — worker excluded from the
    // rebuilt generation (or blamed for the latch).
    "config-load",
    "bundle-unavailable",
    "isolation-uid-collision",
    "render-inputs-unavailable",
    "workers-not-ready",
    "unknown-worker-failure",
];

const MAX_TENANT_FAILURE_ENTRIES: usize = 128;
const MAX_FAILURE_WORKER_IDS: usize = 64;
const MAX_FAILURE_DETAIL_CHARS: usize = 512;
const MAX_FAILURE_NAME_CHARS: usize = 191;

fn sanitize_tenant_failures(raw: Vec<RawTenantFailure>) -> Vec<TenantFailure> {
    let mut out = Vec::with_capacity(raw.len().min(MAX_TENANT_FAILURE_ENTRIES));
    for entry in raw.into_iter().take(MAX_TENANT_FAILURE_ENTRIES) {
        if entry.tenant.is_empty() || entry.tenant.chars().count() > MAX_FAILURE_NAME_CHARS {
            continue;
        }
        let code = if TENANT_FAILURE_CODES.contains(&entry.code.as_str()) {
            entry.code
        } else {
            "unknown-worker-failure".to_string()
        };
        // MIG-4: worker ids are untrusted — only the VRT identifier
        // charset (`[A-Za-z0-9_.:-]`, ≤128, non-empty) may reach an
        // ACK; anything else is dropped, never re-minted.
        let worker_ids: Vec<String> = entry
            .worker_ids
            .into_iter()
            .filter(|w| {
                !w.is_empty()
                    && w.len() <= 128
                    && w.bytes().all(|b| {
                        b.is_ascii_alphanumeric()
                            || matches!(b, b'_' | b'-' | b'.' | b':')
                    })
            })
            .take(MAX_FAILURE_WORKER_IDS)
            .collect();
        let detail = entry
            .detail
            .map(|d| crate::execset::truncate_utf16(&d, MAX_FAILURE_DETAIL_CHARS).0);
        out.push(TenantFailure {
            tenant: entry.tenant,
            worker_ids,
            code,
            detail,
        });
    }
    out
}

impl FileEnvelopePodChannel {
    /// Build from validated pod specs. Every directory must exist with
    /// `0700` and be owned by the current euid (or root); every readyz
    /// base must be loopback http. Any violation → `Err` (startup
    /// refuses to run half-configured).
    pub fn new(specs: &[FilePodSpec], timeout: Duration) -> Result<Self, String> {
        let mut pods = BTreeMap::new();
        for spec in specs {
            if spec.id.is_empty()
                || spec.id.len() > 128
                || !spec
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
            {
                return Err(format!("pod id invalid: {:?}", spec.id));
            }
            let dir = PathBuf::from(&spec.dir);
            validate_pod_dir(&dir).map_err(|e| format!("pod {} dir: {e}", spec.id))?;
            validate_readyz(&spec.readyz).map_err(|e| format!("pod {} readyz: {e}", spec.id))?;
            if pods
                .insert(
                    spec.id.clone(),
                    PodTarget {
                        dir,
                        readyz: spec.readyz.trim_end_matches('/').to_string(),
                    },
                )
                .is_some()
            {
                return Err(format!("duplicate pod id: {}", spec.id));
            }
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("probe client: {e}"))?;
        Ok(Self {
            pods,
            timeout,
            client,
        })
    }

    /// Idempotent apply: identical `(revision, envelopeDigest,
    /// workers)` already on disk → no writes. Otherwise write the
    /// envelope FIRST, then the assignment — the host adopts only a
    /// digest-matched pair, so the inter-write gap is safe.
    fn apply_files(
        &self,
        target: &PodTarget,
        envelope_raw: &[u8],
        assignment: &PodAssignment,
    ) -> Result<(), PodError> {
        let assignment_path = target.dir.join("assignment.json");
        if let Ok(raw) = std::fs::read(&assignment_path) {
            if let Ok(existing) = serde_json::from_slice::<PodAssignment>(&raw) {
                if existing.revision == assignment.revision
                    && existing.envelope_digest == assignment.envelope_digest
                    && existing.workers == assignment.workers
                {
                    return Ok(());
                }
            }
        }
        atomic_write(&target.dir.join("envelope.json"), envelope_raw)?;
        let body = serde_json::to_vec_pretty(assignment).map_err(|_| PodError::Rejected)?;
        atomic_write(&assignment_path, &body)?;
        Ok(())
    }
}

/// Config-time directory hardening: absolute, exists, dir, `0700`,
/// owner == euid|root. `..` components are rejected explicitly.
fn validate_pod_dir(dir: &Path) -> Result<(), String> {
    if !dir.is_absolute() {
        return Err("not absolute".into());
    }
    if dir
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err("contains '..'".into());
    }
    let meta = std::fs::metadata(dir).map_err(|e| format!("stat: {e}"))?;
    if !meta.is_dir() {
        return Err("not a directory".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o777 != 0o700 {
            return Err(format!("mode {:o} != 0700", meta.mode() & 0o777));
        }
        let euid = unsafe { libc::geteuid() };
        if meta.uid() != euid && meta.uid() != 0 {
            return Err(format!("owner uid {} != euid {euid}", meta.uid()));
        }
    }
    Ok(())
}

/// `http://127.0.0.1:<port>` or `http://localhost:<port>` only —
/// anything else (remote host, unix path, other scheme) fails closed.
fn validate_readyz(base: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(base).map_err(|e| format!("url: {e}"))?;
    if url.scheme() != "http" {
        return Err("scheme must be http (loopback)".into());
    }
    let host = url.host_str().ok_or("no host")?;
    let loopback = host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
    if !loopback {
        return Err(format!("host {host} is not loopback"));
    }
    if url.port().is_none() {
        return Err("missing port".into());
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err("readyz base must not carry a path".into());
    }
    Ok(())
}

/// Shared tmp+fsync+rename (T29/R1): unique O_EXCL temp names —
/// the old local copy used a fixed `.json.tmp` name per path, the
/// same concurrent-writer hazard fixed in `vrt_securefs`.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), PodError> {
    vrt_securefs::atomic_write(path, bytes, 0o600).map_err(|_| PodError::Unreachable)
}

#[async_trait::async_trait]
impl PodChannel for FileEnvelopePodChannel {
    fn pods(&self) -> Vec<String> {
        self.pods.keys().cloned().collect()
    }

    async fn apply(
        &self,
        pod_id: &str,
        envelope_raw: &[u8],
        assignment: &PodAssignment,
    ) -> Result<PodApplyResult, PodError> {
        let target = self.pods.get(pod_id).ok_or(PodError::UnknownPod)?;
        if envelope_raw.len() > MAX_ENVELOPE_BYTES {
            return Err(PodError::Rejected);
        }
        if assignment.pod_id != pod_id {
            return Err(PodError::Rejected);
        }
        self.apply_files(target, envelope_raw, assignment)?;
        Ok(PodApplyResult {
            loaded_revision: assignment.revision,
        })
    }

    async fn probe(&self, pod_id: &str) -> Result<PodProbeResult, PodError> {
        let target = self.pods.get(pod_id).ok_or(PodError::UnknownPod)?;
        let url = format!("{}/readyz", target.readyz);
        let mut resp = match tokio::time::timeout(self.timeout, self.client.get(&url).send()).await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                return Err(if e.is_timeout() || e.is_connect() {
                    PodError::Unreachable
                } else {
                    PodError::ProbeFailed
                })
            }
            Err(_) => return Err(PodError::Unreachable),
        };
        // Bounded body read — never buffer an unbounded response.
        let mut body = Vec::with_capacity(1024);
        loop {
            match tokio::time::timeout(self.timeout, resp.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    if body.len() + chunk.len() > MAX_PROBE_BYTES {
                        return Err(PodError::ProbeFailed);
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(Ok(None)) => break,
                Ok(Err(_)) | Err(_) => return Err(PodError::ProbeFailed),
            }
        }
        // Non-2xx still counts as a probe answer when the body parses —
        // a 503 readyz IS the not-ready signal carrying a typed reason.
        match serde_json::from_slice::<PodReadyz>(&body) {
            Ok(r) => Ok(PodProbeResult {
                ready: r.ready,
                loaded_revision: r.revision,
                loaded_digest: r.payload_digest,
                reason: sanitize_reason(r.reason),
                tenant_failures: sanitize_tenant_failures(r.tenant_failures),
            }),
            Err(_) => Err(PodError::ProbeFailed),
        }
    }
}

/// Digest of raw envelope bytes — the writer fills
/// `assignment.envelope_digest` with this so the host can pair-check.
pub fn envelope_digest_of(envelope_raw: &[u8]) -> String {
    hex::encode(Sha256::digest(envelope_raw))
}
