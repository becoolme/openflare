//! `KubernetesLeaseStore` — `LeaseStore` over the real Kubernetes
//! `coordination.k8s.io/v1 Lease` API (B9).
//!
//! Semantics map 1:1 onto [`MemoryLeaseStore`]:
//! - `claim` is GET-then-CAS (`resourceVersion` optimistic concurrency):
//!   same-holder renews `renewTime`; expired/absent leases are taken;
//!   a live foreign hold reports `Lost`; a lost CAS reports `Lost` too
//!   (the winner owns the lease — we must not act as leader);
//! - every store-level failure (HTTP error, timeout, malformed body,
//!   unreachable API) is [`LeaseError::Unavailable`] — callers fail
//!   closed (no cron dispatch);
//! - the fencing `epoch` is `spec.leaseTransitions` — the API-injected
//!   monotonic counter that increments on every holder change, so a
//!   partitioned ex-leader's later writes cannot fake freshness.
//!
//! Credentials follow the in-cluster contract: a bearer token and CA
//! bundle read from files (defaults are the ServiceAccount paths).
//! `allow_insecure` exists ONLY for tests against a local HTTP mock —
//! production wiring must not set it (prod.rs enforces https).

use crate::lease::{LeaseClaim, LeaseError, LeaseOutcome, LeaseStore};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use std::sync::Mutex;

/// Default in-cluster ServiceAccount token path.
pub const DEFAULT_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
/// Default in-cluster CA bundle path.
pub const DEFAULT_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// Connection configuration for one Lease object.
#[derive(Clone, Debug)]
pub struct K8sLeaseConfig {
    /// API server base URL, e.g. `https://kubernetes.default.svc`.
    /// `http://` is rejected unless `allow_insecure` (tests only).
    pub api_url: String,
    /// Namespace holding the Lease.
    pub namespace: String,
    /// Lease object name.
    pub lease_name: String,
    /// Bearer token file (ServiceAccount token).
    pub token_path: String,
    /// CA bundle path; empty = platform roots (tests/dev only).
    pub ca_path: String,
    /// Per-request timeout (also bounds the worst-case stale-write
    /// window — see docs).
    pub timeout: std::time::Duration,
    /// Test-only: permit `http://` API URLs and skip TLS verification.
    /// Must never be reachable from production config parsing.
    pub allow_insecure: bool,
}

/// Kubernetes Lease API client behind the shared [`LeaseStore`] trait.
pub struct KubernetesLeaseStore {
    client: reqwest::Client,
    url: String,
    create_url: String,
    token: Option<String>,
    /// Last successful claim — `held()` is the local staleness probe
    /// the runner calls between dispatches (no API round-trip).
    last: Mutex<Option<LeaseClaim>>,
}

/// `spec` of `coordination.k8s.io/v1 Lease` — only the fields we use.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeaseSpec {
    holder_identity: Option<String>,
    lease_duration_seconds: Option<i64>,
    renew_time: Option<String>,
    lease_transitions: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LeaseObject {
    metadata: LeaseMeta,
    spec: LeaseSpec,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeaseMeta {
    resource_version: String,
}

fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    // MicroTime is RFC3339 with optional fractional seconds — chrono's
    // parser covers both shapes.
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

impl KubernetesLeaseStore {
    /// Build a client from `config`. Reads token/CA files eagerly —
    /// unreadable credentials fail at construction (fail closed at
    /// startup, not on the first tick).
    pub fn new(config: &K8sLeaseConfig) -> Result<Self, String> {
        let url = reqwest::Url::parse(&config.api_url).map_err(|e| format!("api_url: {e}"))?;
        if url.scheme() == "http" && !config.allow_insecure {
            return Err("api_url must be https (set allow_insecure only for tests)".into());
        }
        if url.scheme() != "https" && url.scheme() != "http" {
            return Err("api_url scheme must be https".into());
        }
        let token_raw = std::fs::read_to_string(&config.token_path)
            .map_err(|e| format!("token file {}: {e}", config.token_path))?;
        let token = token_raw.trim().to_string();
        if token.is_empty() {
            return Err("service account token file is empty".into());
        }
        let mut builder = reqwest::Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.timeout.min(std::time::Duration::from_secs(3)));
        if !config.ca_path.is_empty() {
            let pem = std::fs::read(&config.ca_path)
                .map_err(|e| format!("ca file {}: {e}", config.ca_path))?;
            let cert = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| format!("ca bundle: {e}"))?
                .into_iter()
                .next()
                .ok_or_else(|| "ca bundle contains no certificates".to_string())?;
            builder = builder.add_root_certificate(cert);
        }
        if config.allow_insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder.build().map_err(|e| format!("client: {e}"))?;
        let ns = config.namespace.clone();
        let create_url = format!(
            "{}/apis/coordination.k8s.io/v1/namespaces/{}/leases",
            config.api_url.trim_end_matches('/'),
            ns
        );
        Ok(Self {
            client,
            url: format!("{}/{}", create_url, config.lease_name),
            create_url,
            token: Some(token),
            last: Mutex::new(None),
        })
    }

    /// In-cluster defaults: ServiceAccount token/CA paths + the
    /// `kubernetes.default.svc` API URL and `POD_NAMESPACE`-style
    /// explicit namespace (no env probing — prod passes it in).
    pub fn in_cluster(
        namespace: &str,
        lease_name: &str,
        timeout: std::time::Duration,
    ) -> Result<Self, String> {
        Self::new(&K8sLeaseConfig {
            api_url: "https://kubernetes.default.svc".into(),
            namespace: namespace.into(),
            lease_name: lease_name.into(),
            token_path: DEFAULT_TOKEN_PATH.into(),
            ca_path: DEFAULT_CA_PATH.into(),
            timeout,
            allow_insecure: false,
        })
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    /// GET the Lease; `Ok(None)` on 404.
    async fn get(&self) -> Result<Option<LeaseObject>, LeaseError> {
        let resp = self
            .authed(self.client.get(&self.url))
            .send()
            .await
            .map_err(|_| LeaseError::Unavailable)?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(LeaseError::Unavailable);
        }
        resp.json::<LeaseObject>()
            .await
            .map(Some)
            .map_err(|_| LeaseError::Unavailable)
    }

    /// RV-R56: highest epoch this process has been granted (0 = none).
    fn epoch_floor(&self) -> u64 {
        self.last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|c| c.epoch)
            .unwrap_or(0)
    }

    /// PUT a spec update guarded by `resourceVersion` (CAS).
    async fn update(
        &self,
        resource_version: &str,
        holder: &str,
        ttl: Duration,
        now: DateTime<Utc>,
        transitions: u64,
    ) -> Result<LeaseOutcome, LeaseError> {
        let body = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": self.url.rsplit('/').next().unwrap_or("vrt-cron"),
                "resourceVersion": resource_version,
            },
            "spec": {
                "holderIdentity": holder,
                "leaseDurationSeconds": ttl.num_seconds(),
                "renewTime": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "leaseTransitions": transitions,
            }
        });
        let resp = self
            .authed(self.client.put(&self.url))
            .json(&body)
            .send()
            .await
            .map_err(|_| LeaseError::Unavailable)?;
        match resp.status() {
            s if s.is_success() => {
                let claim = LeaseClaim {
                    holder: holder.into(),
                    until: now + ttl,
                    epoch: transitions,
                };
                *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(claim.clone());
                Ok(LeaseOutcome::Held(claim))
            }
            reqwest::StatusCode::CONFLICT => Ok(LeaseOutcome::Lost),
            _ => Err(LeaseError::Unavailable),
        }
    }

    /// POST-create the Lease (first claim ever, or after deletion).
    async fn create(
        &self,
        holder: &str,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<LeaseOutcome, LeaseError> {
        // RV-R56: a (re)created Lease must not restart the epoch below
        // what this process already issued — floor+1, and never 0 (0 is
        // the vrt-host "no fence yet" baseline). Verified 2026-11-15.
        let epoch = next_epoch_after(self.epoch_floor());
        let name = self.url.rsplit('/').next().unwrap_or("vrt-cron");
        let ns = self
            .create_url
            .trim_end_matches('/')
            .rsplit('/')
            .nth(1)
            .unwrap_or("default");
        let body = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": name, "namespace": ns },
            "spec": {
                "holderIdentity": holder,
                "leaseDurationSeconds": ttl.num_seconds(),
                "acquireTime": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "renewTime": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "leaseTransitions": epoch,
            }
        });
        let resp = self
            .authed(self.client.post(&self.create_url))
            .json(&body)
            .send()
            .await
            .map_err(|_| LeaseError::Unavailable)?;
        match resp.status() {
            s if s.is_success() => {
                let claim = LeaseClaim {
                    holder: holder.into(),
                    until: now + ttl,
                    epoch,
                };
                *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(claim.clone());
                Ok(LeaseOutcome::Held(claim))
            }
            // Someone else created first → they own it.
            reqwest::StatusCode::CONFLICT => Ok(LeaseOutcome::Lost),
            _ => Err(LeaseError::Unavailable),
        }
    }
}

/// RV-R56: next epoch strictly above `floor`, never 0.
pub(crate) fn next_epoch_after(floor: u64) -> u64 {
    floor.saturating_add(1).max(1)
}

#[cfg(test)]
mod rv_r56_tests {
    use super::next_epoch_after;

    /// Verified 2026-11-15.
    #[test]
    fn recreated_lease_epoch_never_regresses_or_zero() {
        assert_eq!(next_epoch_after(0), 1); // fresh create: not the 0 baseline
        assert_eq!(next_epoch_after(7), 8); // recreate after we held epoch 7
        assert_eq!(next_epoch_after(u64::MAX), u64::MAX);
    }
}

#[async_trait::async_trait]
impl LeaseStore for KubernetesLeaseStore {
    async fn claim(
        &self,
        holder: &str,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<LeaseOutcome, LeaseError> {
        let Some(obj) = self.get().await? else {
            return self.create(holder, ttl, now).await;
        };
        let spec = &obj.spec;
        let cur_holder = spec.holder_identity.as_deref().unwrap_or("");
        let transitions = spec.lease_transitions.unwrap_or(0);
        let renew = spec.renew_time.as_deref().and_then(parse_time);
        let dur = spec
            .lease_duration_seconds
            .map(Duration::seconds)
            .unwrap_or_else(|| Duration::seconds(0));
        let expiry = renew.map(|r| r + dur);
        if cur_holder == holder {
            // Same holder renews — CAS on resourceVersion. A lost CAS
            // means a peer object changed under us (e.g. deletion +
            // recreation): treat as Lost, not as an error to retry.
            return self
                .update(
                    &obj.metadata.resource_version,
                    holder,
                    ttl,
                    now,
                    // Same holder: epoch unchanged (only raised to the
                    // process floor if the object was recreated lower).
                    transitions.max(1).max(self.epoch_floor()),
                )
                .await;
        }
        match expiry {
            Some(until) if now < until => Ok(LeaseOutcome::Lost),
            // No holder, never renewed, or expired → take it.
            _ => {
                self.update(
                    &obj.metadata.resource_version,
                    holder,
                    ttl,
                    now,
                    (transitions + 1).max(next_epoch_after(self.epoch_floor())),
                )
                .await
            }
        }
    }

    fn held(&self, claim: &LeaseClaim, now: DateTime<Utc>) -> bool {
        self.last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|l| l == claim && now < l.until)
    }
}
