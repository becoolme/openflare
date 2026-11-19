//! MIG-10: startup probe for the CA trust store workerd uses for
//! tenant egress TLS.
//!
//! Tenant workers' outbound `fetch()` runs through the egress worker's
//! `network = (…, tlsOptions = (trustBrowserCas = true))` — that reads
//! the SYSTEM CA bundle from the runtime image at
//! [`CA_BUNDLE_PATH`]. The 2026-11-17 incident: `Dockerfile.vrt-host`
//! shipped a bookworm-slim runtime stage WITHOUT `ca-certificates`, so
//! every tenant HTTPS egress died with
//! "TLS peer's certificate is not trusted; reason = unable to get
//! local issuer certificate" (surfaced as 502 EGRESS_UPSTREAM).
//!
//! Policy: a missing/empty trust store is a structured startup ERROR
//! plus `vrt_host_egress_trust_store_ok = 0` — it does NOT block
//! ingress. Inbound traffic never depends on the CA bundle, and
//! refusing to serve would turn a single degraded capability into a
//! total outage; the typed gauge keeps the degradation alertable
//! instead (documented in workers-runtime-egress.md MIG-10).

/// The one and only definition of the CA bundle path — mirrored by the
/// Dockerfile's `test -s` build gate and documented next to
/// `trustBrowserCas` in `vrt-config/src/capnp.rs`. Do not scatter this
/// literal through the codebase.
pub const CA_BUNDLE_PATH: &str = "/etc/ssl/certs/ca-certificates.crt";

/// What [`check`] found — the reason goes to the log/metric, never to
/// tenants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustStoreState {
    /// Bundle exists and is non-empty.
    Ok,
    /// `metadata` failed — absent, or an I/O error statting it.
    Missing,
    /// File exists but is zero bytes (an empty bundle validates nothing).
    Empty,
    /// Path exists but is not a regular file (dir/symlink to nothing …).
    NotRegular,
}

impl TrustStoreState {
    pub fn ok(self) -> bool {
        matches!(self, Self::Ok)
    }
    /// Stable reason token for logs and the status file.
    pub fn code(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Missing => "missing",
            Self::Empty => "empty",
            Self::NotRegular => "not-regular",
        }
    }
}

/// Probe the CA bundle at `path` (exists + regular file + non-empty).
/// Pure filesystem check — no parsing; workerd/OpenSSL own the format.
pub fn check_at(path: &std::path::Path) -> TrustStoreState {
    match std::fs::metadata(path) {
        Ok(m) if !m.is_file() => TrustStoreState::NotRegular,
        Ok(m) if m.len() == 0 => TrustStoreState::Empty,
        Ok(_) => TrustStoreState::Ok,
        Err(_) => TrustStoreState::Missing,
    }
}

/// Probe [`CA_BUNDLE_PATH`].
pub fn check() -> TrustStoreState {
    check_at(std::path::Path::new(CA_BUNDLE_PATH))
}

/// Record the probe outcome: gauge + status field. The caller logs the
/// ERROR so this stays usable from tests without a tracing subscriber.
pub fn record(state: TrustStoreState) {
    crate::metrics::registry().set(
        "vrt_host_egress_trust_store_ok",
        &[],
        if state.ok() { 1.0 } else { 0.0 },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn missing_empty_and_nonempty() {
        let dir = std::env::temp_dir().join(format!(
            "vrt-ts-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("ca.crt");
        assert_eq!(check_at(&p), TrustStoreState::Missing);
        std::fs::File::create(&p).unwrap();
        assert_eq!(check_at(&p), TrustStoreState::Empty);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(b"pem").unwrap();
        drop(f);
        assert_eq!(check_at(&p), TrustStoreState::Ok);
        assert_eq!(check_at(&dir), TrustStoreState::NotRegular);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
