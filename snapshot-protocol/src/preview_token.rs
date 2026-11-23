//! Signed preview access tokens.
//!
//! A versioned preview URL is public the moment DNS exists, so every
//! preview request must present a bearer token bound to the exact
//! `(version, worker)` pair it is served for. The token is:
//!
//! ```text
//! v1.<expiresAtUnixSeconds>.<hex hmac-sha256>
//! ```
//!
//! where the MAC covers
//!
//! ```text
//! openflare.preview-token.v1\n<versionId>\n<workerId>\n<expiresAt>
//! ```
//!
//! The verifier supplies `versionId`/`workerId` from the ROUTE it already
//! resolved — they are never parsed out of attacker input — so a token
//! minted for one version cannot validate on another, and a token cannot
//! name a hostname of its own. Expiry is part of the MAC, so it cannot be
//! extended by editing the token.
//!
//! Key material is derived from the cell signing seed with a distinct
//! label (and persisted as `keys/preview-token.secret`, `0600`, so
//! `vrt-host` can verify without holding the seed). Rotating the signing
//! key therefore invalidates every outstanding preview token, which is
//! the intended revocation story: tokens are stateless and cannot be
//! revoked individually before expiry.

use std::fmt;

/// Query parameter (and cookie name) carrying the token.
pub const TOKEN_PARAM: &str = "__vrt_preview_token";
/// Wire version prefix.
pub const TOKEN_VERSION: &str = "v1";
/// Default token lifetime for URLs printed by `of` (one hour).
pub const DEFAULT_TTL_SECS: u64 = 60 * 60;
/// Shortest token lifetime an operator may request.
pub const MIN_TTL_SECS: u64 = 60;
/// Longest token lifetime (30 days) accepted by mint AND verify.
pub const MAX_TTL_SECS: u64 = 30 * 24 * 60 * 60;
/// Clock skew allowed when a verifier rejects over-long lifetimes.
pub const MAX_FUTURE_SKEW_SECS: u64 = 300;
/// Token length bound before any parse/HMAC work.
pub const MAX_TOKEN_LEN: usize = 128;

/// HKDF-style domain separation label (HMAC as a PRF over the seed).
const DERIVE_LABEL: &[u8] = b"openflare.preview-token.v1";
/// MAC input prefix (bumped together with [`TOKEN_VERSION`]).
const MAC_LABEL: &str = "openflare.preview-token.v1";

/// Why a token was refused. Stable, secret-free codes for logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreviewTokenError {
    /// Not `v1.<exp>.<64 lowercase hex>` / over the length bound.
    Malformed,
    /// Signature did not verify (wrong version, wrong worker, tampered).
    Mismatch,
    /// Signature is valid but the token is past its expiry.
    Expired,
    /// Signature is valid but the lifetime exceeds [`MAX_TTL_SECS`].
    TooLong,
}

impl PreviewTokenError {
    /// Stable code (never contains token material).
    pub fn code(self) -> &'static str {
        match self {
            Self::Malformed => "preview_token_malformed",
            Self::Mismatch => "preview_token_invalid",
            Self::Expired => "preview_token_expired",
            Self::TooLong => "preview_token_too_long",
        }
    }
}

impl fmt::Display for PreviewTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// HMAC key for preview tokens.
#[derive(Clone)]
pub struct PreviewTokenKey {
    bytes: [u8; 32],
}

impl PreviewTokenKey {
    /// Derive the preview-token key from a 32-byte signing seed.
    ///
    /// HMAC-SHA256(seed, label) is used as a one-way KDF: the resulting
    /// key is independent of the Ed25519 key for any practical purpose,
    /// and the label keeps it from ever aliasing a signing operation.
    pub fn from_signing_seed(seed: &[u8; 32]) -> Self {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, seed);
        let tag = ring::hmac::sign(&key, DERIVE_LABEL);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(tag.as_ref());
        Self { bytes }
    }

    /// Wrap raw key bytes (the persisted `keys/preview-token.secret`).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Parse the persisted lowercase-hex form (64 chars).
    pub fn from_hex(raw: &str) -> Result<Self, PreviewTokenError> {
        let raw = raw.trim();
        if raw.len() != 64 {
            return Err(PreviewTokenError::Malformed);
        }
        let decoded = hex::decode(raw).map_err(|_| PreviewTokenError::Malformed)?;
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|_| PreviewTokenError::Malformed)?;
        Ok(Self { bytes })
    }

    /// Lowercase hex form for `keys/preview-token.secret`.
    pub fn to_hex(&self) -> String {
        hex::encode(self.bytes)
    }

    /// Mint a token valid until `expires_at_unix_s`.
    ///
    /// The caller is responsible for bounds (`MIN_TTL_SECS`…
    /// `MAX_TTL_SECS`); verify enforces the upper bound regardless, so a
    /// bug here cannot mint a long-lived token.
    pub fn mint(&self, version_id: &str, worker_id: &str, expires_at_unix_s: u64) -> String {
        let tag = self.sign(version_id, worker_id, expires_at_unix_s);
        format!(
            "{TOKEN_VERSION}.{expires_at_unix_s}.{}",
            hex::encode(tag.as_ref())
        )
    }

    /// Verify a token for exactly this `(version, worker)` at `now`.
    ///
    /// Signature is checked before expiry so a forged but "expired"
    /// string is never reported as merely expired. Comparison is
    /// constant-time (`ring::hmac::verify`).
    pub fn verify(
        &self,
        version_id: &str,
        worker_id: &str,
        token: &str,
        now_unix_s: u64,
    ) -> Result<(), PreviewTokenError> {
        if token.is_empty() || token.len() > MAX_TOKEN_LEN {
            return Err(PreviewTokenError::Malformed);
        }
        let mut parts = token.split('.');
        let (Some(version), Some(exp), Some(tag), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(PreviewTokenError::Malformed);
        };
        if version != TOKEN_VERSION
            || exp.is_empty()
            || exp.len() > 20
            || !exp.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(PreviewTokenError::Malformed);
        }
        let expires_at: u64 = exp.parse().map_err(|_| PreviewTokenError::Malformed)?;
        if tag.len() != 64 || !tag.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(PreviewTokenError::Malformed);
        }
        let tag = hex::decode(tag).map_err(|_| PreviewTokenError::Malformed)?;
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &self.bytes);
        let message = mac_input(version_id, worker_id, expires_at);
        ring::hmac::verify(&key, message.as_bytes(), &tag)
            .map_err(|_| PreviewTokenError::Mismatch)?;
        if now_unix_s >= expires_at {
            return Err(PreviewTokenError::Expired);
        }
        // Bound the total lifetime even for a correctly signed token: a
        // compromised minting path cannot hand out a permanent URL.
        if expires_at.saturating_sub(now_unix_s) > MAX_TTL_SECS + MAX_FUTURE_SKEW_SECS {
            return Err(PreviewTokenError::TooLong);
        }
        Ok(())
    }

    fn sign(&self, version_id: &str, worker_id: &str, expires_at_unix_s: u64) -> ring::hmac::Tag {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &self.bytes);
        let message = mac_input(version_id, worker_id, expires_at_unix_s);
        ring::hmac::sign(&key, message.as_bytes())
    }
}

fn mac_input(version_id: &str, worker_id: &str, expires_at_unix_s: u64) -> String {
    format!("{MAC_LABEL}\n{version_id}\n{worker_id}\n{expires_at_unix_s}")
}

/// Pull the token out of a query string.
///
/// Returns `(token, remaining_query)`. The token pair is removed from
/// `remaining_query` (other parameters keep their order and encoding), so
/// the worker never sees the bearer token in `request.url`. A DUPLICATE
/// token parameter yields `None` — ambiguity is refused rather than
/// resolved.
pub fn take_from_query(query: &str) -> (Option<String>, String) {
    if query.is_empty() {
        return (None, String::new());
    }
    let mut token: Option<String> = None;
    let mut seen = false;
    let mut ambiguous = false;
    let mut kept: Vec<&str> = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name == TOKEN_PARAM {
            if seen {
                // Duplicate: refuse the request's token entirely (both
                // pairs are dropped from the forwarded query).
                ambiguous = true;
            } else {
                seen = true;
                token = Some(value.to_string());
            }
        } else {
            kept.push(pair);
        }
    }
    if ambiguous {
        token = None;
    }
    (token, kept.join("&"))
}

/// Pull the token out of a `Cookie` header.
///
/// Cookie names are matched exactly; the value is returned verbatim (the
/// verifier rejects anything outside the token grammar).
pub fn take_from_cookie(cookie_header: &str) -> Option<String> {
    cookie_header
        .split(';')
        .filter_map(|pair| {
            let pair = pair.trim();
            pair.split_once('=')
        })
        .find(|(name, _)| *name == TOKEN_PARAM)
        .map(|(_, value)| value.trim().to_string())
}

/// Rewrite a `Cookie` header without our token cookie. `None` when no
/// cookie is left (the header must then be removed entirely).
pub fn strip_cookie(cookie_header: &str) -> Option<String> {
    let kept: Vec<&str> = cookie_header
        .split(';')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
        .filter(|pair| !pair.starts_with(&format!("{TOKEN_PARAM}=")))
        .collect();
    (!kept.is_empty()).then(|| kept.join("; "))
}

/// Expiry encoded in a token, WITHOUT verifying it.
///
/// Only use after [`PreviewTokenKey::verify`] succeeded (or for logging
/// the remaining lifetime); the value is attacker-controlled otherwise.
pub fn token_expiry(token: &str) -> Option<u64> {
    let mut parts = token.split('.');
    let (Some(version), Some(exp), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if version != TOKEN_VERSION {
        return None;
    }
    exp.parse().ok()
}

/// `Set-Cookie` value that lets a browser load subresources of the same
/// preview host after the first authenticated navigation. Host-scoped by
/// construction (each preview hostname is its own origin), `Secure`,
/// `HttpOnly`, `SameSite=Lax`, and bounded by the token's remaining life.
pub fn bootstrap_cookie(token: &str, now_unix_s: u64) -> String {
    let max_age = token_expiry(token)
        .map(|exp| exp.saturating_sub(now_unix_s))
        .unwrap_or(0);
    format!(
        "{TOKEN_PARAM}={token}; Path=/; Max-Age={max_age}; Secure; HttpOnly; SameSite=Lax"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> PreviewTokenKey {
        PreviewTokenKey::from_signing_seed(&[7u8; 32])
    }

    #[test]
    fn key_derivation_is_deterministic_domain_separated_and_roundtrips_hex() {
        let a = PreviewTokenKey::from_signing_seed(&[7u8; 32]);
        let b = PreviewTokenKey::from_signing_seed(&[7u8; 32]);
        assert_eq!(a.to_hex(), b.to_hex());
        assert_ne!(
            a.to_hex(),
            hex::encode([7u8; 32]),
            "the derived key must not be the seed itself"
        );
        assert_eq!(
            PreviewTokenKey::from_hex(&a.to_hex()).unwrap().to_hex(),
            a.to_hex()
        );
        assert_ne!(
            a.to_hex(),
            PreviewTokenKey::from_signing_seed(&[8u8; 32]).to_hex()
        );
        assert!(matches!(
            PreviewTokenKey::from_hex("zz"),
            Err(PreviewTokenError::Malformed)
        ));
    }

    #[test]
    fn mints_and_verifies_for_the_exact_version_and_worker() {
        let token = key().mint("vrt_abc", "w_42", 2_000);
        assert!(token.starts_with("v1.2000."));
        assert_eq!(key().verify("vrt_abc", "w_42", &token, 1_000), Ok(()));
        // Bound to the pair: neither dimension can be swapped.
        assert_eq!(
            key().verify("vrt_other", "w_42", &token, 1_000),
            Err(PreviewTokenError::Mismatch)
        );
        assert_eq!(
            key().verify("vrt_abc", "w_other", &token, 1_000),
            Err(PreviewTokenError::Mismatch)
        );
        // A different key cannot verify.
        assert_eq!(
            PreviewTokenKey::from_signing_seed(&[9u8; 32]).verify(
                "vrt_abc",
                "w_42",
                &token,
                1_000
            ),
            Err(PreviewTokenError::Mismatch)
        );
    }

    #[test]
    fn expiry_is_signed_and_enforced() {
        let token = key().mint("vrt_abc", "w_42", 2_000);
        assert_eq!(
            key().verify("vrt_abc", "w_42", &token, 2_000),
            Err(PreviewTokenError::Expired),
            "expiry is exclusive"
        );
        assert_eq!(
            key().verify("vrt_abc", "w_42", &token, 1_999),
            Ok(())
        );
        // Extending the expiry in the token breaks the MAC.
        let tampered = token.replacen("2000", "9999", 1);
        assert_eq!(
            key().verify("vrt_abc", "w_42", &tampered, 1_000),
            Err(PreviewTokenError::Mismatch)
        );
    }

    #[test]
    fn malformed_and_oversized_tokens_are_refused_before_hashing() {
        for bad in [
            "",
            "v1",
            "v1.1234",
            "v1.1234.",
            "v2.1234.0000",
            "v1.-1.0000",
            "v1.1234.zz",
            &format!("v1.1.{}", "z".repeat(64)),
            &"a".repeat(MAX_TOKEN_LEN + 1),
        ] {
            let err = key().verify("vrt_abc", "w_42", bad, 1);
            assert!(
                matches!(err, Err(PreviewTokenError::Malformed)),
                "{bad:?} -> {err:?}"
            );
        }
    }

    #[test]
    fn correctly_signed_but_overlong_lifetime_is_refused() {
        let now = 1_000;
        let token = key().mint("vrt_abc", "w_42", now + MAX_TTL_SECS + 10_000);
        assert_eq!(
            key().verify("vrt_abc", "w_42", &token, now),
            Err(PreviewTokenError::TooLong)
        );
        let max = key().mint("vrt_abc", "w_42", now + MAX_TTL_SECS);
        assert_eq!(key().verify("vrt_abc", "w_42", &max, now), Ok(()));
    }

    #[test]
    fn query_extraction_removes_only_the_token_parameter() {
        let (token, rest) = take_from_query("a=1&__vrt_preview_token=v1.2.00ff&b=2");
        assert_eq!(token.as_deref(), Some("v1.2.00ff"));
        assert_eq!(rest, "a=1&b=2");

        let (token, rest) = take_from_query("__vrt_preview_token=v1.2.00ff");
        assert_eq!(token.as_deref(), Some("v1.2.00ff"));
        assert!(rest.is_empty());

        let (token, rest) = take_from_query("a=1");
        assert_eq!(token, None);
        assert_eq!(rest, "a=1");

        // Ambiguous duplicates are refused, and both pairs are dropped.
        let (token, rest) = take_from_query(
            "__vrt_preview_token=one&a=1&__vrt_preview_token=two",
        );
        assert_eq!(token, None);
        assert_eq!(rest, "a=1");
    }

    #[test]
    fn cookies_are_read_exactly_and_stripped_from_forwarding() {
        assert_eq!(
            take_from_cookie("a=1; __vrt_preview_token=v1.2.00ff; b=2").as_deref(),
            Some("v1.2.00ff")
        );
        assert_eq!(take_from_cookie("__vrt_preview_tokens=no"), None);
        assert_eq!(
            strip_cookie("a=1; __vrt_preview_token=v1.2.00ff; b=2").as_deref(),
            Some("a=1; b=2")
        );
        assert_eq!(strip_cookie("__vrt_preview_token=x"), None);
        assert_eq!(
            strip_cookie("a=1").as_deref(),
            Some("a=1"),
            "unrelated cookies are untouched"
        );
    }

    #[test]
    fn bootstrap_cookie_is_bounded_and_hardened() {
        let cookie = bootstrap_cookie("v1.2000.00ff", 1_400);
        assert_eq!(
            cookie,
            "__vrt_preview_token=v1.2000.00ff; Path=/; Max-Age=600; Secure; HttpOnly; SameSite=Lax"
        );
        assert!(bootstrap_cookie("garbage", 1).contains("Max-Age=0"));
    }
}
