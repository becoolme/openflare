//! RV-R72: the single, shared loader for every service-to-service secret
//! (vrt-sql `authSecretFile`/`bookmarkSecretFile`, provider cursor secret,
//! cell-controller `d1sql.auth_secret_file`).
//!
//! Before this module the same file was normalised three different ways
//! (raw bytes / strip all whitespace / trim ends), so a trailing `\n`
//! made the signer and the verifier derive different HMAC keys and every
//! request 401'd without any startup error. All binaries now go through
//! [`load_service_secret`] / [`parse_service_secret`] and therefore see
//! byte-identical key material.
//!
//! Rules (fail closed): 0600 + owned by the effective uid, ≤4 KiB, only a
//! single trailing `\r?\n` is stripped, every remaining byte must be
//! printable non-space ASCII, minimum length per [`SecretPurpose`], and
//! known placeholder / low-entropy values are rejected. Error values
//! carry a fixed code only — never any secret byte.

use std::fmt;
use std::io::Read;
use std::path::Path;

/// Hard upper bound on the file size.
pub const SECRET_MAX_BYTES: u64 = 4096;
/// Minimum length for HMAC key material.
pub const SERVICE_SECRET_MIN_LEN: usize = 32;

/// Substrings (compared case-insensitively) that mark a value as a
/// shipped placeholder rather than real key material.
pub const PLACEHOLDER_MARKERS: &[&str] = &[
    "replace_me",
    "replace-me",
    "replaceme",
    "changeme",
    "change_me",
    "change-me",
    "placeholder",
    "your_secret",
    "your-secret",
    "example",
    "todo",
    "dummy",
];

/// What the secret is used for; selects the minimum length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretPurpose {
    /// HMAC key shared between processes (auth, cursor, bookmark).
    ServiceHmac,
    /// Opaque third-party credential (e.g. S3 access key id): the format
    /// is dictated by the remote, so only non-empty is required.
    Credential,
}

impl SecretPurpose {
    fn min_len(self) -> usize {
        match self {
            SecretPurpose::ServiceHmac => SERVICE_SECRET_MIN_LEN,
            SecretPurpose::Credential => 1,
        }
    }
}

/// Fixed-code failure; `Display` never includes secret bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretError {
    Unreadable,
    NotRegularFile,
    BadMode,
    BadOwner,
    TooLarge,
    Empty,
    TooShort,
    InvalidByte,
    Placeholder,
    LowEntropy,
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SecretError::Unreadable => "secret file unreadable",
            SecretError::NotRegularFile => "secret path is not a regular file",
            SecretError::BadMode => "secret file must be mode 0600",
            SecretError::BadOwner => "secret file must be owned by the runtime user",
            SecretError::TooLarge => "secret larger than 4KiB",
            SecretError::Empty => "secret is empty",
            SecretError::TooShort => "secret shorter than the minimum length (32 bytes)",
            SecretError::InvalidByte => {
                "secret contains whitespace/control/non-ASCII bytes (only one trailing newline is allowed)"
            }
            SecretError::Placeholder => "secret is a known placeholder value",
            SecretError::LowEntropy => "secret is a single repeated byte",
        })
    }
}

impl std::error::Error for SecretError {}

impl From<SecretError> for std::io::Error {
    fn from(e: SecretError) -> Self {
        let kind = match e {
            SecretError::Unreadable | SecretError::NotRegularFile => std::io::ErrorKind::NotFound,
            SecretError::BadMode | SecretError::BadOwner => std::io::ErrorKind::PermissionDenied,
            _ => std::io::ErrorKind::InvalidData,
        };
        std::io::Error::new(kind, e)
    }
}

/// Validated secret bytes. `Debug` is redacted; there is no `Display`.
#[derive(Clone, PartialEq, Eq)]
pub struct ServiceSecret(Vec<u8>);

impl ServiceSecret {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for ServiceSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ServiceSecret(<redacted>)")
    }
}

/// Validate raw secret text (file body or env var value) and return the
/// canonical bytes. Only one trailing `\n` / `\r\n` is removed.
pub fn parse_service_secret(
    raw: &[u8],
    purpose: SecretPurpose,
) -> Result<ServiceSecret, SecretError> {
    if raw.len() as u64 > SECRET_MAX_BYTES {
        return Err(SecretError::TooLarge);
    }
    let body = raw
        .strip_suffix(b"\r\n")
        .or_else(|| raw.strip_suffix(b"\n"))
        .unwrap_or(raw);
    if body.is_empty() {
        return Err(SecretError::Empty);
    }
    if !body.iter().all(|b| (0x21..=0x7e).contains(b)) {
        return Err(SecretError::InvalidByte);
    }
    if body.len() < purpose.min_len() {
        return Err(SecretError::TooShort);
    }
    // Credentials are dictated by the remote (any real access key id is
    // fine); placeholder/low-entropy checks apply to HMAC key material.
    if purpose == SecretPurpose::ServiceHmac {
        let lower = String::from_utf8_lossy(body).to_ascii_lowercase();
        if PLACEHOLDER_MARKERS.iter().any(|m| lower.contains(m)) {
            return Err(SecretError::Placeholder);
        }
        if body.iter().all(|b| *b == body[0]) {
            return Err(SecretError::LowEntropy);
        }
    }
    Ok(ServiceSecret(body.to_vec()))
}

/// Load a secret file: 0600, owned by the effective uid, regular file
/// (metadata is read from the opened handle, so k8s projected-secret
/// symlinks keep working), then [`parse_service_secret`].
pub fn load_service_secret(
    path: &Path,
    purpose: SecretPurpose,
) -> Result<ServiceSecret, SecretError> {
    use std::os::unix::fs::MetadataExt;
    let file = std::fs::File::open(path).map_err(|_| SecretError::Unreadable)?;
    let meta = file.metadata().map_err(|_| SecretError::Unreadable)?;
    if !meta.is_file() {
        return Err(SecretError::NotRegularFile);
    }
    if meta.mode() & 0o777 != 0o600 {
        return Err(SecretError::BadMode);
    }
// SAFETY: geteuid(2) has no preconditions and cannot fail.
    if meta.uid() != unsafe { libc::geteuid() } {
        return Err(SecretError::BadOwner);
    }
    if meta.len() > SECRET_MAX_BYTES {
        return Err(SecretError::TooLarge);
    }
    let mut raw = Vec::with_capacity(meta.len() as usize);
    // Bounded read: never pull more than limit+1 bytes even if the file grew.
    file.take(SECRET_MAX_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|_| SecretError::Unreadable)?;
    parse_service_secret(&raw, purpose)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "0123456789abcdef0123456789abcdef";

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("vrt-secret-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn write(dir: &Path, name: &str, bytes: &[u8], mode: u32) -> std::path::PathBuf {
        let p = dir.join(name);
        crate::atomic_write(&p, bytes, mode).unwrap();
        p
    }

    fn load(p: &Path) -> Result<ServiceSecret, SecretError> {
        load_service_secret(p, SecretPurpose::ServiceHmac)
    }

    #[test]
    fn trailing_newline_variants_yield_identical_key() {
        let d = tmp("nl");
        let a = load(&write(&d, "a", GOOD.as_bytes(), 0o600)).unwrap();
        let b = load(&write(&d, "b", format!("{GOOD}\n").as_bytes(), 0o600)).unwrap();
        let c = load(&write(&d, "c", format!("{GOOD}\r\n").as_bytes(), 0o600)).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(a.as_bytes(), GOOD.as_bytes());
    }

    #[test]
    fn rejects_internal_leading_and_double_trailing_whitespace() {
        for bad in [
            format!("{}\n{}", &GOOD[..16], &GOOD[16..]),
            format!("{} {}", &GOOD[..16], &GOOD[16..]),
            format!(" {GOOD}"),
            format!("{GOOD}\n\n"),
            format!("{GOOD}\t"),
            format!("{GOOD}\u{7}"),
        ] {
            assert_eq!(
                parse_service_secret(bad.as_bytes(), SecretPurpose::ServiceHmac).unwrap_err(),
                SecretError::InvalidByte,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn rejects_short_empty_placeholder_and_uniform() {
        let p = |b: &[u8]| parse_service_secret(b, SecretPurpose::ServiceHmac).unwrap_err();
        assert_eq!(p(b"short-secret"), SecretError::TooShort);
        assert_eq!(p(b"\n"), SecretError::Empty);
        assert_eq!(p(b""), SecretError::Empty);
        for v in [
            "REPLACE_ME_with_a_real_secret_value_0123456789",
            "xxxxChangeMexxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "my-placeholder-secret-0123456789abcdef",
        ] {
            assert_eq!(p(v.as_bytes()), SecretError::Placeholder, "{v}");
        }
        assert_eq!(p("q".repeat(32).as_bytes()), SecretError::LowEntropy);
        assert_eq!(p(&vec![b'a'; 5000]), SecretError::TooLarge);
    }

    #[test]
    fn credential_purpose_allows_short_but_not_empty() {
        assert!(parse_service_secret(b"AKIA1234\n", SecretPurpose::Credential).is_ok());
        assert_eq!(
            parse_service_secret(b"", SecretPurpose::Credential).unwrap_err(),
            SecretError::Empty
        );
    }

    #[test]
    fn rejects_bad_mode_missing_and_directory() {
        let d = tmp("mode");
        assert_eq!(load(&write(&d, "s", GOOD.as_bytes(), 0o644)).unwrap_err(), SecretError::BadMode);
        assert_eq!(load(&write(&d, "s2", GOOD.as_bytes(), 0o640)).unwrap_err(), SecretError::BadMode);
        assert_eq!(load(&d.join("none")).unwrap_err(), SecretError::Unreadable);
        assert_eq!(load(&d).unwrap_err(), SecretError::NotRegularFile);
    }

    #[test]
    fn errors_and_debug_never_contain_secret_bytes() {
        let secret = "Zq9-uniq-SECRETBYTES-0123456789abcdef!";
        let ok = parse_service_secret(secret.as_bytes(), SecretPurpose::ServiceHmac).unwrap();
        assert!(!format!("{ok:?}").contains("SECRETBYTES"));
        let d = tmp("leak");
        let e = load(&write(&d, "s", secret.as_bytes(), 0o644)).unwrap_err();
        assert!(!format!("{e} {e:?}").contains("SECRETBYTES"));
        for bad in [format!("SECRETBYTES {secret}"), "SECRETBYTES".to_string()] {
            let e = parse_service_secret(bad.as_bytes(), SecretPurpose::ServiceHmac).unwrap_err();
            assert!(!format!("{e} {e:?}").contains("SECRETBYTES"));
        }
    }

    #[test]
    fn parse_is_the_same_for_env_and_file_paths() {
        let d = tmp("env");
        let f = load(&write(&d, "s", format!("{GOOD}\n").as_bytes(), 0o600)).unwrap();
        let e = parse_service_secret(format!("{GOOD}\n").as_bytes(), SecretPurpose::ServiceHmac).unwrap();
        assert_eq!(f, e);
    }
}
