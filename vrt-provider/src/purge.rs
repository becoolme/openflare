//! RV-R70: bounded provider-side prefix purge — the backend half of
//! the `STORAGE_PURGE` cell op. A purge deletes EVERY object under
//! the resource's physical prefix — KV `vrt:kv:{enc(tenant)}:{enc(ns)}:*`
//! plus its `vrt:kx:*` index; R2 `vrt/{enc(tenant)}/{enc(bucket)}/*`
//! plus unfinished multipart uploads under it.
//!
//! Safety invariants:
//! - **prefix boundary is structural**: `segment_encode` output is
//!   `[A-Za-z0-9_-%]` (no `:`/`/`/glob metachars), and every layout
//!   separates segments with a literal `:` or `/` — so purging
//!   `{t}/a/` can never touch `{t}/ab/`.
//! - **`complete` means verified gone**: a bounded pass returns
//!   `complete:false` whenever work may remain; `true` is emitted only
//!   after a clean final enumeration saw zero targets.
//! - **identity, not prefix**: the executor passes `(tenant, storageId)`
//!   and the backend derives the physical prefix — a payload can never
//!   name an arbitrary prefix.

use crate::error::ProviderError;

/// One bounded purge pass's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PurgeOutcome {
    /// Keys/objects deleted this pass (observability only).
    pub deleted: u64,
    /// True ONLY when the enumeration confirmed nothing remains —
    /// the sole signal that may flip a registry row to DELETED.
    pub complete: bool,
}

/// Registry-minted storage identity (`vrtkv_…` / `vrt-…`), non-empty,
/// ≤128 chars, `[A-Za-z0-9_-]` — matching both random ids and every
/// adoptable legacy shape. Rejecting anything else keeps a forged
/// payload from widening the derived prefix (no `.`, `%`, `:`, `/`,
/// glob metachars, or empty input → never an "everything" or
/// sibling-tenant purge).
pub fn valid_purge_target_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// KV purge targets are `vrtkv_*` (minted `vrtkv_{24}` or legacy
/// `vrtkv_{team}_{name}`) — strictly longer than the bare prefix, so
/// `vrtkv_` alone can never name "every namespace".
pub fn valid_kv_purge_id(id: &str) -> bool {
    valid_purge_target_id(id) && id.starts_with("vrtkv_") && id.len() > "vrtkv_".len()
}

/// R2 purge targets are `vrt-*` (minted `vrt-{16}` or legacy
/// `vrt-{sha256[0..16]}`) — strictly longer than the bare prefix.
pub fn valid_r2_purge_id(id: &str) -> bool {
    valid_purge_target_id(id) && id.starts_with("vrt-") && id.len() > "vrt-".len()
}

/// Team/tenant identity carried by the op payload — bounded,
/// `[A-Za-z0-9_-]` (cuids and team ids never need more).
pub fn valid_purge_tenant(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 191
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Bounded filesystem-directory purge — the shared pass behind the
/// FsKvProvider/FsR2Provider `purge_*` methods. Each call removes at
/// most `max_files` entries (files via `remove_file`, subdirectories
/// via `remove_dir_all` — the only subdirectory an fs layout can hold
/// is `.multipart` staging, which is capacity-bounded by the provider
/// config). An already-absent directory is a clean purge; an
/// emptied one is removed before `complete` is reported.
pub async fn purge_dir_bounded(
    dir: &std::path::Path,
    max_files: usize,
) -> Result<PurgeOutcome, ProviderError> {
    let mut deleted = 0u64;
    loop {
        if deleted as usize >= max_files {
            return Ok(PurgeOutcome {
                deleted,
                complete: false,
            });
        }
        let want = max_files - deleted as usize;
        let mut rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            // The directory is gone — nothing under the prefix can
            // remain. This IS the completion witness.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PurgeOutcome {
                    deleted,
                    complete: true,
                })
            }
            Err(_) => return Err(ProviderError::KvUnavailable),
        };
        let mut names = Vec::with_capacity(want.min(1024));
        for ent in (&mut rd).take(want) {
            let ent = ent.map_err(|_| ProviderError::KvUnavailable)?;
            names.push(ent.file_name());
        }
        drop(rd);
        if names.is_empty() {
            // Enumeration saw nothing left — remove the directory
            // itself; a racing mkdir fails closed (retryable next op).
            match std::fs::remove_dir(dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(ProviderError::KvUnavailable),
            }
            return Ok(PurgeOutcome {
                deleted,
                complete: true,
            });
        }
        for name in &names {
            let path = dir.join(name);
            let meta = std::fs::symlink_metadata(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // Racing delete — already gone; count it purged.
                    ProviderError::KvInvalidOptions
                } else {
                    ProviderError::KvUnavailable
                }
            });
            match meta {
                Ok(m) if m.is_dir() => {
                    std::fs::remove_dir_all(&path).map_err(|_| ProviderError::KvUnavailable)?;
                }
                Ok(_) => match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(ProviderError::KvUnavailable),
                },
                Err(ProviderError::KvInvalidOptions) => {}
                Err(e) => return Err(e),
            }
            deleted += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purge_id_validation() {
        assert!(valid_purge_target_id("vrtkv_abc123"));
        assert!(valid_r2_purge_id("vrt-0123456789abcdef"));
        // The two kinds are disjoint on purpose — and a bare prefix
        // is never a valid kind id (would name "every namespace").
        assert!(!valid_r2_purge_id("vrtkv_abc"));
        assert!(!valid_kv_purge_id("vrt-abc"));
        assert!(!valid_kv_purge_id("vrtkv_"));
        assert!(!valid_r2_purge_id("vrt-"));
        // Empty, oversized, and metachar input all reject — never a
        // wildcard or cross-boundary prefix.
        for bad in [
            "",
            &"v".repeat(129),
            "vrtkv_a:b",
            "vrtkv_a/b",
            "vrtkv_a%2Fb",
            "vrtkv_a*b",
            "vrtkv_a?b",
            "vrtkv_a.b",
            "vrt-a b",
            "vrt-a\tb",
        ] {
            assert!(!valid_purge_target_id(bad), "accepted {bad:?}");
        }
        assert!(!valid_kv_purge_id(&format!("vrtkv_{}", "x".repeat(200))));
    }

    #[tokio::test]
    async fn purge_dir_bounded_deletes_in_budget_and_completes() {
        let dir = std::env::temp_dir().join(format!("purge-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..5 {
            std::fs::write(dir.join(format!("k{i}")), b"v").unwrap();
        }
        // Budget smaller than the file count — incomplete, dir still
        // holds the remainder.
        let out = purge_dir_bounded(&dir, 3).await.unwrap();
        assert!(!out.complete);
        assert_eq!(out.deleted, 3);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 2);
        // Second pass finishes: directory removed, complete reported.
        let out = purge_dir_bounded(&dir, 10).await.unwrap();
        assert!(out.complete);
        assert_eq!(out.deleted, 2);
        assert!(!dir.exists());
        // Idempotent on an already-absent directory.
        let out = purge_dir_bounded(&dir, 10).await.unwrap();
        assert!(out.complete);
        assert_eq!(out.deleted, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_dir_bounded_never_touches_sibling_prefix() {
        // `{t}/a` vs `{t}/ab`: removing the `a` directory must leave
        // `ab` untouched (the boundary is the directory level itself,
        // but the test pins the guarantee).
        let base = std::env::temp_dir().join(format!("purge-sib-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let a = base.join("a");
        let ab = base.join("ab");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&ab).unwrap();
        std::fs::write(a.join("x"), b"v").unwrap();
        let keep = ab.join("x");
        std::fs::write(&keep, b"v").unwrap();
        let out = purge_dir_bounded(&a, 100).await.unwrap();
        assert!(out.complete);
        assert!(keep.exists());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn purge_tenant_validation() {
        assert!(valid_purge_tenant("team_abc-123"));
        assert!(!valid_purge_tenant(""));
        assert!(!valid_purge_tenant(&"t".repeat(192)));
        assert!(!valid_purge_tenant("a:b"));
        assert!(!valid_purge_tenant("a/b"));
    }
}
