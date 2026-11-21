//! databaseId -> storage path derivation (section 7): one-way server-side derivation + path traversal protection.

use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error("invalid database id")]
    InvalidId,
    #[error("path escapes database root")]
    Traversal,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// T37r: resource kind -- structural namespace separation. The `EngineRegistry` key,
/// object-key prefix, local db_root and tombstone marker are all split by kind:
/// opening the same id as D1 and as Queue yields two disjoint databases (object keys `db/{id}`
/// vs `q/{id}`, local `dbs/` vs `queues/` directories, each with its own `.dropped`
/// marker). `_vrt_q_meta.kind` is kept as a second-line check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceKind {
    D1,
    Queue,
    K2,
}

impl ResourceKind {
    /// Object-key prefix: D1 `db/{id}/...` (T36 layout, must not change), Queue `q/{id}/...`,
    /// K2 stream `s/{id}/...`.
    pub fn object_prefix(self) -> &'static str {
        match self {
            Self::D1 => "db",
            Self::Queue => "q",
            Self::K2 => "s",
        }
    }
}

/// queue local root = `queues/` sibling of the D1 db_root (db_root looks like `.../dbs`).
/// When the parent directory is missing it degrades to `db_root/queues`, still disjoint from D1 paths.
pub fn queue_root_for(db_root: &Path) -> PathBuf {
    match db_root.parent() {
        Some(p) => p.join("queues"),
        None => db_root.join("queues"),
    }
}

/// K2 stream local root = `k2/` sibling of the D1 db_root (same rule as `queue_root_for`).
pub fn k2_root_for(db_root: &Path) -> PathBuf {
    match db_root.parent() {
        Some(p) => p.join("k2"),
        None => db_root.join("k2"),
    }
}

/// K2 stream id only allows the CF shape (32 lowercase hex).
pub fn validate_stream_id(id: &str) -> Result<(), PathError> {
    if id.len() != 32
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(PathError::InvalidId);
    }
    Ok(())
}

/// Kind-aware id validation: D1/Queue take the ULID shape, K2 the 32-hex stream id.
pub fn validate_id_for_kind(kind: ResourceKind, id: &str) -> Result<(), PathError> {
    match kind {
        ResourceKind::D1 | ResourceKind::Queue => validate_database_id(id),
        ResourceKind::K2 => validate_stream_id(id),
    }
}

/// databaseId only allows the ULID shape (26 lowercase alphanumerics) or the
/// K2 stream-id shape (32 lowercase hex). The path layer's job is traversal
/// safety — strict per-kind shape policy is enforced at the API layer
/// (`validate_id_for_kind`); both accepted shapes are safe path components.
pub fn validate_database_id(id: &str) -> Result<(), PathError> {
    let ulid = id.len() == 26;
    let k2 = id.len() == 32 && !id.bytes().any(|b| (b'g'..=b'z').contains(&b));
    if !(ulid || k2)
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err(PathError::InvalidId);
    }
    Ok(())
}

/// Derive the `{dbRoot}/{sha256(id)[:8]}/{id}/` directory (created with 0700 if absent),
/// and return the `db.sqlite` path. The sha256 prefix spreads the directory distribution; the id itself is the second level.
/// Any resolution result that traverses / follows a symlink / escapes root is rejected.
pub fn derive_db_file(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    validate_database_id(id)?;
    let shard = &hex::encode(sha2::Sha256::digest(id.as_bytes()))[..8];
    let leaf_dir = db_root.join(shard).join(id);

    vrt_securefs::ensure_secure_dir(&leaf_dir).map_err(PathError::Io)?;

    // After canonicalize it must still be inside canonical(db_root) (guards against symlink/mount escape).
    let canon_root = db_root.canonicalize().map_err(PathError::Io)?;
    let canon_leaf = leaf_dir.canonicalize().map_err(PathError::Io)?;
    if !canon_leaf.starts_with(&canon_root) {
        return Err(PathError::Traversal);
    }
    // If the target db file already exists it must be a regular file (symlinks forbidden).
    let db_file = canon_leaf.join("db.sqlite");
    if let Ok(meta) = fs::symlink_metadata(&db_file) {
        if meta.file_type().is_symlink() {
            return Err(PathError::Traversal);
        }
    }
    Ok(db_file)
}

/// T35b DROP: derives the same `{dbRoot}/{sha256(id)[:8]}/{id}` path, but **does not create**
/// any directory; returns None if absent. The canonical check follows the same rules as derive_db_file
/// (must lie inside canonical(db_root), symlink escapes forbidden); the path is composed only of
/// the whitelisted databaseId.
pub fn derive_db_dir_readonly(db_root: &Path, id: &str) -> Result<Option<PathBuf>, PathError> {
    validate_database_id(id)?;
    let shard = &hex::encode(sha2::Sha256::digest(id.as_bytes()))[..8];
    let leaf_dir = db_root.join(shard).join(id);
    match fs::symlink_metadata(&leaf_dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(PathError::Io(e)),
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(PathError::Traversal);
            }
        }
    }
    let canon_root = db_root.canonicalize().map_err(PathError::Io)?;
    let canon_leaf = leaf_dir.canonicalize().map_err(PathError::Io)?;
    if !canon_leaf.starts_with(&canon_root) {
        return Err(PathError::Traversal);
    }
    Ok(Some(canon_leaf))
}

/// T35c DROP tombstone: persistent marker `{dbRoot}/{shard}/{id}.dropped`,
/// a sibling of the db leaf directory (never inside the leaf -- the leaf is renamed whole into
/// `.trash`). Once written the marker is kept forever: databaseId is randomly minted by the server,
/// never semantically reused, and open must still be refused after a restart.
pub fn marker_path(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    validate_database_id(id)?;
    let shard = &hex::encode(sha2::Sha256::digest(id.as_bytes()))[..8];
    Ok(db_root.join(shard).join(format!("{id}.dropped")))
}

/// Whether the marker exists (regular file, symlinks rejected -- same rule as derive).
pub fn marker_exists(db_root: &Path, id: &str) -> Result<bool, PathError> {
    let p = marker_path(db_root, id)?;
    match fs::symlink_metadata(&p) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(PathError::Io(e)),
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(PathError::Traversal);
            }
            Ok(meta.is_file())
        }
    }
}

/// Write the tombstone marker (idempotent overwrite). A missing shard directory is created with 0700;
/// the content is for forensics only (drop timestamp) and is not parsed.
pub fn write_marker(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    let p = marker_path(db_root, id)?;
    if let Some(shard_dir) = p.parent() {
        vrt_securefs::ensure_secure_dir(shard_dir).map_err(PathError::Io)?;
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    fs::write(&p, format!("dropped {stamp}\n")).map_err(PathError::Io)?;
    Ok(p)
}

/// `{dbRoot}/.trash/` -- the first landing spot of a drop: the leaf is first atomically renamed in
/// (same filesystem), then asynchronously remove_dir_all'd, shrinking the window in which a "half-deleted state can be read"
/// to the rename itself.
pub fn ensure_trash_dir(db_root: &Path) -> Result<PathBuf, PathError> {
    let dir = db_root.join(".trash");
    vrt_securefs::ensure_secure_dir(&dir).map_err(PathError::Io)?;
    Ok(dir)
}

/// Startup scan: return all ids referenced by `{dbRoot}/*/*.dropped` (pre-filled into the
/// registry tombstone set, saving a stat on every cold open).
pub fn scan_dropped_ids(db_root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(shards) = fs::read_dir(db_root) else {
        return out;
    };
    for shard in shards.flatten() {
        let sp = shard.path();
        if !sp.is_dir() || shard.file_name() == ".trash" {
            continue;
        }
        let Ok(entries) = fs::read_dir(&sp) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(id) = name.strip_suffix(".dropped") {
                if validate_database_id(id).is_ok() {
                    out.push(id.to_string());
                }
            }
        }
    }
    out
}

/// RV-R53.3 GC-complete side sentinel: `{shard}/{id}.dropped.gc-done`.
/// The `.dropped` itself is never deleted (the open-refusal record); after GC completes a separate sentinel is written
/// so `scan_gc_pending_ids` skips it -- otherwise permanent markers would fill every round's
/// scan quota and markers at the tail of the queue would starve.
pub fn gc_done_path(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    let mut p = marker_path(db_root, id)?;
    let name = p.file_name().unwrap_or_default().to_os_string();
    p.set_file_name(format!("{}.gc-done", name.to_string_lossy()));
    Ok(p)
}

/// Write the GC-complete sentinel (idempotent).
pub fn write_gc_done(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    let p = gc_done_path(db_root, id)?;
    fs::write(&p, b"gc-done\n").map_err(PathError::Io)?;
    Ok(p)
}

/// Whether a GC-complete sentinel exists (regular file; a symlink counts as not done -- fail
/// closed so the next round re-checks).
pub fn is_gc_done(db_root: &Path, id: &str) -> bool {
    gc_done_path(db_root, id)
        .map(|p| {
            fs::symlink_metadata(&p)
                .map(|m| m.is_file())
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

/// GC scan candidates: all `.dropped` ids minus those with a `.gc-done` already written.
/// Stable sort (read_dir order is unstable) keeps processing within each round's cap deterministic.
pub fn scan_gc_pending_ids(db_root: &Path) -> Vec<String> {
    let mut ids = scan_dropped_ids(db_root);
    ids.sort();
    ids.retain(|id| !is_gc_done(db_root, id));
    ids
}

/// Startup sweep: `.trash` can only hold debris that was "tombstoned, crashed before remove",
/// clear it all. Best-effort: a single failure does not block startup.
pub fn sweep_trash(db_root: &Path) {
    let dir = db_root.join(".trash");
    let Ok(entries) = fs::read_dir(&dir) else {
        return;
    };
    for e in entries.flatten() {
        let _ = fs::remove_dir_all(e.path());
    }
}

use sha2::Digest;

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> &'static str {
        "01jqqm3b4y6zq8f0v2w5x7k9ae"
    }

    #[test]
    fn id_validation() {
        assert!(validate_database_id(id()).is_ok());
        for bad in [
            "",
            "short",
            &"a".repeat(25),
            &"a".repeat(27),
            "A".repeat(26).as_str(),
            "../etc/passwd0000000000x",
            "a/b",
            "a\\b",
            "a.b",
            "a b",
        ] {
            assert!(validate_database_id(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn derive_readonly_no_create_and_traversal() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dbs");
        std::fs::create_dir(&root).unwrap();
        // Missing db → None, and NO directories are created.
        assert!(derive_db_dir_readonly(&root, id()).unwrap().is_none());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        // Bad ids fail before any filesystem touch.
        for bad in ["../escape", "a/b", &"A".repeat(26), "short"] {
            assert!(derive_db_dir_readonly(&root, bad).is_err(), "{bad}");
        }
        // Existing leaf → canonical path inside root.
        let f = derive_db_file(&root, id()).unwrap();
        assert!(f.exists() || f.parent().unwrap().exists());
        let leaf = derive_db_dir_readonly(&root, id()).unwrap().unwrap();
        assert!(leaf.starts_with(root.canonicalize().unwrap()));
        // Symlinked leaf dir → Traversal, never followed.
        let shard = &hex::encode(sha2::Sha256::digest(id().as_bytes()))[..8];
        let shard_dir = root.join(shard);
        let evil = tmp.path().join("evil");
        std::fs::create_dir(&evil).unwrap();
        std::fs::remove_dir_all(shard_dir.join(id())).ok();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&evil, shard_dir.join(id())).unwrap();
            assert!(matches!(
                derive_db_dir_readonly(&root, id()),
                Err(PathError::Traversal)
            ));
        }
    }

    #[test]
    fn derive_layout_and_traversal_guard() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dbs");
        std::fs::create_dir(&root).unwrap();
        let f = derive_db_file(&root, id()).unwrap();
        assert!(f.ends_with("db.sqlite"));
        assert!(f
            .to_str()
            .unwrap()
            .contains(&format!("/{}/db.sqlite", id())));

        // symlinked db file -> rejected
        let shard = &hex::encode(sha2::Sha256::digest(id().as_bytes()))[..8];
        let link = root.join(shard).join(id()).join("db.sqlite");
        std::fs::write(&link, b"x").unwrap();
        let real = tmp.path().join("victim.sqlite");
        std::fs::write(&real, b"").unwrap();
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            derive_db_file(&root, id()),
            Err(PathError::Traversal)
        ));
    }

    #[test]
    fn symlinked_root_leaf_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("dbs");
        std::fs::create_dir(&root).unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let shard = &hex::encode(sha2::Sha256::digest(id().as_bytes()))[..8];
        std::fs::create_dir(root.join(shard)).unwrap();
        std::os::unix::fs::symlink(&outside, root.join(shard).join(id())).unwrap();
        // leaf is a symlink -> rejected by ensure_secure_dir or the canonical prefix check
        assert!(derive_db_file(&root, id()).is_err());
    }
}
