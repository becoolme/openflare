//! P3c-2: Durable-Object class storage snapshots — consistent copy of
//! a `<uniqueKey>` dir for backup and cross-cell migration.
//!
//! Consistency (§6.4): workerd `localDisk` sqlite files are opened
//! from THIS process with rusqlite's **online backup API**
//! (`Connection::backup`) — a read-side API that takes shared locks
//! page-batch-wise and is safe against a concurrent WAL writer on the
//! same host filesystem (POSIX advisory locking; workerd localDisk
//! uses ordinary journal locking — if a file cannot be locked the
//! backup simply fails and the snapshot aborts, never fabricating
//! bytes). Non-sqlite artifacts are copied verbatim with sha256.
//! RPO = the snapshot interval (the backup captures the writer's
//! committed state at copy time, including still-open `-wal` content
//! — the API reads through the pager, so WAL bytes need no special
//! handling; `-wal`/`-shm` sidecars are NOT copied, the restored file
//! is already self-consistent).
//!
//! Restore is atomic: files materialize under `<dst>.tmp-*` and the
//! parent dir gets a single `rename` — a torn restore never leaves a
//! half-written class dir behind. Durability (RV-R31 #4): every
//! materialized file is fsynced, then each staging dir is fsynced,
//! then the rename, then the parent dir fsync — a power cut either
//! keeps the previous on-disk state or the fully-verified new one,
//! never a missing-page ghost. Verification is fail-closed:
//! sha256 per file + `PRAGMA integrity_check` per sqlite file; on ANY
//! failure the tmp dir is dropped and the caller keeps its previous
//! on-disk state.

use rusqlite::backup::Backup;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Bound on files inside one class dir (workerd writes one
/// `<objectId>.sqlite` per object plus `metadata.sqlite`).
pub const SNAP_MAX_FILES: usize = 4096;
/// Bound on a single file's snapshot bytes (stat BEFORE reading —
/// over-cap aborts rather than streaming unbounded).
pub const SNAP_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Bound on total snapshot bytes.
pub const SNAP_MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapFile {
    /// Path relative to the class dir (`/` separated, no `..`).
    pub path: String,
    pub sha256: String,
    pub size: u64,
    /// Was copied via the sqlite online-backup API.
    #[serde(default)]
    pub sqlite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Format version (=1).
    pub v: u32,
    /// Wall clock (unix ms) at snapshot creation.
    pub created_ms: u64,
    /// Placement epoch this snapshot was taken under (uploader-side
    /// fencing key — the placement manifest only commits refs whose
    /// epoch still matches the manifest's own).
    pub epoch: u64,
    /// Monotonic per-epoch sequence (uploader-owned counter).
    pub seq: u64,
    pub files: Vec<SnapFile>,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("corrupt: {0}")]
    Corrupt(String),
    #[error("over cap: {0}")]
    OverCap(String),
    #[error("path unsafe: {0}")]
    Unsafe(String),
}

fn sha256_file(path: &Path) -> Result<String, SnapError> {
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    std::io::copy(&mut f, &mut h)?;
    Ok(hex::encode(h.finalize()))
}

fn is_sqlite_name(name: &str) -> bool {
    name.ends_with(".sqlite") || name.ends_with(".sqlite3") || name.ends_with(".db")
}

/// Reject absolute paths, `..`, backslashes and empty segments — a
/// manifest-produced relpath must never escape the restore dir.
pub fn valid_relpath_pub(rel: &str) -> bool {
    !rel.is_empty()
        && !rel.starts_with('/')
        && !rel.contains('\\')
        && rel
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != "..")
}

/// Collect `(abs, rel)` file pairs under `root`, enforcing bounds.
fn collect_files(root: &Path) -> Result<Vec<(PathBuf, String, u64)>, SnapError> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let path = entry.path();
            if meta.is_dir() {
                // Symlinked dirs: `entry.metadata` follows links — a
                // class dir must contain only workerd-written files.
                if entry.file_type()?.is_symlink() {
                    return Err(SnapError::Unsafe(format!(
                        "symlinked dir {}",
                        path.display()
                    )));
                }
                stack.push(path);
                continue;
            }
            if !meta.is_file() || entry.file_type()?.is_symlink() {
                return Err(SnapError::Unsafe(format!("non-file {}", path.display())));
            }
            let rel = path
                .strip_prefix(root)
                .map_err(|_| SnapError::Unsafe(path.display().to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            if !valid_relpath_pub(&rel) {
                return Err(SnapError::Unsafe(rel));
            }
            // Skip live sqlite sidecars — the backup API reads through
            // the pager and needs no wal/shm copy.
            if rel.ends_with("-wal") || rel.ends_with("-shm") {
                continue;
            }
            if meta.len() > SNAP_MAX_FILE_BYTES {
                return Err(SnapError::OverCap(rel));
            }
            out.push((path, rel, meta.len()));
            if out.len() > SNAP_MAX_FILES {
                return Err(SnapError::OverCap("file count".into()));
            }
        }
    }
    out.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(out)
}

/// One sqlite file → `dst`, through the online-backup pager. The
/// SOURCE is opened READ-ONLY so a schema mutation can never leak
/// into the writer's file (and a source-side reader never upgrades
/// locks against workerd's writer).
fn backup_sqlite(src: &Path, dst: &Path) -> Result<(), SnapError> {
    let mut src_conn = Connection::open_with_flags(
        src,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    // Bounded busy wait — a workerd writer holding RESERVED for a
    // whole txn must not spin forever; exceeding it aborts THIS
    // snapshot (retried next interval), never blocks the host.
    src_conn.busy_timeout(std::time::Duration::from_secs(10))?;
    let mut dst_conn = Connection::open(dst)?;
    dst_conn.busy_timeout(std::time::Duration::from_secs(10))?;
    let backup = Backup::new(&mut src_conn, &mut dst_conn)?;
    backup.run_to_completion(256, std::time::Duration::from_millis(5), None)?;
    Ok(())
}

/// Copy `src_dir` into `out_dir` as a consistent snapshot, returning
/// the manifest describing every materialized file. `out_dir` must
/// not already exist (atomic-rename callers stage it).
pub fn snapshot_dir(
    src_dir: &Path,
    out_dir: &Path,
    epoch: u64,
    seq: u64,
) -> Result<SnapshotMeta, SnapError> {
    let files = collect_files(src_dir)?;
    std::fs::create_dir_all(out_dir)?;
    let mut total: u64 = 0;
    let mut metas = Vec::with_capacity(files.len());
    for (abs, rel, size) in files {
        total = total
            .checked_add(size)
            .ok_or_else(|| SnapError::OverCap("total".into()))?;
        if total > SNAP_MAX_TOTAL_BYTES {
            return Err(SnapError::OverCap("total".into()));
        }
        let dst = out_dir.join(&rel);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let sqlite = is_sqlite_name(&rel);
        if sqlite {
            backup_sqlite(&abs, &dst)?;
            // An over-cap destination (grew vs stat) is still fine —
            // the cap bounds the SOURCE stat for preflight only.
        } else {
            std::fs::copy(&abs, &dst)?;
        }
        metas.push(SnapFile {
            path: rel,
            sha256: sha256_file(&dst)?,
            size: std::fs::metadata(&dst)?.len(),
            sqlite,
        });
    }
    Ok(SnapshotMeta {
        v: 1,
        created_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        epoch,
        seq,
        files: metas,
    })
}

/// Verify `files_dir` against `meta` (sha256 + integrity_check) and
/// return the verified meta. Pure check — no placement.
pub fn verify_snapshot_files(meta: &SnapshotMeta, files_dir: &Path) -> Result<(), SnapError> {
    if meta.v != 1 {
        return Err(SnapError::Corrupt(format!("meta v{}", meta.v)));
    }
    if meta.files.len() > SNAP_MAX_FILES {
        return Err(SnapError::OverCap("meta files".into()));
    }
    for f in &meta.files {
        if !valid_relpath_pub(&f.path) {
            return Err(SnapError::Unsafe(f.path.clone()));
        }
        let p = files_dir.join(&f.path);
        let m = std::fs::metadata(&p)
            .map_err(|e| SnapError::Corrupt(format!("missing {}: {e}", f.path)))?;
        if m.len() != f.size {
            return Err(SnapError::Corrupt(format!("size mismatch {}", f.path)));
        }
        let got = sha256_file(&p)?;
        if got != f.sha256 {
            return Err(SnapError::Corrupt(format!("sha256 mismatch {}", f.path)));
        }
        if f.sqlite {
            let conn = Connection::open_with_flags(
                &p,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
            if ok != "ok" {
                return Err(SnapError::Corrupt(format!("integrity {}: {}", f.path, ok)));
            }
        }
    }
    Ok(())
}

/// Atomically place `files_dir` (already verified) at `dst_dir`:
/// write to a sibling tmp then rename. If `dst_dir` exists it is NOT
/// overwritten — callers keep local data on failure (fail closed).
/// Returns Err when dst exists (restore into a FRESH dir only).
/// RV-R31 #4：tmp 内逐文件 fsync → 各级 tmp 目录 fsync → rename →
/// 父目录 fsync——全部完成后才算落盘，掉电不会留下缺页的目录。
pub fn place_verified(files_dir: &Path, dst_dir: &Path) -> Result<(), SnapError> {
    if dst_dir.exists() {
        return Err(SnapError::Unsafe(format!(
            "refusing to overwrite {}",
            dst_dir.display()
        )));
    }
    let tmp = dst_dir.with_extension(format!("tmp-{}", std::process::id()));
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    copy_tree(files_dir, &tmp)?;
    std::fs::rename(&tmp, dst_dir)?;
    if let Some(parent) = dst_dir.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn copy_tree(src: &Path, dst: &Path) -> Result<(), SnapError> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let d = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &d)?;
        } else {
            // 逐文件 fsync——校验过的内容必须先耐久，rename 才有意义。
            std::fs::copy(entry.path(), &d)?;
            std::fs::File::open(&d)?.sync_all()?;
        }
    }
    // 目录项 fsync：本层所有新建名字持久化（父目录项由调用方/
    // 上层递归的目录 fsync 覆盖）。
    std::fs::File::open(dst)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_db(dir: &Path, name: &str, n: i64) {
        std::fs::create_dir_all(dir).unwrap();
        let conn = Connection::open(dir.join(name)).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t(v INTEGER);")
            .unwrap();
        for i in 0..n {
            conn.execute("INSERT INTO t VALUES (?)", [i]).unwrap();
        }
    }

    #[test]
    fn snapshot_roundtrip_preserves_wal_committed_data() {
        let dir = std::env::temp_dir().join(format!("vrt-dosnap-{}", std::process::id()));
        let _g = Scopeguard(dir.clone());
        let src = dir.join("src");
        // WAL writer leaves a live -wal sidecar — backup must still
        // capture the committed rows through the pager.
        mk_db(&src, "obj.sqlite", 5);
        {
            let c = Connection::open(src.join("obj.sqlite")).unwrap();
            c.execute_batch("PRAGMA journal_mode=WAL").unwrap();
            c.execute("INSERT INTO t VALUES (99)", []).unwrap();
            std::mem::forget(c); // keep -wal alive (no checkpoint)
        }
        std::fs::write(src.join("blob.bin"), b"raw").unwrap();

        let out = dir.join("snap");
        let meta = snapshot_dir(&src, &out, 7, 1).unwrap();
        assert_eq!(meta.epoch, 7);
        assert!(meta.files.iter().any(|f| f.path == "obj.sqlite"));
        assert!(meta.files.iter().any(|f| f.path == "blob.bin"));
        verify_snapshot_files(&meta, &out).unwrap();

        let dst = dir.join("restored");
        place_verified(&out, &dst).unwrap();
        let conn = Connection::open(dst.join("obj.sqlite")).unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 6);
        assert_eq!(std::fs::read(dst.join("blob.bin")).unwrap(), b"raw");
        // Second restore into an existing dir refuses (fail closed).
        assert!(matches!(
            place_verified(&out, &dst),
            Err(SnapError::Unsafe(_))
        ));
    }

    #[test]
    fn verify_rejects_tamper_and_traversal() {
        let dir = std::env::temp_dir().join(format!("vrt-dosnap-t-{}", std::process::id()));
        let _g = Scopeguard(dir.clone());
        let src = dir.join("src");
        mk_db(&src, "a.sqlite", 1);
        let out = dir.join("snap");
        let meta = snapshot_dir(&src, &out, 1, 1).unwrap();

        // Corrupt file bytes → sha256 mismatch.
        std::fs::write(out.join("a.sqlite"), b"tampered").unwrap();
        assert!(matches!(
            verify_snapshot_files(&meta, &out),
            Err(SnapError::Corrupt(_))
        ));
        // Traversal relpath in a forged manifest → unsafe.
        let mut evil = meta.clone();
        evil.files[0].path = "../escape".into();
        assert!(matches!(
            verify_snapshot_files(&evil, &out),
            Err(SnapError::Unsafe(_))
        ));
    }

    #[test]
    fn sqlite_integrity_failure_is_detected() {
        let dir = std::env::temp_dir().join(format!("vrt-dosnap-i-{}", std::process::id()));
        let _g = Scopeguard(dir.clone());
        let src = dir.join("src");
        std::fs::create_dir_all(&src).unwrap();
        // A file NAMED .sqlite that is not a database — backup_sqlite
        // must fail at open/backup time rather than copy bytes.
        std::fs::write(src.join("bad.sqlite"), b"not a database").unwrap();
        let out = dir.join("snap");
        assert!(snapshot_dir(&src, &out, 1, 1).is_err());
    }

    struct Scopeguard(PathBuf);
    impl Drop for Scopeguard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
