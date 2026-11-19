//! databaseId → 存储路径派生（§7）：服务端单向派生 + 路径穿越防护。

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

/// T37r：资源种类——结构性命名空间分离。`EngineRegistry` 的键、
/// 对象键前缀、本地 db_root 与 tombstone marker 全部按 kind 分面：
/// 同一 id 以 D1 与 Queue 打开是两个互不相交的库（对象键 `db/{id}`
/// 与 `q/{id}`、本地 `dbs/` 与 `queues/` 目录、各自 `.dropped`
/// marker）。`_vrt_q_meta.kind` 保留为第二道校验。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceKind {
    D1,
    Queue,
}

impl ResourceKind {
    /// 对象键前缀：D1 `db/{id}/…`（T36 布局，不得变），Queue `q/{id}/…`。
    pub fn object_prefix(self) -> &'static str {
        match self {
            Self::D1 => "db",
            Self::Queue => "q",
        }
    }
}

/// queue 本地根 = D1 db_root 的同级 `queues/`（db_root 形如 `…/dbs`）。
/// 父目录缺失时退化为 `db_root/queues`，仍与 D1 路径不相交。
pub fn queue_root_for(db_root: &Path) -> PathBuf {
    match db_root.parent() {
        Some(p) => p.join("queues"),
        None => db_root.join("queues"),
    }
}

/// databaseId 只允许 ULID 形态（26 位小写字母数字）。
pub fn validate_database_id(id: &str) -> Result<(), PathError> {
    if id.len() != 26
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err(PathError::InvalidId);
    }
    Ok(())
}

/// 派生 `{dbRoot}/{sha256(id)[:8]}/{id}/` 目录（不存在则 0700 创建），
/// 返回 `db.sqlite` 路径。sha256 前缀打散目录分布；id 本身做第二级目录。
/// 任何穿越/软链接/逃逸 root 的解析结果都拒绝。
pub fn derive_db_file(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    validate_database_id(id)?;
    let shard = &hex::encode(sha2::Sha256::digest(id.as_bytes()))[..8];
    let leaf_dir = db_root.join(shard).join(id);

    vrt_securefs::ensure_secure_dir(&leaf_dir).map_err(PathError::Io)?;

    // canonicalize 后必须仍在 canonical(db_root) 内（防软链接/挂载逃逸）。
    let canon_root = db_root.canonicalize().map_err(PathError::Io)?;
    let canon_leaf = leaf_dir.canonicalize().map_err(PathError::Io)?;
    if !canon_leaf.starts_with(&canon_root) {
        return Err(PathError::Traversal);
    }
    // 目标 db 文件若已存在，必须是普通文件（禁软链接）。
    let db_file = canon_leaf.join("db.sqlite");
    if let Ok(meta) = fs::symlink_metadata(&db_file) {
        if meta.file_type().is_symlink() {
            return Err(PathError::Traversal);
        }
    }
    Ok(db_file)
}

/// T35b DROP：同 `{dbRoot}/{sha256(id)[:8]}/{id}` 派生，但**不创建**
/// 任何目录；不存在返回 None。canonical 校验与 derive_db_file 同规则
/// （必须位于 canonical(db_root) 内、禁符号链接逃逸），路径只由
/// 白名单 databaseId 组成。
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

/// T35c DROP tombstone：持久标记 `{dbRoot}/{shard}/{id}.dropped`，
/// 与 db leaf 目录平级（绝不放 leaf 内——leaf 会整体 rename 进
/// `.trash`）。marker 一旦写入永久保留：databaseId 由服务端随机
/// mint，语义上不复用，重启后 open 仍须被拒。
pub fn marker_path(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    validate_database_id(id)?;
    let shard = &hex::encode(sha2::Sha256::digest(id.as_bytes()))[..8];
    Ok(db_root.join(shard).join(format!("{id}.dropped")))
}

/// marker 是否存在（regular file，拒符号链接——同 derive 规则）。
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

/// 写入 tombstone marker（幂等覆盖）。shard 目录缺失时按 0700 创建；
/// 内容仅取证用（drop 时间戳），不作解析。
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

/// `{dbRoot}/.trash/` —— drop 的第一落点：leaf 先原子 rename 进来
/// （同文件系统），再异步 remove_dir_all，把"半删状态可被读到"的
/// 窗口缩到 rename 本身。
pub fn ensure_trash_dir(db_root: &Path) -> Result<PathBuf, PathError> {
    let dir = db_root.join(".trash");
    vrt_securefs::ensure_secure_dir(&dir).map_err(PathError::Io)?;
    Ok(dir)
}

/// 启动扫描：返回 `{dbRoot}/*/*.dropped` 指向的全部 id（预灌进
/// registry tombstone set，省掉每次 cold open 的 stat）。
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

/// RV-R53.3 GC 完成旁挂哨兵：`{shard}/{id}.dropped.gc-done`。
/// `.dropped` 本身永不删除（open 拒绝记录）；GC 完成后另写哨兵，
/// 让 `scan_gc_pending_ids` 跳过它——否则永久 marker 会占满每轮
/// 扫描名额，队尾 marker 被饿死。
pub fn gc_done_path(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    let mut p = marker_path(db_root, id)?;
    let name = p.file_name().unwrap_or_default().to_os_string();
    p.set_file_name(format!("{}.gc-done", name.to_string_lossy()));
    Ok(p)
}

/// 写 GC 完成哨兵（幂等）。
pub fn write_gc_done(db_root: &Path, id: &str) -> Result<PathBuf, PathError> {
    let p = gc_done_path(db_root, id)?;
    fs::write(&p, b"gc-done\n").map_err(PathError::Io)?;
    Ok(p)
}

/// 是否有 GC 完成哨兵（regular file；符号链接视为未 done——fail
/// closed 让下轮重查）。
pub fn is_gc_done(db_root: &Path, id: &str) -> bool {
    gc_done_path(db_root, id)
        .map(|p| {
            fs::symlink_metadata(&p)
                .map(|m| m.is_file())
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

/// GC 扫描候选：全部 `.dropped` 中排除已写 `.gc-done` 的 id。
/// 稳定排序（read_dir 顺序不稳）保证每轮上限内的处理确定性。
pub fn scan_gc_pending_ids(db_root: &Path) -> Vec<String> {
    let mut ids = scan_dropped_ids(db_root);
    ids.sort();
    ids.retain(|id| !is_gc_done(db_root, id));
    ids
}

/// 启动清扫：`.trash` 里只会有"已 tombstone、remove 前崩溃"的残骸，
/// 全部清除。best-effort：单个失败不阻塞启动。
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

        // 软链接 db 文件 → 拒
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
        // leaf 是软链接 → ensure_secure_dir 或 canonical 前缀检查拒绝
        assert!(derive_db_file(&root, id()).is_err());
    }
}
