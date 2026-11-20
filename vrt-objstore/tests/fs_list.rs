//! T36b: FsStore::list longest-directory-prefix walk + prefix validity.

use vrt_objstore::{FsStore, ObjectStore};

fn store_with(keys: &[&str]) -> (tempfile::TempDir, FsStore) {
    let dir = tempfile::tempdir().unwrap();
    let s = FsStore::new(dir.path().to_path_buf()).unwrap();
    for k in keys {
        let p = dir.path().join(k);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, b"x").unwrap();
    }
    (dir, s)
}

fn block<T: Send>(f: impl std::future::Future<Output = T> + Send) -> T {
    vrt_objstore::block_on_store(async move { Ok::<T, ()>(f.await) }).unwrap()
}

#[test]
fn list_scopes_to_deepest_dir_prefix() {
    let (_d, s) = store_with(&[
        "db/a/seg1",
        "db/a/seg2",
        "db/ab/x", // mid-boundary of the `db/a` prefix -- must not match
        "db/b/seg1",
        "other/z",
    ]);
    let out = block(s.list("db/a/")).unwrap().entries;
    let keys: Vec<_> = out.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, ["db/a/seg1", "db/a/seg2"]);
}

#[test]
fn list_mid_boundary_prefix_matches_siblings() {
    let (_d, s) = store_with(&["db/abc-1/k", "db/abc-2/k", "db/abd/k", "db/b/k"]);
    // The prefix lands mid directory name: `db/abc` matches abc-1/abc-2, not abd/b.
    let out = block(s.list("db/abc")).unwrap().entries;
    let keys: Vec<_> = out.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, ["db/abc-1/k", "db/abc-2/k"]);
}

#[test]
fn list_nonexistent_prefix_is_empty_not_error() {
    let (_d, s) = store_with(&["db/a/x"]);
    let out = block(s.list("db/zzz/")).unwrap().entries;
    assert!(out.is_empty());
    // A prefix passing through a nonexistent subdirectory is also empty.
    let out = block(s.list("nope/deep/")).unwrap().entries;
    assert!(out.is_empty());
}

#[test]
fn list_rejects_traversal_and_absolute_prefix() {
    let (_d, s) = store_with(&["db/a/x"]);
    for bad in ["../etc", "a/../../x", "/abs/path", "a//b"] {
        let r = block(s.list(bad));
        match bad {
            // the "a//b" segment contains an empty part -- an fs path join produces no traversal, but the key shape is illegal;
            // no legal key can ever exist under that prefix -> an empty result is acceptable, the point is not reading outside the tree.
            "a//b" => assert!(r.unwrap().entries.is_empty()),
            _ => assert!(r.is_err(), "prefix {bad} must be rejected"),
        }
    }
}

#[test]
fn list_ignores_lock_and_tmp_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let s = FsStore::new(dir.path().to_path_buf()).unwrap();
    std::fs::create_dir_all(dir.path().join("db/a")).unwrap();
    std::fs::write(dir.path().join("db/a/k"), b"x").unwrap();
    std::fs::write(dir.path().join("db/a/k.lock"), b"").unwrap();
    std::fs::write(dir.path().join("db/a/.k.tmp-1-1"), b"").unwrap();
    let out = block(s.list("db/a/")).unwrap().entries;
    let keys: Vec<_> = out.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, ["db/a/k"]);
}
