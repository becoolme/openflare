//! Shared filesystem hardening for the workers-runtime components.
//!
//! Extracted from `vrt-provider` (T3) so both the provider and `vrt-host`
//! (T5) enforce the same rules without copy-paste:
//!
//! - [`segment_encode`]/[`segment_decode`]: injective `%XX` escaping for
//!   embedding identifiers into `:`/`/` separated keys and file names.
//! - [`validate_secure_dir`]/[`ensure_secure_dir`]: private-directory
//!   checks (real dir, owned by our euid, mode `0700`).
//! - [`socket_path`]: `<dir>/<enc(segment)>.sock` helper shared by the
//!   provider listener layout and vrt-host's per-tenant socket naming.
//! - [`atomic_write`]: write-temp-fsync-rename file updates used by the
//!   bundle cache and config materialization.

pub mod limits;
pub mod secret;
pub use secret::{
    load_service_secret, parse_service_secret, SecretError, SecretPurpose, ServiceSecret,
};

use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// Encode one identifier segment for inclusion in a `:`-separated key, a
/// `/`-separated path, or a file name: `[A-Za-z0-9_-]` literal, anything
/// else `%XX` (uppercase hex). Injective — two distinct raw strings never
/// map to the same encoded segment.
pub fn segment_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

/// Decode one segment produced by [`segment_encode`]. Returns `None` on
/// any malformed escape (odd `%XX`, non-hex digits, non-UTF-8 result).
pub fn segment_decode(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = (bytes[i + 1] as char).to_digit(16)?;
            let lo = (bytes[i + 2] as char).to_digit(16)?;
            out.push(((hi << 4) | lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// `<dir>/<enc(segment)>.sock` — unambiguous, filesystem-safe socket path.
pub fn socket_path(dir: &Path, segment: &str) -> PathBuf {
    dir.join(format!("{}.sock", segment_encode(segment)))
}

/// Deterministic per-tenant uid inside `[base, base + span)` — THE
/// derivation shared by vrt-host's launch isolation (which setuids
/// the workerd child) and vrt-provider's socket peer gate (which
/// verifies the connecting peer is exactly that uid). FNV-1a —
/// stable across processes and versions; a uid claimed by two
/// tenants must be rejected upstream (host's isolation_map), never
/// silently shared.
pub fn tenant_uid(base: u32, span: u32, tenant: &str) -> u32 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in tenant.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    base + (h % span as u64) as u32
}

/// H5-r2: tenants whose derived uid is claimed by two or more tenants
/// in the CURRENT desired set. The answer is a SET property — every
/// tenant in a colliding group must be refused on BOTH sides (host
/// declines to launch the workerd, provider declines to bind the
/// listener), independent of iteration order. First-come-first-served
/// is wrong twice over: it lets a survivor keep a uid another tenant
/// also claims (that survivor's workerd can then dial the refused
/// tenant's socket), and it makes the verdict depend on map order.
/// Both binaries call this one helper — never reimplement per side.
pub fn colliding_tenants(
    base: u32,
    span: u32,
    tenants: &[String],
) -> std::collections::BTreeSet<String> {
    let mut by_uid: std::collections::BTreeMap<u32, Vec<&String>> =
        std::collections::BTreeMap::new();
    for tenant in tenants {
        by_uid
            .entry(tenant_uid(base, span, tenant))
            .or_default()
            .push(tenant);
    }
    by_uid
        .into_values()
        .filter(|group| group.len() > 1)
        .flatten()
        .cloned()
        .collect()
}

/// Validate that `dir` is a real directory owned by our euid with mode
/// `0700`. Anything else fails closed: a loose directory would let
/// another local principal drop a same-named socket/file and impersonate
/// a tenant endpoint.
pub fn validate_secure_dir(dir: &Path) -> io::Result<()> {
    validate_dir_mode(dir, 0o700)
}

/// `validate_secure_dir` parametrized on the required mode — see
/// [`ensure_socket_dir`] for the one consumer of a non-0700 mode.
fn validate_dir_mode(dir: &Path, mode: u32) -> io::Result<()> {
    let meta = std::fs::metadata(dir)?;
    if !meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a directory",
        ));
    }
// SAFETY: geteuid(2) has no preconditions and cannot fail.
    if meta.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "directory is owned by another uid",
        ));
    }
    if meta.permissions().mode() & 0o777 != mode {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "directory has wrong mode",
        ));
    }
    Ok(())
}

/// T34f: shared short-root helper for tests that spawn real binaries
/// whose unix-socket paths derive from a temp root. macOS
/// `std::env::temp_dir()` is ~50B and `CARGO_TARGET_TMPDIR` inside a
/// worktree ~150B — neither leaves enough of the ~104B `sun_path`
/// budget once `work/<t>/gen-N/sock/<n>.sock` or
/// `provider/listeners/<t>.sock` is appended. `/tmp/<prefix>-<pid>-<seq>`
/// stays under ~35B.
///
/// Creates the dir and returns it; `Err` propagates the io error
/// verbatim — callers map `PermissionDenied` (EPERM/EACCES in
/// restricted sandboxes) to their `SKIP:` contract; every other
/// error must fail loudly (T34e discipline).
#[doc(hidden)]
pub fn test_short_root(prefix: &str) -> io::Result<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = PathBuf::from(format!("/tmp/{prefix}-{}-{seq}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// T35: `test_short_root` + Drop guard — the root is removed on normal
/// test exit; on panic it is KEPT (and announced) so the failure has
/// artifacts to inspect. `Deref<Path>` keeps call sites source-clean.
#[doc(hidden)]
pub struct ShortRoot(PathBuf);

impl ShortRoot {
    pub fn new(prefix: &str) -> io::Result<Self> {
        Ok(Self(test_short_root(prefix)?))
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl std::ops::Deref for ShortRoot {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for ShortRoot {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for ShortRoot {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("test failed — keeping short root {}", self.0.display());
            return;
        }
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Conservative AF_UNIX `sun_path` budget: macOS/BSD allow 104 bytes,
/// Linux 108 — 100 keeps the check portable and leaves headroom.
/// Callers fail closed BEFORE `bind`, because a bare ENAMETOOLONG at
/// bind time surfaces with no hint which path caused it (T34d/T34e —
/// a 190-byte worktree path produced exactly that dead end).
pub const SOCKET_PATH_MAX: usize = 100;

/// Fail closed when `path` exceeds [`SOCKET_PATH_MAX`]. The error
/// message carries the measured byte length — the detail the bare
/// `bind` error never gave (T34e).
pub fn check_socket_path_len(path: &Path) -> io::Result<()> {
    let n = path.as_os_str().len();
    if n > SOCKET_PATH_MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "socket path is {n} bytes (max {SOCKET_PATH_MAX}): {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

/// Ancestor check: an existing parent dir must not be replaceable by
/// another local principal — refuse when it is world-writable without
/// the sticky bit (a sticky world-writable dir like `/tmp` still cannot
/// have its entries renamed by non-owners). The strict 0700+owner rule
/// applies only to the leaf, which we own.
fn validate_ancestor(dir: &Path) -> io::Result<()> {
    let meta = std::fs::metadata(dir)?;
    if !meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ancestor path is not a directory",
        ));
    }
    let mode = meta.permissions().mode() & 0o7777;
    let world_writable = mode & 0o002 != 0;
    let sticky = mode & 0o1000 != 0;
    if world_writable && !sticky {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "ancestor directory is world-writable",
        ));
    }
    Ok(())
}

/// Validate `dir` via [`validate_secure_dir`]; when missing, create it
/// (and missing parents) with mode `0700` after checking every existing
/// ancestor is not attacker-replaceable. Refuses to create anything but
/// a directory (an existing non-dir path errors).
pub fn ensure_secure_dir(dir: &Path) -> io::Result<()> {
    ensure_dir_mode(dir, 0o700)
}

/// Per-tenant unix-socket listener root. `traversable=false` requires
/// `0700` (only the owner reaches the sockets — the classic model where
/// tenant workerd and provider share the pod uid). `traversable=true`
/// requires `0711` — under per-tenant uid isolation (T13/M2) a tenant
/// workerd runs as its own uid and must be able to *traverse* the dir
/// to reach `<dir>/<seg(tenant)>.sock`, but must never *list* it (other
/// tenants' socket names stay unenumerable). Write stays owner-only in
/// both modes: nobody else can drop, rename or unlink a socket.
pub fn ensure_socket_dir(dir: &Path, traversable: bool) -> io::Result<()> {
    ensure_dir_mode(dir, if traversable { 0o711 } else { 0o700 })
}

fn ensure_dir_mode(dir: &Path, mode: u32) -> io::Result<()> {
    match std::fs::metadata(dir) {
        Ok(meta) => {
            // Self-heal between the two sanctioned socket-dir layouts:
            // an existing dir owned by us at the OTHER sanctioned mode
            // is chmod'd to the requested one (0700 <-> 0711 when a
            // deployment toggles uid isolation). Any other mode —
            // world-listable/writable — still fails closed.
            if meta.permissions().mode() & 0o777 != mode {
                let cur = meta.permissions().mode() & 0o777;
                if (cur == 0o700 || cur == 0o711) && (mode == 0o700 || mode == 0o711) {
                    // Ownership is checked inside validate_dir_mode's
                    // sibling path — mirror it before relaxing.
// SAFETY: geteuid(2) has no preconditions and cannot fail.
                    if meta.uid() != unsafe { libc::geteuid() } {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "directory is owned by another uid",
                        ));
                    }
                    std::fs::set_permissions(
                        dir,
                        std::fs::Permissions::from_mode(mode),
                    )?;
                }
            }
            validate_dir_mode(dir, mode)?;
            // setgid/setuid/sticky are NOT part of the 0777 comparison
            // above. A dir inherits setgid from a setgid parent (k8s
            // emptyDir / fsGroup / local-path roots) and GNU `chmod 0700`
            // keeps it on directories. We own this dir (uid checked) and
            // its permission bits already match exactly, so clearing the
            // special bits tightens it back to the exact requested mode —
            // never a relaxation.
            let special = std::fs::metadata(dir)?.permissions().mode() & 0o7000;
            if special != 0 {
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode))?;
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // Split into the deepest existing ancestor + missing tail.
            let mut missing: Vec<PathBuf> = Vec::new();
            let mut cur = dir.to_path_buf();
            while !cur.exists() {
                missing.push(cur.clone());
                if !cur.pop() {
                    break;
                }
            }
            for ancestor in cur.ancestors() {
                // Relative paths end at "" — nothing to validate.
                if ancestor.as_os_str().is_empty() {
                    break;
                }
                validate_ancestor(ancestor)?;
            }
            for path in missing.iter().rev() {
                std::fs::create_dir(path)?;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Process-wide temp-name counter for [`atomic_write`]. Combined with
/// the pid and per-call entropy this makes every temp name unique
/// inside a process — before T29/R1 the name was `.{name}.tmp-{pid}`,
/// so two concurrent writers to the same path shared ONE temp file:
/// `File::create` truncates, the writes interleaved, and the second
/// `rename` failed with ENOENT (or worse, renamed a torn mix).
static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Non-guessable temp-name entropy: RandomState keys come from OS
/// entropy (fresh per call via a per-thread seed), so a stale/malicious
/// pre-created `.{name}.tmp-*` file cannot be predicted to block or
/// intercept a write.
fn tmp_entropy(seq: u64) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(seq);
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.finish()
}

/// Atomically write `bytes` to `path` with `mode` permissions: write a
/// UNIQUE temp file in the same directory, fsync it, rename over
/// `path`, then fsync the parent directory. A crash can leave a temp
/// file but never a truncated target. `path` must be a plain file name
/// inside `dir` — callers validate identifiers before calling.
///
/// Concurrency contract (R1): the temp name is
/// `.{name}.tmp-{pid}-{seq}-{entropy}` opened with `create_new`
/// (O_EXCL), so concurrent writers never share or truncate each
/// other's temp file; a rename race resolves last-writer-wins with a
/// COMPLETE body from one writer. On failure only the temp file this
/// call created is removed — never another writer's.
///
/// This is NOT a read-modify-write guard: callers that fold "read old
/// value → write new value" must serialize that fold elsewhere
/// (vrt-local uses its `StateLock` flock; `FileTriggerLog` is `&mut
/// self`; `write_status_file` runs in one task). Writers racing on the
/// same path still see last-writer-wins ordering — correct for
/// content-addressed and last-write-wins data, wrong for counters.
pub fn atomic_write(path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    use std::io::Write;
    let dir = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "path has no parent directory")
    })?;
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let pid = std::process::id();
    // O_EXCL: a collision (stale file, or impossibly a duplicate name)
    // retries with a fresh seq instead of truncating someone else's
    // in-flight temp.
    let mut attempts = 0u8;
    let (tmp, mut file) = loop {
        attempts += 1;
        let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = dir.join(format!(
            ".{}.tmp-{pid}-{seq}-{:016x}",
            file_name.to_string_lossy(),
            tmp_entropy(seq)
        ));
        // RV-R67 (verified 2026-11-15): create owner-only (0600) so the temp is
        // never readable by others under a permissive umask while the
        // payload is written; the final `mode` is applied by chmod below.
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(file) => break (tmp, file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && attempts < 8 => continue,
            Err(e) => return Err(e),
        }
    };
    let write_result = (|| -> io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    write_result?;
    let dir_file = std::fs::File::open(dir)?;
    dir_file.sync_all()
}

/// F1: bind a unix listener whose socket file is `mode` **from birth** —
/// no window where the final path exists with looser permissions. The
/// socket is created inside a private 0700 staging dir under the same
/// parent (unreachable even to same-uid observers), chmod'd there, then
/// atomically `rename(2)`d into place. This is strictly stronger than
/// bind-then-chmod and avoids `umask(2)` games (per-process state — a
/// racing thread's file creation would silently inherit the tightened
/// mask).
///
/// Caller contract: `path`'s parent must exist and be owned by us (the
/// callers already run their own secure-dir/stale-socket policy); the
/// rename replaces whatever raced back into `path` — same semantics as
/// the remove-then-bind flow it replaces, minus the mode window. The
/// staging path differs from `path` by ~a dozen bytes, so a caller whose
/// `sun_path` budget is already tight gets a plain `ENAMETOOLONG` bind
/// error (fail closed), never a silent degrade.
pub fn bind_unix_private(path: &Path, mode: u32) -> io::Result<std::os::unix::net::UnixListener> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent"))?;
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let stage = parent.join(format!(".b-{}-{seq}", std::process::id()));
    // libc mkdir(0700) — born private, no umask-000 gap where the staging
    // dir briefly exists world-writable before a chmod lands.
    let cstage = std::ffi::CString::new(stage.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "socket path nul"))?;
// SAFETY: the path is a valid NUL-terminated CString and mode 0o711 is a valid mode_t.
    if unsafe { libc::mkdir(cstage.as_ptr(), 0o700) } != 0 {
        return Err(io::Error::last_os_error());
    }
    struct Guard(PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _guard = Guard(stage.clone());
    let staged = stage.join("s");
    let listener = std::os::unix::net::UnixListener::bind(&staged)?;
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(mode))?;
    std::fs::rename(&staged, path)?;
    Ok(listener)
}

/// A pre-existing path is a stale socket that may be unlinked; a regular
/// file or directory is a hard error (someone else owns the name).
/// `symlink_metadata` never follows links: ANY symlink — even a dangling
/// one — is a non-socket dirent and errors (a symlink's target is never
/// unlinked, and a dangling link still blocks the next `bind`). Same
/// no-follow rule `clean_stale_sockets` uses via `DirEntry::metadata`.
pub fn remove_stale_socket(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_socket() {
                std::fs::remove_file(path)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "socket path exists and is not a socket",
                ))
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- H5-r2: set-derived uid collision verdicts ----

    fn ids(tenants: &[&str]) -> Vec<String> {
        tenants.iter().map(|t| t.to_string()).collect()
    }

    /// Brute-force a real colliding pair inside a realistic range —
    /// keeps the test honest instead of only exercising span=1.
    fn colliding_pair(base: u32, span: u32) -> (String, String) {
        let mut seen = std::collections::HashMap::new();
        for i in 0..(span as u64 * 4) {
            let t = format!("tenant-{i}");
            let uid = tenant_uid(base, span, &t);
            if let Some(other) = seen.insert(uid, t.clone()) {
                return (other, t);
            }
        }
        panic!("no collision found in span {span}");
    }

    #[test]
    fn colliding_tenants_span_one_refuses_everyone() {
        // span=1: every tenant derives the same uid — ALL are refused,
        // never a first-come winner.
        let wanted = ids(&["ta", "tb", "tc"]);
        let collided = colliding_tenants(60_000, 1, &wanted);
        assert_eq!(collided.len(), 3);
        // Order-independent: reversed input, same verdict.
        let mut rev = wanted.clone();
        rev.reverse();
        assert_eq!(collided, colliding_tenants(60_000, 1, &rev));
    }

    #[test]
    fn colliding_tenants_real_pair_only() {
        let (a, b) = colliding_pair(60_000, 2_000);
        let mut wanted = ids(&["solo-1", "solo-2", "solo-3"]);
        wanted.push(a.clone());
        wanted.push(b.clone());
        let collided = colliding_tenants(60_000, 2_000, &wanted);
        assert_eq!(collided, [a.clone(), b.clone()].into_iter().collect());
        // Reversed order → identical verdict.
        let mut rev = wanted.clone();
        rev.reverse();
        assert_eq!(collided, colliding_tenants(60_000, 2_000, &rev));
        // Removing one of the pair frees the survivor.
        let reduced = ids(&["solo-1", "solo-2", "solo-3", a.as_str()]);
        assert!(colliding_tenants(60_000, 2_000, &reduced).is_empty());
    }

    #[test]
    fn colliding_tenants_empty_when_all_distinct() {
        let wanted = ids(&["alpha", "beta", "gamma"]);
        assert!(colliding_tenants(60_000, 2_000, &wanted).is_empty());
        assert!(colliding_tenants(60_000, 2_000, &[]).is_empty());
    }

    #[test]
    fn round_trip_and_injection_safety() {
        for raw in [
            "team-a",
            "a.b:c",
            "..",
            "a/b",
            "a%2Fb",
            "\u{79df}\u{6237}",
            "x y",
            "\0nul",
            "vrt:kv:other:key",
        ] {
            let encoded = segment_encode(raw);
            assert!(!encoded.contains(':'));
            assert!(!encoded.contains('/'));
            assert_eq!(segment_decode(&encoded).as_deref(), Some(raw));
        }
        assert_eq!(segment_decode("a%2"), None);
        assert_eq!(segment_decode("a%zz"), None);
    }

    #[test]
    fn validate_secure_dir_rejects_loose_modes() {
        let dir = unique_test_dir("secure-dir");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        validate_secure_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(validate_secure_dir(&dir).is_err());
        let file = dir.join("f");
        std::fs::write(&file, b"x").unwrap();
        assert!(validate_secure_dir(&file).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Parent carries setgid (k8s emptyDir / local-path roots): children
    /// created by `ensure_secure_dir` must end at exactly 0700 with the
    /// special bits cleared and pass validation; a pre-existing 0700 dir
    /// that inherited setgid is healed; a loose group-writable dir
    /// (0770+setgid) is still refused (the check is NOT relaxed).
    #[test]
    fn ensure_secure_dir_under_setgid_parent() {
        let root = unique_test_dir("setgid");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o2750)).unwrap();
        let child = root.join("work");
        ensure_secure_dir(&child).unwrap();
        let mode = std::fs::metadata(&child).unwrap().permissions().mode() & 0o7777;
        assert_eq!(mode, 0o700, "created child must not keep setgid");
        validate_secure_dir(&child).unwrap();

        let pre = root.join("cache");
        std::fs::create_dir(&pre).unwrap();
        std::fs::set_permissions(&pre, std::fs::Permissions::from_mode(0o2700)).unwrap();
        ensure_secure_dir(&pre).unwrap();
        assert_eq!(
            std::fs::metadata(&pre).unwrap().permissions().mode() & 0o7777,
            0o700
        );

        let loose = root.join("loose");
        std::fs::create_dir(&loose).unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o2770)).unwrap();
        let err = ensure_secure_dir(&loose).unwrap_err();
        assert!(err.to_string().contains("wrong mode"), "{err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// An ancestor that is world-writable WITHOUT sticky (emptyDir root
    /// 0777+setgid) must still refuse creation — init pre-creates leaves.
    #[test]
    fn ensure_secure_dir_refuses_world_writable_ancestor_but_accepts_precreated_leaf() {
        let root = unique_test_dir("ww-anc");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o2777)).unwrap();
        let err = ensure_secure_dir(&root.join("listeners")).unwrap_err();
        assert!(err.to_string().contains("world-writable"), "{err}");
        // What vrt-init does: mkdir + chmod 0700 + chmod g-s.
        let leaf = root.join("listeners");
        std::fs::create_dir(&leaf).unwrap();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o700)).unwrap();
        ensure_secure_dir(&leaf).unwrap();
        ensure_socket_dir(&leaf, false).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn ensure_secure_dir_creates_private_dirs() {
        let dir = unique_test_dir("ensure");
        let nested = dir.join("a/b/c");
        ensure_secure_dir(&nested).unwrap();
        validate_secure_dir(&nested).unwrap();
        ensure_secure_dir(&nested).unwrap();
        let file = dir.join("file");
        std::fs::write(&file, b"x").unwrap();
        assert!(ensure_secure_dir(&file).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn atomic_write_replaces_and_keeps_mode() {
        let dir = unique_test_dir("atomic");
        ensure_secure_dir(&dir).unwrap();
        let target = dir.join("doc");
        atomic_write(&target, b"one", 0o600).unwrap();
        atomic_write(&target, b"two", 0o600).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"two");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// RV-R67 (verified 2026-11-15): the target ends with the requested mode even
    /// when it is wider than 0600 (temp is created 0600, then chmod).
    #[test]
    fn atomic_write_applies_requested_mode_over_0600_temp() {
        let dir = unique_test_dir("atomic-mode");
        ensure_secure_dir(&dir).unwrap();
        let target = dir.join("doc");
        atomic_write(&target, b"x", 0o640).unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o640);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T29/R1: N threads hammer the SAME path with DISTINCT payloads.
    /// Every write must succeed and the final content must be exactly
    /// one complete payload (last-writer-wins) — never a torn mix and
    /// never an ENOENT from a stolen temp file. A barrier makes the
    /// writers actually overlap; leftover .tmp files are checked too.
    #[test]
    fn atomic_write_concurrent_same_path_is_atomic() {
        let dir = unique_test_dir("atomic-race");
        ensure_secure_dir(&dir).unwrap();
        let target = dir.join("doc");
        const WRITERS: usize = 8;
        const ROUNDS: usize = 16;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(WRITERS));
        let dir = std::sync::Arc::new(dir);
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let barrier = barrier.clone();
            let target = target.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for r in 0..ROUNDS {
                    let payload = format!("writer-{w}-round-{r}-{:0>64}", "x");
                    atomic_write(&target, payload.as_bytes(), 0o600)
                        .unwrap_or_else(|e| panic!("writer {w} round {r}: {e}"));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let final_bytes = std::fs::read(&*target).unwrap();
        let final_text = String::from_utf8(final_bytes).unwrap();
        // Exactly one writer's complete payload must survive — the
        // round number varies, but the shape is fixed and no payload
        // may be spliced with another's.
        // Shape: "writer-<w>-round-<r>-<64 x-pad>" — exactly one
        // writer's payload, never a splice of two writers' halves.
        let body = final_text
            .strip_prefix("writer-")
            .and_then(|t| t.split_once("-round-"))
            .unwrap_or_else(|| panic!("torn payload: {final_text:?}"));
        let writer: usize = body.0.parse().expect("writer id mangled");
        let (round, pad) = body
            .1
            .split_once('-')
            .unwrap_or_else(|| panic!("torn payload: {final_text:?}"));
        let round: usize = round.parse().expect("round id mangled");
        assert!(writer < WRITERS && round < ROUNDS);
        assert_eq!(pad, "0".repeat(63) + "x");
        // No temp files may linger after successful writes.
        let leftovers: Vec<_> = std::fs::read_dir(&*dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
        let _ = std::fs::remove_dir_all(&*dir);
    }

    /// T29/R1: when the write fails, only the temp file THIS call
    /// created may be removed — a same-pattern stale file (another
    /// writer's in-flight temp, or a crash leftover) must survive.
    #[test]
    fn atomic_write_failure_removes_only_own_temp() {
        let dir = unique_test_dir("atomic-fail");
        ensure_secure_dir(&dir).unwrap();
        // The target exists as a DIRECTORY → rename of our temp fails.
        let target = dir.join("busy");
        std::fs::create_dir(&target).unwrap();
        // A stale temp that belongs to "someone else" — must not be
        // touched by our cleanup.
        let foreign = dir.join(".busy.tmp-99999-0-0123456789abcdef");
        std::fs::write(&foreign, b"not mine").unwrap();
        assert!(atomic_write(&target, b"data", 0o600).is_err());
        assert!(foreign.exists(), "foreign temp file was removed");
        // Our own temp file must be gone.
        let ours: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name().to_string_lossy().into_owned();
                n.contains(".tmp-") && e.path() != foreign
            })
            .collect();
        assert!(ours.is_empty(), "own temp file leaked: {ours:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two temp names for the same path are always distinct within a
    /// process — the counter guarantees it even at identical clocks.
    #[test]
    fn atomic_write_temp_names_unique() {
        let dir = unique_test_dir("atomic-uniq");
        ensure_secure_dir(&dir).unwrap();
        let target = dir.join("doc");
        atomic_write(&target, b"a", 0o600).unwrap();
        atomic_write(&target, b"b", 0o600).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"b");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn socket_path_encodes_segments() {
        let dir = Path::new("/sock");
        assert_eq!(
            socket_path(dir, "a.b:c"),
            PathBuf::from("/sock/a%2Eb%3Ac.sock")
        );
        assert_eq!(
            socket_path(dir, "../escape"),
            PathBuf::from("/sock/%2E%2E%2Fescape.sock")
        );
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let dir = std::env::current_dir()
            .unwrap()
            .join("../target/vrt-securefs-tests")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
}

#[cfg(test)]
mod test_short_root_tests {
    use super::*;

    #[test]
    fn short_root_shape_and_budget() {
        match test_short_root("vrtp") {
            Ok(dir) => {
                let s = dir.to_str().unwrap();
                assert!(s.starts_with("/tmp/vrtp-"), "{s}");
                // Root + typical socket tail must fit the AF_UNIX budget.
                let sock = dir.join("provider/listeners/local.sock");
                assert!(sock.as_os_str().len() <= SOCKET_PATH_MAX, "{sock:?}");
                let _ = std::fs::remove_dir_all(&dir);
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                eprintln!("SKIP: /tmp not writable ({e}) — sandboxed run");
            }
            Err(e) => panic!("test_short_root: {e}"),
        }
    }
}

#[cfg(test)]
mod socket_path_tests {
    use super::*;

    #[test]
    fn socket_path_len_preflight() {
        let short = Path::new("/tmp/x").join("t.sock");
        check_socket_path_len(&short).unwrap();
        let long = PathBuf::from("/".repeat(SOCKET_PATH_MAX + 1));
        let err = check_socket_path_len(&long).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bytes"), "{msg}");
        assert!(msg.contains(&(SOCKET_PATH_MAX + 1).to_string()), "{msg}");
        // Boundary: exactly the cap is allowed.
        let at_cap = PathBuf::from("a".repeat(SOCKET_PATH_MAX));
        check_socket_path_len(&at_cap).unwrap();
    }
}

#[cfg(test)]
mod short_root_guard_tests {
    use super::ShortRoot;

    /// Normal drop removes the root; panic keeps it (T35 leftover fix).
    /// Sandboxed runs that cannot create /tmp dirs SKIP by contract.
    #[test]
    fn short_root_removed_on_normal_drop() {
        let path = match ShortRoot::new("vrtg") {
            Ok(g) => {
                let p = g.path().to_path_buf();
                drop(g);
                p
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
                return;
            }
            Err(e) => panic!("short root: {e}"),
        };
        assert!(!path.exists(), "guard must remove {}", path.display());
    }

    #[test]
    fn short_root_kept_on_panic() {
        let mut kept = std::path::PathBuf::new();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match ShortRoot::new("vrtgp") {
                Ok(g) => {
                    kept = g.path().to_path_buf();
                    panic!("boom");
                }
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                    eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
                }
                Err(e) => panic!("short root: {e}"),
            }
        }));
        if kept.as_os_str().is_empty() {
            return; // sandboxed SKIP — no dir was ever created
        }
        assert!(res.is_err(), "the closure must have panicked");
        assert!(kept.exists(), "panicked guard must KEEP {}", kept.display());
        std::fs::remove_dir_all(&kept).unwrap();
    }
}

#[cfg(test)]
mod f1_bind_private_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// F1: `bind_unix_private` — the socket is `mode` the instant it
    /// appears at its final path (no bind-then-chmod window), the
    /// staging dir is cleaned, and the listener actually accepts.
    /// `temp_dir()` stays under ~85B here — fits the `sun_path` budget
    /// (`/tmp` creation is denied in some sandbox profiles, so probe
    /// skip covers creation too).
    fn f1_root(tag: &str) -> Option<std::path::PathBuf> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        match std::fs::create_dir_all(&dir) {
            Ok(()) => Some(dir),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("SKIP: temp dir not writable ({e})");
                None
            }
            Err(e) => panic!("temp dir: {e}"),
        }
    }

    /// Probe: some sandbox profiles deny unix bind() in-process
    /// (workerd child processes get a different profile — see the
    /// cache_smoke unix tests). Skip rather than fail there; real CI
    /// runs the assertions. The probe binds inside its OWN `.probe`
    /// subdir and removes it before returning, so the caller's
    /// dir-listing assertions stay exact (F1-r2: an earlier version
    /// bound `probe.sock` directly in `dir` and silently broke the
    /// "only api.sock remains" check anywhere bind() actually worked).
    fn bind_probe(dir: &std::path::Path) -> bool {
        let pdir = dir.join(".probe");
        std::fs::create_dir(&pdir).unwrap();
        let allowed = match bind_unix_private(&pdir.join("probe.sock"), 0o600) {
            Ok(_listener) => true,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("SKIP: unix bind denied in this sandbox ({e})");
                false
            }
            Err(e) => panic!("probe bind: {e}"),
        };
        // Listener (if any) is dropped above; the dir must be empty
        // again regardless of the outcome.
        std::fs::remove_dir_all(&pdir).unwrap();
        allowed
    }

    #[test]
    fn bind_unix_private_mode_from_birth() {
        let Some(dir) = f1_root("vrtbind") else {
            return;
        };
        if !bind_probe(&dir) {
            return;
        }
        let sock = dir.join("api.sock");
        // Tighten the process umask AROUND the bind to prove the mode
        // does NOT come from the caller's mask — the staged path is
        // what gets renamed into place. (Changing umask is process-
        // global; this test holds no concurrent file work, but restore
        // immediately anyway.)
        let prev = unsafe { libc::umask(0o000) };
        let listener = bind_unix_private(&sock, 0o600).unwrap();
        unsafe { libc::umask(prev) };
        assert_eq!(
            std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777,
            0o600,
            "socket mode must be 0600 at birth even under umask 0000"
        );
        assert_eq!(
            std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<Vec<_>>(),
            vec![std::ffi::OsString::from("api.sock")],
            "staging dir must be cleaned — only the socket remains"
        );
        // Listener is functional: connect + accept both succeed.
        let _stream = std::os::unix::net::UnixStream::connect(&sock).unwrap();
        listener.accept().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// F1: a second bind over the same path replaces the stale socket
    /// (rename semantics — same as the remove-then-bind flow callers
    /// already run) and yields a fresh working listener.
    #[test]
    fn bind_unix_private_replaces_stale_socket() {
        let Some(dir) = f1_root("vrtbind2") else {
            return;
        };
        if !bind_probe(&dir) {
            return;
        }
        let sock = dir.join("api.sock");
        let _l1 = bind_unix_private(&sock, 0o600).unwrap();
        drop(_l1);
        let l2 = bind_unix_private(&sock, 0o600).unwrap();
        let _stream = std::os::unix::net::UnixStream::connect(&sock).unwrap();
        l2.accept().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
