//! T25 (B8) — provider standalone-launch contract tests:
//! L6 listener dir self-creation, L7 `--config <path>` argv parsing
//! (the compose command form), L8 cursorSecretFile loading.

use std::path::{Path, PathBuf};

/// T37b: drop guard — unique top-level dir per call; parallel tests
/// never share the parent.
struct TmpDir(PathBuf);

impl std::ops::Deref for TmpDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TmpDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("tmp cleanup {}: {e}", self.0.display());
        }
    }
}

fn tmpdir(tag: &str) -> TmpDir {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-provider-t25-{}-{}-{}",
        std::process::id(),
        tag,
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    TmpDir(dir)
}

// ---------- L7: argv contract ----------

#[test]
fn config_flag_form_parses() {
    // The compose `command: ["--config", "/data/config/vrt-provider.toml"]`
    // form MUST parse to the second element — B8 had the daemon read
    // argv[1] literally ("--config") and die.
    let args = vec![
        "--config".to_string(),
        "/data/config/vrt-provider.toml".to_string(),
    ];
    let path = vrt_provider::cli::resolve_config_path(&args).unwrap();
    assert_eq!(
        path.unwrap(),
        PathBuf::from("/data/config/vrt-provider.toml")
    );
}

#[test]
fn positional_form_still_parses() {
    let args = vec!["/etc/vrt/provider.toml".to_string()];
    let path = vrt_provider::cli::resolve_config_path(&args).unwrap();
    assert_eq!(path.unwrap(), PathBuf::from("/etc/vrt/provider.toml"));
}

#[test]
fn empty_argv_defers_to_env() {
    // `None` = caller falls back to VRT_PROVIDER_CONFIG.
    assert!(vrt_provider::cli::resolve_config_path(&[])
        .unwrap()
        .is_none());
}

#[test]
fn unknown_flags_fail_closed() {
    for args in [
        vec!["--bogus".to_string(), "x".to_string()],
        vec!["-c".to_string(), "x".to_string()],
        vec!["--config".to_string()],           // missing value
        vec!["a".to_string(), "b".to_string()], // two positionals
        vec!["--config".to_string(), "a".to_string(), "b".to_string()],
    ] {
        assert!(
            vrt_provider::cli::resolve_config_path(&args).is_err(),
            "must reject {args:?}"
        );
    }
}

// ---------- L8: cursorSecretFile ----------

#[test]
fn cursor_secret_file_loads_at_0600() {
    let dir = tmpdir("cursor");
    let path = dir.join("cursor.secret");
    vrt_securefs::atomic_write(&path, b"0123456789abcdef0123456789abcdef\n", 0o600).unwrap();
    let secret = vrt_provider::cli::load_cursor_secret(&path).unwrap();
    assert_eq!(secret.len(), 32, "trimmed secret body");
}

#[test]
fn cursor_secret_file_rejects_bad_mode() {
    let dir = tmpdir("cursor-mode");
    let path = dir.join("cursor.secret");
    vrt_securefs::atomic_write(&path, b"0123456789abcdef0123456789abcdef\n", 0o644).unwrap();
    let err = vrt_provider::cli::load_cursor_secret(&path).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
}

#[test]
fn cursor_secret_file_rejects_missing_and_empty() {
    let dir = tmpdir("cursor-missing");
    assert!(vrt_provider::cli::load_cursor_secret(&dir.join("none")).is_err());
    let empty = dir.join("empty");
    vrt_securefs::atomic_write(&empty, b"", 0o600).unwrap();
    assert!(vrt_provider::cli::load_cursor_secret(&empty).is_err());
}

#[test]
fn config_toml_accepts_cursor_secret_file() {
    // The generated provider config uses `cursorSecretFile` — parse
    // the actual template init renders.
    let dir = tmpdir("cursor-toml");
    let secret = dir.join("cursor.secret");
    vrt_securefs::atomic_write(&secret, b"0123456789abcdef0123456789abcdef", 0o600).unwrap();
    let toml = format!(
        r#"
cellId = "local"
listenerDir = "{}/provider/listeners"
cursorSecretFile = "{}"
[desired]
path = "{}/desired/envelope.json"
trustedKeys = []
[kv]
dir = "{}/provider/data/kv"
"#,
        dir.display(),
        secret.display(),
        dir.display(),
        dir.display()
    );
    let cfg: vrt_provider::config::ProviderConfig = toml::from_str(&toml).unwrap();
    assert_eq!(cfg.cursor_secret_file.as_deref(), Some(secret.as_path()));
}

// ---------- L6: listener dir ----------

#[test]
fn listener_dir_is_created_0700_when_missing() {
    // Provider binds listeners under listenerDir; B8 hit
    // "tenant listener bind failed" because nothing created it.
    // `ensure_secure_dir` is what `bind_listener` calls first.
    let dir = tmpdir("listeners").join("listeners");
    assert!(!dir.exists());
    vrt_securefs::ensure_secure_dir(&dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(dir.metadata().unwrap().permissions().mode() & 0o777, 0o700);
    }
    // Idempotent on an already-valid dir.
    vrt_securefs::ensure_secure_dir(&dir).unwrap();
}

#[test]
fn listener_dir_fails_closed_on_insecure_existing() {
    let dir = tmpdir("listeners-insecure").join("listeners");
    std::fs::create_dir_all(&dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        // 0777 dir is a shared-writable listener target → refuse.
        assert!(vrt_securefs::ensure_secure_dir(&dir).is_err());
        // A regular file where the dir belongs → refuse.
        let file = dir.join("f");
        std::fs::write(&file, b"x").unwrap();
        assert!(vrt_securefs::ensure_secure_dir(&file).is_err());
    }
}
