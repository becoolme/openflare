//! T16/B3: the shipped example configs must parse + validate, and the
//! `facadeManifest` derive path must produce working render params.
//! `__VRT_ROOT__`/`__WORKERD__` placeholders are substituted with test
//! temp paths before loading (exactly as the install docs instruct).

use std::path::{Path, PathBuf};
use vrt_host::config::{params_json_from_manifest, HostConfig};

/// T37b: drop guard — target/tmp must not accumulate fixture roots.
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

fn tmp_root() -> TmpDir {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-host-examples-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
}

fn substitute(name: &str, root: &Path, workerd: &Path) -> String {
    let raw = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples")
            .join(name),
    )
    .unwrap();
    // Deep worktree paths exceed the 104-byte unix-socket bound —
    // substitute a short literal socket path (validation only needs
    // the policy check, never an actual bind). The platform example
    // now uses bindingBackendDir (H5-r1) — `__VRT_ROOT__` expands to
    // the short tmp root which stays within the bound.
    raw.replace(
        "__VRT_ROOT__/provider/backend.sock",
        "/vrt-test/backend.sock",
    )
    // H5-r1: same 104-byte bound applies to the dir the platform
    // example transports — substitute a short literal dir.
    .replace(
        "__VRT_ROOT__/provider/listeners",
        "/vrt-test/provider",
    )
    // T25/L13: the standalone template derives the backend socket
    // from provider listenerDir — substitute a short literal for the
    // same 104-byte bound reason.
    .replace("__VRT_BACKEND_SOCK__", "/vrt-test/backend.sock")
    .replace("__VRT_ROOT__", &root.to_string_lossy())
    .replace("__WORKERD__", &workerd.to_string_lossy())
    .replace("__VRT_LISTEN__", "127.0.0.1:8080")
    .replace("__VRT_CONTROL__", "127.0.0.1:9091")
    .replace("__VRT_METRICS__", "127.0.0.1:9494")
    .replace("__VRT_PROVIDER_METRICS__", "127.0.0.1:9493")
    .replace("__VRT_KEY_ID__", "example-key")
    .replace(
        "__VRT_PUBKEY__",
        "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=",
    )
}

fn write(root: &Path, name: &str, text: &str) -> PathBuf {
    let p = root.join(name);
    std::fs::write(&p, text).unwrap();
    p
}

#[test]
fn platform_and_standalone_host_examples_validate_and_emit() {
    let root = tmp_root();
    let workerd = write(&root, "workerd", "#!/bin/sh\n");
    // secrets file must exist at 0600 (both examples reference it).
    let secrets = root.join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secrets_file = write(&root, "secrets/render-secrets.json", "{}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&secrets, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    // standalone references secrets/secrets.json as well.
    std::fs::copy(&secrets_file, root.join("secrets/secrets.json")).unwrap();
    // Copy the real facade manifest into the layout the examples expect.
    std::fs::create_dir_all(root.join("facade")).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
        root.join("facade/manifest.json"),
    )
    .unwrap();

    for name in ["platform-vrt-host.toml", "standalone-vrt-host.toml"] {
        let text = substitute(name, &root, &workerd);
        let cfg_path = write(&root, name, &text);
        let config =
            HostConfig::load(&cfg_path).unwrap_or_else(|e| panic!("{name} failed validation: {e}"));
        // emit_mode exercises the manifest → RuntimeParams derive path
        // (the same code `render-params` and startup use).
        let mode = config
            .emit_mode()
            .unwrap_or_else(|e| panic!("{name} emit_mode failed: {e}"));
        match mode {
            vrt_host::assemble::EmitMode::Capnp { params } => {
                assert_eq!(params.system_workers.len(), 15); // +facade-qconsumer (T37b), +facade-ratelimit (MIG-16), +facade-k2, +facade-email, +facade-images, +facade-vectorize
                assert!(!params.system_compat_date.is_empty());
            }
            #[cfg(feature = "dev-stub")]
            _ => panic!("{name}: unexpected emit mode"),
        }
    }
}

#[test]
fn do_storage_dir_sweep_boundary() {
    // P3c-1-r1: `doStorageDir` must never resolve INSIDE `workDir`
    // (gen-* dirs are swept). `load()` canonicalizes first, so a
    // `..`-smuggled path like `<root>/work/../work/do` is judged on
    // its real location — not the textual one.
    let root = tmp_root();
    let workerd = write(&root, "workerd", "#!/bin/sh\n");
    let secrets = root.join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secrets_file = write(&root, "secrets/render-secrets.json", "{}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&secrets, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::copy(&secrets_file, root.join("secrets/secrets.json")).unwrap();
    std::fs::create_dir_all(root.join("facade")).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
        root.join("facade/manifest.json"),
    )
    .unwrap();
    std::fs::create_dir_all(root.join("work")).unwrap();

    // Inside workDir (canonical) → refused.
    let inside = substitute("standalone-vrt-host.toml", &root, &workerd)
        .replacen("doStorageDir", "doStorageDir", 1)
        .replace("/do\"", "/work/do\"");
    let p = write(&root, "inside.toml", &inside);
    assert!(
        HostConfig::load(&p).is_err(),
        "doStorageDir under workDir must fail"
    );

    // `..` segment that canonicalizes INSIDE workDir → refused.
    let sneaky =
        substitute("standalone-vrt-host.toml", &root, &workerd).replace("/do\"", "/x/../work/do\"");
    let p = write(&root, "sneaky.toml", &sneaky);
    assert!(
        HostConfig::load(&p).is_err(),
        "`..`-smuggled doStorageDir under workDir must fail after canonicalize"
    );
    // A `..` segment that canonicalizes OUTSIDE workDir → accepted
    // (normalization is legitimate — only the resolved location matters).
    let ok = substitute("standalone-vrt-host.toml", &root, &workerd).replace("/do\"", "/x/../do\"");
    let p = write(&root, "ok.toml", &ok);
    let cfg = HostConfig::load(&p).unwrap_or_else(|e| panic!("normalized outside path: {e}"));
    assert!(cfg.do_storage_dir.as_ref().unwrap().ends_with("do"));
}

#[test]
fn params_json_from_manifest_fails_closed() {
    let root = tmp_root();
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json");

    let socket = "/vrt-test/backend.sock".to_string();
    // Relative socket path rejected.
    assert!(
        params_json_from_manifest(&manifest, "provider.sock", "2024-09-25", &[], None).is_err()
    );
    // Bad compat date rejected.
    assert!(params_json_from_manifest(&manifest, &socket, "2024-13-40", &[], None).is_err());
    // Bad flag rejected.
    assert!(params_json_from_manifest(
        &manifest,
        &socket,
        "2024-09-25",
        &["bad flag!".into()],
        None
    )
    .is_err());
    // Missing manifest rejected.
    assert!(
        params_json_from_manifest(&root.join("nope.json"), &socket, "2024-09-25", &[], None)
            .is_err()
    );
    // Valid inputs produce a document matching the real manifest.
    let doc = params_json_from_manifest(&manifest, &socket, "2024-09-25", &[], None).unwrap();
    assert_eq!(doc["systemCompatDate"], "2024-09-25");
    assert_eq!(doc["egress"]["service"], "vrt-egress");
    assert_eq!(doc["denyOutbound"]["service"], "vrt-no-network");
    assert_eq!(doc["facades"]["kv"]["service"], "facade-kv");
    assert_eq!(doc["facades"]["d1"]["service"], "facade-d1");
    assert_eq!(doc["facades"]["queue"]["service"], "facade-queue");
    assert_eq!(doc["facades"]["ratelimit"]["service"], "facade-ratelimit");
    assert_eq!(doc["facades"]["email"]["service"], "facade-email");
    assert_eq!(doc["facades"]["images"]["service"], "facade-images");
    assert_eq!(doc["facades"]["vectorize"]["service"], "facade-vectorize");
    assert_eq!(doc["systemWorkers"].as_array().unwrap().len(), 15);
    // Platform-declared budgets survive the doc round-trip (the
    // document is re-parsed via RenderParamsFile inside the call).
    let sys = doc["systemWorkers"].as_array().unwrap();
    let dim = |name: &str, key: &str| {
        sys.iter()
            .find(|w| w["serviceName"] == name)
            .unwrap_or_else(|| panic!("{name} missing"))[key]
            .as_u64()
            .unwrap_or(0)
    };
    assert_eq!(dim("facade-images", "cpuMs"), 10_000);
    assert_eq!(dim("vrt-egress", "cpuMs"), 2_000);
    assert_eq!(dim("facade-kv", "cpuMs"), 1_000);
    assert_eq!(dim("facade-images", "memoryMb"), 768);
    assert_eq!(dim("vrt-egress", "memoryMb"), 192);
    assert_eq!(dim("facade-kv", "memoryMb"), 128);
}

/// T27/N0: a supervisor config whose failure window cannot hold
/// `failThreshold` full launch cycles must fail closed at load —
/// otherwise a broken tenant retries forever (circuit never trips
/// because each failure ages out before the next lands). Required:
/// `failThreshold*(readyTimeoutMs+backoffMaxMs) <= failWindowMs`.
#[test]
fn circuit_bounds_are_validated_at_load() {
    let root = tmp_root();
    let workerd = write(&root, "workerd", "#!/bin/sh\n");
    let secrets = root.join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secrets_file = write(&root, "secrets/render-secrets.json", "{}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&secrets, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::copy(&secrets_file, root.join("secrets/secrets.json")).unwrap();
    std::fs::create_dir_all(root.join("facade")).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
        root.join("facade/manifest.json"),
    )
    .unwrap();

    let base = substitute("standalone-vrt-host.toml", &root, &workerd);
    let with_bounds = |ready_ms: u64, window_ms: u64| {
        // Edit the existing [supervisor] table in place (TOML forbids
        // repeated keys — the template ships both lines).
        base.replacen(
            "readyTimeoutMs = 10000",
            &format!("readyTimeoutMs = {ready_ms}"),
            1,
        )
        .replacen(
            "failWindowMs = 90000",
            &format!("failWindowMs = {window_ms}"),
            1,
        )
    };

    // 90s window satisfies the bound: 5×(10s+5s)=75s ≤ 90s.
    let ok = write(&root, "ok.toml", &with_bounds(10_000, 90_000));
    HostConfig::load(&ok).expect("default-comparable bounds validate");

    // readyTimeoutMs ≥ ~13s: 5×(13s+5s)=90s… use 15s → 5×20s=100s
    // > 90s — the circuit can never trip (T26 self-check: launch
    // cycles exceed the window).
    let bad = write(&root, "bad.toml", &with_bounds(15_000, 90_000));
    let err = HostConfig::load(&bad).unwrap_err().to_string();
    assert!(err.contains("failThreshold"), "{err}");
    assert!(err.contains("failWindowMs"), "{err}");
}

/// T31g/G1.4: a config that renders the `experimental` compat flag but
/// omits `--experimental` in extraArgs fails at load — the workerd
/// binary refuses that flag unless the CLI carries it.
#[test]
fn experimental_flag_requires_experimental_extra_arg() {
    let root = tmp_root();
    let workerd = write(&root, "workerd", "#!/bin/sh\n");
    // The fixture dir layout the template references must exist.
    std::fs::create_dir_all(root.join("secrets")).unwrap();
    std::fs::create_dir_all(root.join("vars")).unwrap();
    std::fs::create_dir_all(root.join("provider")).unwrap();
    std::fs::create_dir_all(root.join("facade")).unwrap();
    let secrets_file = write(&root, "secrets/render-secrets.json", "{}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(root.join("secrets"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
    }
    std::fs::copy(&secrets_file, root.join("secrets/secrets.json")).unwrap();
    std::fs::write(root.join("vars/vars.json"), "{}").unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
        root.join("facade/manifest.json"),
    )
    .unwrap();

    let base = substitute("platform-vrt-host.toml", &root, &workerd).replacen(
        "systemCompatFlags = []",
        r#"systemCompatFlags = ["experimental", "service_binding_extra_handlers"]"#,
        1,
    );

    // Missing --experimental → load-time error naming both fields.
    let bad = write(&root, "bad-exp.toml", &base);
    let err = HostConfig::load(&bad).unwrap_err().to_string();
    assert!(err.contains("experimental"), "{err}");
    assert!(err.contains("--experimental"), "{err}");

    // Present → the config validates.
    let ok = write(
        &root,
        "ok-exp.toml",
        &base.replacen("extraArgs = []", r#"extraArgs = ["--experimental"]"#, 1),
    );
    HostConfig::load(&ok).expect("with --experimental the config validates");
}


/// H5-r1: the binding-backend socket contract — verbatim
/// `bindingBackendSocket` is a single-tenant contract forbidden in
/// assignment (platform) mode; `bindingBackendDir` is required
/// there; the two are mutually exclusive; the dir must be absolute.
#[test]
fn binding_backend_socket_dir_contract() {
    let root = tmp_root();
    let workerd = write(&root, "workerd", "#!/bin/sh\n");
    // Fixture files the platform template references.
    let secrets = root.join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secrets_file = write(&root, "secrets/render-secrets.json", "{}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&secrets, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::create_dir_all(root.join("facade")).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
        root.join("facade/manifest.json"),
    )
    .unwrap();

    let base = substitute("platform-vrt-host.toml", &root, &workerd);
    let dir_line = r#"bindingBackendDir = "/vrt-test/provider""#;

    // 1) verbatim socket + dir → ambiguous — fail closed.
    let both = base.replacen(
        dir_line,
        "bindingBackendSocket = \"/vrt-test/backend.sock\"\nbindingBackendDir = \"/vrt-test/provider\"",
        1,
    );
    let err = HostConfig::load(&write(&root, "bb-both.toml", &both))
        .unwrap_err()
        .to_string();
    assert!(err.contains("bindingBackendSocket"), "{err}");

    // 2) verbatim socket alone + assignmentFile (the platform mode) →
    //    the multi-tenant vulnerable shape — fail closed.
    let socket_only = base.replacen(
        dir_line,
        r#"bindingBackendSocket = "/vrt-test/backend.sock""#,
        1,
    );
    let err = HostConfig::load(&write(&root, "bb-sock.toml", &socket_only))
        .unwrap_err()
        .to_string();
    assert!(err.contains("bindingBackendSocket"), "{err}");
    assert!(err.contains("bindingBackendDir"), "{err}");

    // 3) neither → facadeManifest mode has no socket source — fail.
    let neither = base.replacen(&format!("{dir_line}\n"), "", 1);
    let err = HostConfig::load(&write(&root, "bb-neither.toml", &neither))
        .unwrap_err()
        .to_string();
    assert!(err.contains("bindingBackend"), "{err}");

    // 4) relative dir → not a provider listenerDir — fail closed.
    let rel = base.replacen("/vrt-test/provider", "relative/dir", 1);
    assert!(HostConfig::load(&write(&root, "bb-rel.toml", &rel)).is_err());

    // 5) dir alone (the shipped shape) + assignmentFile → loads.
    HostConfig::load(&write(&root, "bb-ok.toml", &base))
        .expect("bindingBackendDir alone must validate");

    // 6) dir + paramsFile mode → params file carries its own socket —
    //    two sources of truth fail closed.
    let params_mode = base
        .replacen(
            r#"facadeManifest = "#,
            r#"paramsFile = "/vrt-test/params.json"
facadeManifest = "#,
            1,
        );
    assert!(HostConfig::load(&write(&root, "bb-pf.toml", &params_mode)).is_err());
}

/// RV-R66 (uncompiled): DO storage without an s3 placement is a single
/// copy — the startup posture check refuses it unless the dev waiver is
/// explicit; no DO storage means nothing to protect.
#[test]
fn durability_posture_fails_closed_without_s3_placement() {
    let root = tmp_root();
    let workerd = write(&root, "workerd", "#!/bin/sh\n");
    let secrets = root.join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secrets_file = write(&root, "secrets/render-secrets.json", "{}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&secrets, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::copy(&secrets_file, root.join("secrets/secrets.json")).unwrap();
    std::fs::create_dir_all(root.join("facade")).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
        root.join("facade/manifest.json"),
    )
    .unwrap();
    let p = write(
        &root,
        "dur.toml",
        &substitute("standalone-vrt-host.toml", &root, &workerd),
    );
    let mut cfg = HostConfig::load(&p).expect("template loads");
    cfg.do_storage_dir = Some(root.join("do"));
    cfg.placement = None;
    cfg.allow_local_durability = false;
    let msg = cfg.durability_posture_error().expect("single copy refused");
    assert!(msg.contains("VRT_HOST_DURABILITY_SINGLE_COPY"), "{msg}");
    cfg.allow_local_durability = true;
    assert!(cfg.durability_posture_error().is_none());
    cfg.allow_local_durability = false;
    cfg.do_storage_dir = None;
    assert!(cfg.durability_posture_error().is_none());
}
