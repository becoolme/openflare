//! D9/D10 — static checks on deploy/standalone artifacts: the compose
//! stack, Dockerfile, .dockerignore, .env.example and README must stay
//! internally consistent, non-root, and fail closed.

use std::path::{Path, PathBuf};

fn rt() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let p = rt().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

#[test]
fn dockerfile_stage_selection_and_security() {
    // T24/K2: the canonical file lives at deploy/docker — the
    // standalone path is a one-line pointer to it.
    assert!(read("deploy/standalone/Dockerfile").contains("deploy/docker/Dockerfile.vrt-local"));
    let df = read("deploy/docker/Dockerfile.vrt-local");
    let first_from = df
        .find("\nFROM")
        .or_else(|| df.starts_with("FROM").then_some(0));
    // Global WORKERD_SOURCE ARG must precede the first FROM.
    let arg_pos = df
        .find("ARG WORKERD_SOURCE")
        .expect("global WORKERD_SOURCE arg");
    assert!(
        arg_pos < first_from.unwrap_or(usize::MAX),
        "ARG before first FROM"
    );
    // Selected stage must name an existing stage.
    for stage in [
        "workerd-stub",
        "workerd-file",
        "workerd-image",
        "workerd-none",
    ] {
        assert!(df.contains(&format!("AS {stage}")), "stage {stage} exists");
    }
    assert!(df.contains("FROM workerd-${WORKERD_SOURCE} AS workerd-pick"));
    // Runtime image runs non-root and declares /data owned+0700.
    assert!(df.contains("USER vrt"));
    assert!(df.contains("install -d -o vrt -g vrt -m 0700 /data"));
    // File-source path must exist under deploy/standalone and verify
    // the sha256 contract; install -D creates parents.
    assert!(df.contains("COPY deploy/standalone/workerd /workerd"));
    assert!(df.contains("sha256sum -c -"));
    assert!(df.contains("install -D -m 0755 /workerd /opt/vrt/workerd"));
    // Facade must ship in the image (D6).
    assert!(df.contains("/usr/share/vrt/facade"));
}

#[test]
fn dockerignore_keeps_context_small_and_paths_valid() {
    let di = read(".dockerignore");
    for pat in ["target/", "**/node_modules/", "**/.git/"] {
        assert!(di.contains(pat), ".dockerignore covers {pat}");
    }
    // workerd file-source must NOT be ignored (build needs it in ctx).
    assert!(!di.contains("deploy/standalone/workerd"));
    // Every COPY path in the Dockerfile exists in the context.
    let df = read("deploy/docker/Dockerfile.vrt-local");
    for line in df.lines() {
        if let Some(rest) = line.strip_prefix("COPY ") {
            if rest.starts_with("--from") {
                continue;
            }
            let src = rest.split_whitespace().next().unwrap();
            let p = rt().join(src);
            assert!(
                p.exists() || src.contains("workerd"),
                "COPY source {src} exists (or is the optional workerd file)"
            );
        }
    }
}

#[test]
fn compose_is_non_root_single_port_named_volume() {
    let y = read("deploy/standalone/docker-compose.yml");
    assert!(y.contains("user: \"${VRT_UID:-10001}:${VRT_GID:-10001}\""));
    assert!(y.contains("vrt-data:/data"));
    assert!(!y.contains("type: none"), "no bind driver");
    assert!(y.contains("  vrt-data:"), "named volume declared");
    // Exactly one published port in the BASE profile — vrt-host
    // ingress, bound to loopback by default. The opt-in `monitoring`
    // profile adds VictoriaMetrics on VM_BIND loopback and must not
    // leak into the default surface.
    let base = y.split("profiles:").next().unwrap();
    assert_eq!(base.matches("\n    ports:").count(), 1, "one ports block");
    assert!(y.contains("${VRT_BIND:-127.0.0.1}"), "loopback default");
    // B12: monitoring is opt-in — profile-gated, loopback-published.
    assert!(
        y.contains("profiles: [\"monitoring\"]"),
        "monitoring profile gated"
    );
    assert!(
        y.contains("${VM_BIND:-127.0.0.1}"),
        "VM UI loopback default"
    );
    assert!(
        y.contains("monitoring/promscrape.yml"),
        "scrape config mounted"
    );
    let sc = read("deploy/standalone/monitoring/promscrape.yml");
    for (job, port) in [("vrt-host", 9494u16), ("vrt-provider", 9493)] {
        assert!(
            sc.contains(job) && sc.contains(&format!("{job}:{port}")),
            "scrape job {job}:{port} configured"
        );
    }
    // Healthchecks exist for the three long-running services.
    for svc in ["vrt:", "vrt-host:", "vrt-provider:"] {
        let idx = y.find(&format!("  {svc}")).expect("service exists");
        let seg = &y[idx..y.len().min(idx + 1400)];
        assert!(seg.contains("healthcheck:"), "{svc} has healthcheck");
    }
    // Provider healthcheck must probe listener sockets (D8), not a file.
    assert!(y.contains("\"--dir-sockets\"") && y.contains("/data/provider/listeners"));
}

#[test]
fn stub_overlay_and_host_dir_overlay() {
    let stub = read("deploy/standalone/docker-compose.stub.yml");
    assert!(stub.contains("WORKERD_SOURCE: stub") || stub.contains("WORKERD_SOURCE:-stub"));
    assert!(
        stub.contains("--mode") && stub.contains("stub"),
        "stub overlay pins mode explicitly (detection must agree)"
    );
    let hd = read("deploy/standalone/docker-compose.host-dir.yml");
    assert!(hd.contains("/srv/vrt-data:/data"));
}

#[test]
fn readme_commands_match_compose_files() {
    let readme = read("deploy/standalone/README.md");
    // Every `-f <file>` referenced in README code blocks must exist.
    let mut found_any = false;
    for line in readme.lines() {
        let mut toks = line.split_whitespace().peekable();
        while let Some(t) = toks.next() {
            if t == "-f" {
                if let Some(f) = toks.next() {
                    let f = f.trim_end_matches(['\\', '"', '\'']);
                    if f.ends_with(".yml") {
                        let name = f.rsplit('/').next().unwrap_or(f);
                        let p = rt().join("deploy/standalone").join(name);
                        assert!(p.exists(), "README references missing {name}");
                        found_any = true;
                    }
                }
            }
        }
    }
    assert!(found_any, "README contains at least one compose -f ref");
    // Service names used in README `exec`/`up` commands must exist.
    for svc in ["vrt-init", "vrt", "vrt-host", "vrt-provider"] {
        assert!(
            read("deploy/standalone/docker-compose.yml").contains(&format!("  {svc}:")),
            "service {svc} exists in compose"
        );
    }
}

// ---- T24/K2+K4: unified image + unique workerd path ----

#[test]
fn t24_compose_single_build_shared_image_no_pull() {
    let y = read("deploy/standalone/docker-compose.yml");
    // Exactly one service carries `build:` — vrt-init.
    assert_eq!(y.matches("    build:").count(), 1, "single build block");
    assert_eq!(y.matches("dockerfile:").count(), 1);
    assert!(y.contains("dockerfile: deploy/docker/Dockerfile.vrt-local"));
    // All four services share the tagged local image and never pull.
    assert_eq!(
        y.matches("vrt-standalone:${VRT_IMAGE_TAG:-local}").count(),
        1,
        "shared tag via anchor"
    );
    for svc in ["vrt-init", "vrt:", "vrt-host", "vrt-provider", "vrt-sql"] {
        let idx = y
            .find(&format!("  {svc}"))
            .unwrap_or_else(|| panic!("{svc} exists"));
        let seg = &y[idx..(idx + 1600).min(y.len())];
        let uses = seg.contains("<<: [*vrt-common, *vrt-image]");
        assert!(uses, "{svc} shares the vrt-image anchor");
    }
    // Builder overrides pull_policy to `build`; consumers use `never`.
    assert!(y.contains("pull_policy: never"));
    assert!(y.contains("pull_policy: build"));
}

#[test]
fn t24_workerd_path_contract_is_unique() {
    // The compose init command's --workerd path MUST equal the path the
    // canonical Dockerfile installs — one layout, /usr/local/bin/workerd.
    let y = read("deploy/standalone/docker-compose.yml");
    let df = read("deploy/docker/Dockerfile.vrt-local");
    assert!(y.contains("--workerd"));
    let idx = y.find("--workerd").unwrap();
    let after = &y[idx + "--workerd".len()..];
    let path = after
        .split_whitespace()
        .find(|t| t.starts_with('/'))
        .map(|t| t.trim_end_matches(','))
        .expect("--workerd has a path arg");
    assert_eq!(path, "/usr/local/bin/workerd");
    assert!(
        df.contains("install -m 0755 /opt/vrt/workerd /usr/local/bin/workerd"),
        "Dockerfile installs workerd at the compose-referenced path"
    );
    // No second workerd location is produced for non-stub sources.
    assert!(!df.contains("/usr/local/bin/workerd2"));
    // Every dockerfile referenced by overlays exists.
    for f in [
        "deploy/standalone/docker-compose.deploy-docker.yml",
        "deploy/standalone/docker-compose.stub.yml",
    ] {
        let ov = read(f);
        for line in ov.lines() {
            if let Some(rest) = line.trim().strip_prefix("dockerfile: ") {
                let rel = rest.trim();
                assert!(
                    rt().join(rel).exists(),
                    "{f} references missing dockerfile {rel}"
                );
            }
        }
    }
    // WORKERD_SOURCE=none must not silently succeed: the init binary
    // itself fails closed (cmd_init checks the resolved workerd path —
    // asserted by init_rejects_missing_workerd in t17_fixes.rs).
    assert!(df.contains("workerd-none"));
    // compose's default workerd source is never `none`.
    assert!(!y.contains("WORKERD_SOURCE:-none"));
}

#[test]
fn t25_standalone_compose_passes_the_dev_durability_opt_in() {
    // Docker named volumes share one device, so the quick-start stack
    // cannot boot with vrt-sql's fail-closed durability guard armed.
    // The opt-in must be EXPLICIT in the compose init commands (base +
    // stub overlay) so removing it is a visible diff, not a silent
    // regression to `vrt-sql unhealthy`.
    for f in [
        "deploy/standalone/docker-compose.yml",
        "deploy/standalone/docker-compose.stub.yml",
    ] {
        let y = read(f);
        assert!(
            y.contains("--dev-allow-same-device-durability"),
            "{f} must pass the dev-only same-device durability opt-in"
        );
    }
    // The production-facing template stays armed: the key must only be
    // injected by `vrt init --dev-allow-same-device-durability`.
    let sql = read("examples/standalone-vrt-sql.toml");
    assert!(!sql.contains("allowSameDeviceDurability"), "{sql}");
}

#[test]
fn t24_gitignore_covers_workerd_binaries() {
    let gi = read(".gitignore");
    assert!(
        gi.contains("deploy/standalone/workerd"),
        "gitignore covers file source"
    );
    assert!(
        gi.contains("deploy/docker/workerd"),
        "gitignore covers docker file source"
    );
    // .dockerignore must NOT exclude them (they're build context).
    let di = read(".dockerignore");
    assert!(!di.contains("deploy/standalone/workerd"));
    assert!(!di.contains("deploy/docker/workerd"));
}

// ---- T25 (B8 real-machine): compose/Dockerfile/init contract ----

#[test]
fn t25_base_compose_no_stub_default_requires_workerd_source() {
    // L10+M5: the base file must NOT default WORKERD_SOURCE to a real
    // source — the `:-unset-required` sentinel keeps non-build ops
    // working while docker's stage resolution fails the build naming
    // the missing variable (`workerd-unset-required` not found).
    let y = read("deploy/standalone/docker-compose.yml");
    assert!(!y.contains("WORKERD_SOURCE:-stub"), "no stub default");
    assert!(!y.contains("WORKERD_SOURCE:-file"), "no file default");
    assert!(!y.contains("WORKERD_SOURCE:-image"), "no image default");
    assert!(y.contains("WORKERD_SOURCE:-unset-required"));
    // Prod image target carries NO dev-stub host.
    assert!(
        y.contains("target: runtime"),
        "base build targets prod runtime"
    );
    assert!(!y.contains("runtime-stub"), "no stub target in base");
}

#[test]
fn t25_stub_overlay_builds_devstub_host_distinct_tag() {
    // L10: the stub overlay selects the runtime-stub target (vrt-host
    // WITH dev-stub) and a separate image tag so prod/stub images
    // never collide.
    let stub = read("deploy/standalone/docker-compose.stub.yml");
    assert!(stub.contains("target: runtime-stub"));
    assert!(stub.contains("vrt-standalone-stub:"));
    for svc in ["vrt-init:", "vrt:", "vrt-host:", "vrt-provider:"] {
        assert!(stub.contains(svc), "stub overlay covers {svc}");
    }
    // The Dockerfile must produce the devstub host binary the
    // runtime-stub stage installs.
    let df = read("deploy/docker/Dockerfile.vrt-local");
    assert!(
        df.contains("--bin vrt-host --features dev-stub")
            || df.contains("--bin vrt-stub-workerd --bin vrt-host --features dev-stub"),
        "stub-builder compiles vrt-host with dev-stub"
    );
    assert!(df.contains("FROM runtime AS runtime-stub"));
    assert!(df.contains("/out/vrt-host-devstub"));
    // The prod runtime stage must NOT install the devstub host.
    let runtime_seg = &df[df.find("FROM debian:bookworm-slim AS runtime").unwrap()
        ..df.find("FROM runtime AS runtime-stub").unwrap()];
    assert!(
        !runtime_seg.contains("vrt-host-devstub"),
        "prod runtime never carries dev-stub"
    );
}

#[test]
fn t25_dockerfile_installs_ca_certificates() {
    // L14: workerd outbound TLS fails at startup without CA roots.
    let df = read("deploy/docker/Dockerfile.vrt-local");
    let rt_seg = &df[df.find("FROM debian:bookworm-slim AS runtime").unwrap()..];
    assert!(
        rt_seg.contains("ca-certificates"),
        "runtime stage installs ca-certificates"
    );
    assert!(rt_seg.contains("--no-install-recommends"));
}

#[test]
fn t25_dockerfile_facade_layout_matches_seed_convention() {
    // L3+L4: the image ships a FACADE DIR = {manifest.json, dist/} —
    // the same layout `vrt init --facade-dist <repo>/facade` reads.
    let df = read("deploy/docker/Dockerfile.vrt-local");
    assert!(df.contains("/usr/share/vrt/facade/manifest.json"));
    assert!(
        df.contains("COPY facade/dist /usr/share/vrt/facade/dist")
            || df.contains("COPY facade/dist/ /usr/share/vrt/facade/dist"),
        "dist ships as a dist/ subtree, not flattened"
    );
    // The repo's own facade dir satisfies the same convention —
    // init resolves <dir>/dist/<module.name>.
    let fdir = rt().join("facade");
    assert!(fdir.join("manifest.json").is_file());
    assert!(fdir.join("dist").is_dir());
    let manifest: serde_json::Value = serde_json::from_str(&read("facade/manifest.json")).unwrap();
    // Every declared module's dist tail equals its name and the file
    // exists under <facade>/dist/<name> — the exact seed lookup.
    for (svc, w) in manifest["workers"].as_object().unwrap() {
        for m in w["modules"].as_array().unwrap() {
            let name = m["name"].as_str().unwrap();
            let dist = m["dist"].as_str().unwrap();
            assert!(
                dist.ends_with(&format!("/{name}")),
                "{svc}: dist {dist} does not end with name {name}"
            );
            assert!(
                fdir.join("dist").join(name).is_file(),
                "{svc}: missing dist file for {name}"
            );
        }
    }
}

#[test]
fn t25_compose_paths_agree_with_init_root() {
    // L11: every /data… path referenced in the base compose must be
    // a path `vrt init --root /data` actually produces (configs,
    // listener dir, desired envelope). Whitelist of init-owned paths.
    let y = read("deploy/standalone/docker-compose.yml");
    let init_owned = [
        "/data",
        "/data/config/vrt-host.toml",
        "/data/config/vrt-provider.toml",
        "/data/config/vrt-sql.toml",
        "/data/provider/listeners",
    ];
    let mut found = std::collections::BTreeSet::new();
    for tok in y.split_whitespace() {
        let t = tok.trim_matches(|c| c == '"' || c == '\'' || c == ',' || c == ']');
        if t.starts_with("/data") {
            assert!(
                init_owned.contains(&t),
                "compose references /data path init does not own: {t}"
            );
            found.insert(t);
        }
    }
    assert!(found.contains("/data/config/vrt-host.toml"));
    assert!(found.contains("/data/config/vrt-provider.toml"));
    assert!(found.contains("/data/provider/listeners"));
}

#[test]
fn t25_provider_command_uses_config_flag() {
    // L7: the compose command must be a form the provider binary
    // actually parses (--config <path> or bare positional).
    let y = read("deploy/standalone/docker-compose.yml");
    let idx = y.find("vrt-provider:").expect("provider service");
    let seg = &y[idx..y.len().min(idx + 1200)];
    assert!(
        seg.contains("\"--config\""),
        "provider invoked with --config <path>"
    );
}

#[test]
fn t25_e2e_script_exists_and_is_shell_safe() {
    // Section D: the offline-validated acceptance entry point.
    let e2e = read("deploy/standalone/e2e.sh");
    assert!(e2e.starts_with("#!/usr/bin/env bash") || e2e.starts_with("#!/bin/bash"));
    assert!(e2e.contains("set -euo pipefail"));
    for line in e2e.lines() {
        let t = line.trim_start();
        if !t.starts_with('#') {
            assert!(!t.contains("sudo"), "no sudo outside comments: {line}");
        }
    }
    // Independent compose project + the README path (base file only).
    assert!(
        e2e.contains("PROJECT=")
            && (e2e.contains("-p \"$PROJECT\"")
                || e2e.contains("-p $PROJECT")
                || e2e.contains("--project-name")),
        "isolated project name"
    );
    for line in e2e.lines() {
        let t = line.trim_start();
        if !t.starts_with('#') {
            assert!(
                !t.contains("docker-compose.stub.yml"),
                "e2e never uses the stub overlay: {line}"
            );
        }
    }
    assert!(e2e.contains("up -d --wait"), "compose wait path");
    assert!(e2e.contains("down -v"), "always cleans volumes");
    // bash -n passes (parse check — we cannot run it).
    let out = std::process::Command::new("bash")
        .args([
            "-n",
            &rt().join("deploy/standalone/e2e.sh").display().to_string(),
        ])
        .output()
        .expect("bash -n");
    assert!(
        out.status.success(),
        "bash -n: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// ---- T25/M4: e2e.sh example paths resolve to real dirs ----

#[test]
fn t25_e2e_example_paths_exist_with_entrypoints() {
    let src = std::fs::read_to_string(rt().join("deploy/standalone/e2e.sh")).unwrap();
    // Every `standalone-*` dir the script copies must exist under
    // <rt>/examples with a wrangler config + entry file.
    for cap in src.match_indices("standalone-") {
        let rest = &src[cap.0..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
            .collect();
        let dir = rt().join("examples").join(&name);
        assert!(dir.is_dir(), "e2e.sh references missing examples/{name}");
        assert!(
            dir.join("vrt.toml").is_file()
                || vrt_local::WRANGLER_CANDIDATES
                    .iter()
                    .any(|c| dir.join(c).is_file()),
            "examples/{name} has no deploy config"
        );
        assert!(
            [
                "main.js",
                "index.js",
                "src/index.js",
                "index.ts",
                "src/index.ts",
                "src/index.js"
            ]
            .iter()
            .any(|e| dir.join(e).is_file()),
            "examples/{name} has no entry file"
        );
    }
    // The script must build EXAMPLES from $HERE, not rely on cwd.
    assert!(src.contains("HERE/../../examples"));
    assert!(!src.contains("cp ../examples/"));
}

// ---- T25/M5: WORKERD_SOURCE enforced at build, not at parse ----

#[test]
fn t25_workerd_source_enforced_at_build_not_parse() {
    let compose =
        std::fs::read_to_string(rt().join("deploy/standalone/docker-compose.yml")).unwrap();
    // Non-build ops (config/down/exec) must not require the var —
    // a `:?` interpolation would break them.
    assert!(!compose.contains("${WORKERD_SOURCE:?"));
    assert!(compose.contains("${WORKERD_SOURCE:-unset-required}"));
    // Build stays fail closed: the Dockerfile selects
    // `workerd-${WORKERD_SOURCE}`; the sentinel resolves to a stage
    // that does not exist → docker reports the missing value.
    let df = std::fs::read_to_string(rt().join("deploy/docker/Dockerfile.vrt-local")).unwrap();
    assert!(df.contains("workerd-${WORKERD_SOURCE}"));
    // Stub overlay keeps its own working default.
    let stub =
        std::fs::read_to_string(rt().join("deploy/standalone/docker-compose.stub.yml")).unwrap();
    assert!(stub.contains("${WORKERD_SOURCE:-stub}"));
}

// ---- W5-r2: every shipped example's vrt.toml parses through the
// deploy path ----

#[test]
fn w5r2_example_vrt_tomls_parse_via_load_worker_meta() {
    let root = rt().join("examples");
    let mut checked = 0u32;
    for entry in std::fs::read_dir(&root).unwrap() {
        let dir = entry.unwrap().path();
        if !dir.is_dir() || !dir.join("vrt.toml").is_file() {
            continue;
        }
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        vrt_local::load_worker_meta(&dir).unwrap_or_else(|e| {
            panic!("examples/{name}/vrt.toml rejected by load_worker_meta: {e}")
        });
        checked += 1;
    }
    assert!(checked >= 1, "no examples/*/vrt.toml found under {root:?}");
}

#[test]
fn w5r2_toml_parse_error_reports_span_and_key_not_values() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("vrt.toml"),
        "name = \"w\"\nmainModule = \"m.js\"\ncompatibilityDate = \"2024-01-01\"\nbogus = \"my-secret-value-123\"\n",
    )
    .unwrap();
    let err = vrt_local::load_worker_meta(dir.path()).unwrap_err();
    let s = err.to_string();
    assert!(s.contains("bogus"), "key name missing: {s}");
    assert!(s.contains("4"), "line/col missing: {s}");
    assert!(
        !s.contains("my-secret-value-123"),
        "value leaked into error: {s}"
    );
}

/// W5-r3/P2: values containing escaped quotes must not break the
/// redaction state machine — `Debug` renders `ab"SECRET"cd` as
/// `"ab\"SECRET\"cd"`, and a naive `"` toggle would leak `cd` (and
/// anything after).
#[test]
fn w5r3_toml_parse_error_redacts_escaped_quote_values() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("vrt.toml"),
        // `compatibilityFlags` expects a sequence; a string value with
        // an embedded quote exercises the Debug-escape path.
        "name = \"w\"\nmainModule = \"m.js\"\ncompatibilityDate = \"2024-01-01\"\ncompatibilityFlags = 'ab\"SECRET\"cd'\n",
    )
    .unwrap();
    let err = vrt_local::load_worker_meta(dir.path()).unwrap_err();
    let s = err.to_string();
    // Type errors carry the key in the span, not the message — assert
    // the span position (line 4 = compatibilityFlags) instead.
    assert!(s.contains("4:2"), "span missing: {s}");
    assert!(!s.contains("SECRET"), "escaped value leaked: {s}");
    // The unredacted tail after the escaped quote must not appear
    // either (`cd` was inside the same value).
    assert!(!s.contains("cd\""), "value tail leaked: {s}");
    assert!(s.contains("parse at"), "span missing: {s}");

    // Backticked values (typed non-string `Unexpected` renders in
    // backticks, e.g. `integer `987654321``) must be redacted too —
    // only field/key-position backticks survive.
    let dir2 = tempfile::tempdir().unwrap();
    std::fs::write(
        dir2.path().join("vrt.toml"),
        "name = \"w\"\nmainModule = \"m.js\"\ncompatibilityDate = \"2024-01-01\"\nassets = 987654321\n",
    )
    .unwrap();
    let s2 = vrt_local::load_worker_meta(dir2.path())
        .unwrap_err()
        .to_string();
    assert!(!s2.contains("987654321"), "backticked value leaked: {s2}");
}
