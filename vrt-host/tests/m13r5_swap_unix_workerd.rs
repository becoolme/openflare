//! MIG-13-r5: REAL unix sockets — the ONLY production address family
//! (the m13r2 tcpify proof stays for the rename→marker/storage
//! contract; it could never see this crash).
//!
//! `workerd serve --watch` reloads by `execve()` of the same
//! binary+argv — `CliMain::reloadFromConfigChange` in
//! `workerd.c++` (v1.20260916.1): the pid survives but the whole
//! process image does not. The exec'd image re-binds every `unix:`
//! socket the config declares while the previous image's socket
//! FILES persist on disk — kj's `SocketAddress::bind` never unlinks
//! a stale pathname (`kj/async-io-unix.c++:941`, the e2e24 crash).
//! TCP masks this entirely: `SO_REUSEADDR` has no stale-pathname
//! concept, so the tcpify suite stayed green while production
//! `EADDRINUSE`d. The supervisor's swap therefore calls
//! `vrt_host::supervisor::clear_declared_sockets` — unlink exactly
//! the declared in-`sock/` paths — BEFORE the watched live-config
//! rewrite. This file proves both halves on real workerd + real
//! unix binds:
//!
//! - `watch_reload_unix_bind_failure_is_fatal_without_preclear`: the r4
//!   write form alone cannot land the reload — the exec'd image hits
//!   EADDRINUSE on the stale paths, never serves the new config, and
//!   (with the fork's "socket bind failure is fatal again, even under
//!   --watch" fix, commit `e699fea`) exits non-zero instead of
//!   lingering without a listener: e2e24 reproduced deterministically.
//! - `watch_reload_unix_succeeds_after_preclear`: with the
//!   production pre-clear the reload lands the marker on the SAME
//!   pid, an early-flipped non-DO worker serves on its unix socket,
//!   and the DO worker serves + opens real storage post-marker.
//!
//! Gated on `VRT_WORKERD_BIN` (run outside the dev sandbox — real
//! unix `bind()`s are required):
//!   VRT_WORKERD_BIN=<workerd> cargo test -p vrt-host \
//!     --test m13r5_swap_unix_workerd -- --include-ignored --nocapture

mod common;

use common::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::launch::{ProcessWorkerdLauncher, WorkerdLauncher};

const DO_WORKER: &str = "w-do";
const QC_WORKER: &str = "w-qc";
const CLASS_UKEY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// DO worker — sqlite Counter + a plain fetch entry.
const MODULE_DO: &str = r#"import { DurableObject } from "cloudflare:workers";
export class Counter extends DurableObject {
  async fetch(req) {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + 1;
    await this.ctx.storage.put("n", n);
    return new Response("n=" + n);
  }
}
export default {
  async fetch(req, env) { return new Response("ok"); }
};
"#;

/// The early-flip stand-in: no DO classes, so production routes it
/// to the warm generation before the swap — it must keep serving
/// across the reload.
const MODULE_QC: &str = r#"export default {
  async fetch(req, env) { return new Response("ok-qc"); }
};
"#;

fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// Same shape as the m13r2 params but with ABSOLUTE unix socket
/// paths — the production emit uses `<gen>/sock` verbatim.
fn params(do_dir: &Path, sock_dir: &Path) -> RuntimeParams {
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&raw).expect("facade manifest parses");
    RuntimeParams {
        socket_dir: sock_dir.to_string_lossy().into_owned(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::<EgressAllowEntry>::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 30_000,
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        disk_services: manifest.disk_services.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: do_dir.to_string_lossy().into_owned(),
        wfop_pepper: "m13r5-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: sock_dir.join("vrt-wu.sock").to_string_lossy().into_owned(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

/// Render the 2-worker tenant config (DO + non-DO), materialize every
/// embed, return the final text.
fn render_config(work_dir: &Path, tenant_dir: &Path, sock_dir: &Path) -> String {
    mkdir700(work_dir);
    mkdir700(&tenant_dir.join(DO_WORKER));

    let bundle_do = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "main.js",
            "modules": { "main.js": MODULE_DO },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let bundle_qc = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "main.js",
            "modules": { "main.js": MODULE_QC },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "t1",
        "workers": [
            {
                "workerId": DO_WORKER,
                "name": "do",
                "activeVersion": {
                    "versionId": "v-do-1",
                    "artifactDigest": bundle_do.digest(),
                    "compatibilityDate": "2026-09-08",
                    "compatibilityFlags": []
                },
                "bindings": [{"type":"durableObject","name":"COUNTER","className":"Counter"}],
                "routes": [{"hostname": "do.test"}],
                "crons": [], "queueConsumers": [],
                "durableObjectClasses": [{
                    "className": "Counter",
                    "uniqueKey": CLASS_UKEY,
                    "enableSql": true
                }],
                "workflows": [],
                "quotas": {"cpuMs": 50, "memoryMb": 128,
                           "subrequests": 100, "outboundConnections": 16}
            },
            {
                "workerId": QC_WORKER,
                "name": "qc",
                "activeVersion": {
                    "versionId": "v-qc-1",
                    "artifactDigest": bundle_qc.digest(),
                    "compatibilityDate": "2026-09-08",
                    "compatibilityFlags": []
                },
                "bindings": [],
                "routes": [{"hostname": "qc.test"}],
                "crons": [], "queueConsumers": [],
                "durableObjectClasses": [],
                "workflows": [],
                "quotas": {"cpuMs": 50, "memoryMb": 128,
                           "subrequests": 100, "outboundConnections": 16}
            }
        ]
    }))
    .unwrap();
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle_do, bundle_qc],
        params: &params(tenant_dir, sock_dir),
    })
    .unwrap();

    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
    for embed in &out.embeds {
        let dst = work_dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{service_name} missing from manifest"));
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap_or_else(|| panic!("{service_name}/{module_name} not in manifest"));
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes =
                    std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                assert_eq!(format!("{:x}", Sha256::digest(&bytes)), embed.digest);
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule {
                worker_id,
                module_name,
            } => {
                assert_eq!(module_name, "main.js");
                let module = match worker_id.as_str() {
                    DO_WORKER => MODULE_DO,
                    QC_WORKER => MODULE_QC,
                    other => panic!("unexpected worker {other}"),
                };
                std::fs::write(&dst, module).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }
    out.text
}

/// Read control lines until `needle` appears or the deadline hits.
async fn await_marker(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::UnixStream>>,
    needle: &str,
    deadline: Duration,
) -> bool {
    let end = Instant::now() + deadline;
    loop {
        match tokio::time::timeout(end - Instant::now(), lines.next_line()).await {
            Ok(Ok(Some(line))) => {
                eprintln!("ctl: {line}");
                if line.contains(needle) {
                    return true;
                }
            }
            _ => return false,
        }
    }
}

/// Minimal HTTP GET over a unix socket — enough to prove the worker
/// answers on the re-bound listener post-reload.
async fn http_get_unix(path: &Path, host: &str) -> (u16, String) {
    let mut stream = tokio::net::UnixStream::connect(path)
        .await
        .unwrap_or_else(|e| panic!("connect {}: {e}", path.display()));
    let req = format!("GET / HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed before response headers");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let len: usize = head
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.trim().parse().ok())
        })
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < len {
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed mid-body");
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(len);
    (status, String::from_utf8_lossy(&body).to_string())
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Build the launch spec shared by both tests — REAL unix endpoints.
fn unix_spec(
    bin: &str,
    gen_dir: &Path,
    sock_dir: &Path,
    live_config: &Path,
    tenant_dir: &Path,
) -> vrt_host::spec::TenantLaunchSpec {
    let mut worker_classes = BTreeMap::new();
    worker_classes.insert(DO_WORKER.to_string(), vec![CLASS_UKEY.to_string()]);
    vrt_host::spec::TenantLaunchSpec {
        tenant_id: "t1".into(),
        binary: PathBuf::from(bin),
        config_path: live_config.to_path_buf(),
        config_digest: "m13r5".into(),
        work_dir: gen_dir.to_path_buf(),
        external_addrs: vec![],
        env: BTreeMap::new(),
        base_env: BTreeMap::new(),
        worker_endpoints: BTreeMap::from([
            (
                DO_WORKER.to_string(),
                WorkerEndpoint::Unix(sock_dir.join(format!("{DO_WORKER}.sock"))),
            ),
            (
                QC_WORKER.to_string(),
                WorkerEndpoint::Unix(sock_dir.join(format!("{QC_WORKER}.sock"))),
            ),
        ]),
        qconsumer_endpoints: BTreeMap::new(),
        wfops_endpoints: BTreeMap::new(),
        cron_endpoints: BTreeMap::new(),
        wfop_tokens: BTreeMap::new(),
        limits: vrt_host::spec::ResourceLimits::default(),
        heartbeat_endpoint: None,
        waituntil_endpoint: Some(WorkerEndpoint::Unix(sock_dir.join("vrt-wu.sock"))),
        extra_args: vec!["--experimental".into(), "--watch".into()],
        relaunch: None,
        do_storage: Some(vrt_host::spec::DoStorageSpec {
            tenant_dir: tenant_dir.to_path_buf(),
            worker_classes,
            transfer_sources: Default::default(),
        }),
        generation: 1,
        isolation: None,
    }
}

/// What `boot_twin` hands back — `LaunchedWorkerd` decomposed so the
/// control stream and the child handle can move independently.
struct TwinBoot {
    root: PathBuf,
    gen_dir: PathBuf,
    sock_dir: PathBuf,
    tenant_dir: PathBuf,
    live_config: PathBuf,
    final_text: String,
    pid: u32,
    child: tokio::process::Child,
    stderr_tail: Option<std::sync::Arc<vrt_host::launch::LogTail>>,
    lines: tokio::io::Lines<BufReader<tokio::net::UnixStream>>,
}

/// Boot the twin config under --watch on REAL unix sockets.
async fn boot_twin(bin: &str, tag: &str) -> TwinBoot {
    let root = unix_root(tag);
    let gen_dir = root.join("t1").join("gen-1");
    let sock_dir = gen_dir.join("sock");
    mkdir700(&sock_dir);
    let tenant_dir = root.join("do").join("t1");
    let final_text = render_config(&gen_dir, &tenant_dir, &sock_dir);
    for needle in ["w-do.sock", "w-qc.sock", "vrt-wu.sock", "vrt-sw.sock"] {
        assert!(final_text.contains(needle), "render must declare {needle}");
    }
    let scratch = gen_dir.join("prewarm-disks");
    let (twin_text, marker_stripped) =
        vrt_config::check::prewarm_variant(&final_text, &scratch).expect("twin renders");
    assert!(marker_stripped, "twin must not carry vrt-switch");
    let live_config = gen_dir.join("config.live.capnp");
    vrt_securefs::atomic_write(&live_config, twin_text.as_bytes(), 0o600).unwrap();
    let spec = unix_spec(bin, &gen_dir, &sock_dir, &live_config, &tenant_dir);
    let launched = ProcessWorkerdLauncher
        .launch(&spec)
        .await
        .expect("warm workerd launches");
    let lines = BufReader::new(launched.control).lines();
    TwinBoot {
        root,
        gen_dir,
        sock_dir,
        tenant_dir,
        live_config,
        final_text,
        pid: launched.pid,
        child: launched.child,
        stderr_tail: launched.log_stats.stderr_tail.clone(),
        lines,
    }
}

/// NEGATIVE CONTROL — the r4 write form alone reproduces e2e24: the
/// execve'd image re-binds every declared unix socket, hits the stale
/// files the twin bound, and cannot serve the new config. With the
/// fork's "socket bind failure is fatal again, even under `--watch`"
/// fix (workerd VRT commit `e699fea`) an environmental bind failure
/// kills the exec'd image instead of leaving a live process with no
/// listener — restoring the pre-1.20261120 semantics this suite
/// originally asserted. The hazard is therefore proven twice over: no
/// socket of the twin comes back, and the reloaded image exits
/// non-zero. This is what the tcpify suite structurally could not see.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN + real unix binds"]
async fn watch_reload_unix_bind_failure_is_fatal_without_preclear() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");

    let mut boot = boot_twin(&bin, "m13r5x").await;
    assert!(
        await_marker(&mut boot.lines, "\"socket\":\"sock-", Duration::from_secs(20)).await,
        "twin worker sockets never listened"
    );

    // The bug: rewrite the watched config while every declared unix
    // socket file still exists — the execve'd image rebinds them all.
    vrt_securefs::atomic_write(&boot.live_config, boot.final_text.as_bytes(), 0o600).unwrap();

    // The reload must NOT land: the exec'd image's bind(2) fails on the
    // stale pathnames, so no new listen event can arrive for them and
    // the control stream EOFs. (`vrt-switch` is not a witness here:
    // the twin config carries no marker socket, so its path is free
    // and it binds even in the broken reload.)
    assert!(
        !await_marker(
            &mut boot.lines,
            "\"socket\":\"sock-w-do\"",
            Duration::from_secs(5)
        )
        .await,
        "a reload without the pre-clear must never rebind the stale unix sockets"
    );
    // The bind failure is fatal under --watch again (fork #4,
    // workerd VRT commit e699fea): the exec'd image must exit non-zero
    // instead of lingering as a live process with no usable listener.
    // `await_marker` returns as soon as the control stream EOFs, so the
    // child is usually already reapable here; poll anyway so a slow
    // drain cannot turn a real regression into a flaky failure.
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match boot.child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(e) => panic!("wait on the reloaded workerd failed: {e}"),
        }
        assert!(
            Instant::now() < deadline,
            "bind failure must be fatal under --watch: workerd pid {} is \
             still alive 10s after the failed rebind with no usable \
             listener (fork commit e699fea regressed?)",
            boot.pid
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        !status.success(),
        "a bind failure must exit non-zero (or on a signal), got {status:?}"
    );
    // Give the stderr drain a beat to flush the failed binds.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let tail = boot
        .stderr_tail
        .as_ref()
        .map(|t| t.tail_lines(12))
        .unwrap_or_default();
    assert!(
        tail.contains("Address already in use") || tail.contains("EADDRINUSE"),
        "expected EADDRINUSE evidence in stderr tail: {tail}"
    );
    let _ = std::fs::remove_dir_all(&boot.root);
    eprintln!("M13R5-UNIX-REPRO bind failure on stale unix sockets is fatal (e2e24 shape)");
}

/// THE FIX — the exact production call (`swap_finalize`'s
/// `clear_declared_sockets`) frees every declared bind BEFORE the
/// watched write: the execve'd image binds clean, the marker lands on
/// the same pid, and BOTH worker surfaces serve on real unix sockets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN + real unix binds"]
async fn watch_reload_unix_succeeds_after_preclear() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");

    let mut boot = boot_twin(&bin, "m13r5u").await;
    let pid = boot.pid;
    let gen_dir = boot.gen_dir.clone();
    let sock_dir = boot.sock_dir.clone();
    let tenant_dir = boot.tenant_dir.clone();
    assert!(
        await_marker(&mut boot.lines, "\"socket\":\"sock-", Duration::from_secs(20)).await,
        "twin worker sockets never listened"
    );

    // Single-writer proof, warm side: scratch DO dir live, real
    // storage untouched.
    let scratch = gen_dir.join("prewarm-disks");
    let scratch_class = scratch.join("disk-0").join(CLASS_UKEY);
    let real_class = tenant_dir.join(DO_WORKER).join(CLASS_UKEY);
    wait_for(
        || scratch_class.join("metadata.sqlite").exists(),
        Duration::from_secs(10),
        "scratch DO metadata.sqlite",
    )
    .await;
    assert!(!real_class.exists(), "warm gen opened real DO storage");

    // Early-flip surface pre-swap: the non-DO worker already answers
    // on the warm gen's unix socket.
    let (status, body) = http_get_unix(&sock_dir.join(format!("{QC_WORKER}.sock")), "qc.test").await;
    assert_eq!(status, 200, "early-flip worker must serve pre-swap: {body}");
    assert!(body.contains("ok-qc"), "{body}");

    // THE FIX — exactly what swap_finalize now runs before the write.
    vrt_host::supervisor::clear_declared_sockets(&boot.final_text, &sock_dir)
        .expect("pre-clear frees every declared bind");
    vrt_securefs::atomic_write(&boot.live_config, boot.final_text.as_bytes(), 0o600).unwrap();

    assert!(
        await_marker(&mut boot.lines, "\"socket\":\"vrt-switch\"", Duration::from_secs(30)).await,
        "no vrt-switch listen after pre-cleared swap"
    );
    assert!(pid_alive(pid), "same process survives the execve reload");

    // The early-flipped non-DO worker still serves — the reload
    // re-bound its socket and traffic resumes.
    let (status, body) = http_get_unix(&sock_dir.join(format!("{QC_WORKER}.sock")), "qc.test").await;
    assert_eq!(status, 200, "early-flip worker must serve post-swap: {body}");
    assert!(body.contains("ok-qc"), "{body}");

    // The DO worker answers on its re-bound socket and real storage
    // opened only after the marker (single-writer preserved).
    let (status, body) = http_get_unix(&sock_dir.join(format!("{DO_WORKER}.sock")), "do.test").await;
    assert_eq!(status, 200, "DO worker must serve post-marker: {body}");
    assert!(body.contains("ok"), "{body}");
    wait_for(
        || real_class.join("metadata.sqlite").exists(),
        Duration::from_secs(10),
        "real DO metadata.sqlite post-swap",
    )
    .await;

    let _ = vrt_host::launch::terminate_child(&mut boot.child, pid, Duration::from_secs(2)).await;
    let _ = std::fs::remove_dir_all(&boot.root);
    eprintln!("M13R5-UNIX-OK pre-cleared unix rebind: marker, both workers serve, storage split proven");
}
