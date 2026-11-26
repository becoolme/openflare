//! MIG-13-r2 / M4: REAL workerd proof that the swap-write form the
//! supervisor uses — `vrt_securefs::atomic_write` (same-dir tmp +
//! rename) over the watched `config.live.capnp` — makes `--watch`
//! reload and emit the `vrt-switch` listen event on the control
//! stream. That marker is the serialized-switch completion signal;
//! without it the whole prewarm contract is unprovable.
//!
//! The test also proves the single-writer split: the twin config
//! (prewarm_variant) opens ONLY the scratch DO dirs, and real DO
//! storage is opened solely by the post-swap reload — scratch and
//! real metadata.sqlite files are asserted on both sides of the
//! marker.
//!
//! All declared sockets are rewritten to pre-allocated TCP ports —
//! unix binds are forbidden in the dev sandbox. The control stream
//! rides an inherited fd, so it is unaffected. MIG-13-r5 CORRECTION:
//! the swap is NOT address-family agnostic — `workerd --watch`
//! reloads by execve and re-binds every declared socket, which is
//! `EADDRINUSE` on unix (stale socket FILES survive the old image;
//! TCP's `SO_REUSEADDR` has no stale-pathname concept). The unix
//! proof lives in `tests/m13r5_swap_unix_workerd.rs`; this file keeps
//! TCP for the rename→marker/storage contract only.
//!
//! Gated on `VRT_WORKERD_BIN`:
//!   VRT_WORKERD_BIN=<workerd> cargo test -p vrt-host \
//!     --test m13r2_swap_workerd -- --include-ignored --nocapture

mod common;

use common::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::launch::{ProcessWorkerdLauncher, WorkerdLauncher};

const WORKER_ID: &str = "w-do";
const CLASS_UKEY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// Minimal DO worker — sqlite-backed Counter + a plain fetch entry.
const MODULE: &str = r#"import { DurableObject } from "cloudflare:workers";
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn params(do_dir: &Path) -> RuntimeParams {
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&raw).expect("facade manifest parses");
    RuntimeParams {
        socket_dir: "sock".into(),
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
        wfop_pepper: "m13r2-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        // H0 contract: the marker lives in THIS socket's dir.
        waituntil_socket: "sock/vrt-wu.sock".into(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

/// Render the DO tenant config, materialize every embed, and return
/// the final text plus the DO tenant dir.
fn render_do_config(fx: &Fixture, work_dir: &Path) -> (String, PathBuf) {
    mkdir700(work_dir);
    let tenant_dir = fx.dir.path().join("do").join("t1");
    mkdir700(&tenant_dir.join(WORKER_ID));

    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "main.js",
            "modules": { "main.js": MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "t1",
        "workers": [{
            "workerId": WORKER_ID,
            "name": "do",
            "activeVersion": {
                "versionId": "v-do-1",
                "artifactDigest": bundle.digest(),
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
        }]
    }))
    .unwrap();
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params(&tenant_dir),
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
            EmbedKind::BundleModule { module_name, .. } => {
                assert_eq!(module_name, "main.js");
                std::fs::write(&dst, MODULE).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }
    (out.text, tenant_dir)
}

/// Replace every declared `unix:sock/*.sock` bind with a fresh TCP
/// port (sandbox forbids unix binds; the control stream is an fd).
/// MIG-13-r5: TCP deliberately masks the stale-pathname rebind the
/// execve reload hits on unix — see m13r5_swap_unix_workerd.rs for
/// the address-family that actually runs in production. Returns the
/// port each socket name resolved to.
fn tcpify(text: &str, names: &[&str]) -> (String, BTreeMap<String, u16>) {
    let mut text = text.to_string();
    let mut ports = BTreeMap::new();
    for name in names {
        let unix_addr = format!("unix:sock/{name}.sock");
        if !text.contains(&unix_addr) {
            continue;
        }
        let port = free_port();
        text = text.replace(&unix_addr, &format!("127.0.0.1:{port}"));
        ports.insert(name.to_string(), port);
    }
    (text, ports)
}

/// Read control lines until `needle` appears or the deadline hits.
async fn await_marker(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::UnixStream>>,
    needle: &str,
    deadline: Duration,
) -> bool {
    let end = std::time::Instant::now() + deadline;
    loop {
        match tokio::time::timeout(end - std::time::Instant::now(), lines.next_line()).await {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn watch_rename_swap_emits_marker_and_opens_real_storage() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");

    let fx = Fixture::new();
    let work_dir = fx.dir.path().join("work").join("t1").join("gen-1");
    let (final_text, tenant_dir) = render_do_config(&fx, &work_dir);

    // The twin: scratch disk paths, marker stripped.
    let scratch = work_dir.join("prewarm-disks");
    let (twin_text, marker_stripped) =
        vrt_config::check::prewarm_variant(&final_text, &scratch).expect("twin renders");
    assert!(marker_stripped, "twin must not carry vrt-switch");
    assert!(
        !twin_text.contains(tenant_dir.to_str().unwrap()),
        "twin leaked a live DO dir"
    );

    // Every declared bind → TCP (final still carries the marker so we
    // can watch it land post-swap).
    let sock_names = [
        WORKER_ID.to_string(),
        format!("{WORKER_ID}-wfops"),
        "vrt-wu".to_string(),
        // The marker's file stem differs from its socket name —
        // `vrt-switch` listens on `<probe-dir>/vrt-sw.sock` and
        // tcpify keys on the file stem.
        vrt_config::capnp::SWITCH_MARKER_FILE
            .strip_suffix(".sock")
            .unwrap()
            .to_string(),
    ];
    let sock_names: Vec<&str> = sock_names.iter().map(|s| s.as_str()).collect();
    let (final_tcp, ports) = tcpify(&final_text, &sock_names);
    let (twin_tcp, _) = tcpify(&twin_text, &sock_names);
    assert!(ports.contains_key(WORKER_ID), "worker socket rendered");
    assert!(
        final_tcp.contains("\"vrt-switch\""),
        "final config must declare the marker"
    );

    // Watched file = the live twin — written through the same
    // atomic_write the supervisor uses.
    let live_config = work_dir.join("config.live.capnp");
    vrt_securefs::atomic_write(&live_config, twin_tcp.as_bytes(), 0o600).unwrap();

    let mut worker_classes = BTreeMap::new();
    worker_classes.insert(WORKER_ID.to_string(), vec![CLASS_UKEY.to_string()]);
    let spec = vrt_host::spec::TenantLaunchSpec {
        tenant_id: "t1".into(),
        binary: PathBuf::from(&bin),
        config_path: live_config.clone(),
        config_digest: "m13r2".into(),
        work_dir: work_dir.clone(),
        external_addrs: vec![],
        env: BTreeMap::new(),
        base_env: BTreeMap::new(),
        worker_endpoints: BTreeMap::from([(
            WORKER_ID.to_string(),
            WorkerEndpoint::Tcp(format!("127.0.0.1:{}", ports[WORKER_ID]).parse().unwrap()),
        )]),
        qconsumer_endpoints: BTreeMap::new(),
        wfops_endpoints: BTreeMap::new(),
        cron_endpoints: BTreeMap::new(),
        wfop_tokens: BTreeMap::new(),
        limits: vrt_host::spec::ResourceLimits::default(),
        heartbeat_endpoint: None,
        waituntil_endpoint: ports
            .get("vrt-wu")
            .map(|p| WorkerEndpoint::Tcp(format!("127.0.0.1:{p}").parse().unwrap())),
        extra_args: vec!["--experimental".into(), "--watch".into()],
        relaunch: None,
        do_storage: Some(vrt_host::spec::DoStorageSpec {
            tenant_dir: tenant_dir.clone(),
            worker_classes,
            transfer_sources: Default::default(),
        }),
        generation: 1,
        isolation: None,
    };

    let mut launched = ProcessWorkerdLauncher
        .launch(&spec)
        .await
        .expect("warm workerd launches");
    let pid = launched.pid;
    let mut lines = BufReader::new(launched.control).lines();

    // Boot: wait for the twin's first listen — proves the twin config
    // is live BEFORE the swap (a delayed first load could otherwise
    // coalesce the swap and prove nothing about reload).
    assert!(
        await_marker(&mut lines, "\"socket\":\"sock-", Duration::from_secs(20)).await,
        "twin worker socket never listened"
    );

    // Single-writer proof, warm side: scratch storage exists, real
    // storage untouched.
    let scratch_class = scratch.join("disk-0").join(CLASS_UKEY);
    let real_class = tenant_dir.join(WORKER_ID).join(CLASS_UKEY);
    wait_for(
        || scratch_class.join("metadata.sqlite").exists(),
        Duration::from_secs(10),
        "scratch DO metadata.sqlite",
    )
    .await;
    assert!(
        !real_class.exists(),
        "warm gen opened real DO storage: {}",
        real_class.display()
    );

    // M4: same-dir tmp + rename over the watched file — the exact
    // write form swap_finalize uses.
    vrt_securefs::atomic_write(&live_config, final_tcp.as_bytes(), 0o600).unwrap();

    // The marker lands iff --watch reloaded on the rename.
    assert!(
        await_marker(&mut lines, "\"socket\":\"vrt-switch\"", Duration::from_secs(20)).await,
        "no vrt-switch listen after rename swap — --watch missed it"
    );
    assert!(pid_alive(pid), "same process must survive the reload");

    // Post-marker: REAL DO storage is now open under the same process.
    wait_for(
        || real_class.join("metadata.sqlite").exists(),
        Duration::from_secs(10),
        "real DO metadata.sqlite post-swap",
    )
    .await;

    vrt_host::launch::terminate_child(&mut launched.child, pid, Duration::from_secs(2)).await;
    eprintln!("M13R2-SWAP-OK rename-triggered reload, marker seen, storage split proven");
}
