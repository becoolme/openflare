//! G1 KV/R2 contract tests on REAL workerd — the 9 uncovered
//! high-frequency semantics from the compatibility matrix, asserted
//! to Cloudflare-documented behavior (never weakened to fit).
//!
//! KV: get-null, get-json, getWithMetadata, put-TTL, list-cursor.
//! R2: head-miss, range-get, conditional-put, list-delimiter.
//!
//!   VRT_WORKERD_BIN=... cargo test -p vrt-host --test kv_r2_contract -- --nocapture

mod common;
use common::g1::*;
use serde_json::json;
use snapshot_protocol::compute_desired::verify_compute_desired;
use snapshot_protocol::compute_desired::TenantDesired;
use snapshot_protocol::envelope::{sign, RevisionGuard, SignParams, SigningKey};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use vrt_config::*;
use vrt_provider::authorize::Authorizer;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::desired::MemoryDesiredStore;
use vrt_provider::kv::fs::FsKvProvider;
use vrt_provider::r2::fs::FsR2Provider;
use vrt_provider::server::{tenant_router, TenantService};

const TENANT: &str = "team-gate"; // must match common::g1::render hardcoded tenant
const WORKER: &str = "wk";
const REVISION: u64 = 1;

const BINDINGS: &str = r#"[
    {"type":"kv","name":"KV","namespaceId":"ns-contract",
     "permissions":{"read":true,"write":true}},
    {"type":"r2","name":"BKT","bucket":"bkt-contract",
     "permissions":{"read":true,"write":true}}
]"#;

fn desired_tenant(digest: &str) -> TenantDesired {
    serde_json::from_value(json!({
        "tenantId": TENANT,
        "workers": [{
            "workerId": WORKER, "name": "contractw",
            "activeVersion": {
                "versionId": "v-wk-1", "artifactDigest": digest,
                "compatibilityDate": "2025-01-01", "compatibilityFlags": []
            },
            "bindings": serde_json::from_str::<serde_json::Value>(BINDINGS).unwrap(),
            "routes": [{"hostname": "w-contract.example.test"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap()
}

/// Real in-process provider (kv + r2 fs backends) — same shape as
/// r2_smoke.rs::spawn_provider.
fn spawn_provider(dir: &Path, digest: &str, port: u16) -> std::thread::JoinHandle<()> {
    let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
    let tenant = desired_tenant(digest);
    let payload = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell-contract".to_string(),
        revision: REVISION,
        tenants: vec![tenant],
        deletions: vec![],
};
    let signed = sign(
        &SignParams {
            kind: "compute.desired",
            scope: &snapshot_protocol::compute_desired::cell_scope("cell-contract"),
            revision: REVISION,
            issued_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now() + chrono::Duration::minutes(10),
            payload: &payload,
        },
        &key,
    )
    .unwrap();
    let mut guard = RevisionGuard::new();
    let verified = verify_compute_desired(
        &signed.bytes,
        "cell-contract",
        &[key.trusted_key()],
        &mut guard,
        chrono::Utc::now(),
    )
    .unwrap();
    let store = MemoryDesiredStore::new(1);
    store.apply(&verified).unwrap();
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(Authorizer::new(
            Arc::new(store),
            std::time::Duration::from_millis(30_000),
        )),
        kv: Some(Arc::new(FsKvProvider::new(dir.join("kv")).unwrap())),
        r2: Some(Arc::new(
            FsR2Provider::new(dir.join("r2"), 64 << 20).unwrap(),
        )),
        assets: None,
        signer: Arc::new(CursorSigner::new(b"contract-signing-secret")),
        r2_max_object_bytes: 64 << 20,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(64)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        pg: None,
    });
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
                .await
                .unwrap();
            axum::serve(l, tenant_router(svc).into_make_service())
                .await
                .unwrap();
        });
    })
}

/// Tenant module — returns each API call's result as JSON so the
/// test asserts CF-documented shapes, not VRT's own representation.
const MOD: &str = r#"
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const j = (v) => Response.json(v);
    try {
    if (u.pathname === "/kv") {
      const op = u.searchParams.get("op");
      if (op === "put") {
        const body = await req.json();
        await env.KV.put(body.k, body.v, body.opts ?? {});
        return j({ ok: true });
      }
      if (op === "get") {
        const type = u.searchParams.get("type");
        const v = await env.KV.get(u.searchParams.get("k"), type ? { type } : undefined);
        return j({ v: v === undefined ? "__undef__" : v });
      }
      if (op === "gwm") {
        const r = await env.KV.getWithMetadata(u.searchParams.get("k"));
        return j({ v: r.value === null ? null : r.value, m: r.metadata });
      }
      if (op === "list") {
        const o = {};
        if (u.searchParams.get("limit")) o.limit = +u.searchParams.get("limit");
        if (u.searchParams.get("cursor")) o.cursor = u.searchParams.get("cursor");
        if (u.searchParams.get("prefix")) o.prefix = u.searchParams.get("prefix");
        const r = await env.KV.list(o);
        return j({ keys: r.keys.map(k => k.name), done: r.list_complete, cursor: r.cursor ?? null });
      }
    }
    if (u.pathname === "/r2") {
      const op = u.searchParams.get("op");
      if (op === "put") {
        const body = await req.json();
        const o = body.onlyIf ? { onlyIf: body.onlyIf } : undefined;
        const r = await env.BKT.put(body.k, new Response(body.v).body, o);
        return j({ r: r === null ? null : { etag: r.etag, key: r.key } });
      }
      if (op === "head") {
        const r = await env.BKT.head(u.searchParams.get("k"));
        return j({ r: r === null ? null : { etag: r.etag, size: r.size } });
      }
      if (op === "get") {
        const o = {};
        if (u.searchParams.get("off")) o.range = { offset: +u.searchParams.get("off"), length: +u.searchParams.get("len") };
        const r = await env.BKT.get(u.searchParams.get("k"), o);
        return j({ r: r === null ? null : { text: await new Response(r.body).text(), size: r.size } });
      }
      if (op === "list") {
        const o = {};
        if (u.searchParams.get("delim")) o.delimiter = u.searchParams.get("delim");
        if (u.searchParams.get("prefix")) o.prefix = u.searchParams.get("prefix");
        const r = await env.BKT.list(o);
        return j({ objs: r.objects.map(o => o.key), prefixes: r.delimitedPrefixes, trunc: r.truncated });
      }
    }
    return new Response("contract");
    } catch (e) { return j({ threw: true, err: String(e) }); }
  },
};
"#;

struct Fixture {
    proc: WorkerProc,
    port: u16,
    _dir: PathBuf,
    _guard: DirGuard,
    _prov: std::thread::JoinHandle<()>,
}
use std::path::PathBuf;

fn fixture() -> Fixture {
    let bin = workerd_bin();
    let dir = gate_root("contract");
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": MOD },
            "compatibilityDate": "2025-01-01",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let prov_port = free_port();
    let prov = spawn_provider(&dir, &bundle.digest(), prov_port);
    let bindings = serde_json::from_str::<serde_json::Value>(BINDINGS).unwrap();
    let (cfg, ports, args, text) = render(
        &dir,
        &[(WORKER, "contractw", MOD, bindings)],
        EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: vec![],
            deny_names: vec![],
            request_timeout_ms: 8_000,
            ..Default::default()
        },
    );
    assert!(
        text.contains("facade-kv") && text.contains("facade-r2"),
        "kv/r2 facades missing:\n{text}"
    );
    let guard = DirGuard(dir.clone());
    let proc = spawn_backend(&bin, &cfg, &dir, prov_port, &args);
    let port = ports[WORKER];
    let (st, _) = http(port, "/");
    assert_eq!(st, 200, "ingress not up: {}", proc.log());
    Fixture {
        proc,
        port,
        _dir: dir,
        _guard: guard,
        _prov: prov,
    }
}

/// Blocking http POST helper (kept sync — mirrors `http`).
fn http_post_block(port: u16, path: &str, body: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = String::new();
    let _ = s.read_to_string(&mut buf);
    let status: u16 = buf
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    (
        status,
        buf.split("\r\n\r\n").nth(1).unwrap_or("").to_string(),
    )
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_kv_contract_cf_semantics() {
    let f = fixture();
    let p = f.port;
    let get = |k: &str, ty: &str| -> serde_json::Value {
        let (st, b) = http(p, &format!("/kv?op=get&k={k}&type={ty}"));
        assert_eq!(st, 200, "kv get {k}: {b} {}", f.proc.log());
        serde_json::from_str(&b).unwrap()
    };
    // 1. get(missing) → null (CF: null, not error/undefined).
    let v = get("missing", "text");
    assert_eq!(v["v"], serde_json::Value::Null, "get(missing): {v}");
    // 2. get("json") → parsed object (CF type:"json").
    let b = json!({"k":"jk","v":"{\"a\":1,\"b\":[2,3]}"}).to_string();
    let (st, _) = http_post_block(p, "/kv?op=put", &b);
    assert_eq!(st, 200, "kv put json: {}", f.proc.log());
    let v = get("jk", "json");
    assert_eq!(v["v"], json!({"a":1,"b":[2,3]}), "get json decode: {v}");
    // 3. getWithMetadata → {value, metadata}.
    let b = json!({"k":"mk","v":"mv","opts":{"metadata":{"tag":"t1"}}}).to_string();
    let (st, _) = http_post_block(p, "/kv?op=put", &b);
    assert_eq!(st, 200);
    let (st, b) = http(p, "/kv?op=gwm&k=mk");
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["v"], "mv", "gwm value: {v}");
    assert_eq!(v["m"], json!({"tag":"t1"}), "gwm metadata: {v}");
    let (st, b) = http(p, "/kv?op=gwm&k=missing");
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["v"], serde_json::Value::Null, "gwm missing value: {v}");
    // 4. TTL — CF documents expirationTtl >= 60 and expiration >=
    // now+60s. Both the vendored transport AND the provider enforce
    // the floor (defense-in-depth): <60 is a typed rejection; 60 is
    // accepted and readable. "Expired entries read as absent" is
    // covered at provider level (kv_memory::
    // expired_entries_read_as_absent) — waiting out a real 60s TTL
    // would exceed this test's time budget.
    let b = json!({"k":"tk","v":"tv","opts":{"expirationTtl":1}}).to_string();
    let (st, b) = http_post_block(p, "/kv?op=put", &b);
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert!(
        v["err"]
            .as_str()
            .unwrap_or("")
            .contains("KV_INVALID_OPTIONS"),
        "expirationTtl:1 must be a typed rejection (CF min-60): {v}"
    );
    let b = json!({"k":"tk","v":"tv","opts":{"expirationTtl":60}}).to_string();
    let (st, b) = http_post_block(p, "/kv?op=put", &b);
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["ok"], true, "expirationTtl:60 must be accepted: {v}");
    let v = get("tk", "text");
    assert_eq!(v["v"], "tv", "ttl entry readable pre-expiry: {v}");
    // Absolute expiration < now+60s: the facade's own check passes it
    // (>=1s), the provider rejects it — typed, end-to-end.
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 2;
    let b = json!({"k":"tk2","v":"tv2","opts":{"expiration":exp}}).to_string();
    let (st, b) = http_post_block(p, "/kv?op=put", &b);
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert!(
        v["err"].as_str().unwrap_or("").contains("INVALID"),
        "expiration now+2s must be a typed rejection (CF now+60s floor): {v}"
    );
    // 5. list cursor — limit pages, list_complete false then true.
    for i in 0..5u32 {
        let b = json!({"k":format!("lk{i}"),"v":"v"}).to_string();
        let (st, _) = http_post_block(p, "/kv?op=put", &b);
        assert_eq!(st, 200);
    }
    let (st, b) = http(p, "/kv?op=list&limit=2&prefix=lk");
    assert_eq!(st, 200);
    let page1: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(page1["keys"].as_array().unwrap().len(), 2, "page1: {page1}");
    assert_eq!(page1["done"], false, "page1 must be incomplete: {page1}");
    let cursor = page1["cursor"].as_str().unwrap_or("").to_string();
    assert!(
        !cursor.is_empty(),
        "cursor required for incomplete page: {page1}"
    );
    let (st, b) = http(
        p,
        &format!("/kv?op=list&limit=10&prefix=lk&cursor={cursor}"),
    );
    assert_eq!(st, 200);
    let page2: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(page2["done"], true, "page2 must complete: {page2}");
    // no key duplicated across pages
    let k1: Vec<String> = serde_json::from_value(page1["keys"].clone()).unwrap();
    let k2: Vec<String> = serde_json::from_value(page2["keys"].clone()).unwrap();
    for k in &k1 {
        assert!(!k2.contains(k), "dup key across cursor pages: {k}");
    }
    eprintln!("g1_kv_contract: 5 CF semantics verified — get-null/get-json/getWithMetadata/put-TTL/list-cursor");
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_r2_contract_cf_semantics() {
    let f = fixture();
    let p = f.port;
    let put = |k: &str, v: &str, only_if: serde_json::Value| -> serde_json::Value {
        let b = json!({"k": k, "v": v, "onlyIf": only_if}).to_string();
        let (st, body) = http_post_block(p, "/r2?op=put", &b);
        assert_eq!(st, 200, "r2 put {k}: {body} {}", f.proc.log());
        serde_json::from_str(&body).unwrap()
    };
    // 1. head(missing) → null (CF: returns null).
    let (st, b) = http(p, "/r2?op=head&k=nope");
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["r"], serde_json::Value::Null, "head missing: {v}");
    // 2. range get — CF: {range:{offset,length}} returns the slice.
    let r = put("rk", "abcdefgh", serde_json::Value::Null);
    assert!(r["r"].is_object(), "put rk: {r}");
    let (st, b) = http(p, "/r2?op=get&k=rk&off=2&len=3");
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["r"]["text"], "cde", "range get off=2 len=3: {v}");
    // full get size sanity
    let (st, b) = http(p, "/r2?op=get&k=rk");
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["r"]["size"], 8, "size: {v}");
    // 3. conditional put — CF: onlyIf:{etagMatches} failing → null.
    let r = put("ck", "v1", serde_json::Value::Null);
    let etag = r["r"]["etag"].as_str().unwrap_or("").to_string();
    assert!(!etag.is_empty(), "etag missing: {r}");
    let r = put("ck", "v2", json!({"etagMatches": "bogus-etag"}));
    assert_eq!(
        r["r"],
        serde_json::Value::Null,
        "put with wrong etag must return null (412 semantics): {r}"
    );
    let r = put("ck", "v2", json!({"etagMatches": etag}));
    assert!(
        r["r"].is_object(),
        "put with correct etag must succeed: {r}"
    );
    // 4. list delimiter — CF: delimitedPrefixes groups, no flat keys.
    for k in ["a/1", "a/2", "b/1", "top"] {
        let r = put(k, "x", serde_json::Value::Null);
        assert!(r["r"].is_object(), "put {k}: {r}");
    }
    let (st, b) = http(p, "/r2?op=list&delim=/");
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    let prefixes: Vec<String> = serde_json::from_value(v["prefixes"].clone()).unwrap();
    let objs: Vec<String> = serde_json::from_value(v["objs"].clone()).unwrap();
    assert!(
        prefixes.contains(&"a/".to_string()) && prefixes.contains(&"b/".to_string()),
        "delimitedPrefixes missing a/ b/: {v}"
    );
    assert!(
        objs.contains(&"top".to_string()),
        "top-level key missing: {v}"
    );
    assert!(
        !objs.iter().any(|k| k.contains('/')),
        "delimited list must not return nested keys flat: {v}"
    );
    eprintln!(
        "g1_r2_contract: 4 CF semantics verified — head-miss/range/conditional-put/list-delimiter"
    );
}
