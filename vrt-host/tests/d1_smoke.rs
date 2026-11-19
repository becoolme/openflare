//! Real-workerd D1 smoke (T34): facade-d1 → provider-wire → real vrt-sql.
//!
//! Renders the REAL `vrt_config::render_tenant_config` capnp with a
//! `{"type":"d1"}` binding → workerd wrapped `cloudflare-internal:d1-api`
//! → inner fetcher `facade-d1` (real dist module) → `binding-backend`.
//! The provider process itself is stubbed (same rationale as
//! workerd_entry_smoke: a real vrt-provider needs its desired store +
//! unix listener plumbing); the stub is a same-name worker that
//!   1. rejects if any `x-vrt-*` header leaked (adversarial probe), and
//!   2. forwards to a REAL `vrt-sql` process with the signed
//!      `x-vrt-sql-auth` token (HMAC over ts|method|path|body|epoch|seq)
//!      — exactly what `vrt_provider::d1::SqlAuthSigner` emits.
//! So `vrt-sql` is a real process and the whole d1-api wire is real;
//! the provider's authorization layer is covered by unit tests.
//!
//! Gated on `VRT_WORKERD_BIN`; also needs `cargo build -p vrt-sql` once
//! (the test locates target/debug/vrt-sql itself).

use std::io::Write;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressPolicy, EmbedKind,
    RuntimeParams, TenantRenderInput, VersionBundle,
};

// vrt-sql requires >= 32-byte auth secrets.
const SECRET: &str = "d1-smoke-shared-secret-pad-to-32-bytes-min";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn workerd_bin() -> Option<PathBuf> {
    std::env::var("VRT_WORKERD_BIN")
        .ok()
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
}

fn vrt_sql_bin() -> PathBuf {
    if let Ok(p) = std::env::var("VRT_SQL_BIN") {
        return PathBuf::from(p);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/debug/vrt-sql")
}

/// T37r2：所有 smoke socket 都有 connect/read/write 硬超时——real_*
/// 测试绝不能再把一个挂起的 workerd/vrt-sql 拖成整个 workspace
/// 超时（上一轮队列 smoke 曾挂死 2400s）。超时即 panic（Proc::drop
/// 在 panic 时附带 workerd.log 尾部）。
const SMOKE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const SMOKE_CONN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

fn tcp_conn(port: u16) -> std::net::TcpStream {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let s = std::net::TcpStream::connect_timeout(&addr, SMOKE_CONN_TIMEOUT)
        .unwrap_or_else(|e| panic!("tcp connect :{port}: {e}"));
    s.set_read_timeout(Some(SMOKE_IO_TIMEOUT)).unwrap();
    s.set_write_timeout(Some(SMOKE_IO_TIMEOUT)).unwrap();
    s
}

fn read_all(s: &mut std::net::TcpStream, what: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match std::io::Read::read(s, &mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("socket read timed out/failed for {what}: {e}"),
        }
    }
    buf
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    false
}

fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut s = tcp_conn(port);
    s.write_all(format!("GET {path} HTTP/1.0\r\nhost: x\r\n\r\n").as_bytes())
        .unwrap();
    let buf = read_all(&mut s, &format!("GET {path}"));
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

/// Process guard: graceful TERM → bounded wait → KILL → wait.
/// Children are spawned with `.process_group(0)` (own process-group
/// leader), so TERM/KILL go to the whole group — grandchildren can
/// never be orphaned.
///
/// T37r2：另起一个 DETACHED babysitter 盯住本测试进程——若测试进程
/// 本身被杀（harness 超时 SIGKILL 不会跑 Drop），babysitter 在 ~1s
/// 内 KILL 子进程整个组：子进程绝不可能比测试进程活得更久。
/// babysitter 在自身独立进程组中，不被随测试组一起 kill。
struct Proc {
    child: Child,
    babysitter: Option<Child>,
    /// (label, log path) — tail printed when the test is panicking.
    log: Option<(&'static str, PathBuf)>,
}

impl Proc {
    fn new(child: Child) -> Self {
        Self::with(child, None)
    }

    fn logged(child: Child, label: &'static str, log: PathBuf) -> Self {
        Self::with(child, Some((label, log)))
    }

    fn with(child: Child, log: Option<(&'static str, PathBuf)>) -> Self {
        Self {
            babysitter: Some(spawn_babysitter(child.id())),
            child,
            log,
        }
    }
}

/// 自终止 watcher：轮询测试进程 pid；一旦消失（正常退出或被杀——
/// SIGKILL 下 Drop 不跑，只有它能兜住），KILL 子进程整组后退出。
fn spawn_babysitter(child_pid: u32) -> Child {
    Command::new("sh")
        .arg("-c")
        .arg(format!(
            "while kill -0 {parent} 2>/dev/null; do sleep 1; done;              {{ kill -KILL -- -{pid} || true; }} 2>/dev/null; exit 0",
            parent = std::process::id(),
            pid = child_pid,
        ))
        // 独立进程组——测试组的批量 kill 波及不到它；它只等测试进程死。
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn proc babysitter")
}

impl Drop for Proc {
    fn drop(&mut self) {
        // T34f3: SIGTERM first — vrt-host must shut down GRACEFULLY so
        // its supervisor drains/SIGKILLs its own workerd generations;
        // an instant SIGKILL orphans them (the R2 failure — product
        // behavior the test must not paper over). The window exceeds
        // the drain chain (kill_grace 5s + workerd's own teardown).
        // vrt-sql/vrt-provider install no TERM handler → the default
        // disposition ends them immediately, so the shared window
        // costs nothing for them.
        // T37r2：信号发给整个进程组（spawn 时 process_group(0) 使子
        // 进程是组长，-pid = 其 pgid）——覆盖组内一切后代。
        let pid = self.child.id() as i32;
        let _ = unsafe { libc::kill(-pid, libc::SIGTERM) };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let exited = loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break true,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Ok(None) => break false,
            }
        };
        if !exited {
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(mut b) = self.babysitter.take() {
            let _ = b.kill();
            let _ = b.wait();
        }
        if std::thread::panicking() {
            if let Some((label, log)) = &self.log {
                eprintln!("-- {label} tail --\n{}", tail(log));
            }
        }
    }
}

/// Root-dir guard: declare FIRST so it drops LAST — always removes the
/// dir (T37r2：失败时保留曾让挂死/崩溃测试在 target/tmp 泄漏几十个
/// 目录；forensics 已由 Proc::drop 在 panic 时把日志尾部打进
/// stderr)。`VRT_SMOKE_KEEP` 仍可选保留。
struct SmokeDir(PathBuf);

impl Drop for SmokeDir {
    fn drop(&mut self) {
        if std::env::var_os("VRT_SMOKE_KEEP").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        } else {
            eprintln!("kept smoke dir: {}", self.0.display());
        }
    }
}

/// The user worker: exercises the D1 API surface and returns JSON the
/// test asserts on.
const USER_MODULE: &str = r#"export default {
  async fetch(req, env) {
    const out = {};
    try {
      await env.DB.exec("CREATE TABLE IF NOT EXISTS t(a INTEGER, b TEXT)");
      const r1 = await env.DB.prepare("INSERT INTO t(a,b) VALUES (?,?)").bind(1,"x").run();
      out.changes = r1.meta.changes;
      out.lastRowId = r1.meta.last_row_id;
      out.results = (await env.DB.prepare("SELECT a,b FROM t").all()).results;
      out.first = await env.DB.prepare("SELECT a FROM t").first();
      out.raw = await env.DB.prepare("SELECT a FROM t").raw();
      try {
        await env.DB.batch([
          env.DB.prepare("INSERT INTO t(a,b) VALUES (2,'y')"),
          env.DB.prepare("SELECT * FROM nosuch_table"),
        ]);
        out.batchRolledBack = false;
      } catch (e) {
        out.batchErr = String(e).includes("D1_ERROR");
        const c = await env.DB.prepare("SELECT COUNT(*) c FROM t").first();
        out.batchRolledBack = c.c === 1;
      }
      const sess = env.DB.withSession();
      out.session = (await sess.prepare("SELECT 1 AS one").all()).results;
      // adversarial: env.fetcher is not exposed to user code; prove the
      // Worker cannot smuggle x-vrt-* (d1-api builds fetcher headers).
      return Response.json(out);
    } catch (e) {
      return Response.json({ error: String(e && e.stack || e) });
    }
  }
}
"#;

/// binding-backend stub: asserts no x-vrt-* leaked, then signs +
/// forwards to the real vrt-sql (provider's downstream role).
const STUB: &str = r#"let seq = 0;
const enc = new TextEncoder();
const hex = (b) => [...new Uint8Array(b)].map(x => x.toString(16).padStart(2,"0")).join("");
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const p = url.pathname;
    if (!p.startsWith("/internal/bindings/v1/d1/")) {
      return new Response("unknown", { status: 404 });
    }
    // adversarial check: any leaked x-vrt-* → fail the request loudly.
    for (const [k] of req.headers) {
      if (k.toLowerCase().startsWith("x-vrt-")) {
        return new Response(JSON.stringify({success:false,error:"X-VRT-LEAK:"+k}),
          { headers: {"content-type":"application/json"} });
      }
    }
    const leaf = p.split("/").pop();
    if (leaf !== "query" && leaf !== "execute") {
      return new Response("bad op", { status: 400 });
    }
    const body = new Uint8Array(await req.arrayBuffer());
    const epoch = 1;
    const s = ++seq;
    const ts = Math.floor(Date.now()/1000);
    const pq = `/dbs/<DBID>/${leaf}${url.search}`;
    const bodyHex = hex(await crypto.subtle.digest("SHA-256", body));
    const msg = `${ts}|POST|${pq}|${bodyHex}|${epoch}|${s}`;
    const key = await crypto.subtle.importKey("raw", enc.encode("<SECRET>"),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const mac = hex(await crypto.subtle.sign("HMAC", key, enc.encode(msg)));
    const auth = `v1:${ts}:${mac}`;
    const resp = await env.SQL.fetch(`http://vrt-sql${pq}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-vrt-epoch": String(epoch),
        "x-vrt-seq": String(s),
        "x-vrt-sql-auth": auth,
      },
      body,
    });
    return resp;
  }
}
"#;

/// T37b-r2: workerd argv is DERIVED from the same tenant/params the
/// helper rendered — never hand-written (`--experimental` appears only
/// when the emitted config actually needs it, production parity).
fn render_cfg(dir: &Path, port: u16, sql_port: u16, db_id: &str) -> (PathBuf, Vec<String>) {
    let worker_id = "wd1";
    let binding_name = "DB";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": USER_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-d1",
        "workers": [{
            "workerId": worker_id,
            "name": "d1w",
            "activeVersion": {
                "versionId": "v-d1-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [{
                "type": "d1", "name": binding_name,
                "databaseId": db_id, "epoch": 1,
                "permissions": {"read": true, "write": true}
            }],
            "routes": [{"hostname": "d1--team-d1.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let manifest = parse_facade_manifest(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    let params = RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy::default(),
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "d1-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    })
    .unwrap();
    // F1-style: the wrapped binding keeps the user's declared name.
    assert!(
        out.text
            .contains("moduleName = \"cloudflare-internal:d1-api\""),
        "{}",
        out.text
    );
    assert!(
        out.text
            .contains("(name = \"fetcher\", service = (name = \"facade-d1\""),
        "{}",
        out.text
    );

    // materialize embeds
    let manifest_doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap();
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap();
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes = std::fs::read(&src).unwrap();
                assert_eq!(format!("{:x}", Sha256::digest(&bytes)), embed.digest);
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, USER_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { ref name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                assert_eq!(
                    format!("{:x}", Sha256::digest(bytes)),
                    embed.digest,
                    "vendored embed {name} digest drift"
                );
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }
    let mut text = out.text.clone();
    text = text.replace(
        &format!("unix:sock/{worker_id}.sock"),
        &format!("127.0.0.1:{port}"),
    );
    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(
        text.contains(external),
        "no binding-backend external:\n{text}"
    );
    std::fs::write(
        dir.join("backend-stub.js"),
        STUB.replace("<DBID>", db_id)
            .replace("<SECRET>", SECRET)
            .replace("<SQLPORT>", &sql_port.to_string()),
    )
    .unwrap();
    // stub needs raw outbound to reach vrt-sql → globalOutbound internet.
    text = text.replace(
        external,
        "(name = \"binding-backend\", worker = (\n            \
           modules = [(name = \"backend.js\", esModule = embed \"backend-stub.js\")],\n            \
           compatibilityDate = \"2026-09-08\",\n            \
           bindings = [(name = \"SQL\", service = (name = \"vrt-sql\"))],\n        )),\n        \
           (name = \"vrt-sql\", external = (http = ()))",
    );
    let cfg = dir.join("cfg.capnp");
    std::fs::write(&cfg, text).unwrap();
    (
        cfg,
        vrt_host::assemble::experimental_extra_args(
            &params.system_compat_flags,
            tenant.workers.iter().any(|w| !w.queue_consumers.is_empty()),
            tenant
                .workers
                .iter()
                .any(|w| !w.durable_object_classes.is_empty()),
            false,
        ),
    )
}

/// 测试临时根：`target/tmp/vrt-d1-smoke-<port>`（仓库约定，
/// 不落 /tmp/claude——T34c/F1）。
fn smoke_dir(suffix: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp")
        .join(format!("vrt-d1-smoke-{}-{}", free_port(), suffix));
    std::fs::create_dir_all(&dir).unwrap();
    // P3c-1-r1: `CARGO_MANIFEST_DIR/../target` leaves a `..` segment in
    // the path — capnp `valid_path` (production) rejects it, so every
    // consumer of smoke_dir would render an unrenderable config.
    // Canonicalize once here: the dir exists now, so this resolves the
    // segment (and any tmp-dir symlinks) for all callers.
    std::fs::canonicalize(&dir).unwrap()
}

fn spawn_vrt_sql(dir: &Path, port: u16) -> Child {
    let secrets = dir.join("keys");
    std::fs::create_dir_all(&secrets).unwrap();
    for (name, val) in [
        ("bm.secret", SECRET.repeat(2)),
        ("auth.secret", SECRET.to_string()),
    ] {
        let p = secrets.join(name);
        std::fs::write(&p, val).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    let cfg = json!({
        "dbRoot": dir.join("db"),
        "listenTcp": format!("127.0.0.1:{port}"),
        "bookmarkSecretFile": secrets.join("bm.secret"),
        "authSecretFile": secrets.join("auth.secret"),
        "globalConcurrency": 8,
        "operationTimeoutMs": 5000,
        "drainTimeoutMs": 5000,
        "bodyMaxBytes": 1048576,
        // T36b/P0-A: `[persistence]` 必填——fs 后端 root 在测试
        // 临时目录内（非 `none`，端到端覆盖持久化路径）。
        "persistence": {
            "backend": "fs",
            // RV-R66: objstore and dbRoot share the test tmp device by
            // construction; acknowledge the single copy (dev only).
            "allowSameDeviceDurability": true,
            "fs": { "root": dir.join("objstore") }
        }
    });
    let cfg_path = dir.join("vrt-sql.json");
    std::fs::write(&cfg_path, cfg.to_string()).unwrap();
    Command::new(vrt_sql_bin())
        .args(["serve", "--config"])
        .arg(&cfg_path)
        // T37r2：独立进程组——Proc 的 group TERM/KILL 与 babysitter
        // group-kill 覆盖它可能派生的任何子进程。
        .process_group(0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vrt-sql (cargo build -p vrt-sql first)")
}

#[test]
fn real_workerd_d1_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN");
        return;
    };
    let sql_bin = vrt_sql_bin();
    if !sql_bin.exists() {
        panic!(
            "vrt-sql binary missing at {}; run `cargo build -p vrt-sql` or set VRT_SQL_BIN",
            sql_bin.display()
        );
    }
    let dir = smoke_dir("stub");
    let _dir_guard = SmokeDir(dir.clone());
    let sql_port = free_port();
    // vrt-sql 同样进 Proc guard——任一 assert/panic 都会 Drop 回收。
    let _sql = Proc::new(spawn_vrt_sql(&dir, sql_port));
    assert!(wait_ready(sql_port), "vrt-sql never listened on {sql_port}");
    let db_id = "d1smoke00000000000000000aa";
    let port = free_port();
    let (cfg, argv_extra) = render_cfg(&dir, port, sql_port, db_id);
    let deny = dir.join("deny.js");
    std::fs::write(
        &deny,
        "export default { async fetch() { return new Response(\"deny\",{status:403}); } }",
    )
    .unwrap();
    let log = dir.join("workerd.log");
    // T37b-r: ONE File::create + try_clone — two File::create()s on the
    // same path would truncate each other (workerd.log came out empty).
    let out_file = std::fs::File::create(&log).unwrap();
    let err_file = out_file.try_clone().unwrap();
    let proc = Command::new(&bin)
        .arg("serve")
        .args(&argv_extra)
        .arg(format!("--external-addr=vrt-sql=127.0.0.1:{sql_port}"))
        .arg(&cfg)
        .current_dir(&dir)
        .process_group(0)
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .expect("spawn workerd");
    let _wd = Proc::logged(proc, "workerd", log.clone());
    if !wait_ready(port) {
        panic!(
            "workerd never listened:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
    }
    let (st, body) = http_get(port, "/");
    let v: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("bad json {st}: {body}"));
    assert_eq!(
        st,
        200,
        "{body}
-- workerd.log --
{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert!(
        v.get("error").is_none(),
        "worker error: {body}
-- workerd.log --
{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert_eq!(v["changes"], 1, "{body}");
    assert_eq!(v["lastRowId"], 1, "{body}");
    assert_eq!(v["results"], json!([{"a":1,"b":"x"}]), "{body}");
    assert_eq!(v["first"], json!({"a":1}), "{body}");
    assert_eq!(v["raw"], json!([[1]]), "{body}");
    assert!(
        v["batchErr"].as_bool().unwrap_or(false),
        "batch error must wrap D1_ERROR: {body}"
    );
    assert!(
        v["batchRolledBack"].as_bool().unwrap_or(false),
        "batch must roll back atomically: {body}"
    );
    assert_eq!(v["session"], json!([{"one":1}]), "{body}");
    // sql/wd 的 Proc guard 在作用域末自动 kill+wait+清目录。
}

/// T34c/F1 回归：guard 在 panic 展开时也必须 kill+wait 子进程。
/// 用 `/bin/kill -0 <pid>` 探测（ESRCH ⇒ 已被回收，无新依赖）。
#[test]
fn proc_guard_reaps_child_on_panic() {
    let dir = smoke_dir("stub");
    let _dir_guard = SmokeDir(dir.clone());
    let pid = std::cell::Cell::new(0u32);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = Command::new("sleep")
            .arg("60")
            .process_group(0)
            .spawn()
            .unwrap();
        pid.set(child.id());
        let _guard = Proc::new(child);
        panic!("boom");
    }));
    assert!(r.is_err());
    let alive = Command::new("kill")
        .args(["-0", &pid.get().to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(!alive, "child {} leaked after guard drop", pid.get());
    let _ = std::fs::remove_dir_all(&dir);
}

fn http_get_host(port: u16, host: &str, path: &str) -> (u16, String) {
    let mut s = tcp_conn(port);
    s.write_all(format!("GET {path} HTTP/1.0\r\nhost: {host}\r\n\r\n").as_bytes())
        .unwrap();
    let buf = read_all(&mut s, &format!("GET {host}{path}"));
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn run_ok(bin: &Path, args: &[&str]) {
    let out = Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
    assert!(
        out.status.success(),
        "{} {:?} failed:\nstdout: {}\nstderr: {}",
        bin.display(),
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn spawn_logged(bin: &Path, args: &[&str], log: &Path) -> Child {
    // T37b-r: one File::create + try_clone (see workerd.log call sites).
    let out_file = std::fs::File::create(log).unwrap();
    let err_file = out_file.try_clone().unwrap();
    Command::new(bin)
        .args(args)
        .process_group(0)
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()))
}

fn tail(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.lines().rev().take(30).collect::<Vec<_>>().join("\n")
}

/// T34d: REAL chain — no stubs anywhere. Replicates standalone e2e:
/// `vrt init` (capnp) → `vrt deploy` a d1 worker → REAL vrt-sql +
/// vrt-provider + vrt-host (which spawns real workerd) → ingress →
/// `env.DB` round-trip. The stub test above cannot see the provider's
/// authorization layer; this is the test that would have caught the
/// standalone e2e failure.
#[test]
fn real_provider_d1_end_to_end() {
    let Some(workerd) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN");
        return;
    };
    let debug = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/debug");
    for name in ["vrt", "vrt-host", "vrt-provider", "vrt-sql"] {
        assert!(
            debug.join(name).is_file(),
            "{name} missing at {} — run `cargo build --workspace --bins` first",
            debug.join(name).display()
        );
    }
    // Real chain needs unix sockets (provider tenant listener +
    // workerd service sockets) — the root MUST stay well under the
    // 104-byte AF_UNIX sun_path budget: `std::env::temp_dir()` on
    // macOS alone is ~50 chars and nested worktree paths are ~190
    // chars (the T34e failure). Use the shared short-root helper;
    // SKIP only when the OS forbids it (sandbox EPERM/EACCES) —
    // every other failure, including a too-long path, panics loudly.
    let dir = match vrt_securefs::test_short_root("vrtd1r") {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            return;
        }
        Err(e) => panic!("short root: {e}"),
    };
    let root = dir.join("root");
    let sock = root.join("provider/listeners/local.sock");
    assert!(
        sock.as_os_str().len() <= 100,
        "test root too long: {} is {} bytes — shorten the root",
        sock.display(),
        sock.as_os_str().len()
    );
    match std::os::unix::net::UnixListener::bind(dir.join("probe.sock")) {
        Ok(l) => {
            drop(l);
            let _ = std::fs::remove_file(dir.join("probe.sock"));
        }
        Err(e) if matches!(e.kind(), std::io::ErrorKind::PermissionDenied) => {
            eprintln!("SKIP: unix socket bind denied ({e}) — sandboxed run");
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        Err(e) => panic!("probe unix bind in {}: {e}", dir.display()),
    }
    let ingress = free_port();
    let control = free_port();
    let host_metrics = free_port();
    let provider_metrics = free_port();
    let sql_port = free_port();
    let sql_metrics = free_port();
    let facade = facade_root();
    run_ok(
        &debug.join("vrt"),
        &[
            "init",
            "--root",
            root.to_str().unwrap(),
            "--cell",
            "local",
            "--tenant",
            "local",
            "--facade-dist",
            facade.to_str().unwrap(),
            "--workerd",
            workerd.to_str().unwrap(),
            "--ingress-listen",
            &format!("127.0.0.1:{ingress}"),
            "--control-listen",
            &format!("127.0.0.1:{control}"),
            "--metrics-listen",
            &format!("127.0.0.1:{host_metrics}"),
            "--provider-metrics-listen",
            &format!("127.0.0.1:{provider_metrics}"),
            "--vrt-sql-addr",
            &format!("http://127.0.0.1:{sql_port}"),
            "--vrt-sql-listen",
            &format!("127.0.0.1:{sql_port}"),
            "--vrt-sql-metrics-listen",
            &format!("127.0.0.1:{sql_metrics}"),
        ],
    );
    // The generated vrt-sql.toml puts objstore on a SEPARATE volume (compose
    // layout) and vrt-sql fails closed when it shares a device with dbRoot
    // (RV-R66). This smoke root lives on one tmp device by construction, so
    // acknowledge the single copy for this dev-only harness (same as the
    // hand-written config in the other cases).
    let sql_toml = root.join("config/vrt-sql.toml");
    let sql_doc = std::fs::read_to_string(&sql_toml).unwrap();
    assert!(sql_doc.contains("[persistence]\n"), "template has [persistence]");
    std::fs::write(
        &sql_toml,
        sql_doc.replacen(
            "[persistence]\n",
            "[persistence]\nallowSameDeviceDurability = true\n",
            1,
        ),
    )
    .unwrap();
    let wdir = dir.join("worker");
    std::fs::create_dir_all(&wdir).unwrap();
    std::fs::write(
        wdir.join("wrangler.jsonc"),
        r#"{
  "name": "d1-real",
  "main": "main.js",
  "compatibility_date": "2024-09-25",
  "routes": ["d1r.vrt.local/*"],
  "d1_databases": [
    { "binding": "DB", "database_name": "demo", "database_id": "demo-db" }
  ]
}"#,
    )
    .unwrap();
    std::fs::write(
        wdir.join("main.js"),
        r#"export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/api/hits") {
      try {
        await env.DB.exec("CREATE TABLE IF NOT EXISTS hits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL)");
        await env.DB.prepare("INSERT INTO hits (id, n) VALUES (1, 1) ON CONFLICT(id) DO UPDATE SET n = n + 1").run();
        const row = await env.DB.prepare("SELECT n FROM hits WHERE id = 1").first();
        return Response.json({ hits: row.n });
      } catch (e) {
        return Response.json({ error: String(e && e.stack || e) }, { status: 500 });
      }
    }
    return Response.json({ ok: true });
  }
}
"#,
    )
    .unwrap();
    run_ok(
        &debug.join("vrt"),
        &[
            "deploy",
            "--root",
            root.to_str().unwrap(),
            wdir.to_str().unwrap(),
        ],
    );
    let sql_log = dir.join("vrt-sql.log");
    let prov_log = dir.join("vrt-provider.log");
    let host_log = dir.join("vrt-host.log");
    let cfg = root.join("config");
    // Declared before the procs → drops AFTER them: a failing assert
    // (incl. the orphan check below) keeps the root + all log tails.
    let _dir_guard = SmokeDir(dir.clone());
    let sql_args = [
        "serve".to_string(),
        "--config".to_string(),
        cfg.join("vrt-sql.toml").to_str().unwrap().to_string(),
    ];
    let sql_args: Vec<&str> = sql_args.iter().map(|a| a.as_str()).collect();
    let mut sql = Proc::logged(
        spawn_logged(&debug.join("vrt-sql"), &sql_args, &sql_log),
        "vrt-sql",
        sql_log.clone(),
    );
    let _prov = Proc::logged(
        spawn_logged(
            &debug.join("vrt-provider"),
            &["--config", cfg.join("vrt-provider.toml").to_str().unwrap()],
            &prov_log,
        ),
        "vrt-provider",
        prov_log.clone(),
    );
    let _host = Proc::logged(
        spawn_logged(
            &debug.join("vrt-host"),
            &["--config", cfg.join("vrt-host.toml").to_str().unwrap()],
            &host_log,
        ),
        "vrt-host",
        host_log.clone(),
    );
    // readiness: provider tenant socket + host control healthz.
    let sock_dir = root.join("provider").join("listeners");
    let mut ok = false;
    for _ in 0..100 {
        if std::fs::read_dir(&sock_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
        {
            ok = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(
        ok,
        "provider never bound a tenant socket:\n{}",
        tail(&prov_log)
    );
    let mut healthy = false;
    for _ in 0..150 {
        if std::net::TcpStream::connect(("127.0.0.1", control)).is_ok()
            && http_get_host(control, "x", "/healthz").0 == 200
        {
            healthy = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(healthy, "vrt-host never healthy:\n{}", tail(&host_log));
    let dump = || {
        format!(
            "\n-- vrt-host --\n{}\n-- vrt-provider --\n{}\n-- vrt-sql --\n{}",
            tail(&host_log),
            tail(&prov_log),
            tail(&sql_log)
        )
    };
    let mut last = String::new();
    for _ in 0..100 {
        let (st, body) = http_get_host(ingress, "d1r.vrt.local", "/api/hits");
        last = format!("{st} {body}");
        if st == 200 && body.contains("\"hits\":1") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert!(
        last.contains("\"hits\":1"),
        "first hit failed: {last}{}",
        dump()
    );
    let (st2, body2) = http_get_host(ingress, "d1r.vrt.local", "/api/hits");
    assert_eq!(st2, 200, "{body2}{}", dump());
    assert!(
        body2.contains("\"hits\":2"),
        "second hit: {body2}{}",
        dump()
    );
    // T36b: 重启 vrt-sql——fs 持久化 + 重启 claim/lease 释放路径
    // 必须让 counter 延续（hits:3 而非重置回 1）。SIGTERM 下优雅
    // 释放 lease，新进程无需等满 15s。
    drop(sql);
    sql = Proc::logged(
        spawn_logged(&debug.join("vrt-sql"), &sql_args, &sql_log),
        "vrt-sql",
        sql_log.clone(),
    );
    let mut last3 = String::new();
    for _ in 0..100 {
        let (st, body) = http_get_host(ingress, "d1r.vrt.local", "/api/hits");
        last3 = format!("{st} {body}");
        if st == 200 && body.contains("\"hits\":3") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert!(
        last3.contains("\"hits\":3"),
        "post-restart hit failed (persistence/reclaim broken): {last3}{}",
        dump()
    );
    // T34f: graceful teardown BEFORE the orphan assertion — vrt-host
    // must SIGTERM→drain→SIGKILL its workerd generations itself; only
    // then is "nothing under the root" meaningful. Host first, while
    // provider/sql still serve the drain.
    drop(_host);
    drop(_prov);
    drop(sql);
    // No orphaned process may still reference this root — workerd's
    // argv carries its config/socket paths, so `pgrep -f <root>`
    // finds every leak (the T34f bug: SIGKILL'd host → live workerd).
    // Poll with grace: a workerd mid-SIGTERM-drain can legitimately
    // outlive its host for a few seconds; a true orphan never exits.
    let needle = dir.to_string_lossy().to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match Command::new("pgrep").args(["-f", &needle]).output() {
            Ok(o) if o.status.code() == Some(0) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "orphaned processes still reference {}:\n{}{}",
                    dir.display(),
                    String::from_utf8_lossy(&o.stdout),
                    // Procs already dropped normally — their panic
                    // branch won't print; include the tails here.
                    dump()
                );
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Ok(o) if o.status.code() == Some(1) => break,
            _ => {
                eprintln!("note: pgrep unavailable — orphan check inconclusive");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// T37: real workerd queue producer smoke — `env.Q.send()`/`sendBatch()` →
// facade-queue → binding-backend stub → REAL vrt-sql queue backend.
// ENV-gated like the D1 test (VRT_WORKERD_BIN); sandbox CI skips.
// ---------------------------------------------------------------------------

const QUEUE_USER_MODULE: &str = r#"// T37r2：workerd 对 send 响应里的 metadata.metrics.oldestMessageTimestamp
// 按 `date` 解析——stub 返回入队前 metrics，第一次 send 正好覆盖
// "空队列 oldest=0" 规范化路径；该字段为 null/字符串会让 send()
// 直接 TypeError 而不是 QueueOperationError（沙箱外实测）。
export default {
  async fetch(req, env) {
    const out = {};
    try {
      await env.Q.send("hello-queue", { contentType: "text" });
      out.sendOk = true;
      await env.Q.sendBatch([
        { body: "a" , contentType: "text" },
        { body: "b", contentType: "text" },
      ]);
      out.batchOk = true;
      // adversarial: 101 messages must reject (broker cap = 100).
      try {
        await env.Q.sendBatch(
          Array.from({ length: 101 }, (_, i) => ({ body: "x" + i, contentType: "text" }))
        );
        out.overRejected = false;
      } catch {
        out.overRejected = true;
      }
      return Response.json(out);
    } catch (e) {
      return Response.json({ error: String(e && e.stack || e) });
    }
  }
}
"#;

/// Queue binding-backend stub: same ServiceAuth signing as the D1 stub;
/// translates the provider-shaped queue ops to vrt-sql queue wire.
/// Asserts no x-vrt-* leaked inbound (facade must strip).
const QUEUE_STUB: &str = r#"let seq = 0;
const enc = new TextEncoder();
const hex = (b) => [...new Uint8Array(b)].map(x => x.toString(16).padStart(2,"0")).join("");
const b64 = (bytes) => btoa(String.fromCharCode(...bytes));
const unb64 = (s) => Uint8Array.from(atob(s), c => c.charCodeAt(0));
const hexToBytes = (h) => { const a = new Uint8Array(h.length/2); for (let i=0;i<a.length;i++) a[i]=parseInt(h.substr(i*2,2),16); return a; };
const bytesToHex = (b) => [...b].map(x => x.toString(16).padStart(2,"0")).join("");
async function sqlOnce(env, method, pq, body) {
  const epoch = 1;
  const s = ++seq;
  const ts = Math.floor(Date.now()/1000);
  const bodyHex = hex(await crypto.subtle.digest("SHA-256", body ?? new Uint8Array()));
  const msg = `${ts}|${method}|${pq}|${bodyHex}|${epoch}|${s}`;
  const key = await crypto.subtle.importKey("raw", enc.encode("<SECRET>"),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = hex(await crypto.subtle.sign("HMAC", key, enc.encode(msg)));
  return env.SQL.fetch(`http://vrt-sql${pq}`, {
    method,
    headers: {
      "content-type": "application/json",
      "x-vrt-epoch": String(epoch),
      "x-vrt-seq": String(s),
      "x-vrt-sql-auth": `v1:${ts}:${mac}`,
    },
    body: method === "GET" ? undefined : body,
  });
}
// WAL1-r2 P2: under parallel-suite load the stub→vrt-sql subrequest can
// hit the just-spawned accept path and throw (→ workerd turns it into
// Internal Error 15000 in env.Q.send). Bounded retry on transport
// failures only; GETs are idempotent so they may also retry non-OK.
// POST /send has no idempotency key — a lost response could already
// have enqueued — so its non-OK responses are NOT retried.
async function sql(env, method, pq, body) {
  let last;
  for (let i = 0; i < 4; i++) {
    try {
      const r = await sqlOnce(env, method, pq, body);
      if (method !== "GET" || r.ok || i === 3) return r;
      last = r;
    } catch (e) {
      last = e;
      if (i === 3) throw e;
    }
    await new Promise((res) => setTimeout(res, 75));
  }
  return last;
}
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const p = url.pathname;
    if (!p.startsWith("/internal/bindings/v1/queue/")) {
      return new Response("unknown", { status: 404 });
    }
    // T37r2：wrapBindingBackend 合法注入的 props 身份头（provider 的
    // 授权面靠它们）必须放行——T37 旧检查把它们误判成 props 泄漏，
    // 在读 body 前返回 500，实测让 workerd 的 send 永久挂起。规则与
    // facade 的剥离面一致：x-vrt-* 一律拒；x-open-compute-* 只允许
    // 身份头全集 + facade mint 的 request-id，其余拒。
    const IDENTITY_HEADERS = new Set([
      "x-open-compute-binding-id",
      "x-open-compute-version-id",
      "x-open-compute-worker-id",
      "x-open-compute-descriptor-sha256",
      "x-open-compute-namespace-resource-id",
      "x-open-compute-resource-spec-generation",
      "x-open-compute-request-id",
    ]);
    for (const [k] of req.headers) {
      const kl = k.toLowerCase();
      if (kl.startsWith("x-vrt-")) {
        return new Response(JSON.stringify({error:"X-VRT-LEAK:"+k}),
          { status: 500, headers: {"content-type":"application/json"} });
      }
      if (kl.startsWith("x-open-compute-") && !IDENTITY_HEADERS.has(kl)) {
        return new Response(JSON.stringify({error:"HDR-LEAK:"+k}),
          { status: 500, headers: {"content-type":"application/json"} });
      }
    }
    const leaf = p.split("/").pop();
    const qid = "<QID>";
    // workerd 按 `date` 解析 oldestMessageTimestamp——必须是 epoch-ms
    // 数字，空队列 0，绝不能 null/字符串（T37r2；miniflare broker
    // `.getTime() ?? 0` 同口径）。vrt-sql 内部字段是
    // `oldestMessageTimestampMs`。
    const metricsOf = (m) => ({
      backlogCount: m.backlogCount ?? 0,
      backlogBytes: m.backlogBytes ?? 0,
      oldestMessageTimestamp:
        typeof m.oldestMessageTimestampMs === "number"
          ? m.oldestMessageTimestampMs
          : typeof m.oldestMessageTimestamp === "number"
            ? m.oldestMessageTimestamp : 0,
    });
    if (leaf === "metrics") {
      const r = await sql(env, "GET", `/queues/${qid}/metrics`);
      const v = await r.json();
      return Response.json(metricsOf(v.metrics ?? {}));
    }
    let messages;
    if (leaf === "message") {
      const body = new Uint8Array(await req.arrayBuffer());
      const fmt = req.headers.get("x-msg-fmt") ?? "v8";
      const delay = Number(req.headers.get("x-msg-delay-secs") ?? "0");
      messages = [{ body: bytesToHex(body), contentType: fmt, delaySecs: delay }];
    } else if (leaf === "batch") {
      const reqJson = await req.json();
      messages = reqJson.messages.map((m) => ({
        id: m.id,
        body: bytesToHex(unb64(m.body)),
        contentType: m.contentType,
        delaySecs: m.delaySecs ?? 0,
        timestampMs: m.timestamp,
      }));
    } else {
      return new Response("bad op", { status: 400 });
    }
    // broker 语义（miniflare message/batch 响应的 metrics 在 enqueue
    // 前计算——broker.worker.js:345-377）：取入队前 metrics。这也让
    // smoke 的第一次 send 精确覆盖"空队列 oldestMessageTimestamp=0"
    // 的规范化路径（T37r2）。
    const pre = await sql(env, "GET", `/queues/${qid}/metrics`);
    const preV = await pre.json();
    const r = await sql(env, "POST", `/queues/${qid}/send`, enc.encode(JSON.stringify({ messages })));
    const v = await r.json();
    if (!r.ok || v.success !== true) {
      return new Response(JSON.stringify({ error: v.error ?? `sql ${r.status}` }),
        { status: r.ok ? 400 : r.status, headers: {"content-type":"application/json"} });
    }
    return Response.json({ metadata: { metrics: metricsOf(preV.metrics ?? {}) } });
  }
}
"#;

fn render_queue_cfg(
    dir: &Path,
    port: u16,
    _sql_port: u16,
    queue_id: &str,
) -> (PathBuf, Vec<String>) {
    render_queue_cfg_full(dir, port, queue_id, QUEUE_USER_MODULE, false, 0)
}

/// T37b: consumer variant — same render pipeline, but the worker also
/// declares `queueConsumers[]` (emitting `facade-qconsumer-<id>` +
/// `sock-qconsumer-<id>`) and `module` carries a `queue()` handler.
/// `qport` substitutes the qconsumer unix socket with a TCP listener —
/// the test drives `POST /vrt/queue-dispatch` exactly like vrt-host's
/// `/v1/queue/dispatch` proxy does (same payload shape, same socket).
fn render_queue_cfg_full(
    dir: &Path,
    port: u16,
    queue_id: &str,
    module: &str,
    consumers: bool,
    qport: u16,
) -> (PathBuf, Vec<String>) {
    let worker_id = "wq";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": module },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let mut worker_json = json!({
        "workerId": worker_id,
        "name": "qw",
        "activeVersion": {
            "versionId": "v-q-1",
            "artifactDigest": bundle.digest(),
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        },
        "bindings": [{
            "type": "queue", "name": "Q",
            "queueId": queue_id, "queueName": "q-smoke",
            "permissions": {"read": true, "write": true}
        }],
        "routes": [{"hostname": "q--team-q.vibeploy.app"}],
        "crons": [],
        "quotas": {"cpuMs": 50, "memoryMb": 128,
                   "subrequests": 100, "outboundConnections": 16}
    });
    if consumers {
        worker_json["queueConsumers"] = json!([{
            "queueId": queue_id,
            "queueName": "q-smoke",
            "maxBatchSize": 5,
            "maxBatchTimeoutSecs": 1,
            "maxRetries": 2,
            "retryDelaySecs": 0,
            "maxConcurrency": 1
        }]);
    }
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-q",
        "workers": [worker_json]
    }))
    .unwrap();
    let manifest = parse_facade_manifest(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    let params = RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy::default(),
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "d1-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    })
    .unwrap();
    assert!(out.text.contains("facade-queue"), "{}", out.text);

    let manifest_doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap();
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap();
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes = std::fs::read(&src).unwrap();
                assert_eq!(format!("{:x}", Sha256::digest(&bytes)), embed.digest);
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, module).unwrap();
            }
            EmbedKind::VendoredModule { ref name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                assert_eq!(
                    format!("{:x}", Sha256::digest(bytes)),
                    embed.digest,
                    "vendored embed {name} digest drift"
                );
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }
    let mut text = out.text.clone();
    text = text.replace(
        &format!("unix:sock/{worker_id}.sock"),
        &format!("127.0.0.1:{port}"),
    );
    if consumers {
        assert!(
            text.contains(&format!("facade-qconsumer-{worker_id}")),
            "qconsumer service missing:
{text}"
        );
        // T37b-r: Fetcher.queue() throws "queue event not supported"
        // without this flag — the emitter MUST always union it in.
        let qsvc = text
            .split(&format!("facade-qconsumer-{worker_id}"))
            .nth(1)
            .unwrap_or("");
        assert!(
            qsvc[..qsvc.find("modules = [").unwrap_or(qsvc.len().min(2000))]
                .contains("service_binding_extra_handlers"),
            "qconsumer missing service_binding_extra_handlers:
{text}"
        );
        text = text.replace(
            &format!("unix:sock/{worker_id}-qconsumer.sock"),
            &format!("127.0.0.1:{qport}"),
        );
    }
    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(
        text.contains(external),
        "no binding-backend external:\n{text}"
    );
    std::fs::write(
        dir.join("backend-stub.js"),
        QUEUE_STUB
            .replace("<QID>", queue_id)
            .replace("<SECRET>", SECRET),
    )
    .unwrap();
    text = text.replace(
        external,
        "(name = \"binding-backend\", worker = (\n            \
           modules = [(name = \"backend.js\", esModule = embed \"backend-stub.js\")],\n            \
           compatibilityDate = \"2026-09-08\",\n            \
           bindings = [(name = \"SQL\", service = (name = \"vrt-sql\"))],\n        )),\n        \
           (name = \"vrt-sql\", external = (http = ()))",
    );
    let cfg = dir.join("cfg.capnp");
    std::fs::write(&cfg, text).unwrap();
    (
        cfg,
        vrt_host::assemble::experimental_extra_args(
            &params.system_compat_flags,
            tenant.workers.iter().any(|w| !w.queue_consumers.is_empty()),
            tenant
                .workers
                .iter()
                .any(|w| !w.durable_object_classes.is_empty()),
            false,
        ),
    )
}

#[test]
fn real_workerd_queue_producer() {
    let Some(bin) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN");
        return;
    };
    let sql_bin = vrt_sql_bin();
    if !sql_bin.exists() {
        panic!(
            "vrt-sql binary missing at {}; run `cargo build -p vrt-sql` or set VRT_SQL_BIN",
            sql_bin.display()
        );
    }
    let dir = smoke_dir("queue");
    let _dir_guard = SmokeDir(dir.clone());
    let sql_port = free_port();
    let _sql = Proc::new(spawn_vrt_sql(&dir, sql_port));
    assert!(wait_ready(sql_port), "vrt-sql never listened on {sql_port}");
    // queueId 按 mint 规则（vrt-q:{tenant}:{name} 的 sha256 前 26 hex）
    // 派生——hex ⊂ [a-z0-9]{26}，构造上不可能非法（T37 硬编码 25 位
    // 在 vrt-sql 入口被 VRT_QUEUE_INVALID 拒）。
    let queue_id_s = format!("{:x}", Sha256::digest(b"vrt-q:team-q:q-smoke"))[..26].to_string();
    let queue_id: &str = &queue_id_s;
    assert_eq!(queue_id.len(), 26);
    let port = free_port();
    let (cfg, argv_extra) = render_queue_cfg(&dir, port, sql_port, queue_id);
    let log = dir.join("workerd.log");
    // T37b-r: ONE File::create + try_clone — two File::create()s on the
    // same path would truncate each other (workerd.log came out empty).
    let out_file = std::fs::File::create(&log).unwrap();
    let err_file = out_file.try_clone().unwrap();
    let proc = Command::new(&bin)
        .arg("serve")
        .args(&argv_extra)
        .arg(format!("--external-addr=vrt-sql=127.0.0.1:{sql_port}"))
        .arg(&cfg)
        .current_dir(&dir)
        .process_group(0)
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .expect("spawn workerd");
    let _wd = Proc::logged(proc, "workerd", log.clone());
    if !wait_ready(port) {
        panic!(
            "workerd never listened:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
    }
    let (st, body) = http_get(port, "/");
    let v: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("bad json {st}: {body}"));
    assert_eq!(
        st,
        200,
        "{body}\n-- workerd.log --\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert!(
        v.get("error").is_none(),
        "worker error: {body}\n-- workerd.log --\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert_eq!(v["sendOk"], json!(true), "{body}");
    assert_eq!(v["batchOk"], json!(true), "{body}");
    assert_eq!(
        v["overRejected"],
        json!(true),
        "101-message batch must reject: {body}"
    );

    // backlog 直查 vrt-sql metrics（手工 ServiceAuth sign —
    // vrt-host 不依赖 vrt-sql crate；签名格式 v1:<ts>:<hmac-sha256>
    // 与 service.rs::message 逐字节一致）。
    let pq = format!("/queues/{queue_id}/metrics");
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let body_sha = format!("{:x}", Sha256::digest([]));
    let msg = format!("{ts}|GET|{pq}|{body_sha}|1|1");
    let auth_hdr = format!(
        "v1:{ts}:{}",
        hmac_sha256_hex(SECRET.as_bytes(), msg.as_bytes())
    );
    let mut s = tcp_conn(sql_port);
    s.write_all(
        format!("GET {pq} HTTP/1.0\r\nhost: x\r\nx-vrt-sql-auth: {auth_hdr}\r\nx-vrt-epoch: 1\r\nx-vrt-seq: 1\r\n\r\n").as_bytes(),
    )
    .unwrap_or_else(|e| panic!("metrics socket write: {e}\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()));
    let buf = read_all(&mut s, "vrt-sql metrics");
    let text = String::from_utf8_lossy(&buf);
    let metrics_body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    let m: serde_json::Value = serde_json::from_str(&metrics_body)
        .unwrap_or_else(|_| panic!("bad metrics json: {metrics_body}"));
    // wire 形状 `{success, metrics:{backlogCount, backlogBytes,
    // oldestMessageTimestampMs}}`（vrt-sql wire.rs:825 同 send 响应）。
    assert_eq!(m["success"].as_bool(), Some(true), "{metrics_body}");
    let wm = &m["metrics"];
    // send(1) + batch(2) = 3；101-条被拒的不入队。
    assert_eq!(wm["backlogCount"].as_i64(), Some(3), "{metrics_body}");
    assert!(
        wm["backlogBytes"].as_i64().unwrap_or(0) > 0,
        "{metrics_body}"
    );
    // oldest 为服务端时钟 epoch-ms：正整数且不晚于 now+5s 容差。
    let oldest = wm["oldestMessageTimestampMs"]
        .as_i64()
        .unwrap_or_else(|| panic!("oldestMessageTimestampMs not int: {metrics_body}"));
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(
        oldest > 0 && oldest <= now_ms + 5_000,
        "oldestMessageTimestampMs {oldest} out of range (now {now_ms}): {metrics_body}"
    );
}

/// ServiceAuth 等价签名（vrt-host 不依赖 vrt-sql 且不加 hmac 依赖）：
/// RFC2104 HMAC-SHA256 — 两个 64B 分组 sha256。
fn hmac_sha256_hex(key: &[u8], msg: &[u8]) -> String {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let d = Sha256::digest(key);
        k[..32].copy_from_slice(&d);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::digest(&[ipad.as_slice(), msg].concat());
    hex::encode(Sha256::digest(
        &[opad.as_slice(), inner.as_slice()].concat(),
    ))
}

/// T37b consumer module — producer send via `env.Q` plus a `queue()`
/// handler that records every delivery (same isolate ⇒ globalThis is
/// shared with `fetch`), retries attempt 1 and acks attempt ≥2 —
/// mirroring the dispatcher's redelivery contract.
const QCONSUMER_USER_MODULE: &str = r#"export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/seen") {
      return Response.json(globalThis.__seen || []);
    }
    try {
      await env.Q.send("hello-queue", { contentType: "text" });
      return Response.json({ sendOk: true });
    } catch (e) {
      return Response.json({ error: String(e && e.stack || e) });
    }
  },
  async queue(batch, env, ctx) {
    globalThis.__seen = globalThis.__seen || [];
    for (const m of batch.messages) {
      globalThis.__seen.push({
        id: m.id,
        attempts: m.attempts,
        body: m.body,
        queue: batch.queue,
        tsIsDate: m.timestamp instanceof Date,
      });
    }
    const maxAtt = Math.max(...batch.messages.map((m) => m.attempts));
    if (maxAtt < 2) {
      batch.retryAll();
      return;
    }
    batch.ackAll();
  },
}
"#;

fn http_post(port: u16, path: &str, body: &str) -> (u16, String) {
    let mut s = tcp_conn(port);
    s.write_all(
        format!(
            "POST {path} HTTP/1.0\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )
    .unwrap_or_else(|e| panic!("dispatch write: {e}"));
    let buf = read_all(&mut s, "dispatch response");
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

/// Signed vrt-sql queue op (same ServiceAuth contract as the metrics
/// probe in real_workerd_queue_producer — v1:<ts>:<hmac> over
/// `<ts>|<method>|<path>|<bodySha256Hex>|<epoch>|<seq>`).
fn sql_queue_call(sql_port: u16, seq: i64, pq: &str, body: &[u8]) -> serde_json::Value {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let body_sha = format!("{:x}", Sha256::digest(body));
    let msg = format!("{ts}|POST|{pq}|{body_sha}|1|{seq}");
    let auth_hdr = format!(
        "v1:{ts}:{}",
        hmac_sha256_hex(SECRET.as_bytes(), msg.as_bytes())
    );
    let mut s = tcp_conn(sql_port);
    s.write_all(
        format!(
            "POST {pq} HTTP/1.0\r\nhost: x\r\ncontent-type: application/json\r\nx-vrt-sql-auth: {auth_hdr}\r\nx-vrt-epoch: 1\r\nx-vrt-seq: {seq}\r\ncontent-length: {}\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )
    .unwrap_or_else(|e| panic!("vrt-sql {pq} write: {e}"));
    s.write_all(body)
        .unwrap_or_else(|e| panic!("vrt-sql {pq} body: {e}"));
    let buf = read_all(&mut s, "vrt-sql queue op");
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body_text = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    assert_eq!(status, 200, "vrt-sql {pq} → {status}: {body_text}");
    serde_json::from_str(&body_text)
        .unwrap_or_else(|_| panic!("bad vrt-sql json for {pq}: {body_text}"))
}

/// T37b — real workerd consumer path: producer `env.Q.send()` lands a
/// message in vrt-sql; the test claims it (real vrt-sql wire), then
/// POSTs the dispatcher's dispatch payload to the REAL
/// `facade-qconsumer` service (rendered by the real capnp emitter onto
/// a TCP port in place of the unix socket). The facade invokes the
/// tenant worker's `queue(batch)` handler via workerd's
/// `Fetcher.queue()` RPC — the same call vrt-host's
/// `/v1/queue/dispatch` proxies. Attempt 1 → `retryAll` outcome;
/// attempt 2 → `ackAll`, with attempts/queue/body/timestamp asserted
/// inside the real handler.
/// T37b-r (NOT env-gated): the emitted qconsumer service MUST carry
/// `service_binding_extra_handlers` — `Fetcher.queue()` throws
/// "queue event not supported" without it. This assertion runs
/// without a workerd binary so the invariant is always checked.
#[test]
fn qconsumer_emit_carries_extra_handlers_flag() {
    let dir = smoke_dir("qconsumer-emit");
    let _dir_guard = SmokeDir(dir.clone());
    let queue_id_s = format!("{:x}", Sha256::digest(b"vrt-q:team-q:q-smoke"))[..26].to_string();
    let (cfg, argv_extra) =
        render_queue_cfg_full(&dir, 1, &queue_id_s, QCONSUMER_USER_MODULE, true, 0);
    let text = std::fs::read_to_string(&cfg).unwrap();
    let idx = text.find("facade-qconsumer-").expect("qconsumer missing");
    let seg = &text[idx..idx + 3000.min(text.len() - idx)];
    assert!(
        seg.contains("service_binding_extra_handlers"),
        "qconsumer must emit service_binding_extra_handlers:\n{text}"
    );
    // The DERIVED argv must carry `--experimental` for a consumer
    // tenant — the flag the emit just forced is useless without it.
    assert!(
        argv_extra.iter().any(|a| a == "--experimental"),
        "consumer tenant argv must derive --experimental: {argv_extra:?}"
    );
}

#[test]
fn real_workerd_queue_consumer() {
    let Some(bin) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN");
        return;
    };
    let sql_bin = vrt_sql_bin();
    if !sql_bin.exists() {
        panic!(
            "vrt-sql binary missing at {}; run `cargo build -p vrt-sql` or set VRT_SQL_BIN",
            sql_bin.display()
        );
    }
    let dir = smoke_dir("qconsumer");
    let _dir_guard = SmokeDir(dir.clone());
    let sql_port = free_port();
    let _sql = Proc::new(spawn_vrt_sql(&dir, sql_port));
    assert!(wait_ready(sql_port), "vrt-sql never listened on {sql_port}");
    let queue_id_s = format!("{:x}", Sha256::digest(b"vrt-q:team-q:q-smoke"))[..26].to_string();
    let queue_id: &str = &queue_id_s;
    assert_eq!(queue_id.len(), 26);
    let port = free_port();
    let qport = free_port();
    let (cfg, argv_extra) =
        render_queue_cfg_full(&dir, port, queue_id, QCONSUMER_USER_MODULE, true, qport);
    let log = dir.join("workerd.log");
    // T37b-r: ONE File::create + try_clone — two File::create()s on the
    // same path would truncate each other (workerd.log came out empty).
    let out_file = std::fs::File::create(&log).unwrap();
    let err_file = out_file.try_clone().unwrap();
    let proc = Command::new(&bin)
        .arg("serve")
        .args(&argv_extra)
        .arg(format!("--external-addr=vrt-sql=127.0.0.1:{sql_port}"))
        .arg(&cfg)
        .current_dir(&dir)
        .process_group(0)
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .expect("spawn workerd");
    let _wd = Proc::logged(proc, "workerd", log.clone());
    for p in [port, qport] {
        if !wait_ready(p) {
            panic!(
                "workerd never listened on {p}:\n{}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
    }

    // T37b-r6：dispatch 必须走 vrt-host `/v1/queue/dispatch` 生产
    // 路由（真实 router/handler/响应编码 + 上游请求形状），不再
    // 直连 facade socket——上一跳正是 r6 缺陷藏身处。
    let disp_port = free_port();
    {
        let rt = vrt_host::supervisor::TenantRuntime::new("team-q");
        let mut eps = std::collections::BTreeMap::new();
        eps.insert(
            "wq".to_string(),
            vrt_host::endpoint::WorkerEndpoint::Tcp(format!("127.0.0.1:{qport}").parse().unwrap()),
        );
        rt.test_install_qconsumer(eps);
        let registry = vrt_host::supervisor::TenantRegistry::new();
        registry.insert("team-q".to_string(), rt);
        let app = vrt_host::queue_dispatch::router(registry);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let l = tokio::net::TcpListener::bind(("127.0.0.1", disp_port))
                    .await
                    .unwrap();
                axum::serve(l, app).await.unwrap();
            });
        });
        assert!(
            wait_ready(disp_port),
            "vrt-host dispatch router never listened"
        );
    }

    // 1. producer send → message lands in vrt-sql.
    let (st, body) = http_get(port, "/");
    let v: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("bad json {st}: {body}"));
    assert_eq!(
        st,
        200,
        "{body}\n-- workerd.log --\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert_eq!(v["sendOk"], json!(true), "{body}");

    // 2. claim the batch on the real vrt-sql wire (the dispatcher's
    //    first step — holder lease + message lease atomically).
    let claim = sql_queue_call(
        sql_port,
        1,
        &format!("/queues/{queue_id}/claim"),
        br#"{"holderId":"smoke-dispatch","holderTtlMs":15000,"max":10,"leaseTtlMs":60000}"#,
    );
    assert_eq!(claim["success"], json!(true), "{claim}");
    let msgs = claim["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1, "{claim}");
    let m = &msgs[0];
    assert_eq!(m["contentType"], json!("text"), "{claim}");
    assert_eq!(m["attempts"].as_i64(), Some(1), "{claim}");
    let dispatch_msg = |attempts: i64| {
        // vrt-host `/v1/queue/dispatch` envelope（tenantId/workerId +
        // opaque dispatch 载荷）——经生产路由转发到 facade。
        serde_json::json!({
            "tenantId": "team-q",
            "workerId": "wq",
            "dispatch": {
                "queue": "q-smoke",
                "messages": [{
                    "id": m["id"].as_str().unwrap(),
                    "contentType": "text",
                    "timestampMs": m["timestampMs"].as_i64().unwrap(),
                    "attempts": attempts,
                    "bodyHex": m["body"].as_str().unwrap(),
                }],
            },
        })
        .to_string()
    };

    // 3. attempt 1 → consumer retryAll → facade reports retry outcome.
    let (st, body) = http_post(disp_port, "/v1/queue/dispatch", &dispatch_msg(1));
    let r1: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("dispatch1 {st}: {body}"));
    assert_eq!(
        st,
        200,
        "dispatch1 → {st}: {body}\n-- workerd.log --\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    let wlog = || std::fs::read_to_string(&log).unwrap_or_default();
    assert_eq!(
        r1["retryBatch"]["retry"],
        json!(true),
        "{r1}\n-- workerd.log --\n{}",
        wlog()
    );
    assert_eq!(
        r1["ackAll"],
        json!(false),
        "{r1}\n-- workerd.log --\n{}",
        wlog()
    );

    // 4. attempt 2 (the dispatcher's redelivery) → ackAll.
    let (st, body) = http_post(disp_port, "/v1/queue/dispatch", &dispatch_msg(2));
    let r2: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("dispatch2 {st}: {body}"));
    assert_eq!(
        st,
        200,
        "dispatch2 → {st}: {body}\n-- workerd.log --\n{}",
        wlog()
    );
    assert_eq!(
        r2["ackAll"],
        json!(true),
        "{r2}\n-- workerd.log --\n{}",
        wlog()
    );

    // 5. inside the real handler: attempts=[1,2], queue name, text
    //    body decoded, timestamp a real Date — never null.
    let (st, body) = http_get(port, "/seen");
    let seen: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("seen {st}: {body}"));
    let arr = seen.as_array().unwrap();
    assert_eq!(arr.len(), 2, "{seen}\n-- workerd.log --\n{}", wlog());
    assert_eq!(arr[0]["attempts"], json!(1), "{seen}");
    assert_eq!(arr[1]["attempts"], json!(2), "{seen}");
    assert_eq!(arr[0]["queue"], json!("q-smoke"), "{seen}");
    assert_eq!(arr[0]["body"], json!("hello-queue"), "{seen}");
    assert_eq!(arr[0]["tsIsDate"], json!(true), "{seen}");

    // 6. settle the lease (the dispatcher's last step) — retry the
    //    message back so nothing dangles past the test.
    let settle = sql_queue_call(
        sql_port,
        2,
        &format!("/queues/{queue_id}/retry"),
        &serde_json::json!({
            "leaseId": claim["leaseId"].as_str().unwrap(),
            "items": [{"id": m["id"].as_str().unwrap(), "delaySecs": 0}],
        })
        .to_string()
        .into_bytes(),
    );
    assert_eq!(settle["success"], json!(true), "{settle}");
}

// ---------- P3c-1: Durable Objects (real workerd) ----------

/// DO worker module: Counter class exercises the FULL required surface
/// — storage.get/put, storage.sql.exec, transaction, alarm() handler,
/// blockConcurrencyWhile — behind one binding.
const DO_USER_MODULE: &str = r#"export class Counter {
  constructor(state, env) {
    this.state = state;
    this.ready = state.blockConcurrencyWhile(async () => {
      await state.storage.put('initialized', true);
    });
  }
  async fetch(req) {
    await this.ready;
    const url = new URL(req.url);
    const s = this.state.storage;
    if (url.pathname === '/inc') {
      const n = (await s.get('n') ?? 0) + 1;
      await s.put('n', n);
      s.sql.exec('CREATE TABLE IF NOT EXISTS t(k INTEGER)');
      s.sql.exec('INSERT INTO t VALUES (?)', n);
      return Response.json({ n });
    }
    if (url.pathname === '/get') {
      return Response.json({ n: (await s.get('n')) ?? 0 });
    }
    if (url.pathname === '/txn') {
      const out = await s.transaction(async () => {
        const n = (await s.get('t') ?? 0) + 1;
        await s.put('t', n);
        return n;
      });
      return Response.json({ t: out });
    }
    if (url.pathname === '/sql') {
      const rows = [...s.sql.exec('SELECT k FROM t ORDER BY k')];
      return Response.json({ rows: rows.map(r => r.k) });
    }
    if (url.pathname === '/fill') {
      // WAL1-r4 PoC: n commits of sz-byte blobs — drives the DO's
      // own wal through auto-checkpoint generations.
      const n = parseInt(url.searchParams.get('n') ?? '100', 10);
      const sz = parseInt(url.searchParams.get('sz') ?? '4096', 10);
      s.sql.exec('CREATE TABLE IF NOT EXISTS blobt(b BLOB)');
      for (let i = 0; i < n; i++) s.sql.exec('INSERT INTO blobt VALUES (randomblob(?))', sz);
      return Response.json({ n, sz });
    }
    if (url.pathname === '/pragma') {
      // WAL1-r4 PoC: arbitrary pragmas against the DO's own sqlite.
      const q = url.searchParams.get('q');
      try {
        const rows = [...s.sql.exec(q)];
        return Response.json({ rows });
      } catch (e) {
        return Response.json({ error: String(e) });
      }
    }
    if (url.pathname === '/alarm') {
      const ms = parseInt(url.searchParams.get('ms') ?? '1500', 10);
      await s.setAlarm(Date.now() + ms);
      return Response.json({ ok: true });
    }
    if (url.pathname === '/alarm-fired') {
      return Response.json({ fired: (await s.get('alarmFired')) ?? 0 });
    }
    return new Response('nf', { status: 404 });
  }
  async alarm() {
    const n = ((await this.state.storage.get('alarmFired')) ?? 0) + 1;
    await this.state.storage.put('alarmFired', n);
  }
}
export default {
  async fetch(req, env) {
    const stub = env.COUNTER.get(env.COUNTER.idFromName('one'));
    return stub.fetch(req);
  }
}
"#;

/// Render a DO-capable tenant config: one worker declaring Counter
/// (enableSql) + a durableObject binding, `do_storage_dir` under `dir`.
fn render_do_cfg(dir: &Path, port: u16) -> (PathBuf, Vec<String>, PathBuf, String) {
    let worker_id = "wdo";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": DO_USER_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let do_root = dir.join("do-store");
    std::fs::create_dir_all(do_root.join("team-do").join(worker_id)).unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-do",
        "workers": [{
            "workerId": worker_id,
            "name": "dow",
            "activeVersion": {
                "versionId": "v-do-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "durableObjectClasses": [{
                "className": "Counter",
                "uniqueKey": "c".repeat(64),
                "enableSql": true
            }],
            "bindings": [{
                "type": "durableObject", "name": "COUNTER", "className": "Counter"
            }],
            "routes": [{"hostname": "do--team-do.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let manifest = parse_facade_manifest(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    let sock_dir = dir.join("sock");
    std::fs::create_dir_all(&sock_dir).unwrap();
    let params = RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy::default(),
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: do_root.join("team-do").display().to_string(),
        wfop_pepper: "d1-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    })
    .unwrap();
    // DO emission landed: namespace + localDisk + per-worker disk svc.
    assert!(out.text.contains("durableObjectNamespaces"), "{}", out.text);
    assert!(
        out.text.contains("durableObjectStorage = (localDisk ="),
        "{}",
        out.text
    );
    assert!(out.text.contains("writable = true"), "{}", out.text);
    assert!(
        out.text.contains(&do_root.display().to_string()),
        "{}",
        out.text
    );

    let manifest_doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap();
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap();
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                std::fs::write(&dst, std::fs::read(&src).unwrap()).unwrap();
            }
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, DO_USER_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { ref name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                assert_eq!(
                    format!("{:x}", Sha256::digest(bytes)),
                    embed.digest,
                    "vendored embed {name} digest drift"
                );
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }
    let text = out.text.replace(
        &format!("unix:sock/{worker_id}.sock"),
        &format!("127.0.0.1:{port}"),
    );
    let cfg = dir.join("cfg.capnp");
    std::fs::write(&cfg, text).unwrap();
    // argv from the PRODUCTION derivation (T31g/H4): durableObjectClasses
    // present ⇒ --experimental must be on the command line.
    let mut argv = vrt_host::assemble::experimental_extra_args(
        &params.system_compat_flags,
        tenant.workers.iter().any(|w| !w.queue_consumers.is_empty()),
        tenant
            .workers
            .iter()
            .any(|w| !w.durable_object_classes.is_empty()),
        false,
    );
    assert!(argv.iter().any(|a| a == "--experimental"), "{argv:?}");
    // P3c-1-r2: `--external-addr` comes from the SAME production
    // derivation the supervisor uses — `assemble` copies
    // `rendered.external_addrs` into `TenantLaunchSpec` (assemble.rs)
    // and `ProcessWorkerdLauncher::argv` formats it through
    // `external_addr_args` (launch.rs). Never hand-format the flag.
    argv.extend(vrt_host::launch::external_addr_args(&out.external_addrs));
    // Binary-independent invariant: EVERY `external = (…)` service in
    // the rendered text must have a production-derived address, or
    // workerd refuses the config (`has no address in the config`) in
    // environments the skip gate used to hide.
    // `(name = "X", external = (` — the service name sits in the same
    // tuple immediately before the keyword, so the closest preceding
    // `(name = "` is the external's own name.
    let declared: Vec<String> = out
        .text
        .match_indices("external = (")
        .map(|(i, _)| {
            let prefix = &out.text[..i];
            let start = prefix
                .rfind("(name = \"")
                .map(|i| i + "(name = \"".len())
                .expect("external service tuple without name");
            let rest = &prefix[start..];
            rest[..rest.find('"').unwrap()].to_string()
        })
        .collect();
    for service in &declared {
        assert!(
            out.external_addrs.iter().any(|a| a.service == *service),
            "external service {service:?} has no --external-addr: {:?}",
            out.external_addrs
        );
    }
    // Parse the localDisk path workerd will ACTUALLY use out of the
    // rendered text — never re-derive it by hand downstream, so an
    // emit-layout change breaks here instead of drifting silently.
    let disk_dir = {
        let marker = "disk = (path = \"";
        let i = out.text.find(marker).expect("no localDisk service in cfg");
        let rest = &out.text[i + marker.len()..];
        rest[..rest.find('"').unwrap()].to_string()
    };
    (cfg, argv, do_root, disk_dir)
}

fn spawn_workerd(
    bin: &Path,
    cfg: &Path,
    argv: &[String],
    dir: &Path,
    log: &Path,
) -> std::io::Result<Proc> {
    let out_file = std::fs::File::create(log).unwrap();
    let err_file = out_file.try_clone().unwrap();
    Ok(Proc::logged(
        Command::new(bin)
            .arg("serve")
            .args(argv)
            .arg(cfg)
            .current_dir(dir)
            .process_group(0)
            .stdout(Stdio::from(out_file))
            .stderr(Stdio::from(err_file))
            .spawn()?,
        "workerd",
        log.to_path_buf(),
    ))
}

#[test]
fn real_workerd_durable_object() {
    // Render BEFORE the binary gate: capnp/path validation (the part
    // that panicked on `..` in P3c-1-r1) runs even when workerd is
    // absent, so a broken emit never hides behind the skip.
    let dir = smoke_dir("do");
    let _dir_guard = SmokeDir(dir.clone());
    let port = free_port();
    let (cfg, argv, do_root, disk_dir) = render_do_cfg(&dir, port);
    let Some(bin) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN (config render already exercised)");
        return;
    };
    let log = dir.join("workerd.log");
    // Sandbox EPERM (binary present but exec denied) is a skip —
    // every other spawn failure panics loudly.
    let spawn = |log: &Path| match spawn_workerd(&bin, &cfg, &argv, &dir, log) {
        Ok(p) => Some(p),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: workerd exec denied ({e}) — sandboxed run");
            None
        }
        Err(e) => panic!("spawn workerd: {e}"),
    };

    // Boot 1: exercise the storage surface, persist, arm alarm, kill.
    {
        let Some(_wd) = spawn(&log) else { return };
        assert!(
            wait_ready(port),
            "workerd never listened:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        let (st, body) = http_get(port, "/inc");
        assert_eq!(st, 200, "{body}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["n"],
            1
        );
        let (_, body) = http_get(port, "/txn");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["t"],
            1
        );
        let (_, body) = http_get(port, "/sql");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["rows"],
            json!([1])
        );
        // Alarm armed just before the kill — workerd must RELOAD it
        // from metadata.sqlite and fire it after restart.
        let (st, body) = http_get(port, "/alarm?ms=800");
        assert_eq!(st, 200, "{body}");
    } // _wd drops → SIGKILL

    // Boot 2: same storage dir — object state AND the armed alarm survive.
    {
        let Some(_wd) = spawn(&log) else { return };
        assert!(
            wait_ready(port),
            "workerd never re-listened:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        let (_, body) = http_get(port, "/get");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["n"],
            1,
            "DO storage did not persist across workerd restart"
        );
        // The 800ms alarm armed in boot 1 fires here (built-in
        // AlarmScheduler reloads _cf_ALARM on startup).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let (_, body) = http_get(port, "/alarm-fired");
            let fired = serde_json::from_str::<serde_json::Value>(&body)
                .unwrap_or(json!({"fired":0}))["fired"]
                .as_i64()
                .unwrap_or(0);
            if fired >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "armed alarm never fired after restart"
            );
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
    // localDisk layout (parsed from the rendered capnp above):
    // workerd writes <doStorageDir>/<workerId>/<uniqueKey>/… — the
    // disk service path is <doStorageDir>/<workerId> = disk_dir.
    assert!(
        std::path::Path::new(&disk_dir)
            .join("c".repeat(64))
            .exists(),
        "no class dir under {} (capnp disk path = {})",
        disk_dir,
        disk_dir
    );
    // No cross-tenant writes: do_root holds ONLY the tenant dir, and
    // the tenant dir holds ONLY this worker's storage dir.
    let top: Vec<String> = std::fs::read_dir(&do_root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(top, ["team-do"], "unexpected dirs under do_root: {top:?}");
    let tenant_dirs: Vec<String> = std::fs::read_dir(do_root.join("team-do"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    let worker_dir = std::path::Path::new(&disk_dir)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        tenant_dirs,
        [worker_dir],
        "unexpected dirs under do_root/team-do: {tenant_dirs:?}"
    );
}

/// WAL1-r4 PoC on REAL workerd: DO sqlite's `journal_size_limit`
/// must be the SQLite default (-1 → a wal RESTART never truncates
/// the file), and a restart must leave old-generation bytes behind
/// the new valid region. This is the fact the frontier rule in
/// `do_wal.rs::tail_db` is built on.
#[test]
fn real_workerd_do_wal_restart_residual() {
    let dir = smoke_dir("walpoc");
    let _dir_guard = SmokeDir(dir.clone());
    let port = free_port();
    let (cfg, argv, _do_root, disk_dir) = render_do_cfg(&dir, port);
    let Some(bin) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN");
        return;
    };
    let log = dir.join("workerd.log");
    let Some(_wd) = (match spawn_workerd(&bin, &cfg, &argv, &dir, &log) {
        Ok(p) => Some(p),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: workerd exec denied ({e})");
            None
        }
        Err(e) => panic!("spawn workerd: {e}"),
    }) else {
        return;
    };
    assert!(
        wait_ready(port),
        "workerd never listened:\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );

    let wal_path = || -> PathBuf {
        fn walk(d: &Path, out: &mut Vec<PathBuf>) {
            for e in std::fs::read_dir(d).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.to_string_lossy().ends_with(".sqlite-wal") {
                    out.push(p);
                }
            }
        }
        let mut found = vec![];
        walk(Path::new(&disk_dir), &mut found);
        found
            .first()
            .cloned()
            .unwrap_or_else(|| panic!("no -wal under {disk_dir}"))
    };
    // Minimal percent-encoding for the pragma query (space & parens).
    let enc = |q: &str| -> String {
        q.replace(' ', "%20")
            .replace('(', "%28")
            .replace(')', "%29")
    };
    let pragma = |q: &str| -> serde_json::Value {
        let (st, body) = http_get(port, &format!("/pragma?q={}", enc(q)));
        assert_eq!(st, 200, "{body}");
        serde_json::from_str(&body).unwrap()
    };

    // (1) journal_size_limit: workerd's DO sql authorizer blocks
    // the pragma — record that, then prove the CLAIM it guards
    // directly from the on-disk wal file instead.
    let j = pragma("PRAGMA journal_size_limit");
    eprintln!("journal_size_limit -> {j}");

    // (2) Drive real generations: fill past the 1000-page auto-
    // checkpoint threshold, then a short write — the restart write
    // overwrites the header (salt1+1) but NOT the file; old-gen
    // bytes must remain behind the new valid region.
    let (st, b) = http_get(port, "/fill?n=1400&sz=4096");
    assert_eq!(st, 200, "{b}");
    let (st, b) = http_get(port, "/fill?n=40&sz=512");
    assert_eq!(st, 200, "{b}");
    let (st, b) = http_get(port, "/inc");
    assert_eq!(st, 200, "{b}");

    let walp = wal_path();
    let f = std::fs::read(&walp).unwrap();
    assert!(
        f.len() > 4096 + 32,
        "wal must hold residue (len={})",
        f.len()
    );
    let hdr = vrt_sql::wal::wal_header(&f[..32]).expect("wal header");
    let salt = (hdr.salt1, hdr.salt2);
    let ck0 = (
        u32::from_be_bytes(f[24..28].try_into().unwrap()),
        u32::from_be_bytes(f[28..32].try_into().unwrap()),
    );
    // Scan the CURRENT generation from offset 32 — identical to
    // the tailer's frontier logic. The valid region must end
    // BEFORE the file end: the byte-range after it is residue.
    let (vend, heads, _ck) =
        vrt_sql::wal::scan_frames(&f[32..], 0, hdr.page_size(), hdr.cksum_be(), salt, ck0);
    assert!(!heads.is_empty(), "current generation must have frames");
    let valid_end = 32 + vend;
    assert!(
        valid_end < f.len() as u64,
        "expected old-generation residue after valid end {valid_end} (len={})",
        f.len()
    );
    // The frame at the residue boundary must carry a DIFFERENT salt.
    let resid_salt1 = u32::from_be_bytes(
        f[valid_end as usize..valid_end as usize + 4]
            .try_into()
            .unwrap(),
    );
    assert_ne!(
        resid_salt1, salt.0,
        "residual frame must be an older generation (different salt1)"
    );
    eprintln!(
        "workerd wal: len={} valid_end={} residue={}B salt1={:#x} resid_salt1={:#x}",
        f.len(),
        valid_end,
        f.len() as u64 - valid_end,
        salt.0,
        resid_salt1
    );
}
