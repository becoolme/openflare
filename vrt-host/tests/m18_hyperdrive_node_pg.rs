//! MIG-18: the REAL `node-postgres` driver (`pg@8.16.3`) over the
//! Hyperdrive binding on real workerd — same chain as MIG-17
//! (`cloudflare:sockets` → egress `VRT-PG1` preamble → vrt-provider
//! `PgProxy` → throwaway PG16), but the tenant worker is an esbuild
//! bundle of npm `pg` exactly the way whois-api runs it:
//!
//!   new Pool({ connectionString: env.HD.connectionString, max: 1,
//!              idleTimeoutMillis: 1000, connectionTimeoutMillis: 3000,
//!              query_timeout: 3000 })   …   pool.end()
//!
//! MIG-17's worker spoke a hand-written pg-wire client; this proof
//! exercises the compatibility surface only a real driver stresses:
//! `pg-cloudflare`'s `connect("host:port")` string authority through
//! the egress `internalTcpTargets` match, `connectionString` parsing,
//! `Pool` acquire/release/`end()` (Terminate `X` per client), the
//! extended query protocol, ErrorResponse propagation and per-request
//! pool lifecycle under `nodejs_compat` +
//! `streams_enable_constructors`.
//!
//! The bundle is built AT TEST TIME by the repo's own
//! `node_modules/.pnpm/esbuild@0.25.12` + `pg@8.16.3` (no network):
//! `platform=neutral`, `conditions=workerd,worker,browser`, externals
//! `cloudflare:*` + `node:*`, and a banner that supplies the CJS
//! globals wrangler injects under `nodejs_compat` (`require`,
//! `Buffer`, `process`, `global`, `setImmediate`).
//!
//! Gated like MIG-17 — `VRT_WORKERD_BIN`, a PostgreSQL 16 bin dir
//! (`VRT_TEST_PG_BIN`), plus node + the pnpm packages; any absence
//! prints SKIP with the reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test m18_hyperdrive_node_pg -- --ignored --nocapture

mod common;

use common::hyperdrive::*;

use std::path::{Path, PathBuf};
use std::process::Command;

/// `2022-11-29` precedes both enable dates (`nodejs_compat`
/// 2026-08-04, `streams_enable_constructors` 2022-11-30), so
/// `flags_for_compat_date` emits BOTH flags into the workerd config
/// instead of stripping them as redundant — the assertion below checks
/// they land in the render.
const SPEC: HdSpec = HdSpec {
    tenant: "team-m18",
    cell: "cell-m18",
    worker: "wm18hd",
    compat_date: "2022-11-29",
    compat_flags: &["nodejs_compat", "streams_enable_constructors"],
};

/// `HD_OK` → good upstream credential; `HD_BADPW` → wrong password;
/// `HD_UNKNOWN` → rendered into the worker but absent from the
/// proxy's signed desired (fail-closed `PG_BINDING_UNKNOWN`).
const BINDINGS: [&str; 3] = ["HD_OK", "HD_BADPW", "HD_UNKNOWN"];
const OMIT: [&str; 1] = ["HD_UNKNOWN"];
const BAD_PASS: &str = "definitely-wrong-password";

// -------------------------------------------------------- the pg worker

/// Tenant worker entry source — bundled at test time. Mirrors
/// whois-api's `Pool` options and `pool.end()`; every request shape is
/// bounded by a 20 s race so a socket stall surfaces as
/// `{error:"timeout"}` instead of a hung test. Endpoints:
///   /q?b=<B>&sql=<sql>[&p=a,b]  fresh Pool → query → pool.end()
///   /qshared?b=<B>&sql=<sql>    module-level Pool reused across
///                               requests (rebuilt once on error)
///   /endshared                  pool.end() the module-level Pool
///   /tx?b=<B>                   BEGIN/INSERT/COMMIT then read back
///   /errrecover?b=<B>           SELECT 1/0 then SELECT 2 on one Pool
const ENTRY: &str = r#"import pg from "pg";
const { Pool } = pg;

const OPTS = {
  max: 1,
  idleTimeoutMillis: 1000,
  connectionTimeoutMillis: 3000,
  query_timeout: 3000,
};

let shared = null;
function sharedPool(cs) {
  if (!shared) {
    shared = new Pool({ connectionString: cs, ...OPTS });
    shared.on("error", () => {});
  }
  return shared;
}

async function withPool(cs, fn) {
  const pool = new Pool({ connectionString: cs, ...OPTS });
  pool.on("error", () => {});
  try {
    return await fn(pool);
  } finally {
    const t0 = Date.now();
    const ok = await Promise.race([
      pool.end().then(() => true),
      new Promise((r) => setTimeout(() => r(false), 2000)),
    ]).catch(() => false);
    endStats.total += 1;
    if (!ok) endStats.timedOut += 1;
    endStats.maxMs = Math.max(endStats.maxMs, Date.now() - t0);
  }
}

const endStats = { total: 0, timedOut: 0, maxMs: 0 };

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const hd = env[u.searchParams.get("b") || "HD_OK"];
    const cs = hd && hd.connectionString;
    try {
      if (u.pathname === "/q") {
        const sql = u.searchParams.get("sql") || "SELECT 1 AS one";
        const p = u.searchParams.get("p");
        const args = p === null ? [] : p.split(",");
        const out = await withPool(cs, async (pool) => {
          const r = await pool.query(sql, args);
          return { rows: r.rows };
        });
        return Response.json(out);
      }
      if (u.pathname === "/qshared") {
        const sql = u.searchParams.get("sql") || "SELECT 1 AS one";
        try {
          const r = await sharedPool(cs).query(sql);
          return Response.json({ rows: r.rows, recreated: false });
        } catch (e) {
          const old = shared;
          shared = null;
          if (old) await Promise.race([old.end(), new Promise((r) => setTimeout(r, 2000))]).catch(() => {});
          const r = await sharedPool(cs).query(sql);
          return Response.json({ rows: r.rows, recreated: true });
        }
      }
      if (u.pathname === "/endstats") {
        return Response.json(endStats);
      }
      if (u.pathname === "/endshared") {
        const old = shared;
        shared = null;
        if (old) await Promise.race([old.end(), new Promise((r) => setTimeout(r, 2000))]).catch(() => {});
        return Response.json({ ended: true });
      }
      if (u.pathname === "/tx") {
        const out = await withPool(cs, async (pool) => {
          await pool.query("CREATE TABLE IF NOT EXISTS m18_tx (id int)");
          const c = await pool.connect();
          try {
            await c.query("BEGIN");
            await c.query("INSERT INTO m18_tx VALUES ($1)", [41]);
            await c.query("COMMIT");
          } finally {
            c.release();
          }
          const r = await pool.query("SELECT id FROM m18_tx");
          return { rows: r.rows };
        });
        return Response.json(out);
      }
      if (u.pathname === "/errrecover") {
        const out = await withPool(cs, async (pool) => {
          let code = null;
          try {
            await pool.query("SELECT 1/0");
          } catch (e) {
            code = e.code;
          }
          const r = await pool.query("SELECT 2 AS two");
          return { code, rows: r.rows };
        });
        return Response.json(out);
      }
      return new Response("m18", { status: 404 });
    } catch (e) {
      return Response.json({ error: { message: String((e && e.message) || e), code: e && e.code } });
    }
  },
};
"#;

/// CJS globals a nodejs_compat bundle expects (what wrangler injects):
/// `require` backed by `node:module`'s createRequire — workerd resolves
/// `node:*` specifiers through it — plus `Buffer`, `process`, `global`
/// (pg reads `global.Promise`) and `setImmediate`.
const BANNER: &str = concat!(
    "import { createRequire as __cr } from \"node:module\";",
    "import { Buffer as __B } from \"node:buffer\";",
    "import __proc from \"node:process\";",
    "globalThis.require ??= __cr(\"/worker.mjs\");",
    "globalThis.Buffer ??= __B;",
    "globalThis.process ??= __proc;",
    "globalThis.global ??= globalThis;",
    "try { globalThis.process.env ??= {}; } catch {}",
    "globalThis.setImmediate ??= (f, ...a) => setTimeout(f, 0, ...a);",
    "globalThis.clearImmediate ??= clearTimeout;"
);

// --------------------------------------------------------- bundle build

fn node_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("VRT_TEST_NODE_BIN") {
        if !p.is_empty() {
            return Some(PathBuf::from(p));
        }
    }
    Command::new("node")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| PathBuf::from("node"))
}

fn pnpm_dir() -> PathBuf {
    repo_root().join("node_modules/.pnpm")
}

fn esbuild_js() -> Option<PathBuf> {
    let p = pnpm_dir().join("esbuild@0.25.12/node_modules/esbuild/bin/esbuild");
    p.is_file().then_some(p)
}

fn pg_pkg_dir() -> Option<PathBuf> {
    let p = pnpm_dir().join("pg@8.16.3/node_modules/pg");
    p.join("package.json").is_file().then_some(p)
}

/// `pg-cloudflare` is the sockets backend pg selects under workerd —
/// the bundle is meaningless without it, so check it too.
fn pg_cloudflare_dir() -> Option<PathBuf> {
    let p = pnpm_dir().join("pg-cloudflare@1.4.0/node_modules/pg-cloudflare");
    p.join("package.json").is_file().then_some(p)
}

/// `node <esbuild>/bin/esbuild entry.mjs --bundle …` — same flags
/// wrangler uses for nodejs_compat deploys (neutral platform, workerd
/// conditions, node:/cloudflare: externals) plus bare-builtin aliases
/// so CJS `require("events")` lands on `node:events`.
fn build_worker_bundle(dir: &Path, node: &Path, esbuild: &Path, pg_dir: &Path) -> String {
    let entry = dir.join("entry.mjs");
    std::fs::write(&entry, ENTRY).unwrap();
    let out = dir.join("worker-bundle.mjs");
    let builtins = [
        "events",
        "net",
        "tls",
        "crypto",
        "stream",
        "util",
        "buffer",
        "path",
        "fs",
        "dns",
        "url",
        "string_decoder",
    ];
    let mut args: Vec<String> = vec![
        entry.display().to_string(),
        "--bundle".into(),
        "--format=esm".into(),
        "--platform=neutral".into(),
        "--conditions=workerd,worker,browser".into(),
        "--main-fields=module,main".into(),
        "--external:cloudflare:*".into(),
        "--external:node:*".into(),
        format!("--alias:pg={}", pg_dir.display()),
    ];
    for b in builtins {
        args.push(format!("--alias:{b}=node:{b}"));
    }
    args.push(format!("--banner:js={BANNER}"));
    args.push(format!("--outfile={}", out.display()));
    // On install esbuild replaces `bin/esbuild` with the native
    // executable (macOS/Linux); only the JS shim needs `node`.
    let is_js = std::fs::read(esbuild)
        .map(|b| b.starts_with(b"#!") || b.starts_with(b"\"use strict\""))
        .unwrap_or(false);
    let mut cmd = if is_js {
        let mut c = Command::new(node);
        c.arg(esbuild);
        c
    } else {
        Command::new(esbuild)
    };
    let o = cmd
        .args(&args)
        .output()
        .expect("spawn esbuild");
    assert!(
        o.status.success(),
        "esbuild failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    let src = std::fs::read_to_string(&out).expect("bundle output");
    // Cloudflare sockets must stay a real external import, and the
    // pg-cloudflare backend must have been bundled in.
    assert!(
        src.contains("cloudflare:sockets"),
        "bundle lost the cloudflare:sockets import"
    );
    src
}

// ----------------------------------------------------------- HTTP front

fn q(port: u16, binding: &str, sql: &str, params: Option<&str>) -> serde_json::Value {
    let mut path = format!("/q?b={binding}&sql={}", pct(sql));
    if let Some(p) = params {
        path.push_str(&format!("&p={}", pct(p)));
    }
    http_json(port, &path)
}

fn qshared(port: u16, binding: &str, sql: &str) -> serde_json::Value {
    http_json(port, &format!("/qshared?b={binding}&sql={}", pct(sql)))
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN, a PostgreSQL 16 bin dir, node + repo esbuild/pg"]
fn mig18_hyperdrive_node_pg_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real-workerd test not run");
        return;
    };
    let Some(node) = node_bin() else {
        eprintln!("SKIP: no node binary (set VRT_TEST_NODE_BIN) — bundle build not run");
        return;
    };
    let (Some(esbuild), Some(pg_dir), Some(_pgcf)) =
        (esbuild_js(), pg_pkg_dir(), pg_cloudflare_dir())
    else {
        eprintln!(
            "SKIP: repo esbuild@0.25.12/pg@8.16.3/pg-cloudflare@1.4.0 missing — bundle build not run"
        );
        return;
    };
    let Some(pg_bin) = pg_bin_dir() else {
        eprintln!(
            "SKIP: no initdb/pg_ctl (set VRT_TEST_PG_BIN or install postgresql@16) — real-PG test not run"
        );
        return;
    };
    let Some(dir) = smoke_root("vrt-m18") else {
        return;
    };

    let module_src = build_worker_bundle(&dir, &node, &esbuild, &pg_dir);

    let Some(pg) = start_temp_pg(pg_bin, "vrt-m18pg") else {
        return;
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let creds_path = write_creds(
        &dir,
        &SPEC,
        pg.port,
        &[
            ("HD_OK", PgCred::Tcp(PG_PASS.into())),
            ("HD_BADPW", PgCred::Tcp(BAD_PASS.into())),
            // HD_UNKNOWN: intentionally no credential AND omitted from
            // the proxy snapshot below.
        ],
    );
    let digest = user_bundle(&SPEC, &module_src).digest().to_string();
    let proxy_port = spawn_pgproxy(&rt, &dir, &SPEC, &digest, &BINDINGS, &OMIT, creds_path);

    assert_proxy_baseline(&rt, proxy_port, &synthetic_host(&SPEC, "HD_OK"));

    let http_port = free_port();
    let cfg = render_and_write(&dir, http_port, proxy_port, &SPEC, &module_src, &BINDINGS);
    {
        let text = std::fs::read_to_string(&cfg).unwrap();
        for flag in SPEC.compat_flags {
            assert!(
                text.contains(&format!("\"{flag}\"")),
                "rendered config missing compat flag {flag}:\n{text}"
            );
        }
    }
    let proc = spawn(&bin, &cfg, &dir);
    if !wait_ready(http_port) {
        panic!("workerd never listened\n-- log --\n{}", log_text(&proc));
    }
    let ctx = || {
        let prom: String = vrt_provider::metrics::registry()
            .render()
            .lines()
            .filter(|l| l.contains("pgproxy"))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "\n-- workerd log --\n{}\n-- pgproxy metrics --\n{prom}",
            log_text(&proc)
        )
    };

    // 1. SELECT 1 — real driver, whole chain.
    let v = q(http_port, "HD_OK", "SELECT 1 AS one", None);
    assert_eq!(v["rows"][0]["one"], 1, "SELECT 1: {v}{}", ctx());

    // 1b. Upstream identity is the provider-held login, never the
    //     isolate-visible `vrt-hd` placeholder in connectionString.
    let v = q(http_port, "HD_OK", "SELECT current_user", None);
    assert_eq!(
        v["rows"][0]["current_user"], PG_SUPER,
        "upstream identity: {v}"
    );

    // 1c. Parameterized query — the driver's extended protocol
    //     (Parse/Bind/Describe/Execute/Sync) through the relay.
    let v = q(
        http_port,
        "HD_OK",
        "SELECT $1::int + $2::int AS sum",
        Some("20,22"),
    );
    assert_eq!(v["rows"][0]["sum"], 42, "parameterized: {v}{}", ctx());

    // 1d. A transaction actually commits on the provider-held login.
    let v = http_json(http_port, "/tx?b=HD_OK");
    assert_eq!(
        v["rows"][0]["id"],
        41,
        "transaction read-back: {v}{}",
        ctx()
    );

    // 1e. Server SQL error → typed fields, and the SAME pool keeps
    //     working afterwards.
    let v = http_json(http_port, "/errrecover?b=HD_OK");
    assert_eq!(v["code"], "22012", "division_by_zero code: {v}{}", ctx());
    assert_eq!(
        v["rows"][0]["two"],
        2,
        "pool usable after error: {v}{}",
        ctx()
    );

    // 2. Twenty sequential requests on fresh Pools (each pool.end()) —
    //    the per-request lifecycle whois-api uses.
    for i in 0..20 {
        let v = q(http_port, "HD_OK", "SELECT 2 AS two", None);
        assert_eq!(v["rows"][0]["two"], 2, "fresh-pool iter {i}: {v}{}", ctx());
    }

    // 2b. A module-level Pool does NOT survive across requests: workerd
    //     (like Cloudflare) binds a socket's I/O to the request that
    //     opened it, so the second request's query on the cached client
    //     stalls until `query_timeout` (3 s). The worker must rebuild
    //     the Pool and still succeed; this pins the documented "create
    //     the client per request" contract instead of hiding a 3 s stall
    //     per request in a loop.
    let v = qshared(http_port, "HD_OK", "SELECT 3 AS three");
    assert_eq!(v["rows"][0]["three"], 3, "shared-pool first: {v}{}", ctx());
    assert_eq!(v["recreated"], false, "shared-pool first: {v}");
    let v = qshared(http_port, "HD_OK", "SELECT 3 AS three");
    assert_eq!(v["rows"][0]["three"], 3, "shared-pool reuse: {v}{}", ctx());
    assert_eq!(
        v["recreated"], true,
        "cross-request socket reuse unexpectedly worked — revisit the per-request contract: {v}"
    );
    let v = http_json(http_port, "/endstats");
    eprintln!("pool.end stats: {v}");
    assert_eq!(
        v["timedOut"], 0,
        "pool.end() must complete (Terminate + socket close), not hit the 2 s fallback: {v}{}",
        ctx()
    );
    let v = http_json(http_port, "/endshared");
    assert_eq!(v["ended"], true, "shared pool end: {v}{}", ctx());

    // 3. Wrong upstream password → typed refusal within 3 s, and the
    //    worker's response carries neither the real password nor the
    //    injected wrong one.
    let t0 = std::time::Instant::now();
    let v = q(http_port, "HD_BADPW", "SELECT 1 AS one", None);
    let elapsed = t0.elapsed();
    assert!(
        err_text(&v).contains("PG_UPSTREAM_AUTH"),
        "wrong password must be typed PG_UPSTREAM_AUTH: {v}{}",
        ctx()
    );
    assert!(
        elapsed.as_secs() < 3,
        "must fail within 3 s (took {elapsed:?}), not hang"
    );
    let body = serde_json::to_string(&v).unwrap();
    assert!(!body.contains(BAD_PASS), "no wrong-password echo: {v}");
    assert!(!body.contains(PG_PASS), "no real-password echo: {v}");

    // 4. A binding the proxy does not know still fails closed through
    //    the real driver.
    let v = q(http_port, "HD_UNKNOWN", "SELECT 1 AS one", None);
    assert!(
        v.get("rows").is_none(),
        "HD_UNKNOWN must not return rows: {v}"
    );
    assert!(
        err_text(&v).contains("PG_BINDING_UNKNOWN"),
        "HD_UNKNOWN must fail closed PG_BINDING_UNKNOWN: {v}{}",
        ctx()
    );

    // 5. Every session released: the active gauge drains to 0.
    let mut active = active_sessions();
    for _ in 0..100 {
        if active == 0.0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        active = active_sessions();
    }
    assert_eq!(active, 0.0, "pgproxy sessions leaked{}", ctx());

    drop(proc);
    drop(rt); // aborts the serve task
    drop(pg);
}
