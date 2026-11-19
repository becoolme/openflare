//! MIG-17: real-workerd + real-PostgreSQL proof of the Hyperdrive binding.
//!
//! Whole chain on real binaries (no fakes in the data path):
//!   tenant worker `env.HD.host/port` → `cloudflare:sockets` connect() →
//!   vrt-egress (`internalTcpTargets`, injects `VRT-PG1 <label>\n` BEFORE
//!   any tenant byte) → `INTERNAL_NET` → vrt-provider `PgProxy` (routes
//!   from a SIGNED `compute.desired` snapshot + a 0600 credential file)
//!   → SCRAM-SHA-256 login with provider-held credentials → a temporary
//!   `initdb`/`pg_ctl` PostgreSQL 16 started by this test.
//!
//! The worker speaks a minimal pg-wire v3 client (startup, simple Query,
//! extended Parse/Bind/Describe/Execute/Sync) written inline — no npm
//! dependency.
//!
//! Covered: `SELECT 1`; parameterized query; upstream identity is the
//! provider-held login (not the isolate placeholder); wrong upstream
//! password → typed `PG_UPSTREAM_AUTH` (no hang); label absent from
//! desired / credential absent / `tcpBlocked` → fail closed
//! `PG_BINDING_UNKNOWN`; `replication` startup → `PG_STARTUP_UNSUPPORTED`;
//! all pgproxy sessions released at the end
//! (`vrt_provider_pgproxy_sessions_active` → 0).
//!
//! Not exercised: `PG_MIGRATION_READ_ONLY` as a wire code — `routes()`
//! drops a `tcpBlocked` credential before admission, so the observable
//! behaviour is `PG_BINDING_UNKNOWN` (asserted below); the dedicated code
//! has its own unit test in vrt-provider.
//!
//! Gated: `VRT_WORKERD_BIN` (real workerd) AND a PostgreSQL 16 bin dir
//! (`VRT_TEST_PG_BIN`, default `/opt/homebrew/opt/postgresql@16/bin`) —
//! either absent → explicit SKIP with the reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test m17_hyperdrive_workerd -- --ignored --nocapture
//!
//! MIG-18: all fixtures live in `tests/common/hyperdrive.rs` (behaviour
//! unchanged); this file keeps only the m17 worker source, the `/q`
//! contract and the assertions.

mod common;

use common::hyperdrive::*;

use serde_json::json;

const SPEC: HdSpec = HdSpec {
    tenant: "team-m17",
    cell: "cell-m17",
    worker: "wm17hd",
    compat_date: "2026-09-08",
    compat_flags: &[],
};

// ------------------------------------------------------------ the worker

/// Tenant worker with a minimal pg-wire v3 client.
///   /q?b=<BINDING>&sql=<sql>[&p=a,b,c][&repl=1]
/// → JSON `{rows:[[...]]}` or `{error:{code?,message}}`. Every socket op
/// is bounded by a 15 s timer so a hang surfaces as `{error:"timeout"}`.
const USER_MODULE: &str = r#"import { connect } from "cloudflare:sockets";
const enc = new TextEncoder();
const dec = new TextDecoder();
const cat = (...a) => {
  const out = new Uint8Array(a.reduce((n, x) => n + x.length, 0));
  let o = 0;
  for (const x of a) { out.set(x, o); o += x.length; }
  return out;
};
const i32 = (n) => { const b = new Uint8Array(4); new DataView(b.buffer).setInt32(0, n); return b; };
const i16 = (n) => { const b = new Uint8Array(2); new DataView(b.buffer).setInt16(0, n); return b; };
const cstr = (s) => cat(enc.encode(s), new Uint8Array([0]));
const frame = (t, body) => cat(new Uint8Array([t.charCodeAt(0)]), i32(body.length + 4), body);

class Rd {
  constructor(sock) { this.r = sock.readable.getReader(); this.buf = new Uint8Array(0); }
  async fill() {
    const { value, done } = await this.r.read();
    if (done) throw new Error("connection closed by peer");
    this.buf = cat(this.buf, value);
  }
  async msg() {
    while (this.buf.length < 5) await this.fill();
    const len = new DataView(this.buf.buffer, this.buf.byteOffset).getUint32(1);
    while (this.buf.length < 1 + len) await this.fill();
    const t = String.fromCharCode(this.buf[0]);
    const body = this.buf.slice(5, 1 + len);
    this.buf = this.buf.slice(1 + len);
    return { t, body };
  }
}

function errFields(body) {
  const f = {};
  let i = 0;
  while (i < body.length && body[i] !== 0) {
    const k = String.fromCharCode(body[i++]);
    let j = i;
    while (body[j] !== 0) j++;
    f[k] = dec.decode(body.slice(i, j));
    i = j + 1;
  }
  return { code: f.C, message: f.M };
}

async function run(hd, sql, params, repl) {
  const sock = connect({ hostname: hd.host, port: hd.port });
  try {
    const w = sock.writable.getWriter();
    const rd = new Rd(sock);
    const kv = [["user", hd.user], ["database", hd.database]];
    if (repl) kv.push(["replication", "database"]);
    const sbody = cat(i32(196608), ...kv.flatMap(([k, v]) => [cstr(k), cstr(v)]), new Uint8Array([0]));
    await w.write(cat(i32(sbody.length + 4), sbody));
    // Startup phase: AuthenticationOk, ParameterStatus*, BackendKeyData, ReadyForQuery.
    for (;;) {
      const m = await rd.msg();
      if (m.t === "E") return { error: errFields(m.body) };
      if (m.t === "Z") break;
    }
    if (params === null) {
      await w.write(frame("Q", cstr(sql)));
    } else {
      const bind = cat(
        cstr(""), cstr(""), i16(0), i16(params.length),
        ...params.flatMap((p) => { const b = enc.encode(p); return [i32(b.length), b]; }),
        i16(0));
      await w.write(cat(
        frame("P", cat(cstr(""), cstr(sql), i16(0))),
        frame("B", bind),
        frame("D", cat(enc.encode("P"), cstr(""))),
        frame("E", cat(cstr(""), i32(0))),
        frame("S", new Uint8Array(0))));
    }
    const rows = [];
    let error = null;
    for (;;) {
      const m = await rd.msg();
      if (m.t === "E") { error = errFields(m.body); continue; }
      if (m.t === "D") {
        const dv = new DataView(m.body.buffer, m.body.byteOffset);
        const n = dv.getInt16(0);
        let o = 2;
        const row = [];
        for (let c = 0; c < n; c++) {
          const l = dv.getInt32(o); o += 4;
          if (l < 0) { row.push(null); continue; }
          row.push(dec.decode(m.body.slice(o, o + l))); o += l;
        }
        rows.push(row);
      }
      if (m.t === "Z") break;
    }
    await w.write(frame("X", new Uint8Array(0))).catch(() => {});
    return error ? { error } : { rows };
  } finally {
    await sock.close().catch(() => {});
  }
}

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname !== "/q") return new Response("m17", { status: 404 });
    const hd = env[u.searchParams.get("b")];
    const sql = u.searchParams.get("sql") || "";
    const p = u.searchParams.get("p");
    const params = p === null ? null : p.split(",");
    const repl = u.searchParams.get("repl") === "1";
    const timer = new Promise((r) => setTimeout(() => r({ error: "timeout" }), 15000));
    try {
      const out = await Promise.race([run(hd, sql, params, repl), timer]);
      // Never echo the isolate-visible placeholder password back; prove
      // it is the placeholder (not a real credential) separately.
      return Response.json(out);
    } catch (e) {
      return Response.json({ error: { message: String((e && e.message) || e) } });
    }
  },
};
"#;

/// Binding names → credential ref. `HD_OK`/`HD_BADPW`/`HD_REPL` are in
/// the proxy's signed desired snapshot with a credential; `HD_NOCRED`
/// is in desired without a credential; `HD_BLOCKED` has a
/// `tcpBlocked` credential; `HD_UNKNOWN` is rendered into the worker
/// but absent from the proxy's snapshot.
const BINDINGS: [&str; 6] = [
    "HD_OK",
    "HD_BADPW",
    "HD_REPL",
    "HD_NOCRED",
    "HD_BLOCKED",
    "HD_UNKNOWN",
];
const OMIT: [&str; 1] = ["HD_UNKNOWN"];
const BAD_PASS: &str = "definitely-wrong-password";

fn q(port: u16, binding: &str, sql: &str, params: Option<&str>, repl: bool) -> serde_json::Value {
    let mut path = format!("/q?b={binding}&sql={}", pct(sql));
    if let Some(p) = params {
        path.push_str(&format!("&p={}", pct(p)));
    }
    if repl {
        path.push_str("&repl=1");
    }
    http_json(port, &path)
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN (real workerd) and a PostgreSQL 16 bin dir"]
fn mig17_hyperdrive_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real-workerd test not run");
        return;
    };
    let Some(pg_bin) = pg_bin_dir() else {
        eprintln!(
            "SKIP: no initdb/pg_ctl (set VRT_TEST_PG_BIN or install postgresql@16) — real-PG test not run"
        );
        return;
    };
    let Some(pg) = start_temp_pg(pg_bin, "vrt-m17pg") else {
        return;
    };
    let Some(dir) = smoke_root("vrt-m17") else {
        return;
    };

    // pgproxy on its own runtime (the test body is blocking).
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
            ("HD_REPL", PgCred::Tcp(PG_PASS.into())),
            // HD_NOCRED: intentionally no entry.
            ("HD_BLOCKED", PgCred::Blocked),
        ],
    );
    let digest = user_bundle(&SPEC, USER_MODULE).digest().to_string();
    let proxy_port = spawn_pgproxy(&rt, &dir, &SPEC, &digest, &BINDINGS, &OMIT, creds_path);

    assert_proxy_baseline(&rt, proxy_port, &synthetic_host(&SPEC, "HD_OK"));

    let http_port = free_port();
    let cfg = render_and_write(&dir, http_port, proxy_port, &SPEC, USER_MODULE, &BINDINGS);
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

    // 1. SELECT 1 through the whole chain.
    let v = q(http_port, "HD_OK", "SELECT 1", None, false);
    assert_eq!(v["rows"], json!([["1"]]), "SELECT 1: {v}{}", ctx());

    // 1b. The login is the provider-held one — the isolate only ever
    //     held the placeholder `vrt-hd` user.
    let v = q(http_port, "HD_OK", "SELECT current_user", None, false);
    assert_eq!(v["rows"], json!([[PG_SUPER]]), "upstream identity: {v}");

    // 1c. Parameterized (extended protocol) query.
    let v = q(
        http_port,
        "HD_OK",
        "SELECT $1::int + $2::int, $3::text",
        Some("20,22,hello"),
        false,
    );
    assert_eq!(
        v["rows"],
        json!([["42", "hello"]]),
        "parameterized: {v}{}",
        ctx()
    );

    // 1d. A server-side SQL error is passed through untouched (relay is
    //     transparent after ReadyForQuery) and the session still closes.
    let v = q(http_port, "HD_OK", "SELECT 1/0", None, false);
    assert_eq!(
        v["error"]["code"], "22012",
        "division_by_zero passthrough: {v}"
    );

    // 1e. Repeated sessions reuse released slots (no leak/wedge).
    for i in 0..12 {
        let v = q(http_port, "HD_OK", "SELECT 2", None, false);
        assert_eq!(v["rows"], json!([["2"]]), "iteration {i}: {v}{}", ctx());
    }

    // 2. Wrong upstream password → typed refusal, never a hang.
    let t0 = std::time::Instant::now();
    let v = q(http_port, "HD_BADPW", "SELECT 1", None, false);
    assert!(
        err_text(&v).contains("PG_UPSTREAM_AUTH"),
        "wrong password must be typed PG_UPSTREAM_AUTH: {v}{}",
        ctx()
    );
    assert!(t0.elapsed().as_secs() < 14, "must fail fast, not hang");
    assert!(
        !err_text(&v).contains("definitely-wrong"),
        "no secret echo: {v}"
    );

    // 3. Fail closed on unmapped bindings.
    for b in ["HD_UNKNOWN", "HD_NOCRED", "HD_BLOCKED"] {
        let v = q(http_port, b, "SELECT 1", None, false);
        assert!(v.get("rows").is_none(), "{b} must not return rows: {v}");
        assert!(
            err_text(&v).contains("PG_BINDING_UNKNOWN"),
            "{b} must fail closed PG_BINDING_UNKNOWN: {v}{}",
            ctx()
        );
    }

    // 4. replication startup is refused before any upstream work.
    let v = q(http_port, "HD_REPL", "IDENTIFY_SYSTEM", None, true);
    assert!(
        err_text(&v).contains("PG_STARTUP_UNSUPPORTED"),
        "replication must be PG_STARTUP_UNSUPPORTED: {v}{}",
        ctx()
    );

    // 5. Every session released: the active gauge drains to 0 (worker
    //    sockets are closed; egress idle/abandon reaping bounds this).
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
