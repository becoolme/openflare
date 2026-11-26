//! G1 PoC7/PoC8/PoC9 — resource-limit gap documentation, drain
//! semantics, per-tenant memory. REAL workerd, no probe-skip.
//!
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test isolation_gate_limits -- --nocapture

mod common;
use common::g1::*;
use serde_json::json;
use std::io::Write;
use std::time::{Duration, Instant};
use vrt_config::EgressPolicy;

/// PoC 7 — `_documents_gap` form: `--include-ignored` must not fail,
/// so this test ASSERTS the verified gap and prints the numbers.
///
/// W2 (per-worker `limits`) landed with the quotas work: a declared
/// deviation now renders, but an UNDECLARED dimension stays stock.
/// This fixture leaves `cpuMs` at the platform default (50), so no
/// `cpuMs` limit is emitted and the fairness gap below is unchanged
/// for default-quota workers. The test fails if a build starts
/// rendering CPU limits for a default declaration, or stops rendering
/// the declared `subRequests`.
///
/// Measured 2026-11-12 on workerd 1.20260916.1 (pre-W2); re-verified
/// 2026-11-20 on the pinned VRT fork.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc7_limits_gap_documented() {
    // Latency gates (base<500ms) must not run under a concurrent
    // whole-core saturation test in another binary.
    let _cpu = common::cpu_load_shared();
    let bin = workerd_bin();
    let dir = gate_root("poc7");
    let _guard = DirGuard(dir.clone());

    // Tenant A burns CPU for a bounded window; tenant B is latency-
    // probed concurrently. Loop bound 15s keeps the test < 60s hard.
    let mod_a = r#"
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const secs = Math.min(parseInt(u.searchParams.get("secs") ?? "8"), 15);
    const until = Date.now() + secs * 1000;
    let n = 0;
    while (Date.now() < until) n++;
    return new Response("spun " + n);
  },
};
"#;
    let mod_b = r#"export default { fetch() { return new Response("ok"); } }"#;
    let (cfg, ports, args, text) = render(
        &dir,
        &[
            ("wa", "burner", mod_a, json!([])),
            ("wb", "neighbor", mod_b, json!([])),
        ],
        EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: vec![],
            deny_names: vec![],
            request_timeout_ms: 8_000,
            ..Default::default()
        },
    );

    // FACT 1 (W2): quotas reach workerd as a `limits` group, but only
    // for dimensions that deviate from the platform default. This
    // fixture declares `subrequests: 100` (default 50) and leaves
    // `cpuMs`/`memoryMb` at their defaults, so the render carries the
    // subrequest budget and NO CPU budget — the fairness gap asserted
    // below is the default-quota shape.
    //
    // Scope the assertions to the TENANT worker blocks: system workers
    // now carry platform-declared cpuMs budgets (facade/manifest.json),
    // which is correct and unrelated to the default-quota gap this test
    // documents — system workers never run the tenant burn/spin probes.
    let tenant_block = |id: &str| {
        let start = text
            .find(&format!("(name = \"worker-{id}\", worker = ("))
            .unwrap_or_else(|| panic!("worker-{id} service not rendered: {text}"));
        let rest = &text[start..];
        let end = rest[1..]
            .find("\n    (name = \"")
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        &rest[..end]
    };
    for id in ["wa", "wb"] {
        let block = tenant_block(id);
        assert!(
            block.contains("limits = (subRequests = 100)"),
            "declared subRequests quota must render on worker-{id}: {block}"
        );
        assert!(
            !block.contains("cpuMs = ") && !block.contains("memoryMb = "),
            "default tenant dimensions must not render (stock behaviour): {block}"
        );
    }
    assert!(
        !text.contains("isolatedVm") && !text.contains("actorMode"),
        "workers not process-shared — this gate only applies to shared mode"
    );

    let mut proc = spawn(&bin, &cfg, &dir, &args);
    for id in ["wa", "wb"] {
        let (st, _) = http(ports[id], "/");
        assert_eq!(st, 200, "{id} not up: {}", proc.log());
    }
    let wb = ports["wb"];

    // Baseline latency for tenant B. G1-r4: `http_once` now stops at
    // body-complete, so this is REAL latency (~10-40ms), not the old
    // 5s keep-alive-drain artifact. The bound keeps the measurement
    // honest — a baseline ≥500ms would mean we're measuring sockets,
    // not worker work.
    let base = {
        let t = Instant::now();
        http_once(wb, "/", Duration::from_secs(5)).unwrap();
        t.elapsed()
    };
    eprintln!("poc7 B baseline latency = {base:?}");
    assert!(
        base < Duration::from_millis(500),
        "baseline {base:?} too high — http read must stop at body-complete"
    );

    // FACT 2 (verified, still true post-W2 because no cpuMs limit is
    // declared): a bounded CPU burn in A starves B — one process,
    // single V8 isolate pool, no per-tenant fairness unless a CPU
    // budget is emitted. Measured (G1-r4, body-complete timing):
    // 2591ms starved vs 235µs baseline during a 3s burn.
    let burn = std::thread::spawn({
        let pa = ports["wa"];
        move || {
            let _ = http_once(pa, "/?secs=3", Duration::from_secs(10));
        }
    });
    std::thread::sleep(Duration::from_millis(400)); // ensure spin is running
    let starved = {
        let t = Instant::now();
        http_once(wb, "/", Duration::from_secs(15)).unwrap();
        t.elapsed()
    };
    burn.join().unwrap();
    eprintln!("poc7 B latency under A 3s burn = {starved:?} (baseline {base:?})");
    // Assert the GAP, not the goal: on stock workerd a 3s neighbor
    // burn stretches B's request by >10x AND >1s wall-clock (single
    // shared event loop — B cannot even start until A's spin ends;
    // measured ~2.4s vs ~10-40ms baseline).
    // Assert the GAP, not the goal: without a rendered cpuMs limit a
    // 3s neighbor burn stretches B's request by >10x AND >1s
    // wall-clock (single shared event loop — B cannot even start
    // until A's spin ends; measured ~2.4s vs ~10-40ms baseline).
    assert!(
        starved > base * 10 && starved > Duration::from_secs(1),
        "unexpected fairness — starved {starved:?} vs baseline {base:?};          if a default cpuMs limit started rendering, flip this gate"
    );

    // FACT 3 (verified): nothing terminates the spin — the worker
    // declared no cpuMs budget, so no CPU limit is emitted and the
    // request completes only when the worker's OWN loop bound ends;
    // the process keeps running. An unbounded loop ran 32s+ in the G1
    // probe with no limiter intervention.
    let t = Instant::now();
    let (st, body) = http(ports["wa"], "/?secs=2");
    let spin = t.elapsed();
    assert_eq!(st, 200, "spin request: {body}");
    // Wall-clock can land a hair under the bound: `Date.now()` is
    // millisecond-granular and the loop exits at `now >= until`
    // (measured 1.9997s for secs=2). A limiter would cut at CPU-time
    // bounds (ms scale), never at 1.9s — 1.5s is the gap-proving line.
    assert!(
        spin >= Duration::from_millis(1500),
        "spin returned early — a limiter fired? {spin:?}"
    );
    assert_eq!(
        proc.child.try_wait().unwrap(),
        None,
        "workerd died — unexpected"
    );
    eprintln!("poc7 spin completed at {spin:?} by worker's own bound; process alive —                DOCUMENTED: a default (undeclared) cpuMs renders no limit, so                           shared cells need an explicit CPU budget; declared quotas now                           render (W2 landed) per docs/quotas.md");
}

/// Percentile over a latency sample set (nearest-rank).
fn pct(samples: &[Duration], p: f64) -> Duration {
    let mut v = samples.to_vec();
    v.sort();
    let idx = ((v.len() as f64) * p).ceil() as usize;
    v[idx.saturating_sub(1).min(v.len() - 1)]
}

/// The burner + neighbor modules, shared by both PoC7b scenarios —
/// the SAME modules PoC7 uses so numbers are directly comparable.
fn poc7b_modules() -> (&'static str, &'static str) {
    (
        r#"
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const secs = Math.min(parseInt(u.searchParams.get("secs") ?? "0"), 8);
    const until = Date.now() + secs * 1000;
    let n = 0;
    while (Date.now() < until) n++;
    return new Response("spun " + n);
  },
};
"#,
        r#"export default { fetch() { return new Response("ok"); } }"#,
    )
}

/// Spawn one single-worker tenant process (`<tag>-proc`) and return
/// (guard, proc, ingress port). Each tenant gets its OWN workerd —
/// the multi-process cell shape (workers-runtime.md §6).
fn poc7b_spawn(tag: &str, module: &str) -> (DirGuard, WorkerProc, u16) {
    let bin = workerd_bin();
    let dir = gate_root(tag);
    let guard = DirGuard(dir.clone());
    // Early-exit retry: a cross-process port race can still make a
    // spawn lose its bind (g1.rs ALLOC_PORTS only dedupes in-process).
    // Worker startup got slower with the system-worker payload, so
    // the old fixed +400ms liveness check let later exits slip past
    // the retry — wait for the actual bind instead.
    for attempt in 0..3 {
        let (cfg, ports, args, _text) = render(
            &dir,
            &[("w", tag, module, json!([]))],
            EgressPolicy {
                allowed_ports: vec![80, 443],
                allow: vec![],
                deny_names: vec![],
                request_timeout_ms: 8_000,
                ..Default::default()
            },
        );
        let mut proc = spawn(&bin, &cfg, &dir, &args);
        let deadline = Instant::now() + Duration::from_secs(30);
        let up = loop {
            if proc.child.try_wait().unwrap().is_some() {
                break false;
            }
            if std::net::TcpStream::connect(("127.0.0.1", ports["w"])).is_ok() {
                break true;
            }
            assert!(
                Instant::now() < deadline,
                "worker on :{} never bound",
                ports["w"]
            );
            std::thread::sleep(Duration::from_millis(100));
        };
        if up {
            return (guard, proc, ports["w"]);
        }
        // Preserve the dead attempt's log — the next spawn's
        // File::create truncates `workerd.log`.
        let _ = std::fs::copy(
            dir.join("workerd.log"),
            dir.join(format!("workerd-attempt{attempt}.log")),
        );
        if attempt == 2 {
            panic!("workerd failed to come up after 3 spawns\n{}", proc.log())
        }
    }
    unreachable!()
}

/// PoC 7b scenario 1 — UNSATURATED multi-process fairness.
/// A burns 3s in ITS OWN workerd process while B (separate process)
/// is latency-probed 24x. Design claims (workers-runtime.md §6-74):
/// per-tenant process => a spinning neighbor is invisible. PoC7
/// proved the opposite for the shared process (~2.6s starve); this
/// test quantifies the multi-process form on the same machine.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc7b_multi_proc_unsaturated() {
    // p99<250ms is a latency gate — shared CPU lock.
    let _cpu = common::cpu_load_shared();
    let (mod_a, mod_b) = poc7b_modules();
    let (guard_a, mut pa, port_a) = poc7b_spawn("poc7b-a", mod_a);
    let (guard_b, mut pb, port_b) = poc7b_spawn("poc7b-b", mod_b);
    let _ = (guard_a, guard_b);
    let mut procs = [
        ("a", &mut pa, port_a),
        ("b", &mut pb, port_b),
    ];
    for (tag, proc, port) in procs.iter_mut() {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(st) = proc.child.try_wait().unwrap() {
                // Dump the whole dir listing — a missing/empty
                // workerd.log after a confirmed bind means the log
                // was truncated or the fd went elsewhere; the attempt
                // copies from poc7b_spawn keep the earlier rounds.
                let listing = std::fs::read_dir(proc.log_path.parent().unwrap())
                    .map(|rd| {
                        rd.filter_map(|e| {
                            e.ok().map(|e| {
                                format!(
                                    "{}:{}B",
                                    e.file_name().to_string_lossy(),
                                    e.metadata().map(|m| m.len()).unwrap_or(0)
                                )
                            })
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                    })
                    .unwrap_or_else(|e| format!("read_dir failed: {e}"));
                panic!(
                    "proc-{tag} :{port} exited early: {st}\n=== workerd.log ===\n{}\n=== dir ===\n{listing}",
                    proc.log()
                );
            }
            match http_once(*port, "/", Duration::from_secs(3)) {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "worker on :{port} never came up: {e}\n=== workerd.log ===\n{}",
                        proc.log()
                    );
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    // Baseline: >=20 sequential probes of B. Same body-complete
    // http_once as PoC7 (G1-r4) — numbers are wall latency, not
    // keep-alive drain.
    let mut base = Vec::new();
    for _ in 0..24 {
        let t = Instant::now();
        http_once(port_b, "/", Duration::from_secs(5)).unwrap();
        base.push(t.elapsed());
    }
    let b50 = pct(&base, 0.50);
    eprintln!(
        "poc7b base: n={} p50={:?} p99={:?} max={:?}",
        base.len(),
        b50,
        pct(&base, 0.99),
        base.iter().max().unwrap()
    );
    assert!(
        b50 < Duration::from_millis(50),
        "baseline p50 {b50:?} too high — measurement invalid"
    );

    // A spins 3s in its own process; sample B >=20x inside the burn.
    let spin = std::thread::spawn(move || {
        let _ = http_once(port_a, "/?secs=3", Duration::from_secs(10));
    });
    std::thread::sleep(Duration::from_millis(300)); // let the spin start
    let mut stv = Vec::new();
    for _ in 0..24 {
        let t = Instant::now();
        http_once(port_b, "/", Duration::from_secs(5)).unwrap();
        stv.push(t.elapsed());
    }
    spin.join().unwrap();
    let s50 = pct(&stv, 0.50);
    let s99 = pct(&stv, 0.99);
    eprintln!(
        "poc7b starved-run: n={} p50={:?} p99={:?} max={:?}",
        stv.len(),
        s50,
        s99,
        stv.iter().max().unwrap()
    );
    eprintln!("poc7b contrast: same-proc PoC7 starved ~= 2.6s; multi-proc p99 = {s99:?}");
    // Two processes, distinct schedulable entities: the OS scheduler
    // gives B its own core-time, so a 3s neighbor burn must not
    // register. 250ms is ~10x a healthy tail yet an order of
    // magnitude under PoC7's 2.6s same-proc starve — if this ever
    // trips, process isolation regressed (or the box is saturated —
    // see the saturated scenario below for that bound).
    assert!(
        s99 < Duration::from_millis(250),
        "B p99 {s99:?} under A's spin — process isolation failed?"
    );
    assert!(
        s99 < Duration::from_secs(1),
        "multi-proc starve {s99:?} approached PoC7's same-proc ~2.6s"
    );
    assert_eq!(pa.child.try_wait().unwrap(), None, "A died");
    assert_eq!(pb.child.try_wait().unwrap(), None, "B died");
}

/// PoC 7b scenario 2 — CORE-SATURATED multi-process fairness.
/// `k = min(available_parallelism, 8)` spinner processes + 1 B.
/// Print only — quantifies how far per-process isolation degrades
/// once all cores are busy (evidence for the cell admission bound /
/// W3 cgroup work, NOT a gate).
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc7b_multi_proc_saturated() {
    // k-core CPU saturation must not overlap latency/watchdog tests.
    let _cpu = common::cpu_load_exclusive();
    let (mod_a, mod_b) = poc7b_modules();
    let k = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(2);
    eprintln!("poc7b saturated: k={k} spinner processes + 1 neighbor");
    let mut guards = Vec::new();
    let mut procs = Vec::new();
    let mut ports = Vec::new();
    for i in 0..k {
        let (g, p, port) = poc7b_spawn(&format!("poc7b-s{i}"), mod_a);
        guards.push(g);
        procs.push(p);
        ports.push(port);
    }
    let (guard_b, mut pb, port_b) = poc7b_spawn("poc7b-sb", mod_b);
    let _ = guard_b;
    http(port_b, "/");
    for p in &ports {
        http(*p, "/");
    }
    // Fire a bounded 4s burn on every spinner, then sample B.
    let mut spins = Vec::new();
    for p in ports {
        spins.push(std::thread::spawn(move || {
            let _ = http_once(p, "/?secs=4", Duration::from_secs(15));
        }));
    }
    std::thread::sleep(Duration::from_millis(400)); // all spins running
    let mut stv = Vec::new();
    for _ in 0..24 {
        let t = Instant::now();
        http_once(port_b, "/", Duration::from_secs(10)).unwrap();
        stv.push(t.elapsed());
    }
    for h in spins {
        h.join().unwrap();
    }
    eprintln!(
        "poc7b saturated: k={k} procs busy -> B n={} p50={:?} p99={:?} max={:?}",
        stv.len(),
        pct(&stv, 0.50),
        pct(&stv, 0.99),
        stv.iter().max().unwrap()
    );
    for p in &mut procs {
        assert_eq!(p.child.try_wait().unwrap(), None, "spinner died");
    }
    assert_eq!(pb.child.try_wait().unwrap(), None, "B died");
}

/// PoC 8 — SIGTERM exits workerd in ~0.02s WITHOUT waiting for a
/// pending waitUntil. Verified 2026-11-12: waitUntil work is dropped,
/// not drained. Divergence from Cloudflare semantics (drain on
/// update) is documented in workers-runtime-phase1-gate.md.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc8_sigterm_drops_waituntil() {
    // Exit-latency measurement — shared CPU lock.
    let _cpu = common::cpu_load_shared();
    let bin = workerd_bin();
    let dir = gate_root("poc8");
    let _guard = DirGuard(dir.clone());
    let mod_wu = r#"
export default {
  async fetch(req, env, ctx) {
    const u = new URL(req.url);
    if (u.pathname === "/hang") {
      ctx.waitUntil(new Promise(() => {})); // never resolves
      return new Response("armed");
    }
    return new Response("ok");
  },
};
"#;
    let (cfg, ports, args, _t) = render(
        &dir,
        &[("wu", "waituntil", mod_wu, json!([]))],
        EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: vec![],
            deny_names: vec![],
            request_timeout_ms: 8_000,
            ..Default::default()
        },
    );
    let mut proc = spawn(&bin, &cfg, &dir, &args);
    let (st, _) = http(ports["wu"], "/hang");
    assert_eq!(st, 200, "arm waitUntil: {}", proc.log());
    std::thread::sleep(Duration::from_millis(200));

    let t = Instant::now();
    unsafe { libc::kill(proc.child.id() as i32, libc::SIGTERM) };
    let status = proc.child.wait().unwrap();
    let elapsed = t.elapsed();
    eprintln!("poc8 SIGTERM->exit = {elapsed:?} status={status}");
    // Verified: 0.02s — workerd never drains pending waitUntil work.
    // Generous bound (5s) so slow CI still passes; the printed number
    // is the gate's evidence, not the threshold.
    assert!(
        elapsed < Duration::from_secs(5),
        "SIGTERM did not exit within 5s: {elapsed:?}"
    );
    eprintln!("poc8 DOCUMENTED: waitUntil promises are DROPPED on shutdown —                vrt-host must not rely on workerd for graceful drain;                supervisor drain_deadline (supervisor.rs) remains the drain owner");
}

/// PoC 9 — per-tenant RSS. Prints a table; NO threshold asserted.
/// Numbers feed the density recommendation in the gate doc.
/// G1-r4: three measurement rounds, min/median/max reported so the
/// doc can quote a reproducible range, not a single sample.
fn poc9_measure(tag: &str, bound: bool) {
    let bin = workerd_bin();
    let mut samples: std::collections::BTreeMap<usize, Vec<u64>> =
        std::collections::BTreeMap::new();
    for round in 1..=3 {
        for n in [1usize, 5, 10] {
            let dir = gate_root(&format!("poc9-{tag}-{n}-r{round}"));
            let _guard = DirGuard(dir.clone());
            let bindings = if bound {
                json!([
                    {"type":"kv","name":"KV","namespaceId":"ns-m",
                     "permissions":{"read":true,"write":true}},
                    {"type":"r2","name":"B","bucket":"b-m",
                     "permissions":{"read":true,"write":true}},
                ])
            } else {
                json!([])
            };
            let modstrs: Vec<String> = (0..n)
                .map(|i| {
                    format!(
                        r#"export default {{ fetch() {{ return new Response("x{i}"); }} }} /*t{i}r{round}*/"#
                    )
                })
                .collect();
            let workers: Vec<(String, String, &str, serde_json::Value)> = (0..n)
                .map(|i| {
                    (
                        format!("w{i}"),
                        format!("tenant-{i}"),
                        modstrs[i].as_str(),
                        bindings.clone(),
                    )
                })
                .collect();
            let refs: Vec<(&str, &str, &str, serde_json::Value)> = workers
                .iter()
                .map(|(a, b, c, d)| (a.as_str(), b.as_str(), *c, d.clone()))
                .collect();
            let (cfg, ports, args, _t) = render(
                &dir,
                &refs,
                EgressPolicy {
                    allowed_ports: vec![80, 443],
                    allow: vec![],
                    deny_names: vec![],
                    request_timeout_ms: 8_000,
                    ..Default::default()
                },
            );
            let proc = spawn(&bin, &cfg, &dir, &args);
            for (id, p) in &ports {
                let (st, _) = http(*p, "/");
                assert_eq!(st, 200, "{id} not up: {}", proc.log());
            }
            // Fixed settle point: every worker has served one request
            // (isolate + module init done), then 300ms for allocator
            // quiesce. Whole-process metric — includes facade system
            // workers + egress service, i.e. the REAL cell shape.
            std::thread::sleep(Duration::from_millis(300));
            let rss = rss_bytes(proc.child.id());
            eprintln!(
                "poc9 r{round} n={n:2} {tag:5} phys_footprint = {:.1} MiB ({rss} B)",
                rss as f64 / 1048576.0
            );
            samples.entry(n).or_default().push(rss);
        }
    }
    for (n, v) in &samples {
        let mut srt = v.clone();
        srt.sort();
        let (lo, med, hi) = (srt[0], srt[srt.len() / 2], srt[srt.len() - 1]);
        eprintln!(
            "poc9 SUMMARY n={n:2} {tag:5}: {:.1} / {:.1} / {:.1} MiB (min/med/max)",
            lo as f64 / 1048576.0,
            med as f64 / 1048576.0,
            hi as f64 / 1048576.0
        );
    }
    eprintln!("poc9 method: proc_pid_rusage RUSAGE_INFO_V4 ri_phys_footprint;              full-render config; probe-all-workers + 300ms settle; 3 rounds");
}

/// PoC 9 (empty workers) — prints numbers only; NO threshold.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc9_per_tenant_rss_empty() {
    poc9_measure("empty", false);
}

/// PoC 9 (KV+R2 bound workers) — prints numbers only; NO threshold.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc9_per_tenant_rss_bound() {
    poc9_measure("bound", true);
}

/// Extra PoC5/8 guard: verify process reaps on Drop even mid-flight.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_proc_cleanup_verified() {
    let bin = workerd_bin();
    let dir = gate_root("cleanup");
    let _guard = DirGuard(dir.clone());
    let (cfg, _p, args, _t) = render(
        &dir,
        &[(
            "wx",
            "x",
            r#"export default { fetch() { return new Response("x"); } }"#,
            json!([]),
        )],
        EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: vec![],
            deny_names: vec![],
            request_timeout_ms: 8_000,
            ..Default::default()
        },
    );
    let mut proc = spawn(&bin, &cfg, &dir, &args);
    proc.child.kill().unwrap();
    let st = proc.child.wait().unwrap();
    assert!(!st.success(), "killed workerd reported success");
    drop(proc);
    let _ = std::io::stderr().write_all(b"g1 cleanup guard ok\n");
}
