//! WAL1 step-0 PoC: is EXTERNAL WAL tailing of workerd's DO sqlite
//! feasible? (`#[ignore]`d + `VRT_WORKERD_BIN` gated; run with
//! `--include-ignored --nocapture`.)
//!
//! Measures, against a live `workerd serve` running a Counter DO
//! (`localDisk`, `enableSql`):
//!   P1. journal mode + `locking_mode` + `-shm` existence — is the
//!       WAL index shareable or EXCLUSIVE-held?
//!   P2. while an external reader holds a read transaction, can
//!       workerd's autocheckpoint/truncate reset the WAL below the
//!       reader's mark? (SQLite semantics say NO — WAL grows instead.)
//!   P3. can an external process parse WAL frames and verify the
//!       cumulative checksum chain (i.e. detect a WAL reset via salt
//!       or a torn tail)?
//!   P4. workerd autocheckpoint threshold/frequency + WAL growth
//!       rate under a pinned reader.
//!   P5. is `wal_checkpoint(PASSIVE)` from an external connection
//!       safe alongside workerd?
//! If any of P1-P3 falsifies tailing, the test FAILS LOUDLY — WAL1
//! must stop and document alternatives, not ship a broken design.

mod common;

use common::init_logs;
use rusqlite::{Connection, OpenFlags};
use serde_json::json;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressPolicy, EmbedKind,
    RuntimeParams, TenantRenderInput, VersionBundle,
};

// ---------- minimal self-contained harness (mirrors d1_smoke) ----------

fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn http_get(port: u16, path: &str) -> (u16, String) {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let mut s = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .unwrap_or_else(|e| panic!("connect :{port}: {e}"));
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
    use std::io::{Read, Write};
    s.write_all(format!("GET {path} HTTP/1.0\r\nhost: x\r\n\r\n").as_bytes())
        .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("http read {path}: {e}"),
        }
    }
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

struct Proc {
    child: Child,
    babysitter: Option<Child>,
}
impl Proc {
    fn new(child: Child) -> Self {
        let pid = child.id();
        let parent = std::process::id();
        let babysitter = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "while kill -0 {parent} 2>/dev/null; do sleep 1; done; \
                 {{ kill -KILL -- -{pid} || true; }} 2>/dev/null; exit 0"
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()
            .ok();
        Self { child, babysitter }
    }
}
impl Drop for Proc {
    fn drop(&mut self) {
        unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };
        let _ = self.child.wait();
        if let Some(mut b) = self.babysitter.take() {
            unsafe { libc::kill(-(b.id() as i32), libc::SIGKILL) };
            let _ = b.wait();
        }
    }
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

const DO_MODULE: &str = r#"export class Counter {
  constructor(state, env) {
    this.state = state;
    this.ready = state.blockConcurrencyWhile(async () => {
      await state.storage.sql.exec('CREATE TABLE IF NOT EXISTS t(k INTEGER)');
      await state.storage.sql.exec('CREATE TABLE IF NOT EXISTS blob(v TEXT)');
    });
  }
  async fetch(req) {
    await this.ready;
    const url = new URL(req.url);
    const s = this.state.storage;
    if (url.pathname === '/inc') {
      const n = (await s.get('n') ?? 0) + 1;
      await s.put('n', n);
      s.sql.exec('INSERT INTO t VALUES (?)', n);
      return Response.json({ n });
    }
    if (url.pathname === '/big') {
      // ~64KiB payload per insert — grows the WAL fast for threshold
      // measurement without needing thousands of HTTP calls.
      const n = parseInt(url.searchParams.get('n') ?? '8', 10);
      const v = 'x'.repeat(64 * 1024);
      for (let i = 0; i < n; i++) s.sql.exec('INSERT INTO blob VALUES (?)', v + i);
      return Response.json({ n });
    }
    if (url.pathname === '/count') {
      const t = [...s.sql.exec('SELECT COUNT(*) c FROM t')][0].c;
      const b = [...s.sql.exec('SELECT COUNT(*) c FROM blob')][0].c;
      return Response.json({ t, b });
    }
    return Response.json({ ok: true });
  }
}
export default {
  async fetch(req, env) {
    const stub = env.COUNTER.get(env.COUNTER.idFromName('one'));
    return stub.fetch(req);
  }
}
"#;

/// One DO-capable config: worker `wdo` declares Counter (enableSql) →
/// `localDisk` under `dir/do-store/team-do/wdo`.
fn render_do(dir: &Path, port: u16) -> (PathBuf, Vec<String>, PathBuf) {
    let worker_id = "wdo";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": DO_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let do_root = dir.join("do-store");
    std::fs::create_dir_all(do_root.join("team-do").join(worker_id)).unwrap();
    let tenant: snapshot_protocol::compute_desired::TenantDesired = serde_json::from_value(json!({
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
        disk_services: manifest.disk_services.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: do_root.join("team-do").display().to_string(),
        wfop_pepper: "poc-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
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
                std::fs::write(&dst, DO_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { ref name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
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
    let mut argv = vrt_config::runtime_experimental_extra_args(&params.system_compat_flags, &tenant, &params.system_workers);
    argv.extend(vrt_host::launch::external_addr_args(&out.external_addrs));
    (cfg, argv, do_root)
}

fn spawn_workerd(bin: &Path, cfg: &Path, argv: &[String], dir: &Path, log: &Path) -> Proc {
    let out = std::fs::File::create(log).unwrap();
    let err = out.try_clone().unwrap();
    Proc::new(
        Command::new(bin)
            .arg("serve")
            .args(argv)
            .arg(cfg)
            .current_dir(dir)
            .process_group(0)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .expect("spawn workerd"),
    )
}

// ---------- WAL parsing (format per sqlite.org/fileformat2) ----------

const WAL_HDR: usize = 32;
const WAL_FRAME_HDR: usize = 24;

fn be32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(b[off..off + 4].try_into().unwrap())
}

#[derive(Debug, Clone)]
struct WalHdr {
    magic: u32,
    page_size: u32,
    ckpt_seq: u32,
    salt1: u32,
    salt2: u32,
}

fn wal_hdr(buf: &[u8]) -> Option<WalHdr> {
    if buf.len() < WAL_HDR {
        return None;
    }
    let magic = be32(buf, 0);
    if magic != 0x377f_0682 && magic != 0x377f_0683 {
        return None;
    }
    Some(WalHdr {
        magic,
        page_size: be32(buf, 8),
        ckpt_seq: be32(buf, 12),
        salt1: be32(buf, 16),
        salt2: be32(buf, 20),
    })
}

/// SQLite WAL cumulative checksum. `be` = magic ends in 3.
fn wal_cksum(be: bool, seed: (u32, u32), data: &[u8]) -> (u32, u32) {
    let (mut s1, mut s2) = seed;
    for c in data.chunks_exact(8) {
        let (x0, x1) = if be {
            (
                u32::from_be_bytes(c[0..4].try_into().unwrap()),
                u32::from_be_bytes(c[4..8].try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(c[0..4].try_into().unwrap()),
                u32::from_le_bytes(c[4..8].try_into().unwrap()),
            )
        };
        s1 = s1.wrapping_add(x0).wrapping_add(s2);
        s2 = s2.wrapping_add(x1).wrapping_add(s1);
    }
    (s1, s2)
}

struct WalScan {
    /// Bytes covered by fully checksum-verified frames (>= WAL_HDR).
    verified_end: u64,
    frames: u64,
    commits: u64,
    /// Frame index (0-based) of the last commit frame seen.
    last_commit_idx: Option<u64>,
    cksum: (u32, u32),
    bad_at: Option<u64>,
}

/// Verify the header checksum then walk frames, checking salt match +
/// cumulative checksum per frame. Stops at the first invalid frame —
/// `verified_end` is the largest byte position safe to consume.
fn wal_scan(buf: &[u8]) -> Option<WalScan> {
    let h = wal_hdr(buf)?;
    let be = h.magic & 1 == 1;
    let mut cksum = wal_cksum(be, (0, 0), &buf[..24]);
    if cksum != (be32(buf, 24), be32(buf, 28)) {
        return Some(WalScan {
            verified_end: WAL_HDR as u64,
            frames: 0,
            commits: 0,
            last_commit_idx: None,
            cksum,
            bad_at: Some(0),
        });
    }
    let fsz = WAL_FRAME_HDR + h.page_size as usize;
    let mut off = WAL_HDR;
    let mut scan = WalScan {
        verified_end: WAL_HDR as u64,
        frames: 0,
        commits: 0,
        last_commit_idx: None,
        cksum,
        bad_at: None,
    };
    while off + fsz <= buf.len() {
        let f = &buf[off..off + fsz];
        if be32(f, 8) != h.salt1 || be32(f, 12) != h.salt2 {
            scan.bad_at = Some(off as u64);
            break;
        }
        cksum = wal_cksum(be, cksum, &f[..8]);
        cksum = wal_cksum(be, cksum, &f[WAL_FRAME_HDR..]);
        if cksum != (be32(f, 16), be32(f, 20)) {
            scan.bad_at = Some(off as u64);
            break;
        }
        scan.frames += 1;
        if be32(f, 4) != 0 {
            scan.commits += 1;
            scan.last_commit_idx = Some(scan.frames - 1);
        }
        off += fsz;
        scan.verified_end = off as u64;
        scan.cksum = cksum;
    }
    Some(scan)
}

// ---------- the PoC ----------

#[test]
#[ignore = "PoC: real workerd; run explicitly with VRT_WORKERD_BIN"]
fn wal_tail_poc() {
    init_logs();
    let Ok(bin) = std::env::var("VRT_WORKERD_BIN") else {
        eprintln!("skip: VRT_WORKERD_BIN unset");
        return;
    };
    let bin = PathBuf::from(bin);
    assert!(bin.exists(), "VRT_WORKERD_BIN missing: {}", bin.display());
    let dir = Path::new("target/tmp-wal1/poc");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let dir = &std::fs::canonicalize(dir).unwrap(); // doStorageDir wants absolute
    let port = free_port();
    let (cfg, argv, do_root) = render_do(dir, port);
    let log = dir.join("workerd.log");
    let wd = spawn_workerd(&bin, &cfg, &argv, dir, &log);
    assert!(
        wait_ready(port),
        "workerd never listened:\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    eprintln!("== PoC workerd up at :{port}");

    // Drive the DO once so its object sqlite exists.
    let (st, body) = http_get(port, "/inc");
    assert_eq!(st, 200, "{body}");
    let class_dir = do_root.join("team-do").join("wdo").join("c".repeat(64));
    let db = std::fs::read_dir(&class_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.extension().is_some_and(|x| x == "sqlite")
                && p.file_name().unwrap() != "metadata.sqlite"
        })
        .expect("no object sqlite under class dir");
    let wal = db.with_file_name(format!("{}-wal", db.file_name().unwrap().to_string_lossy()));
    let shm = db.with_file_name(format!("{}-shm", db.file_name().unwrap().to_string_lossy()));
    eprintln!("== object db: {}", db.display());

    // ---------- P1: mode checks ----------
    let ro = |p: &Path| {
        let c = Connection::open_with_flags(
            p,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        c.busy_timeout(Duration::from_secs(10)).unwrap();
        c
    };
    let conn = ro(&db);
    let jm: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    let lm: String = conn
        .query_row("PRAGMA locking_mode", [], |r| r.get(0))
        .unwrap();
    let shm_exists = shm.exists();
    eprintln!("P1 journal_mode={jm} locking_mode={lm} shm_exists={shm_exists}");
    assert_eq!(jm, "wal", "workerd DO db is not WAL — tailing moot");
    assert_ne!(
        lm, "exclusive",
        "P1 FALSIFIED: locking_mode=EXCLUSIVE — external reads will fail"
    );
    assert!(
        shm_exists,
        "P1 FALSIFIED: no -shm — workerd holds the WAL index exclusively"
    );
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))
        .unwrap();
    eprintln!("P1 external read OK (sqlite_master rows={n})");

    // ---------- P2+P4: pin a read txn, grow the WAL ----------
    let reader = ro(&db);
    reader.execute_batch("BEGIN").unwrap();
    // Materialize the read mark — an idle BEGIN pins nothing.
    let _: i64 = reader
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))
        .unwrap();
    let pin_frame = {
        let buf = std::fs::read(&wal).unwrap_or_default();
        wal_scan(&buf).map(|s| s.frames).unwrap_or(0)
    };
    eprintln!("P2 read txn pinned at ~frame {pin_frame}");

    let t0 = Instant::now();
    let mut wal_peak = 0u64;
    let mut shrink_observed = false;
    let mut writes = 0u64;
    while t0.elapsed() < Duration::from_secs(90) {
        let (st, body) = http_get(port, "/big?n=8");
        assert_eq!(st, 200, "{body}");
        writes += 1;
        let sz = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        if sz < wal_peak {
            shrink_observed = true;
        }
        wal_peak = wal_peak.max(sz);
        if wal_peak > 6 * 1024 * 1024 && writes >= 10 {
            break;
        }
    }
    let ps = wal_hdr(&std::fs::read(&wal).unwrap_or_default())
        .map(|h| h.page_size)
        .unwrap_or(4096);
    eprintln!(
        "P4 page_size={ps} writes={writes} wal_peak={}B (~{} frames) \
         shrink_observed={shrink_observed} elapsed={:?}",
        wal_peak,
        wal_peak / (WAL_FRAME_HDR + ps as usize) as u64,
        t0.elapsed()
    );
    assert!(
        wal_peak > 2 * 1024 * 1024,
        "WAL never grew past ~autocheckpoint size — measurement inconclusive"
    );
    // The pinned range must still verify — a checkpoint could not
    // recycle frames below a held read mark.
    let buf_now = std::fs::read(&wal).unwrap();
    let scan_now = wal_scan(&buf_now).expect("wal header vanished while reader held");
    assert!(
        scan_now.frames >= pin_frame && scan_now.bad_at.is_none(),
        "P2 FALSIFIED: WAL reset below a held read mark \
         (pinned={pin_frame} now={:?} bad={:?})",
        scan_now.frames,
        scan_now.bad_at
    );
    eprintln!(
        "P2 pinned range intact after {} committed frames — reset blocked by reader",
        scan_now.frames
    );

    // ---------- P3: frame parse + checksum chain ----------
    eprintln!(
        "P3 frames={} commits={} verified_end={}B — external frame parse OK",
        scan_now.frames, scan_now.commits, scan_now.verified_end
    );
    assert!(scan_now.commits > 0, "no commit frames parsed");
    assert!(scan_now.last_commit_idx.is_some());

    // ---------- P5: external wal_checkpoint(PASSIVE) while pinned ----------
    let rw = Connection::open(&db).unwrap();
    rw.busy_timeout(Duration::from_secs(5)).unwrap();
    let passive: rusqlite::Result<(i64, i64, i64)> =
        rw.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        });
    eprintln!("P5 passive checkpoint while reader pinned: {passive:?}");
    let (st, _) = http_get(port, "/inc");
    assert_eq!(st, 200, "workerd broke after external PASSIVE checkpoint");

    // ---------- salt / reset observation ----------
    let hdr_pre = wal_hdr(&std::fs::read(&wal).unwrap()).unwrap();
    eprintln!(
        "salts pre-release: salt1={:08x} salt2={:08x} ckpt_seq={}",
        hdr_pre.salt1, hdr_pre.salt2, hdr_pre.ckpt_seq
    );
    drop(reader); // release the pin
    std::thread::sleep(Duration::from_millis(300));
    let trunc: rusqlite::Result<(i64, i64, i64)> =
        rw.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        });
    let wal_after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    eprintln!("truncate-after-release: {trunc:?} wal_len={wal_after}");
    let (st, _) = http_get(port, "/inc");
    assert_eq!(st, 200);
    let hdr_post = wal_hdr(&std::fs::read(&wal).unwrap()).unwrap();
    eprintln!(
        "salts post-truncate+write: salt1={:08x} salt2={:08x} ckpt_seq={} \
         (same_salt={})",
        hdr_post.salt1,
        hdr_post.salt2,
        hdr_post.ckpt_seq,
        hdr_post.salt1 == hdr_pre.salt1
    );

    drop(wd);
    eprintln!("== PoC complete: external WAL tailing is FEASIBLE");
}

// ---------- WAL1-r2 PoC: read-mark-0 + ckpt_seq semantics ----------
// SQLite-level facts (not workerd-specific) — runs un-ignored.

fn wal_hdr_ckpt_seq(p: &Path) -> (u32, u32, u32) {
    let b = std::fs::read(p).unwrap_or_default();
    if b.len() < 32 {
        return (0, 0, 0);
    }
    let g = |o: usize| u32::from_be_bytes(b[o..o + 4].try_into().unwrap());
    (g(12), g(16), g(20)) // ckpt_seq, salt1, salt2
}

#[test]
fn poc_read_mark0_and_ckpt_seq() {
    let dir = Path::new("target/tmp-wal1r2/poc-mark0");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let db = dir.join("m.sqlite");
    let wal = dir.join("m.sqlite-wal");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    c.execute_batch("CREATE TABLE t(x)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    // Fully backfill + truncate → WAL empty, all frames in main db.
    let r: (i32, i32, i32) = c
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    eprintln!("P6 truncate: {r:?}");
    let (ck0, s1a, s2a) = wal_hdr_ckpt_seq(&wal);
    eprintln!(
        "P6 hdr after truncate: ckpt_seq={ck0} salt={s1a:08x}/{s2a:08x} wal_len={}",
        std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0)
    );

    // P6: reader opens AFTER full backfill — does its mark block a
    // subsequent restart? (read-mark-0 suspicion)
    let pin = vrt_sql::wal::WalPin::acquire(&db).unwrap();
    let _pin_is_held = &pin;
    c.execute_batch("INSERT INTO t VALUES (2)").unwrap();
    // attempt a reset while the "pin" is held:
    let r2: (i32, i32, i32) = c
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    eprintln!("P6 truncate while mark0-pin held: {r2:?} (0,0,0 => pin did NOT block)");
    let wal_len = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    eprintln!("P6 wal_len after: {wal_len}");

    // Now pin while frames ARE present (mark > 0) — must block reset.
    c.execute_batch("INSERT INTO t VALUES (3)").unwrap();
    let pin2 = vrt_sql::wal::WalPin::acquire(&db).unwrap();
    c.execute_batch("INSERT INTO t VALUES (4)").unwrap();
    let r3: (i32, i32, i32) = c
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    eprintln!("P6 truncate while real-pin held: {r3:?} (busy>0 or partial => blocked)");
    drop(pin2);
    drop(pin);

    // P7: ckpt_seq semantics — one bump per checkpoint of ANY mode?
    let (ck_a, _, _) = wal_hdr_ckpt_seq(&wal);
    c.execute_batch("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let (ck_b, _, _) = wal_hdr_ckpt_seq(&wal);
    c.execute_batch("INSERT INTO t VALUES (5)").unwrap();
    c.execute_batch("PRAGMA wal_checkpoint(RESTART)").unwrap();
    let (ck_c, _, _) = wal_hdr_ckpt_seq(&wal);
    c.execute_batch("INSERT INTO t VALUES (6)").unwrap();
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let (ck_d, _s1b, s2b) = wal_hdr_ckpt_seq(&wal);
    eprintln!(
        "P7 ckpt_seq: {ck_a}->{ck_b}(passive)->{ck_c}(restart)->{ck_d}(truncate) salt2={s2b:08x}"
    );
    // Observed semantics: a no-op PASSIVE does NOT bump; RESTART +1;
    // after TRUNCATE the wal file is EMPTY (no header to read at all)
    // => ckpt_seq is NOT a reliable reset counter — a missing/short
    // header or salt change is the only trustworthy reset signal.
    assert_eq!(ck_b, ck_a, "no-op passive did not bump");
    assert_eq!(ck_c, ck_b + 1, "restart bumps ckpt_seq");
    assert_eq!(ck_d, 0, "truncate leaves empty wal (no header)");
}
