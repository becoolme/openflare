//! RV-R68：跨语言 wire 契约 fixture —— cell 侧真实输出。
//!
//! 每个 fixture 都是 REAL 序列化（serde 输出 / 真实 whitelist /
//! 真实 normalize_execute_body），不是手写对象：
//! - `cell-op-ack-*.json`：`CellOpAck` 的 serde_json 输出——
//!   `result`/`error` 的 `Option::None` 必须省略（绝不出现 null）；
//! - `cell-op-ack-done-legacy-null.json`：旧 cell 的真实线上形状
//!   （修复前的 serde 输出），控制面在滚动升级期间必须仍接受；
//! - `d1-ack-*.json`：已提交的 `d1-wire-*.json`（vrt-sql 真实
//!   输出）经 `normalize_execute_body` 规范化后包进 `CellOpAck`；
//! - `wf-ack-*.json`：`whitelist()` 对 facade 结果的真实规范化；
//! - `desired-ack-partial.json`：`AckReport` 的真实序列化
//!   （PARTIAL + bounded reason + capabilities）。
//!
//! `UPDATE_FIXTURES=1` 重写 `server/src/test/fixtures/vrt-wire/`；
//! 否则与已提交 fixture 做值级比对。`d1-ack-*` 依赖 `d1-wire-*`
//! 先行生成（`UPDATE_FIXTURES=1 cargo test -p vrt-sql`）。

use cell_controller::ack::{AckCapabilities, AckReport, AckStatus};
use cell_controller::d1ops::normalize_execute_body;
use cell_controller::execset::CellOpAck;
use cell_controller::wfops::whitelist;
use chrono::{TimeZone, Utc};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/vrt-wire")
}

fn check(name: &str, actual: Value) {
    let path = fixture_dir().join(name);
    if std::env::var("UPDATE_FIXTURES").is_ok() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&actual).unwrap()),
        )
        .unwrap();
        return;
    }
    let committed: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!("fixture {name} missing — run UPDATE_FIXTURES=1 (vrt-sql 先行)")
        }),
    )
    .unwrap();
    assert_eq!(committed, actual, "wire fixture {name} drifted");
}

fn read_committed(name: &str) -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(fixture_dir().join(name)).unwrap_or_else(|_| {
            panic!("fixture {name} missing — 先 UPDATE_FIXTURES=1 cargo test -p vrt-sql")
        }),
    )
    .unwrap()
}

/// `CellOpAck` 的 serde 输出 = cell 真实 POST body 的单条。
fn ack(seq: i64, status: &'static str, result: Option<Value>, error: Option<String>) -> Value {
    serde_json::to_value(CellOpAck {
        seq,
        status,
        result,
        error,
    })
    .unwrap()
}

#[test]
fn cell_op_ack_fixtures() {
    // DONE：result 有值、error 省略——线上绝不允许 "error":null
    // （顶层可选字段必须为 None 时省略；result 内部的 null 值合法）。
    let v = ack(7, "DONE", Some(json!({"status": "running"})), None);
    let s = serde_json::to_string(&v).unwrap();
    assert!(v.get("error").is_none(), "error:null leaked: {s}");
    assert!(v.get("result").is_some(), "result dropped: {s}");
    check("cell-op-ack-done.json", v);

    // FAILED：error 有值、result 省略。
    let v = ack(8, "FAILED", None, Some("boom".to_string()));
    check("cell-op-ack-failed.json", v);

    // EXPIRED。
    let v = ack(9, "EXPIRED", None, Some("dispatch deadline passed".to_string()));
    check("cell-op-ack-expired.json", v);

    // 旧 cell（修复前 serde 输出）会把 None 写成 null——该形状在
    // 滚动升级期间合法，控制面必须接受并按缺省处理。
    check(
        "cell-op-ack-done-legacy-null.json",
        json!({"seq": 7, "status": "DONE", "result": {"status": "running"}, "error": null}),
    );
    check(
        "cell-op-ack-failed-legacy-null.json",
        json!({"seq": 8, "status": "FAILED", "result": null, "error": "boom"}),
    );
}

#[test]
fn d1_ack_fixtures() {
    // 成功：vrt-sql 裸数组 → 规范化 {results:[...]} → DONE ack。
    for (wire, name) in [
        ("d1-wire-execute.json", "d1-ack-execute.json"),
        ("d1-wire-execute-batch.json", "d1-ack-execute-batch.json"),
    ] {
        let body = read_committed(wire);
        let result = normalize_execute_body(body);
        assert!(result.get("results").is_some(), "{wire} not normalized");
        check(name, ack(1, "DONE", Some(result), None));
    }

    // SQL 错误体（{success:false,error}）是引擎 OUTCOME → DONE +
    // 原样 result；控制面按既有契约 200 透传。
    let body = read_committed("d1-wire-sql-error.json");
    check(
        "d1-ack-sql-error.json",
        ack(2, "DONE", Some(normalize_execute_body(body)), None),
    );

    // fenced/unavailable：{status,body}——cell 把 typed body 作为
    // DONE result 透传（retryable/code 由控制面映射 HTTP 语义）。
    for (wire, name, seq) in [
        ("d1-wire-fenced.json", "d1-ack-fenced.json", 3),
        ("d1-wire-unavailable.json", "d1-ack-unavailable.json", 4),
    ] {
        let body = read_committed(wire).get("body").unwrap().clone();
        check(name, ack(seq, "DONE", Some(normalize_execute_body(body)), None));
    }
}

#[test]
fn wf_ack_fixtures() {
    // running：whitelist 对 facade 响应的真实投影。
    let out = whitelist(
        &json!({"status": "running", "output": "hello", "secret": "nope"}),
        &["status", "output", "error"],
    );
    check("wf-ack-running.json", ack(5, "DONE", Some(out), None));

    // errored：facade 的 {name,message} 对象经 whitelist 保留结构、
    // 双字段有界（修复前对象原样透传会被 strict schema 拒，WF_GET
    // 永远失败）。
    let out = whitelist(
        &json!({"status": "errored", "error": {"name": "WorkflowFatalError", "message": "step blew up", "extra": "dropped"}}),
        &["status", "output", "error"],
    );
    assert_eq!(out["error"]["name"], "WorkflowFatalError");
    assert_eq!(out["error"]["message"], "step blew up");
    assert!(out["error"].get("extra").is_none());
    check("wf-ack-errored-object.json", ack(6, "DONE", Some(out), None));
}

#[test]
fn desired_ack_fixture() {
    // PARTIAL：真实 AckReport 序列化（reason 有界、capabilities 全位）。
    let report = AckReport {
        revision: 7,
        payload_digest: "a".repeat(64),
        envelope_digest: "b".repeat(64),
        fence_revision: 5,
        status: AckStatus::Partial,
        reason: Some("pods-not-applied:pod-1,pod-2".to_string()),
        applied_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        // MIG-6: `Some` always emits the key — `[]` is the explicit
        // "no tenant failures" clear for this revision.
        tenant_failures: Some(Vec::new()),
        capabilities: Some(AckCapabilities {
            cron: true,
            assets: true,
            d1: true,
            queue: true,
            workflows: true,
            workflows_ops: true,
            d1_cell_ops: true,
            workflows_cross_worker: true,
            storage_purge: true,
            kv: false,
            r2: false,
        }),
    };
    check(
        "desired-ack-partial.json",
        serde_json::to_value(&report).unwrap(),
    );

    // MIG-4: tenantFailures 的真实线形——具名 workerIds 与省略
    // workerIds（tenant-wide）两种形状都必须固定；具名集合绝不出现
    // 空数组，服务端以字段缺席判定 tenant-wide。
    use cell_controller::pod::TenantFailure;
    let mut named = report.clone();
    named.status = AckStatus::Partial;
    named.reason = Some("tenant-failed".to_string());
    named.tenant_failures = Some(vec![
        TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w-bad".to_string(), "w-also-bad".to_string()],
            code: "config-load".to_string(),
            detail: Some("top-level error".to_string()),
        },
        TenantFailure {
            tenant: "team-b".to_string(),
            worker_ids: Vec::new(),
            code: "switch-failed".to_string(),
            detail: None,
        },
    ]);
    let v = serde_json::to_value(&named).unwrap();
    assert_eq!(v["tenantFailures"][0]["workerIds"], json!(["w-bad", "w-also-bad"]));
    assert!(
        v["tenantFailures"][1].get("workerIds").is_none(),
        "tenant-wide failure must omit workerIds entirely"
    );
    check("desired-ack-tenant-failures.json", v);

    // MIG-6: `Some([])` 显式序列化为 `[]` —— 服务端把它持久化为
    // 该 revision 的空诊断集（覆盖 PARTIAL 阶段的残留）；`None`
    // 才从 wire 上消失（pre-MIG-1 控制面 400 降级专用）。
    let mut empty = report.clone();
    empty.tenant_failures = Some(Vec::new());
    assert_eq!(serde_json::to_value(&empty).unwrap()["tenantFailures"], json!([]));
    let mut stripped = report.clone();
    stripped.tenant_failures = None;
    assert!(serde_json::to_value(&stripped).unwrap().get("tenantFailures").is_none());

    // reason 超过 256 UTF-16 单元必须被截断——否则整条 ack 被
    // 服务端 400 拒收、converge 永远卡住（RV-R68 配套修复）。
    let long = "pods-not-applied:".to_string() + &"pod-x,".repeat(200);
    let bounded = cell_controller::ack::bounded_reason(Some(long));
    assert!(bounded.unwrap().encode_utf16().count() <= 256);
}

#[test]
fn truncation_is_utf16_units_and_never_splits_pairs() {
    use cell_controller::execset::truncate_utf16;
    // ~40k emoji（2 UTF-16 单元/字符）：控制面 zod `.max` 按
    // JS `.length`（UTF-16 单元）计数，两侧度量必须一致——旧实现
    // 按 Unicode 标量截断会放行超额两倍的 payload。
    let emoji = "😀".repeat(40_000);
    let (out, truncated) = truncate_utf16(&emoji, 64 * 1024);
    assert!(truncated);
    assert_eq!(out.encode_utf16().count(), 64 * 1024); // 恰好满界
    assert_eq!(out.chars().count(), 32_768); // 恰好整字 —— 代理对绝不拆开
    // 奇数边界：下一字符是 2 单元 → 不取，输出不满界但绝不残缺。
    let (out, truncated) = truncate_utf16("ab😀cd", 3);
    assert!(truncated);
    assert_eq!(out, "ab");
    // CJK（1 UTF-16 单元/字符）与 ASCII 同权。
    let (out, truncated) = truncate_utf16("你好世界", 3);
    assert!(truncated);
    assert_eq!(out, "你好世");
}

/// `kv` is omitted while false (old strict control planes keep parsing
/// acks) and present once claimed; an absent field parses as false.
#[test]
fn kv_capability_wire_shape() {
    let off = serde_json::to_value(AckCapabilities::default()).unwrap();
    assert!(off.get("kv").is_none(), "kv must be omitted while false");
    let on = serde_json::to_value(AckCapabilities {
        kv: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(on["kv"], serde_json::json!(true));
    // R2 object data ops: same omit-while-false rolling-upgrade shape.
    assert!(off.get("r2").is_none(), "r2 must be omitted while false");
    let r2_on = serde_json::to_value(AckCapabilities {
        r2: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(r2_on["r2"], serde_json::json!(true));
    assert!(r2_on.get("kv").is_none());
    let back: AckCapabilities =
        serde_json::from_value(serde_json::json!({"cron": true, "assets": true})).unwrap();
    assert!(!back.kv);
}
