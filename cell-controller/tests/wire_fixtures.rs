//! RV-R68: cross-language wire contract fixtures -- the cell side's real output.
//!
//! Every fixture is REAL serialization (serde output / the real whitelist /
//! the real normalize_execute_body), not a hand-written object:
//! - `cell-op-ack-*.json`: serde_json output of `CellOpAck` --
//!   `Option::None` for `result`/`error` must be omitted (never null);
//! - `cell-op-ack-done-legacy-null.json`: the real on-the-wire shape of an old cell
//!   (serde output before the fix); the control plane must still accept it during a rolling upgrade;
//! - `d1-ack-*.json`: the committed `d1-wire-*.json` (real vrt-sql
//!   output) normalized by `normalize_execute_body` and wrapped into `CellOpAck`;
//! - `wf-ack-*.json`: the real normalization of facade results by `whitelist()`;
//! - `desired-ack-partial.json`: the real serialization of `AckReport`
//!   (PARTIAL + bounded reason + capabilities).
//!
//! `UPDATE_FIXTURES=1` rewrites `server/src/test/fixtures/vrt-wire/`;
//! otherwise values are compared against the committed fixtures. `d1-ack-*` depends on `d1-wire-*`
//! being generated first (`UPDATE_FIXTURES=1 cargo test -p vrt-sql`).

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
            panic!("fixture {name} missing — run UPDATE_FIXTURES=1 (vrt-sql first)")
        }),
    )
    .unwrap();
    assert_eq!(committed, actual, "wire fixture {name} drifted");
}

fn read_committed(name: &str) -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(fixture_dir().join(name)).unwrap_or_else(|_| {
            panic!("fixture {name} missing — first run UPDATE_FIXTURES=1 cargo test -p vrt-sql")
        }),
    )
    .unwrap()
}

/// The serde output of `CellOpAck` = a single entry of the cell's real POST body.
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
    // DONE: result has a value, error omitted -- "error":null is never allowed on the wire
    // (top-level optional fields must be omitted when None; null values inside result are legal).
    let v = ack(7, "DONE", Some(json!({"status": "running"})), None);
    let s = serde_json::to_string(&v).unwrap();
    assert!(v.get("error").is_none(), "error:null leaked: {s}");
    assert!(v.get("result").is_some(), "result dropped: {s}");
    check("cell-op-ack-done.json", v);

    // FAILED: error has a value, result omitted.
    let v = ack(8, "FAILED", None, Some("boom".to_string()));
    check("cell-op-ack-failed.json", v);

    // EXPIRED.
    let v = ack(9, "EXPIRED", None, Some("dispatch deadline passed".to_string()));
    check("cell-op-ack-expired.json", v);

    // An old cell (pre-fix serde output) writes None as null -- that shape is
    // legal during a rolling upgrade, and the control plane must accept it and treat it as default.
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
    // Success: vrt-sql bare array -> normalized {results:[...]} -> DONE ack.
    for (wire, name) in [
        ("d1-wire-execute.json", "d1-ack-execute.json"),
        ("d1-wire-execute-batch.json", "d1-ack-execute-batch.json"),
    ] {
        let body = read_committed(wire);
        let result = normalize_execute_body(body);
        assert!(result.get("results").is_some(), "{wire} not normalized");
        check(name, ack(1, "DONE", Some(result), None));
    }

    // The SQL error body ({success:false,error}) is an engine OUTCOME -> DONE +
    // result verbatim; the control plane passes it through as 200 per the existing contract.
    let body = read_committed("d1-wire-sql-error.json");
    check(
        "d1-ack-sql-error.json",
        ack(2, "DONE", Some(normalize_execute_body(body)), None),
    );

    // fenced/unavailable: {status,body} -- the cell passes the typed body through as the
    // DONE result (retryable/code are mapped to HTTP semantics by the control plane).
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
    // running: the real projection of the facade response by whitelist.
    let out = whitelist(
        &json!({"status": "running", "output": "hello", "secret": "nope"}),
        &["status", "output", "error"],
    );
    check("wf-ack-running.json", ack(5, "DONE", Some(out), None));

    // errored: the facade's {name,message} object keeps its structure through the whitelist,
    // both fields bounded (before the fix the object was passed through verbatim and rejected by the strict schema, so WF_GET
    // always failed).
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
    // PARTIAL: real AckReport serialization (reason bounded, capabilities all bits).
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
            service_entrypoint: true,
            worker_loader: true,
            quotas: true,
            analytics_engine: true,
            tail_consumers: true,
            version_metadata: true,
            service_binding_props: true,
        }),
    };
    check(
        "desired-ack-partial.json",
        serde_json::to_value(&report).unwrap(),
    );

    // MIG-4: the real wire shape of tenantFailures -- both the named workerIds and the omitted
    // workerIds (tenant-wide) shapes must be pinned; a named set never appears as an
    // empty array, and the server decides tenant-wide by the field's absence.
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

    // MIG-6: `Some([])` is explicitly serialized as `[]` -- the server persists it as
    // the empty diagnostic set of that revision (overriding leftovers from the PARTIAL phase); `None`
    // is the one that disappears from the wire (used only for the pre-MIG-1 control plane 400 downgrade).
    let mut empty = report.clone();
    empty.tenant_failures = Some(Vec::new());
    assert_eq!(serde_json::to_value(&empty).unwrap()["tenantFailures"], json!([]));
    let mut stripped = report.clone();
    stripped.tenant_failures = None;
    assert!(serde_json::to_value(&stripped).unwrap().get("tenantFailures").is_none());

    // A reason over 256 UTF-16 units must be truncated -- otherwise the whole ack is
    // rejected with a server 400 and converge is stuck forever (companion fix of RV-R68).
    let long = "pods-not-applied:".to_string() + &"pod-x,".repeat(200);
    let bounded = cell_controller::ack::bounded_reason(Some(long));
    assert!(bounded.unwrap().encode_utf16().count() <= 256);
}

#[test]
fn truncation_is_utf16_units_and_never_splits_pairs() {
    use cell_controller::execset::truncate_utf16;
    // ~40k emoji (2 UTF-16 units per char): the control plane zod `.max` counts by
    // JS `.length` (UTF-16 units), and both sides must measure the same -- the old implementation
    // truncating by Unicode scalar would let through a payload twice over the limit.
    let emoji = "😀".repeat(40_000);
    let (out, truncated) = truncate_utf16(&emoji, 64 * 1024);
    assert!(truncated);
    assert_eq!(out.encode_utf16().count(), 64 * 1024); // exactly at the limit
    assert_eq!(out.chars().count(), 32_768); // exactly whole chars -- surrogate pairs are never split
    // Odd boundary: the next char is 2 units -> not taken, output is under the limit but never mangled.
    let (out, truncated) = truncate_utf16("ab😀cd", 3);
    assert!(truncated);
    assert_eq!(out, "ab");
    // CJK (1 UTF-16 unit per char) weighs the same as ASCII.
    let (out, truncated) = truncate_utf16("\u{4f60}\u{597d}\u{4e16}\u{754c}", 3);
    assert!(truncated);
    assert_eq!(out, "\u{4f60}\u{597d}\u{4e16}");
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
    // Named service entrypoints: same omit-while-false shape.
    assert!(off.get("serviceEntrypoint").is_none());
    let ep_on = serde_json::to_value(AckCapabilities {
        service_entrypoint: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(ep_on["serviceEntrypoint"], serde_json::json!(true));
    // WorkerLoader bindings: same omit-while-false shape.
    assert!(off.get("workerLoader").is_none());
    let wl_on = serde_json::to_value(AckCapabilities {
        worker_loader: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(wl_on["workerLoader"], serde_json::json!(true));
    // Per-worker quotas: same omit-while-false shape — a cell on a stock
    // workerd must never make the control plane admit declared limits.
    assert!(off.get("quotas").is_none());
    let q_on = serde_json::to_value(AckCapabilities {
        quotas: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(q_on["quotas"], serde_json::json!(true));
    // Analytics Engine: same omit-while-false shape — a pre-AE cell
    // would reject the binding field, so dataset bindings only land on
    // cells that claim it.
    assert!(off.get("analyticsEngine").is_none());
    let ae_on = serde_json::to_value(AckCapabilities {
        analytics_engine: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(ae_on["analyticsEngine"], serde_json::json!(true));
    // Tail consumers: same omit-while-false shape.
    assert!(off.get("tailConsumers").is_none());
    let tc_on = serde_json::to_value(AckCapabilities {
        tail_consumers: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(tc_on["tailConsumers"], serde_json::json!(true));
    // `versionMetadata` (json bindings): same omit-while-false shape.
    assert!(off.get("versionMetadata").is_none());
    let vm_on = serde_json::to_value(AckCapabilities {
        version_metadata: true,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(vm_on["versionMetadata"], serde_json::json!(true));
    let back: AckCapabilities =
        serde_json::from_value(serde_json::json!({"cron": true, "assets": true})).unwrap();
    assert!(!back.kv);
    assert!(!back.service_entrypoint);
    assert!(!back.worker_loader);
    assert!(!back.quotas);
    assert!(!back.analytics_engine);
    assert!(!back.version_metadata);
    assert!(!back.tail_consumers);
}
