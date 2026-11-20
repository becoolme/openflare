//! golden gate (section 9): replay the same matrix against vrt-sql and the miniflare fixture, then compare after normalization.
//! Re-record: node vrt-sql/golden/capture.mjs (miniflare 5.20260828.0-alpha).

use serde_json::Value;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use vrt_sql::wire::{self, AppState, TrustedCallCtx};

fn tctx() -> TrustedCallCtx {
    TrustedCallCtx::test(Instant::now() + Duration::from_secs(30))
}

const KEY: [u8; 32] = [9u8; 32];
const DBID: &str = "01jqqm3b4y6zq8f0v2w5x7k9ae";

fn fixture() -> Value {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden/fixtures/d1-golden.json");
    serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
}

/// Normalize: drop dynamic fields that necessarily differ across implementations.
/// - meta.duration / served_by / size_after: implementation-specific (approximate semantics, section 10).
/// - meta.rows_read / rows_written: approximate algorithm, the engine side counts differently.
/// - error text goes through the DIFF_ERROR allowlist entry by entry.
fn normalize_body(b: &mut Value) {
    if let Some(arr) = b.as_array_mut() {
        for r in arr {
            if let Some(m) = r.get_mut("meta").and_then(|m| m.as_object_mut()) {
                for k in [
                    "duration",
                    "served_by",
                    "size_after",
                    "rows_read",
                    "rows_written",
                ] {
                    m.remove(k);
                }
            }
        }
    }
}

/// error text allowlist: fixture (miniflare better-sqlite3) -> vrt-sql (rusqlite) prefix.
/// better-sqlite3 appends an `: SQLITE_*` extended code after the text and rusqlite does not -- this is
/// client-irrelevant information difference; D1 clients only use the success/error shape.
const DIFF_ERROR: &[(&str, &str)] = &[
    ("no such table: nope", "no such table: nope"),
    (
        "UNIQUE constraint failed: uq.x",
        "UNIQUE constraint failed: uq.x",
    ),
    (
        "no such table: missing_table",
        "no such table: missing_table",
    ),
    // comment-only statement (a leading `--` is not stripped): better-sqlite3 reports an empty statement,
    // rusqlite prepare returns SQLITE_MISUSE -- same semantics, different text.
    (
        "SQL code did not contain a statement",
        "bad parameter or other API misuse",
    ),
];

/// Malformed-request surface: miniflare emits 500 + plain-text ZodError; vrt-sql uses 400 + JSON.
/// The key R1 contract is "not the {success:false} surface"; the HTTP code is implementation freedom.
const MALFORMED_CASES: &[&str] = &["malformed-sql-type", "malformed-scalar"];

#[tokio::test]
async fn golden_compare() {
    let fx = fixture();
    let tmp = tempfile::tempdir().unwrap();
    let st = AppState::new(tmp.path().join("dbs"), KEY);
    let mut mismatches: Vec<String> = Vec::new();

    // replay the same base data as capture.mjs
    for s in fx["setup"].as_array().unwrap() {
        let body = serde_json::to_vec(&s["body"]).unwrap();
        let r = wire::handle_d1(&st, DBID, None, tctx(), &body).await;
        assert_eq!(r.status.as_u16(), 200, "setup failed: {:?}", r.body);
    }

    for case in fx["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let path = case["path"].as_str().unwrap();
        // path "/query?resultsFormat=X" → (endpoint, format)
        let (ep, fmt) = path.split_once('?').map_or((path, None), |(e, qs)| {
            let f = qs.strip_prefix("resultsFormat=");
            (e, f)
        });
        assert!(ep == "/query" || ep == "/execute", "{name}: bad path {ep}");
        let req_body = serde_json::to_vec(&case["requestBody"]).unwrap();
        let got = wire::handle_d1(&st, DBID, fmt, tctx(), &req_body).await;

        let exp_status = case["status"].as_u64().unwrap() as u16;
        let exp_bm = case["bookmarkHeader"].as_bool().unwrap();
        let exp_body = &case["bodyJson"];

        if MALFORMED_CASES.contains(&name) {
            // malformed surface: non-200 and not the {success:false} shape satisfies the contract (code allowlist difference)
            if got.status == axum::http::StatusCode::OK || got.body["success"] == false {
                mismatches.push(format!(
                    "{name}: malformed should not be 200/{{success:false}}: {:?}",
                    got.body
                ));
            }
            continue;
        }

        if got.status.as_u16() != exp_status {
            mismatches.push(format!("{name}: status {} != {exp_status}", got.status));
            continue;
        }
        if got.bookmark.is_some() != exp_bm {
            mismatches.push(format!("{name}: bookmark {exp_bm} != {:?}", got.bookmark));
        }

        let mut got_body = got.body.clone();
        normalize_body(&mut got_body);
        let mut exp_body_norm = exp_body.clone();
        normalize_body(&mut exp_body_norm);

        // error surface: text goes through the allowlist
        if got_body["success"] == false {
            let exp_err = exp_body_norm["error"].as_str().unwrap_or("");
            let got_err = got_body["error"].as_str().unwrap_or("");
            if exp_err != got_err {
                let ok = DIFF_ERROR
                    .iter()
                    .any(|(e, g)| exp_err.starts_with(e) && got_err.starts_with(g))
                    || (exp_err.starts_with("No SQL statements detected.")
                        && got_err.starts_with("No SQL statements detected."))
                    || (exp_err.contains("syntax error") && got_err.contains("syntax error"))
                    || (exp_err.contains("did not contain a statement")
                        && got_err.contains("did not contain a statement"));
                if !ok {
                    mismatches.push(format!("{name}: error '{got_err}' != '{exp_err}'"));
                }
            }
            continue;
        }

        if got_body != exp_body_norm {
            mismatches.push(format!(
                "{name}: body mismatch\n got: {}\n exp: {}",
                serde_json::to_string_pretty(&got_body).unwrap(),
                serde_json::to_string_pretty(&exp_body_norm).unwrap()
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "golden mismatches:\n{}",
        mismatches.join("\n---\n")
    );
}
