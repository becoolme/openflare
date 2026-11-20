//! `vrt logs` client: tail_once against a stub control listener and
//! the follow loop's termination paths (max_polls + stop flag).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use vrt_local::logtail::{logs_run, tail_once, LogsOpts};

/// One-shot HTTP server: reads one request, replies with `body`.
fn stub_server(body: &'static str, status: &'static str) -> (String, Arc<AtomicUsize>) {
    let lis = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    std::thread::spawn(move || {
        while let Ok((mut s, _)) = lis.accept() {
            hits2.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0u8; 8192];
            let _ = s.read(&mut buf);
            let resp = format!(
                "HTTP/1.0 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(resp.as_bytes());
        }
    });
    (addr, hits)
}

const TAIL_BODY: &str = r#"{"bootId":"b1","records":[{"seq":7,"workerId":"w1","tsMs":1700000000000,"level":"error","kind":"exception","message":"boom","status":500,"truncated":false}],"nextSeq":7}"#;

#[test]
fn tail_once_parses_records_and_cursor() {
    let (addr, hits) = stub_server(TAIL_BODY, "200 OK");
    let r = tail_once(&addr, "w1", None, 0, 100).unwrap().unwrap();
    assert_eq!(r.records.len(), 1);
    assert_eq!(r.records[0].message, "boom");
    assert_eq!(r.records[0].level, "error");
    assert_eq!(r.next_seq, 7);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn tail_once_404_is_none_not_error() {
    let (addr, _) = stub_server("{\"error\":{\"code\":\"unknown_worker\"}}", "404 Not Found");
    let r = tail_once(&addr, "ghost", None, 0, 100).unwrap();
    assert!(r.is_none());
}

#[test]
fn tail_once_error_status_is_typed() {
    let (addr, _) = stub_server("{}", "500 Internal Server Error");
    assert!(tail_once(&addr, "w1", None, 0, 100).is_err());
}

#[test]
fn logs_run_prints_and_terminates_on_max_polls() {
    let (addr, hits) = stub_server(TAIL_BODY, "200 OK");
    let mut out: Vec<u8> = Vec::new();
    let opts = LogsOpts {
        follow: true,
        color: false,
        poll: std::time::Duration::from_millis(10),
        max_polls: Some(3),
        ..Default::default()
    };
    logs_run(&mut out, &addr, "w1", &opts).unwrap();
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("ERROR"), "got: {text}");
    assert!(text.contains("boom"), "got: {text}");
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

#[test]
fn logs_run_json_mode_emits_one_object_per_record() {
    let (addr, _) = stub_server(TAIL_BODY, "200 OK");
    let mut out: Vec<u8> = Vec::new();
    let opts = LogsOpts {
        json: true,
        ..Default::default()
    };
    logs_run(&mut out, &addr, "w1", &opts).unwrap();
    let text = String::from_utf8_lossy(&out);
    let line = text.lines().next().unwrap();
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(v["message"], "boom");
    assert_eq!(v["level"], "error");
}

#[test]
fn logs_run_stop_flag_ends_follow() {
    let (addr, _) = stub_server(TAIL_BODY, "200 OK");
    let mut out: Vec<u8> = Vec::new();
    let stop = Arc::new(AtomicBool::new(true));
    let opts = LogsOpts {
        follow: true,
        poll: std::time::Duration::from_millis(10),
        stop: Some(stop),
        // no max_polls — the flag alone must end the loop
        ..Default::default()
    };
    logs_run(&mut out, &addr, "w1", &opts).unwrap();
}

#[test]
fn logs_run_level_filter_suppresses_other_levels() {
    let (addr, _) = stub_server(TAIL_BODY, "200 OK");
    let mut out: Vec<u8> = Vec::new();
    let opts = LogsOpts {
        level: Some("warn".into()),
        ..Default::default()
    };
    logs_run(&mut out, &addr, "w1", &opts).unwrap();
    assert!(out.is_empty(), "error record must not pass --level warn");
}
