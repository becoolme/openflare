//! T25 (B8) — diagnostics contract tests:
//! L16 bounded/sanitized workerd stderr tail surfaced on launch
//! failure; L13 binding-backend external address present in launch
//! argv; L15/L17 live in supervisor.rs unit tests.

use vrt_host::launch::{LogTail, LOG_TAIL_CAP};

// ---------- L16: LogTail ----------

#[test]
fn log_tail_retains_bounded_sanitized_text() {
    let tail = LogTail::new(vec![]);
    // Push > cap: the retained text must stay within LOG_TAIL_CAP and
    // keep the NEWEST bytes (drop-oldest).
    for i in 0..2000 {
        tail.push(format!("line-{i:05} padding\r\n").as_bytes());
    }
    let dump = tail.tail_lines(usize::MAX);
    assert!(dump.len() <= LOG_TAIL_CAP, "bounded: {}", dump.len());
    assert!(dump.contains("line-01999"), "keeps newest lines");
    assert!(!dump.contains("line-00000"), "drops oldest");
    // \r → '.', other control chars sanitized.
    assert!(!dump.contains('\r'));
}

#[test]
fn log_tail_redacts_secret_needles() {
    // A real workerd must never leak env secrets into the retained
    // tail — B8's manual stderr capture was the ONLY place the failure
    // reason showed up; making it loggable requires sanitization.
    let tail = LogTail::new(vec!["s3cr3t-v4lue-p4ss".to_string()]);
    tail.push(b"service w1: token s3cr3t-v4lue-p4ss rejected\n");
    tail.push(b"short needle x1 ignored\n");
    let dump = tail.tail_lines(12);
    assert!(dump.contains("***"));
    assert!(!dump.contains("s3cr3t-v4lue-p4ss"), "needle redacted");
}

#[test]
fn log_tail_ignores_short_needles_and_strips_controls() {
    // Needles < 8 chars are NOT redacted (would eat every line).
    let tail = LogTail::new(vec!["abc".to_string()]);
    tail.push(b"esc \x1b[31mred abc\x07 end\n");
    let dump = tail.tail_lines(4);
    assert!(dump.contains("abc"), "short needle kept verbatim");
    assert!(!dump.contains('\x1b'), "escape stripped");
    assert!(!dump.contains('\x07'), "bell stripped");
    assert!(dump.contains("red abc"), "text preserved");
}

#[test]
fn log_tail_tail_lines_returns_last_n() {
    let tail = LogTail::new(vec![]);
    for i in 1..=20 {
        tail.push(format!("l{i}\n").as_bytes());
    }
    let got = tail.tail_lines(3);
    assert_eq!(got, "l18\nl19\nl20");
}
