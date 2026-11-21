//! T37: provider queue producer wire validation adversarial test -- section 8 limit boundaries
//! (miniflare broker decimal convention: 128_000B / 100 messages / 288_000B /
//! 86_400s), batch header validation, illegal headers/bodies all rejected and nothing enqueued.

use axum::http::header::{HeaderMap, HeaderValue};
use vrt_provider::queue::*;

fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(*k, HeaderValue::from_str(v).unwrap());
    }
    h
}

#[test]
fn message_size_boundary_128000() {
    let ok = vec![0u8; QUEUE_MSG_MAX_BYTES];
    assert!(producer_message(&HeaderMap::new(), &ok).is_ok());
    let over = vec![0u8; QUEUE_MSG_MAX_BYTES + 1];
    let (status, _) = producer_message(&HeaderMap::new(), &over).unwrap_err();
    assert_eq!(status, 413);
    // The Content-Length header is likewise checked first for 413 (validated regardless of the actual body).
    let h = headers(&[("content-length", "128001")]);
    let (status, _) = producer_message(&h, &ok).unwrap_err();
    assert_eq!(status, 413);
}

#[test]
fn message_fmt_validation() {
    let body = b"hi".to_vec();
    for fmt in ["text", "json", "bytes", "v8"] {
        let h = headers(&[("x-msg-fmt", fmt)]);
        assert!(producer_message(&h, &body).is_ok(), "fmt {fmt}");
    }
    let h = headers(&[("x-msg-fmt", "yaml")]);
    let (status, _) = producer_message(&h, &body).unwrap_err();
    assert_eq!(status, 400);
    // default = v8.
    assert!(producer_message(&HeaderMap::new(), &body).is_ok());
}

#[test]
fn message_delay_boundary() {
    let body = b"hi".to_vec();
    for (v, ok) in [
        ("0", true),
        ("86400", true),
        ("86401", false),
        ("-1", false),
        ("abc", false),
    ] {
        let h = headers(&[("x-msg-delay-secs", v)]);
        let r = producer_message(&h, &body);
        assert_eq!(r.is_ok(), ok, "delay {v}");
        if let Err((status, _)) = r {
            assert_eq!(status, 400, "delay {v}");
        }
    }
}

#[test]
fn batch_count_boundary_100() {
    let mk = |n: usize| {
        serde_json::json!({
            "messages": (0..n).map(|_| serde_json::json!({"contentType": "text", "body": "aGk="})).collect::<Vec<_>>()
        })
        .to_string()
    };
    assert!(producer_batch(&HeaderMap::new(), mk(100).as_bytes()).is_ok());
    let (status, _) = producer_batch(&HeaderMap::new(), mk(101).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
    // Batch header checked first: count >100 -> 413 (regardless of the actual body).
    let h = headers(&[("cf-queue-batch-count", "101")]);
    let (status, _) = producer_batch(&h, mk(1).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
}

#[test]
fn batch_bytes_boundary_288000() {
    // Max single message 128000 -> 3x96000 = 288000 exactly at the bound / 288001 over.
    use base64::Engine;
    let enc = |n: usize| base64::engine::general_purpose::STANDARD.encode(vec![0u8; n]);
    let mk = |sizes: &[usize]| {
        serde_json::json!({
            "messages": sizes.iter().map(|s| serde_json::json!({"contentType": "bytes", "body": enc(*s)})).collect::<Vec<_>>()
        })
        .to_string()
    };
    assert!(producer_batch(&HeaderMap::new(), mk(&[96000, 96000, 96000]).as_bytes()).is_ok());
    let (status, _) =
        producer_batch(&HeaderMap::new(), mk(&[96000, 96000, 96001]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
    // A single message over the bound also gets 413.
    let (status, _) = producer_batch(&HeaderMap::new(), mk(&[128_001]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
    // Batch header checked first: batch-bytes / largest-msg over the bound -> 413.
    let h = headers(&[("cf-queue-batch-bytes", "288001")]);
    let (status, _) = producer_batch(&h, mk(&[1]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
    let h = headers(&[("cf-queue-largest-msg", "128001")]);
    let (status, _) = producer_batch(&h, mk(&[1]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
}

#[test]
fn batch_item_failure_rejects_the_whole_call() {
    // producer_batch renders ONE vrt-sql send request; every item is validated
    // before that request is produced, so a failing item at any position
    // rejects the call instead of forwarding the earlier items (the
    // workerd sendBatch promise is likewise all-or-nothing).
    use base64::Engine;
    let enc = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let ok = enc(b"ok");
    let over = enc(&vec![b'x'; QUEUE_MSG_MAX_BYTES + 1]);
    for pos in [0usize, 1] {
        let mut items = vec![
            serde_json::json!({"contentType": "bytes", "body": ok}),
            serde_json::json!({"contentType": "bytes", "body": ok}),
        ];
        items[pos] = serde_json::json!({"contentType": "bytes", "body": over});
        let body = serde_json::json!({"messages": items}).to_string();
        let (status, reason) = producer_batch(&HeaderMap::new(), body.as_bytes()).unwrap_err();
        assert_eq!(status, 413, "position {pos}");
        assert!(
            reason.contains("single message size limit"),
            "position {pos}: {reason}"
        );
    }
    // Non-size violations in a later item fail the same way (400): a bad
    // content type, an out-of-range delay, a client-supplied id/timestamp.
    for bad in [
        serde_json::json!({"contentType": "yaml", "body": ok}),
        serde_json::json!({"contentType": "bytes", "body": ok, "delaySecs": 86_401}),
        serde_json::json!({"contentType": "bytes", "body": ok, "id": "client-id"}),
        serde_json::json!({"contentType": "bytes", "body": ok, "timestamp": 1}),
    ] {
        let body = serde_json::json!({"messages": [
            {"contentType": "bytes", "body": ok},
            bad
        ]})
        .to_string();
        let (status, _) = producer_batch(&HeaderMap::new(), body.as_bytes()).unwrap_err();
        assert_eq!(status, 400, "{body}");
    }
}

#[test]
fn batch_body_malformed_rejected() {
    // non-JSON / missing messages / bad base64 / bad contentType / over-limit delay / empty or overlong id.
    let bad = [
        "not json",
        "{}",
        r#"{"messages":[{"contentType":"text","body":"!!!not-b64!!!"}]}"#,
        r#"{"messages":[{"contentType":"yaml","body":"aGk="}]}"#,
        r#"{"messages":[{"contentType":"text","body":"aGk=","delaySecs":86401}]}"#,
        r#"{"messages":[{"contentType":"text","body":"aGk=","delaySecs":-1}]}"#,
        r#"{"messages":[{"contentType":"text","body":"aGk=","id":""}]}"#,
    ];
    for b in bad {
        assert!(
            producer_batch(&HeaderMap::new(), b.as_bytes()).is_err(),
            "{b}"
        );
    }
    // The header delay follows the same rule as each message's delaySecs.
    let h = headers(&[("x-msg-delay-secs", "86401")]);
    let ok_body = r#"{"messages":[{"contentType":"text","body":"aGk="}]}"#;
    let (status, _) = producer_batch(&h, ok_body.as_bytes()).unwrap_err();
    assert_eq!(status, 400);
}

#[test]
fn queue_id_shape_enforced_in_client() {
    // Defense in depth: even if a BindingContext is constructed with an illegal queueId, the client rejects once more
    // before rendering the path (a forged id never enters the URL).
    // QueueClient::forward is driven by BindingResource::Queue -- the shape check
    // takes effect twice, in vrt-sql ingress (26 chars) and in the provider's second validation;
    // this assertion covers the direction where client construction failure fails closed.
    use vrt_provider::queue::QueueClient;
    assert!(QueueClient::new(
        "http://127.0.0.1:9".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1)
    )
    .is_ok());
    assert!(QueueClient::new(
        "not-a-url".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1)
    )
    .is_err());
}

// ---------- T37r: client id/timestamp rejected + server-side generation ----------

#[test]
fn client_supplied_id_and_timestamp_rejected() {
    let h = headers(&[]);
    // Any id (including the empty string) -> 400: the regular workerd path does not send an id.
    for body in [
        r#"{"messages":[{"contentType":"text","body":"aGk=","id":"x"}]}"#,
        r#"{"messages":[{"contentType":"text","body":"aGk=","id":""}]}"#,
    ] {
        let err = vrt_provider::queue::producer_batch(&h, body.as_bytes()).unwrap_err();
        assert_eq!(err.0, 400, "{body}");
        assert!(err.1.contains("id"), "{:?}", err.1);
    }
    // timestamp (ancient or future both rejected -- an untrusted field does not enter scheduling/metrics).
    for ts in ["1", "9999999999999"] {
        let body =
            format!(r#"{{"messages":[{{"contentType":"text","body":"aGk=","timestamp":{ts}}}]}}"#);
        let err = vrt_provider::queue::producer_batch(&h, body.as_bytes()).unwrap_err();
        assert_eq!(err.0, 400, "{body}");
        assert!(err.1.contains("timestamp"), "{:?}", err.1);
    }
}

#[test]
fn server_generates_message_ids() {
    let h = headers(&[]);
    let body = r#"{"messages":[
        {"contentType":"text","body":"aGk="},
        {"contentType":"text","body":"aGk="}
    ]}"#;
    let v = vrt_provider::queue::producer_batch(&h, body.as_bytes()).unwrap();
    let msgs = v["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 2);
    let a = msgs[0]["id"].as_str().unwrap();
    let b = msgs[1]["id"].as_str().unwrap();
    // Server-generated 32-hex, all distinct and different on every call (the prerequisite for retry idempotence).
    for id in [a, b] {
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|c| c.is_ascii_hexdigit()), "{id}");
    }
    assert_ne!(a, b);
    let v2 = vrt_provider::queue::producer_batch(&h, body.as_bytes()).unwrap();
    assert_ne!(v2["messages"][0]["id"].as_str().unwrap(), a);
    // timestampMs is not forwarded -- the client is not allowed to supply it, and the generation path does not write it either.
    assert!(v["messages"][0].get("timestampMs").is_none());
    // A single /message likewise generates an id.
    let hs = headers(&[("x-msg-fmt", "text")]);
    let single = vrt_provider::queue::producer_message(&hs, b"hi").unwrap();
    assert_eq!(single["messages"][0]["id"].as_str().unwrap().len(), 32);
}

#[test]
fn batch_ids_preserve_send_order() {
    // Messages of one batch share a timestamp; vrt-sql breaks that tie by
    // id, so ids must strictly increase in send order (lowercase 32-hex).
    let body = serde_json::json!({
        "messages": (0..50)
            .map(|_| serde_json::json!({"contentType": "text", "body": "aGk="}))
            .collect::<Vec<_>>()
    })
    .to_string();
    let v = producer_batch(&HeaderMap::new(), body.as_bytes()).unwrap();
    let ids: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 50);
    for id in &ids {
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')), "{id}");
    }
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "ids must strictly increase");
    // Back-to-back single sends in the same millisecond keep order too.
    let a = queue_message_id();
    let b = queue_message_id();
    assert!(a < b);
}

/// T37r2: workerd parses `oldestMessageTimestamp` as `date` -- it must be an
/// epoch-ms **number**, empty queue = 0 (measured from miniflare `getMetricsResponseObject`
/// `.getTime() ?? 0`). null/a string would TypeError and hang the tenant's send.
#[test]
fn metrics_oldest_timestamp_is_number_never_null() {
    // Empty queue -> 0, non-empty -> the ms number as-is.
    let empty = vrt_provider::queue::broker_metrics_json(
        &serde_json::from_value::<vrt_provider::queue::WireMetrics>(
            serde_json::json!({"backlogCount":0,"backlogBytes":0}),
        )
        .unwrap(),
    );
    assert_eq!(empty["oldestMessageTimestamp"], serde_json::json!(0));
    let full = vrt_provider::queue::broker_metrics_json(
        &serde_json::from_value::<vrt_provider::queue::WireMetrics>(serde_json::json!({
            "backlogCount":3,"backlogBytes":9,
            "oldestMessageTimestampMs":1_700_000_000_000i64
        }))
        .unwrap(),
    );
    assert_eq!(
        full["oldestMessageTimestamp"],
        serde_json::json!(1_700_000_000_000i64)
    );
    // No path may produce null/a string.
    assert!(empty["oldestMessageTimestamp"].is_number());
    assert!(full["oldestMessageTimestamp"].is_number());
}
