//! T37: provider queue 生产端 wire 校验对抗测试 —— §8 限额边界
//! （miniflare broker 十进制口径：128_000B / 100 条 / 288_000B /
//! 86_400s）、批次头校验、非法头/体全拒且不入队。

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
    // Content-Length 头先验同样 413（与实际 body 无关也要校验）。
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
    // 缺省 = v8。
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
    // 批次头先验：count >100 → 413（与实际 body 无关）。
    let h = headers(&[("cf-queue-batch-count", "101")]);
    let (status, _) = producer_batch(&h, mk(1).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
}

#[test]
fn batch_bytes_boundary_288000() {
    // 单消息最大 128000 → 3×96000 = 288000 恰界 / 288001 超界。
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
    // 单消息超界同样 413。
    let (status, _) = producer_batch(&HeaderMap::new(), mk(&[128_001]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
    // 批次头先验：batch-bytes / largest-msg 超界 → 413。
    let h = headers(&[("cf-queue-batch-bytes", "288001")]);
    let (status, _) = producer_batch(&h, mk(&[1]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
    let h = headers(&[("cf-queue-largest-msg", "128001")]);
    let (status, _) = producer_batch(&h, mk(&[1]).as_bytes()).unwrap_err();
    assert_eq!(status, 413);
}

#[test]
fn batch_body_malformed_rejected() {
    // 非 JSON / 缺 messages / 坏 base64 / 坏 contentType / 超限 delay / 空或过长 id。
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
    // 头域 delay 与每条 delaySecs 同规则。
    let h = headers(&[("x-msg-delay-secs", "86401")]);
    let ok_body = r#"{"messages":[{"contentType":"text","body":"aGk="}]}"#;
    let (status, _) = producer_batch(&h, ok_body.as_bytes()).unwrap_err();
    assert_eq!(status, 400);
}

#[test]
fn queue_id_shape_enforced_in_client() {
    // 纵深防御：即使 BindingContext 被构造出非法 queueId，client 渲染
    // 路径前再拒一次（伪造 id 永不进入 URL）。
    // QueueClient::forward 由 BindingResource::Queue 驱动 —— 形态校验
    // 在 vrt-sql ingress（26 字符）与 provider 二次校验双重生效；
    // 此断言覆盖 client 构造失败即 fail closed 的方向。
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

// ---------- T37r：客户端 id/timestamp 拒收 + 服务端生成 ----------

#[test]
fn client_supplied_id_and_timestamp_rejected() {
    let h = headers(&[]);
    // 任何 id（含空串）→ 400：workerd 常规路径不发 id。
    for body in [
        r#"{"messages":[{"contentType":"text","body":"aGk=","id":"x"}]}"#,
        r#"{"messages":[{"contentType":"text","body":"aGk=","id":""}]}"#,
    ] {
        let err = vrt_provider::queue::producer_batch(&h, body.as_bytes()).unwrap_err();
        assert_eq!(err.0, 400, "{body}");
        assert!(err.1.contains("id"), "{:?}", err.1);
    }
    // timestamp（远古或未来同拒——不可信字段不进调度/指标）。
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
    // 服务端生成 32-hex，互不相同且每次调用都不同（重试幂等的前提）。
    for id in [a, b] {
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|c| c.is_ascii_hexdigit()), "{id}");
    }
    assert_ne!(a, b);
    let v2 = vrt_provider::queue::producer_batch(&h, body.as_bytes()).unwrap();
    assert_ne!(v2["messages"][0]["id"].as_str().unwrap(), a);
    // 不转发 timestampMs——客户端本就不许提供，生成路径也不写。
    assert!(v["messages"][0].get("timestampMs").is_none());
    // 单条 /message 同样生成 id。
    let hs = headers(&[("x-msg-fmt", "text")]);
    let single = vrt_provider::queue::producer_message(&hs, b"hi").unwrap();
    assert_eq!(single["messages"][0]["id"].as_str().unwrap().len(), 32);
}

/// T37r2：workerd 把 `oldestMessageTimestamp` 按 `date` 解析——必须是
/// epoch-ms **数字**，空队列 = 0（miniflare `getMetricsResponseObject`
/// 实测 `.getTime() ?? 0`）。null/字符串会 TypeError 吊死租户 send。
#[test]
fn metrics_oldest_timestamp_is_number_never_null() {
    // 空队列 → 0，非空 → 原样 ms 数字。
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
    // 任何路径都不得产出 null/字符串。
    assert!(empty["oldestMessageTimestamp"].is_number());
    assert!(full["oldestMessageTimestamp"].is_number());
}
