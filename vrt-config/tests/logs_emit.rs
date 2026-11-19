//! Worker log collection: every tenant worker gets a platform tail
//! worker (`tails = [...]`), the shared `vrt-logsink` external is
//! emitted once, platform/system workers never get a tail, and the
//! disabled default renders byte-identically to the legacy output.

mod common;

use common::*;
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

fn enabled_params() -> RuntimeParams {
    let mut p = runtime_params();
    p.log_tail = LogTailParams {
        sink_addr: "127.0.0.1:9494".into(),
        pepper: "pepper-1".into(),
    };
    p
}

#[test]
fn logs_disabled_emits_nothing() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("vrt-tail"), "{}", r.text);
    assert!(!r.text.contains("tails ="), "{}", r.text);
    assert!(!r.text.contains("vrt-logsink"), "{}", r.text);
    assert!(r.external_addrs.iter().all(|a| a.service != "vrt-logsink"));
}

#[test]
fn logs_every_tenant_worker_has_tail_and_one_sink() {
    let t = tenant();
    let params = enabled_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;
    assert_eq!(
        out.matches("name = \"vrt-logsink\", external = (").count(),
        1,
        "{out}"
    );
    for w in &t.workers {
        let id = &w.worker_id;
        assert!(
            out.contains(&format!("tails = [(name = \"vrt-tail-{id}\")]")),
            "worker {id} missing tails\n{out}"
        );
        assert!(
            out.contains(&format!("(name = \"vrt-tail-{id}\", worker = (")),
            "tail service for {id} missing"
        );
        // Identity + token bound from config, token == HMAC of id.
        let token = log_token("pepper-1", id);
        assert!(out.contains(&format!("(name = \"WORKER_ID\", text = \"{id}\")")));
        assert!(out.contains(&format!("(name = \"TOKEN\", text = \"{token}\")")));
    }
    assert_eq!(out.matches("tails = [").count(), t.workers.len());
    let addr = r
        .external_addrs
        .iter()
        .find(|a| a.service == "vrt-logsink")
        .expect("sink addr");
    assert_eq!(addr.address, "127.0.0.1:9494");
}

#[test]
fn logs_tail_workers_never_tail_themselves_or_system_workers() {
    let t = tenant();
    let params = enabled_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    // Tail service blocks carry no `tails =` and use deny-all outbound.
    for w in &t.workers {
        let id = &w.worker_id;
        let start = r
            .text
            .find(&format!("(name = \"vrt-tail-{id}\", worker = ("))
            .unwrap();
        let rest = &r.text[start..];
        let end = rest.find("\n    )),").unwrap();
        let block = &rest[..end];
        assert!(!block.contains("tails ="), "{block}");
        assert!(
            block.contains(&format!(
                "globalOutbound = (name = \"{}\", entrypoint = \"{}\")",
                params.deny_outbound.service, params.deny_outbound.entrypoint
            )),
            "{block}"
        );
        // Only the sink service binding exists.
        assert_eq!(block.matches("service =").count(), 1, "{block}");
    }
    // Number of `tails =` equals the tenant worker count: system
    // workers (facades, egress, router) carry none.
    assert_eq!(r.text.matches("tails = [").count(), t.workers.len());
}

#[test]
fn logs_tokens_are_per_worker_and_per_pepper() {
    assert_ne!(log_token("p", "w1"), log_token("p", "w2"));
    assert_ne!(log_token("p", "w1"), log_token("q", "w1"));
    assert_eq!(log_token("p", "w1"), log_token("p", "w1"));
    assert_eq!(log_token("p", "w1").len(), 64);
}

#[test]
fn logs_sink_without_pepper_fails_closed() {
    let t = tenant();
    let mut params = enabled_params();
    params.log_tail.pepper = String::new();
    assert!(render_tenant_config(&input(&t, &bs(), &params)).is_err());
}

#[test]
fn logs_render_is_deterministic() {
    let t = tenant();
    let params = enabled_params();
    let a = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let b = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert_eq!(a.digest, b.digest);
}
