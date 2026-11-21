//! Tail Workers emit — `tailConsumers[]` render as `tails` designators
//! pointing at the consumer's `worker-<workerId>` service (merged with
//! the platform log-tail collector when enabled). A dangling consumer id
//! fails the whole render closed — same-tenant membership is validated
//! upstream AND re-checked here (defense in depth).

mod common;

use common::*;
use snapshot_protocol::compute_desired::{TailConsumerDesired, TenantDesired};
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

fn tenant_with_tails(consumers: &[&str]) -> TenantDesired {
    let mut t = tenant();
    t.workers[0].tail_consumers = consumers
        .iter()
        .map(|id| TailConsumerDesired {
            worker_id: (*id).into(),
        })
        .collect();
    t
}

#[test]
fn tails_absent_emits_nothing() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("tails ="), "{}", r.text);
}

#[test]
fn tails_emit_consumer_designators() {
    let t = tenant_with_tails(&["w-alpha-2", "w-alpha-1"]);
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;
    // Self + sibling consumers both emit `worker-<id>` designators.
    assert!(
        out.contains("tails = [(name = \"worker-w-alpha-2\"), (name = \"worker-w-alpha-1\")]"),
        "{out}"
    );
    // The consumer worker itself emits no `tails` (it declares none).
    let w2 = out
        .find("name = \"worker-w-alpha-2\", worker = (")
        .expect("worker block");
    let w2_block = &out[w2..];
    let end = w2_block.find("    )),\n").unwrap_or(w2_block.len());
    assert!(!w2_block[..end].contains("tails ="), "{out}");
}

#[test]
fn tails_merge_with_platform_log_tail() {
    let t = tenant_with_tails(&["w-alpha-2"]);
    let mut params = runtime_params();
    params.log_tail.pepper = "pepper".into();
    params.log_tail.sink_addr = "unix:/run/vrt/logs.sock".into();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;
    // Platform collector first, declared consumer second.
    assert!(
        out.contains("tails = [(name = \"vrt-tail-w-alpha-1\"), (name = \"worker-w-alpha-2\")]"),
        "{out}"
    );
}

#[test]
fn tails_dangling_consumer_skips_only_that_worker() {
    // Per-worker fault isolation: a consumer id absent from the tenant's
    // worker set fails THAT worker's dry-run emit — it lands in
    // `skipped_workers` (routes serve worker_unavailable) while the
    // sibling keeps serving. The whole tenant never poisons.
    let t = tenant_with_tails(&["w-nope"]);
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert_eq!(
        r.skipped_workers,
        vec!["w-alpha-1".to_string()],
        "{:?}",
        r.skipped_reasons
    );
    assert!(!r.text.contains("worker-w-alpha-1"));
    assert!(r.text.contains("worker-w-alpha-2"));
}
